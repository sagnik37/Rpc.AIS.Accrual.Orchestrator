using System;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain.Validation;
using Rpc.AIS.Accrual.Orchestrator.Core.Utilities;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Options;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Resilience;

namespace Rpc.AIS.Accrual.Orchestrator.Infrastructure.Clients.Posting;

public sealed class FscmWoPayloadValidationClient
    : Rpc.AIS.Accrual.Orchestrator.Core.Abstractions.IFscmWoPayloadValidationClient
{
    private readonly HttpClient _http;
    private readonly FscmOptions _endpoints;
    private readonly IFscmPostRequestFactory _reqFactory;
    private readonly IResilientHttpExecutor _executor;
    private readonly ILogger<FscmWoPayloadValidationClient> _logger;
    private readonly IAisLogger _aisLogger;
    private readonly IAisDiagnosticsOptions _diag;

    public FscmWoPayloadValidationClient(
        HttpClient http,
        FscmOptions endpoints,
        IFscmPostRequestFactory reqFactory,
        IResilientHttpExecutor executor,
        IAisLogger aisLogger,
        IAisDiagnosticsOptions diag,
        ILogger<FscmWoPayloadValidationClient> logger)
    {
        _http = http ?? throw new ArgumentNullException(nameof(http));
        _endpoints = endpoints ?? throw new ArgumentNullException(nameof(endpoints));
        _reqFactory = reqFactory ?? throw new ArgumentNullException(nameof(reqFactory));
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        _aisLogger = aisLogger ?? throw new ArgumentNullException(nameof(aisLogger));
        _diag = diag ?? throw new ArgumentNullException(nameof(diag));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<Rpc.AIS.Accrual.Orchestrator.Core.Abstractions.RemoteWoPayloadValidationResult> ValidateAsync(
        RunContext ctx,
        JournalType journalType,
        string normalizedWoPayloadJson,
        CancellationToken ct)
    {
        if (ctx is null) throw new ArgumentNullException(nameof(ctx));

        if (string.IsNullOrWhiteSpace(normalizedWoPayloadJson))
        {
            return new Rpc.AIS.Accrual.Orchestrator.Core.Abstractions.RemoteWoPayloadValidationResult(
                IsSuccessStatusCode: true,
                StatusCode: 204,
                FilteredPayloadJson: "{}",
                Failures: Array.Empty<WoPayloadValidationFailure>(),
                RawResponse: null,
                ElapsedMs: 0,
                Url: string.Empty);
        }

        var baseUrl = _endpoints.ResolveBaseUrl(_endpoints.BaseUrl);
        if (string.IsNullOrWhiteSpace(_endpoints.WoPayloadValidationPath))
        {
            _logger.LogInformation(
                "FSCM WO payload validation path not configured. Skipping remote validation.");

            return new Rpc.AIS.Accrual.Orchestrator.Core.Abstractions.RemoteWoPayloadValidationResult(
                IsSuccessStatusCode: true,
                StatusCode: (int)HttpStatusCode.OK,
                FilteredPayloadJson: normalizedWoPayloadJson,
                Failures: Array.Empty<WoPayloadValidationFailure>(),
                RawResponse: null,
                ElapsedMs: 0,
                Url: string.Empty
            );
        }

        var url = CombineUrl(baseUrl, _endpoints.WoPayloadValidationPath);

        var (woGuid, woId) = TryGetFirstWorkOrderIdentity(normalizedWoPayloadJson);

        await _aisLogger.LogJsonPayloadAsync(
            runId: ctx.RunId,
            step: "FSCM_WO_PAYLOAD_VALIDATE",
            message: "Outbound payload to FSCM validation",
            payloadType: "FSCM_VALIDATE_REQUEST",
            workOrderGuid: woGuid,
            workOrderNumber: woId,
            json: normalizedWoPayloadJson,
            logBody: _diag.LogPayloadBodies && (_diag.LogMultiWoPayloadBody || !string.Equals(woGuid, "MULTI", StringComparison.OrdinalIgnoreCase)),
            snippetChars: _diag.PayloadSnippetChars,
            chunkChars: _diag.PayloadChunkChars,
            ct: ct).ConfigureAwait(false);

        _logger.LogInformation(
            "Calling FSCM custom validation. JournalType={JournalType} Url={Url} RunId={RunId} CorrelationId={CorrelationId}",
            journalType, url, ctx.RunId, ctx.CorrelationId);

        var sw = Stopwatch.StartNew();

        HttpResponseMessage response;

        try
        {
            response = await _executor.SendAsync(
                    http: _http,
                    requestFactory: () => _reqFactory.CreateJsonPost(ctx, url, normalizedWoPayloadJson),
                    ctx: ctx,
                    operationName: "FSCM_WO_PAYLOAD_VALIDATE",
                    ct: ct)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            sw.Stop();

            _logger.LogError(ex,
                "FSCM validation call threw exception. Fail-closed. Url={Url}",
                url);

            return new Rpc.AIS.Accrual.Orchestrator.Core.Abstractions.RemoteWoPayloadValidationResult(
                IsSuccessStatusCode: false,
                StatusCode: 500,
                FilteredPayloadJson: normalizedWoPayloadJson,
                Failures: Array.Empty<WoPayloadValidationFailure>(),
                RawResponse: ex.Message,
                ElapsedMs: sw.ElapsedMilliseconds,
                Url: url);
        }

        var body = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);

        var statusCode = (int)response.StatusCode;
        var forceBodyLog = statusCode >= 500;

        await _aisLogger.LogJsonPayloadAsync(
            runId: ctx.RunId,
            step: "FSCM_WO_PAYLOAD_VALIDATE",
            message: "Inbound response from FSCM validation",
            payloadType: "FSCM_VALIDATE_RESPONSE",
            workOrderGuid: woGuid,
            workOrderNumber: woId,
            json: body ?? string.Empty,
            logBody: _diag.LogPayloadBodies || forceBodyLog,
            snippetChars: _diag.PayloadSnippetChars,
            chunkChars: _diag.PayloadChunkChars,
            ct: ct).ConfigureAwait(false);

        sw.Stop();

        var ok = statusCode >= 200 && statusCode <= 299;

        if (!ok)
        {
            _logger.LogWarning(
                "FSCM validation failed. StatusCode={StatusCode} ElapsedMs={ElapsedMs} Url={Url} BodySnippet={BodySnippet}",
                statusCode, sw.ElapsedMilliseconds, url, SafeSnippet(body));

            if (statusCode >= 500)
            {
                _logger.LogError(
                    "FSCM validation 5xx detail. StatusCode={StatusCode} Url={Url} RunId={RunId} CorrelationId={CorrelationId} Body={Body}",
                    statusCode,
                    url,
                    ctx.RunId,
                    ctx.CorrelationId,
                    LogText.TrimForLog(body ?? string.Empty));
            }

            return new Rpc.AIS.Accrual.Orchestrator.Core.Abstractions.RemoteWoPayloadValidationResult(
                IsSuccessStatusCode: false,
                StatusCode: statusCode,
                FilteredPayloadJson: normalizedWoPayloadJson,
                Failures: Array.Empty<WoPayloadValidationFailure>(),
                RawResponse: body,
                ElapsedMs: sw.ElapsedMilliseconds,
                Url: url);
        }

        try
        {
            var parsed = ParseValidationResponse(body, normalizedWoPayloadJson);

            return new Rpc.AIS.Accrual.Orchestrator.Core.Abstractions.RemoteWoPayloadValidationResult(
                IsSuccessStatusCode: true,
                StatusCode: statusCode,
                FilteredPayloadJson: parsed.FilteredPayloadJson,
                Failures: parsed.Failures,
                RawResponse: body,
                ElapsedMs: sw.ElapsedMilliseconds,
                Url: url);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "FSCM validation response parsing failed. Fail-closed. Url={Url}",
                url);

            return new Rpc.AIS.Accrual.Orchestrator.Core.Abstractions.RemoteWoPayloadValidationResult(
                IsSuccessStatusCode: false,
                StatusCode: 500,
                FilteredPayloadJson: normalizedWoPayloadJson,
                Failures: Array.Empty<WoPayloadValidationFailure>(),
                RawResponse: body,
                ElapsedMs: sw.ElapsedMilliseconds,
                Url: url);
        }
    }

    private static string CombineUrl(string baseUrl, string path)
    {
        baseUrl = (baseUrl ?? string.Empty).TrimEnd('/');
        path = (path ?? string.Empty).TrimStart('/');
        return string.IsNullOrWhiteSpace(path) ? baseUrl : $"{baseUrl}/{path}";
    }

    private static string SafeSnippet(string? body)
    {
        if (string.IsNullOrWhiteSpace(body)) return string.Empty;
        return body.Length <= 600 ? body : body.Substring(0, 600);
    }

    private sealed record ParsedValidationResponse(
     string FilteredPayloadJson,
     WoPayloadValidationFailure[] Failures);

    private static ParsedValidationResponse ParseValidationResponse(string responseBody, string originalPayloadJson)
    {
        using var doc = JsonDocument.Parse(responseBody);
        var root = doc.RootElement;

        // FSCM contract: { "WO Headers": [ { "Status": "...", "Errors": [..], ... } ] }
        if (!TryGetPropertyCaseInsensitive(root, "WO Headers", out var woHeaders) ||
            woHeaders.ValueKind != JsonValueKind.Array)
        {
            // If FSCM returned something unexpected, fail closed.
            throw new InvalidOperationException("Unexpected FSCM validation response: missing 'WO Headers' array.");
        }

        var failures = new List<WoPayloadValidationFailure>(capacity: 16);

        foreach (var header in woHeaders.EnumerateArray())
        {
            var woNumber = GetStringCaseInsensitive(header, "WO Number");
            var woGuidStr = GetStringCaseInsensitive(header, "Work order GUID");
            _ = Guid.TryParse(woGuidStr, out var woGuid);

            var status = GetStringCaseInsensitive(header, "Status") ?? string.Empty;

            if (status.Equals("Failed", StringComparison.OrdinalIgnoreCase) ||
                status.Equals("Error", StringComparison.OrdinalIgnoreCase))
            {
                // Prefer "Errors" array if present; otherwise fall back to "Info Message"
                if (TryGetPropertyCaseInsensitive(header, "Errors", out var errorsNode) &&
                    errorsNode.ValueKind == JsonValueKind.Array)
                {
                    foreach (var err in errorsNode.EnumerateArray())
                    {
                        var msg = err.ValueKind == JsonValueKind.String ? err.GetString() : err.ToString();
                        if (string.IsNullOrWhiteSpace(msg)) continue;

                        failures.Add(new WoPayloadValidationFailure(
                            woGuid,
                            woNumber,
                            JournalType.Item,               // caller will override if needed; see note below
                            Guid.Empty,
                            "AIS_FSCM_REMOTE_VALIDATION_FAILED",
                            msg!,
                            ValidationDisposition.Invalid));
                    }
                }
                else
                {
                    var info = GetStringCaseInsensitive(header, "Info Message");
                    if (!string.IsNullOrWhiteSpace(info))
                    {
                        failures.Add(new WoPayloadValidationFailure(
                            woGuid,
                            woNumber,
                            JournalType.Item,
                            Guid.Empty,
                            "AIS_FSCM_REMOTE_VALIDATION_FAILED",
                            info!,
                            ValidationDisposition.Invalid));
                    }
                }
            }
        }

        // FSCM is not returning a filtered payload; keep original.
        return new ParsedValidationResponse(
            FilteredPayloadJson: originalPayloadJson,
            Failures: failures.ToArray());
    }

    private static (string WorkOrderGuid, string? WorkOrderId) TryGetFirstWorkOrderIdentity(string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            if (!doc.RootElement.TryGetProperty("_request", out var req) || req.ValueKind != JsonValueKind.Object)
                return ("MULTI", null);
            if (!req.TryGetProperty("WOList", out var list) || list.ValueKind != JsonValueKind.Array)
                return ("MULTI", null);
            using var e = list.EnumerateArray();
            if (!e.MoveNext()) return ("MULTI", null);

            var wo = e.Current;
            var guid = GetStringCaseInsensitive(wo, "WorkOrderGUID") ?? GetStringCaseInsensitive(wo, "WorkOrderGuid");
            var id = GetStringCaseInsensitive(wo, "WorkOrderID") ?? GetStringCaseInsensitive(wo, "WorkOrderId") ?? GetStringCaseInsensitive(wo, "WO Number");
            if (string.IsNullOrWhiteSpace(guid)) return ("MULTI", id);
            return (guid.Trim(), id);
        }
        catch
        {
            return ("MULTI", null);
        }
    }

    private static bool TryGetPropertyCaseInsensitive(JsonElement obj, string name, out JsonElement value)
    {
        foreach (var p in obj.EnumerateObject())
        {
            if (p.NameEquals(name) || p.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
            {
                value = p.Value;
                return true;
            }
        }
        value = default;
        return false;
    }

    private static string? GetStringCaseInsensitive(JsonElement obj, string name)
    {
        return TryGetPropertyCaseInsensitive(obj, name, out var v) && v.ValueKind == JsonValueKind.String
            ? v.GetString()
            : (TryGetPropertyCaseInsensitive(obj, name, out var v2) ? v2.ToString() : null);
    }

    private static bool TryGetProperty(JsonElement root, string name, out JsonElement value)
    {
        foreach (var p in root.EnumerateObject())
        {
            if (string.Equals(p.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                value = p.Value;
                return true;
            }
        }

        value = default;
        return false;
    }

    private static string? TryGetString(JsonElement root, string name)
    {
        return TryGetProperty(root, name, out var el) && el.ValueKind == JsonValueKind.String
            ? el.GetString()
            : null;
    }
}
