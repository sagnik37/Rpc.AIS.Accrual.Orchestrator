using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain.Validation;
using Rpc.AIS.Accrual.Orchestrator.Core.Options;

namespace Rpc.AIS.Accrual.Orchestrator.Infrastructure.Clients;

/// <summary>
/// Calls the FSCM custom validation endpoint. Response schema is customer-specific, so parsing is best-effort:
/// - If HTTP 2xx and no recognizable failure array is found, validation is treated as successful.
/// - If HTTP non-2xx, a FailFast or Retryable failure is returned depending on <see cref="PayloadValidationOptions.FailClosedOnFscmCustomValidationError"/>.
/// </summary>
public sealed class FscmCustomValidationClient : IFscmCustomValidationClient
{
    private readonly HttpClient _http;
    private readonly ILogger<FscmCustomValidationClient> _log;
    private readonly PayloadValidationOptions _policy;
    private readonly FscmCustomValidationOptions _options;

    public FscmCustomValidationClient(
        HttpClient http,
        ILogger<FscmCustomValidationClient> log,
        IOptions<PayloadValidationOptions> policy,
        IOptions<FscmCustomValidationOptions> options)
    {
        _http = http ?? throw new ArgumentNullException(nameof(http));
        _log = log ?? throw new ArgumentNullException(nameof(log));
        _policy = policy?.Value ?? new PayloadValidationOptions();
        _options = options?.Value ?? new FscmCustomValidationOptions();
    }

    public async Task<IReadOnlyList<WoPayloadValidationFailure>> ValidateAsync(
        RunContext context,
        JournalType journalType,
        string company,
        string woPayloadJson,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(company))
        {
            return new[]
            {
                new WoPayloadValidationFailure(
                    Guid.Empty,
                    null,
                    journalType,
                    null,
                    "FSCM_REMOTE_COMPANY_MISSING",
                    "Company is missing; cannot call FSCM custom validation endpoint.",
                    ValidationDisposition.FailFast)
            };
        }

        if (string.IsNullOrWhiteSpace(woPayloadJson))
            return Array.Empty<WoPayloadValidationFailure>();

        var path = _options.EndpointPath ?? "/api/services/AIS/Validate";
        if (!path.StartsWith('/'))
            path = "/" + path;

        using var req = new HttpRequestMessage(HttpMethod.Post, path);
        req.Headers.TryAddWithoutValidation("x-company", company); // optional; safe if endpoint ignores
        req.Headers.TryAddWithoutValidation("x-journalType", journalType.ToString());

        req.Content = new StringContent(woPayloadJson, Encoding.UTF8, "application/json");

        try
        {
            using var resp = await _http.SendAsync(req, ct).ConfigureAwait(false);
            var body = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);

            if (!resp.IsSuccessStatusCode)
                return BuildTransportFailure(context, journalType, company, (int)resp.StatusCode, body);

            // Best-effort parse. If unknown schema, treat as success.
            if (string.IsNullOrWhiteSpace(body))
                return Array.Empty<WoPayloadValidationFailure>();

            return TryParseFailures(body, journalType);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "FSCM custom validation call failed. JournalType={JournalType}, Company={Company}", journalType, company);
            return BuildExceptionFailure(context, journalType, company, ex);
        }
    }

    private IReadOnlyList<WoPayloadValidationFailure> BuildTransportFailure(
        RunContext context,
        JournalType journalType,
        string company,
        int statusCode,
        string? responseBody)
    {
        var disposition = _policy.FailClosedOnFscmCustomValidationError
            ? ValidationDisposition.FailFast
            : ValidationDisposition.Retryable;

        var msg = $"FSCM custom validation endpoint returned HTTP {statusCode}.";
        if (!string.IsNullOrWhiteSpace(responseBody))
            msg += " Response snippet: " + Truncate(responseBody, 500);

        return new[]
        {
            new WoPayloadValidationFailure(
                Guid.Empty,
                null,
                journalType,
                null,
                "FSCM_REMOTE_HTTP_ERROR",
                msg,
                disposition)
        };
    }

    private IReadOnlyList<WoPayloadValidationFailure> BuildExceptionFailure(
        RunContext context,
        JournalType journalType,
        string company,
        Exception ex)
    {
        var disposition = _policy.FailClosedOnFscmCustomValidationError
            ? ValidationDisposition.FailFast
            : ValidationDisposition.Retryable;

        return new[]
        {
            new WoPayloadValidationFailure(
                Guid.Empty,
                null,
                journalType,
                null,
                "FSCM_REMOTE_EXCEPTION",
                $"FSCM custom validation call failed: {ex.GetType().Name}: {ex.Message}",
                disposition)
        };
    }

    private static IReadOnlyList<WoPayloadValidationFailure> TryParseFailures(string json, JournalType journalType)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            // Common shapes supported:
            // 1) { "failures": [ { workOrderGuid, workOrderLineGuid, code, message, disposition } ] }
            // 2) { "errors": [ { ... } ] } or { "validationErrors": [ ... ] }
            // 3) { "isValid": false, "messages": [ ... ] }  -> treated as FailFast without per-line details
            foreach (var key in new[] { "failures", "errors", "validationErrors" })
            {
                if (root.TryGetProperty(key, out var arr) && arr.ValueKind == JsonValueKind.Array)
                {
                    var list = new List<WoPayloadValidationFailure>();
                    foreach (var e in arr.EnumerateArray())
                    {
                        // Best-effort extraction
                        var woGuid = ReadGuid(e, "workOrderGuid") ?? ReadGuid(e, "workOrderId") ?? Guid.Empty;
                        var woLineGuid = ReadGuid(e, "workOrderLineGuid") ?? ReadGuid(e, "lineGuid");
                        var code = ReadString(e, "code") ?? ReadString(e, "errorCode") ?? "FSCM_REMOTE_VALIDATION_ERROR";
                        var msg = ReadString(e, "message") ?? ReadString(e, "errorMessage") ?? "Remote validation failed.";
                        var disp = ReadDisposition(e) ?? ValidationDisposition.Invalid;

                        list.Add(new WoPayloadValidationFailure(
                            woGuid,
                            ReadString(e, "workOrderNumber"),
                            journalType,
                            woLineGuid,
                            code,
                            msg,
                            disp));
                    }
                    return list;
                }
            }

            // If the endpoint uses { isValid: false, message: "..." } shape
            if (root.TryGetProperty("isValid", out var isValid) && isValid.ValueKind == JsonValueKind.False)
            {
                var msg = root.TryGetProperty("message", out var m) && m.ValueKind == JsonValueKind.String
                    ? m.GetString()
                    : "Remote validation returned isValid=false.";
                return new[]
                {
                    new WoPayloadValidationFailure(Guid.Empty, null, journalType, null, "FSCM_REMOTE_INVALID", msg ?? "Remote validation failed.", ValidationDisposition.Invalid)
                };
            }

            return Array.Empty<WoPayloadValidationFailure>();
        }
        catch
        {
            // Unknown schema / non-JSON -> treat as success to avoid breaking runs.
            return Array.Empty<WoPayloadValidationFailure>();
        }
    }

    private static string? ReadString(JsonElement e, string name)
        => e.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;

    private static Guid? ReadGuid(JsonElement e, string name)
    {
        if (!e.TryGetProperty(name, out var v)) return null;
        if (v.ValueKind == JsonValueKind.String && Guid.TryParse(v.GetString(), out var g)) return g;
        return null;
    }

    private static ValidationDisposition? ReadDisposition(JsonElement e)
    {
        var raw = ReadString(e, "disposition") ?? ReadString(e, "severity");
        if (string.IsNullOrWhiteSpace(raw)) return null;

        // Accept either enum names or common words.
        if (Enum.TryParse<ValidationDisposition>(raw, ignoreCase: true, out var disp))
            return disp;

        return raw.ToLowerInvariant() switch
        {
            "error" => ValidationDisposition.Invalid,
            "warning" => ValidationDisposition.Invalid,
            "retryable" => ValidationDisposition.Retryable,
            "failfast" => ValidationDisposition.FailFast,
            _ => null
        };
    }

    private static string Truncate(string s, int max)
        => s.Length <= max ? s : s.Substring(0, max);
}
