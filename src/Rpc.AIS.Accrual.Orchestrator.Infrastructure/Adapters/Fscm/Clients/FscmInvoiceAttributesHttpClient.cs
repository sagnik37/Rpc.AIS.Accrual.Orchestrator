using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain.InvoiceAttributes;
using Rpc.AIS.Accrual.Orchestrator.Core.Utilities;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Options;

namespace Rpc.AIS.Accrual.Orchestrator.Infrastructure.Clients;

/// <summary>
/// FSCM custom endpoints for invoice attributes:
/// - Definitions ("attribute table")
/// - Current values snapshot
/// - Update values (InvoiceAttributes: [{AttributeName, AttributeValue}])
///
///  This client is additive and does not change existing posting behavior.
/// </summary>
public sealed class FscmInvoiceAttributesHttpClient : IFscmInvoiceAttributesClient
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = null,
        WriteIndented = false
    };

    private readonly HttpClient _http;
    private readonly FscmOptions _opt;
    private readonly ILogger<FscmInvoiceAttributesHttpClient> _log;
    private readonly IAisLogger _aisLogger;
    private readonly IAisDiagnosticsOptions _diag;

    public FscmInvoiceAttributesHttpClient(
        HttpClient http,
        IOptions<FscmOptions> options,
        IAisLogger aisLogger,
        IAisDiagnosticsOptions diag,
        ILogger<FscmInvoiceAttributesHttpClient> log)
    {
        _http = http ?? throw new ArgumentNullException(nameof(http));
        _opt = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _aisLogger = aisLogger ?? throw new ArgumentNullException(nameof(aisLogger));
        _diag = diag ?? throw new ArgumentNullException(nameof(diag));
        _log = log ?? throw new ArgumentNullException(nameof(log));
    }

    public async Task<IReadOnlyList<InvoiceAttributeDefinition>> GetDefinitionsAsync(
        RunContext ctx,
        string company,
        string subProjectId,
        CancellationToken ct)
    {
        if (ctx is null) throw new ArgumentNullException(nameof(ctx));
        if (string.IsNullOrWhiteSpace(company)) throw new ArgumentException("Company is required.", nameof(company));
        if (string.IsNullOrWhiteSpace(subProjectId)) throw new ArgumentException("SubProjectId is required.", nameof(subProjectId));

        if (string.IsNullOrWhiteSpace(_opt.InvoiceAttributeDefinitionsPath))
        {
            _log.LogWarning(
                "FSCM invoice attribute definitions endpoint is not configured. Skipping. RunId={RunId} CorrelationId={CorrelationId}",
                ctx.RunId, ctx.CorrelationId);
            return Array.Empty<InvoiceAttributeDefinition>();
        }

        var url = BuildUrl(_opt.BaseUrl, _opt.InvoiceAttributeDefinitionsPath);

        var payload = new
        {
            Company = company,
            SubProjectId = subProjectId
        };

        var json = JsonSerializer.Serialize(payload, JsonOptions);

        var (status, body, elapsed) = await PostJsonAsync(ctx, url, json, "FSCM_INV_ATTR_DEFS", ct).ConfigureAwait(false);
        if ((int)status >= 400)
        {
            _log.LogWarning("FSCM attribute definitions failed. Status={Status} Body={Body}", (int)status, LogText.TrimForLog(body));
            return Array.Empty<InvoiceAttributeDefinition>();
        }

        try
        {
            using var doc = JsonDocument.Parse(body);

            // Accept either: { "InvoiceAttributeDefinitions": [...] } OR { "attributeDefinitions": [...] }
            JsonElement arr;
            if (doc.RootElement.TryGetProperty("InvoiceAttributeDefinitions", out arr) ||
                doc.RootElement.TryGetProperty("attributeDefinitions", out arr))
            {
                if (arr.ValueKind == JsonValueKind.Array)
                {
                    var defs = new List<InvoiceAttributeDefinition>();
                    foreach (var el in arr.EnumerateArray())
                    {
                        var name = el.TryGetProperty("AttributeName", out var an) ? an.GetString() :
                                   el.TryGetProperty("name", out var n) ? n.GetString() : null;

                        if (string.IsNullOrWhiteSpace(name)) continue;

                        var type = el.TryGetProperty("Type", out var t) ? t.GetString() :
                                   el.TryGetProperty("type", out var t2) ? t2.GetString() : null;

                        var active = el.TryGetProperty("Active", out var a) ? a.GetBoolean() :
                                     el.TryGetProperty("active", out var a2) ? a2.GetBoolean() : true;

                        defs.Add(new InvoiceAttributeDefinition(name!, type, active));
                    }

                    _log.LogInformation("FSCM attribute definitions parsed. Count={Count} ElapsedMs={ElapsedMs}", defs.Count, elapsed);
                    return defs;
                }
            }
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "Failed to parse FSCM attribute definitions response. Treating as empty.");
        }

        return Array.Empty<InvoiceAttributeDefinition>();
    }

    public async Task<IReadOnlyList<InvoiceAttributePair>> GetCurrentValuesAsync(
        RunContext ctx,
        string company,
        string subProjectId,
        IReadOnlyList<string> attributeNames,
        CancellationToken ct)
    {
        if (ctx is null) throw new ArgumentNullException(nameof(ctx));
        if (string.IsNullOrWhiteSpace(company)) throw new ArgumentException("Company is required.", nameof(company));
        if (string.IsNullOrWhiteSpace(subProjectId)) throw new ArgumentException("SubProjectId is required.", nameof(subProjectId));

        if (string.IsNullOrWhiteSpace(_opt.InvoiceAttributeValuesPath))
        {
            _log.LogWarning(
                "FSCM invoice attribute values endpoint is not configured. Skipping. RunId={RunId} CorrelationId={CorrelationId}",
                ctx.RunId, ctx.CorrelationId);
            return Array.Empty<InvoiceAttributePair>();
        }

        var url = BuildUrl(_opt.BaseUrl, _opt.InvoiceAttributeValuesPath);

        var payload = new
        {
            Company = company,
            SubProjectId = subProjectId,
            AttributeNames = attributeNames ?? Array.Empty<string>()
        };

        var json = JsonSerializer.Serialize(payload, JsonOptions);
        var (status, body, elapsed) = await PostJsonAsync(ctx, url, json, "FSCM_INV_ATTR_VALUES", ct).ConfigureAwait(false);

        if ((int)status >= 400)
        {
            _log.LogWarning("FSCM attribute values failed. Status={Status} Body={Body}", (int)status, LogText.TrimForLog(body));
            return Array.Empty<InvoiceAttributePair>();
        }

        try
        {
            using var doc = JsonDocument.Parse(body);

            JsonElement arr;
            if (doc.RootElement.TryGetProperty("InvoiceAttributes", out arr) ||
                doc.RootElement.TryGetProperty("invoiceAttributes", out arr) ||
                doc.RootElement.TryGetProperty("attributes", out arr))
            {
                if (arr.ValueKind == JsonValueKind.Array)
                {
                    var list = new List<InvoiceAttributePair>();
                    foreach (var el in arr.EnumerateArray())
                    {
                        var name = el.TryGetProperty("AttributeName", out var n) ? n.GetString() :
                                   el.TryGetProperty("name", out var n2) ? n2.GetString() : null;

                        if (string.IsNullOrWhiteSpace(name)) continue;

                        var val = el.TryGetProperty("AttributeValue", out var v) ? v.ToString() :
                                  el.TryGetProperty("value", out var v2) ? v2.ToString() : null;

                        list.Add(new InvoiceAttributePair(name!, val));
                    }

                    _log.LogInformation("FSCM attribute values parsed. Count={Count} ElapsedMs={ElapsedMs}", list.Count, elapsed);
                    return list;
                }

                if (arr.ValueKind == JsonValueKind.Object)
                {
                    var list = new List<InvoiceAttributePair>();
                    foreach (var p in arr.EnumerateObject())
                        list.Add(new InvoiceAttributePair(p.Name, p.Value.ValueKind == JsonValueKind.Null ? null : p.Value.ToString()));

                    _log.LogInformation("FSCM attribute values parsed (object). Count={Count} ElapsedMs={ElapsedMs}", list.Count, elapsed);
                    return list;
                }
            }
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "Failed to parse FSCM attribute values response. Treating as empty.");
        }

        return Array.Empty<InvoiceAttributePair>();
    }

    public async Task<FscmInvoiceAttributesUpdateResult> UpdateAsync(
        RunContext ctx,
        string company,
        string subProjectId,
        Guid workOrderGuid,
        string workOrderId,
        string? countryRegionId,
        string? county,
        string? state,
        string? dimensionDisplayValue,
        string? fsaTaxabilityType,
        string? fsaWellAge,
        string? fsaWorkType,
        IReadOnlyList<InvoiceAttributePair> updates,
        CancellationToken ct)
    {
        if (ctx is null) throw new ArgumentNullException(nameof(ctx));
        if (string.IsNullOrWhiteSpace(company)) throw new ArgumentException("Company is required.", nameof(company));
        if (string.IsNullOrWhiteSpace(subProjectId)) throw new ArgumentException("SubProjectId is required.", nameof(subProjectId));
        if (string.IsNullOrWhiteSpace(workOrderId)) throw new ArgumentException("WorkOrderId is required.", nameof(workOrderId));

        if (string.IsNullOrWhiteSpace(_opt.UpdateInvoiceAttributesPath))
        {
            _log.LogWarning(
                "FSCM UpdateInvoiceAttributesPath is not configured. Skipping update. RunId={RunId} CorrelationId={CorrelationId}",
                ctx.RunId, ctx.CorrelationId);
            return new FscmInvoiceAttributesUpdateResult(true, 200, "");
        }

        updates ??= Array.Empty<InvoiceAttributePair>();

        var url = BuildUrl(_opt.BaseUrl, _opt.UpdateInvoiceAttributesPath);

        // Expected envelope:
        // {
        //   "_request": {
        //     "WOList": [
        //       {
        //         "Company": "425",
        //         "SubProjectId": "425-P...",
        //         "WorkOrderGUID": "{...}",
        //         "WorkOrderID": "J-RPC-...",
        //         "InvoiceAttributes": [ { "AttributeName": "...", "AttributeValue": "..." } ]
        //       }
        //     ]
        //   }
        // }
        var payload = new
        {
            _request = new
            {
                WOList = new[]
                {
                    new
                    {
                        Company = company,
                        SubProjectId = subProjectId,
                        WorkOrderGUID = workOrderGuid.ToString("B").ToUpperInvariant(),
                        WorkOrderID = workOrderId,
                        CountryRegionId = countryRegionId ?? string.Empty,
                        County = county ?? string.Empty,
                        State = state ?? string.Empty,
                        DimensionDisplayValue = dimensionDisplayValue ?? string.Empty,
                        FSATaxabilityType = fsaTaxabilityType ?? string.Empty,
                        FSAWellAge = fsaWellAge ?? string.Empty,
                        FSAWorkType = fsaWorkType ?? string.Empty,
                        InvoiceAttributes = updates.Select(u => new { u.AttributeName, u.AttributeValue }).ToArray()
                    }
                }
            }
        };

        var json = JsonSerializer.Serialize(payload, JsonOptions);
        var (status, body, elapsed) = await PostJsonAsync(ctx, url, json, "FSCM_INV_ATTR_UPDATE", ct).ConfigureAwait(false);

        var ok = (int)status >= 200 && (int)status <= 299;
        if (!ok)
            _log.LogWarning("FSCM invoice attributes update failed. Status={Status} Body={Body}", (int)status, LogText.TrimForLog(body));
        else
            _log.LogInformation("FSCM invoice attributes update succeeded. UpdatedCount={Count} ElapsedMs={ElapsedMs}", updates.Count, elapsed);

        return new FscmInvoiceAttributesUpdateResult(ok, (int)status, body);
    }

    private static string BuildUrl(string baseUrl, string path)
    {
        var b = (baseUrl ?? string.Empty).TrimEnd('/');
        var p = (path ?? string.Empty).TrimStart('/');
        return $"{b}/{p}";
    }

    private async Task<(HttpStatusCode status, string body, long elapsedMs)> PostJsonAsync(
        RunContext ctx,
        string url,
        string payloadJson,
        string operation,
        CancellationToken ct)
    {
        var payloadBytes = Encoding.UTF8.GetByteCount(payloadJson);

        var (woGuid, woId) = TryGetFirstWorkOrderIdentity(payloadJson);
        await _aisLogger.LogJsonPayloadAsync(
            runId: ctx.RunId,
            step: operation,
            message: "Outbound payload to FSCM invoice attributes endpoint",
            payloadType: "FSCM_INV_ATTR_REQUEST",
            workOrderGuid: woGuid,
            workOrderNumber: woId,
            json: payloadJson,
            logBody: _diag.LogPayloadBodies && (_diag.LogMultiWoPayloadBody || !string.Equals(woGuid, "MULTI", StringComparison.OrdinalIgnoreCase)),
            snippetChars: _diag.PayloadSnippetChars,
            chunkChars: _diag.PayloadChunkChars,
            ct: ct).ConfigureAwait(false);

        _log.LogInformation(
            "{Op} START Url={Url} RunId={RunId} CorrelationId={CorrelationId} PayloadBytes={Bytes}",
            operation, url, ctx.RunId, ctx.CorrelationId, payloadBytes);

        var sw = Stopwatch.StartNew();

        using var msg = new HttpRequestMessage(HttpMethod.Post, url)
        {
            Content = new StringContent(payloadJson, Encoding.UTF8, "application/json")
        };

        if (!string.IsNullOrWhiteSpace(ctx.RunId))
            msg.Headers.TryAddWithoutValidation("x-run-id", ctx.RunId);

        if (!string.IsNullOrWhiteSpace(ctx.CorrelationId))
            msg.Headers.TryAddWithoutValidation("x-correlation-id", ctx.CorrelationId);

        using var resp = await _http.SendAsync(msg, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
        var body = resp.Content is null ? string.Empty : await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);

        // For 5xx errors, force logging the response body (even if diagnostics are disabled)
        // so App Insights captures the FSCM error detail.
        var forceBodyLog = (int)resp.StatusCode >= 500;

        await _aisLogger.LogJsonPayloadAsync(
            runId: ctx.RunId,
            step: operation,
            message: "Inbound response from FSCM invoice attributes endpoint",
            payloadType: "FSCM_INV_ATTR_RESPONSE",
            workOrderGuid: woGuid,
            workOrderNumber: woId,
            json: body ?? string.Empty,
            logBody: _diag.LogPayloadBodies || forceBodyLog,
            snippetChars: _diag.PayloadSnippetChars,
            chunkChars: _diag.PayloadChunkChars,
            ct: ct).ConfigureAwait(false);

        sw.Stop();

        _log.LogInformation(
            "{Op} END Status={Status} Url={Url} RunId={RunId} CorrelationId={CorrelationId} ElapsedMs={ElapsedMs} ResponseBytes={ResponseBytes}",
            operation, (int)resp.StatusCode, url, ctx.RunId, ctx.CorrelationId, sw.ElapsedMilliseconds, Encoding.UTF8.GetByteCount(body ?? string.Empty));

        if ((int)resp.StatusCode >= 500)
        {
            _log.LogError(
                "FSCM 5xx response. Op={Op} Status={Status} Url={Url} RunId={RunId} CorrelationId={CorrelationId} Body={Body}",
                operation,
                (int)resp.StatusCode,
                url,
                ctx.RunId,
                ctx.CorrelationId,
                LogText.TrimForLog(body ?? string.Empty));
        }

        if (resp.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)
            throw new UnauthorizedAccessException($"FSCM auth failure at {operation}. HTTP {(int)resp.StatusCode}. Body: {LogText.TrimForLog(body)}");

        if (resp.StatusCode == (HttpStatusCode)429 || (int)resp.StatusCode >= 500)
            throw new HttpRequestException($"FSCM transient failure at {operation}. HTTP {(int)resp.StatusCode}. Body: {LogText.TrimForLog(body)}", null, resp.StatusCode);

        return (resp.StatusCode, body ?? string.Empty, sw.ElapsedMilliseconds);
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

            static string? ReadString(JsonElement obj, string prop)
            {
                if (obj.ValueKind != JsonValueKind.Object) return null;
                if (obj.TryGetProperty(prop, out var v))
                {
                    if (v.ValueKind == JsonValueKind.String) return v.GetString();
                    if (v.ValueKind == JsonValueKind.Number || v.ValueKind == JsonValueKind.True || v.ValueKind == JsonValueKind.False) return v.ToString();
                }
                return null;
            }

            var guid = ReadString(wo, "WorkOrderGUID") ?? ReadString(wo, "WorkOrderGuid") ?? ReadString(wo, "workOrderGuid");
            var id = ReadString(wo, "WorkOrderID") ?? ReadString(wo, "WorkOrderId") ?? ReadString(wo, "workOrderId") ?? ReadString(wo, "WONumber");
            if (string.IsNullOrWhiteSpace(guid)) return ("MULTI", id);
            return (guid.Trim(), id);
        }
        catch
        {
            return ("MULTI", null);
        }
    }
}
