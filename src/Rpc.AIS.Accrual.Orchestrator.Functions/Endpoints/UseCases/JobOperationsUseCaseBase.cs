using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Services;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Options;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Logging;
using Rpc.AIS.Accrual.Orchestrator.Core.Utilities;

namespace Rpc.AIS.Accrual.Orchestrator.Functions.Functions;

/// <summary>
/// Shared helpers for JobOperations HTTP use cases.
/// Keeps endpoint adapters/use cases SOLID while preserving existing behavior.
/// </summary>
public abstract class JobOperationsUseCaseBase
{
    protected readonly ILogger _log;
    protected readonly IAisLogger _aisLogger;
    protected readonly IAisDiagnosticsOptions _diag;

    protected JobOperationsUseCaseBase(ILogger log, IAisLogger aisLogger, IAisDiagnosticsOptions diag)
    {
        _log = log ?? throw new ArgumentNullException(nameof(log));
        _aisLogger = aisLogger ?? throw new ArgumentNullException(nameof(aisLogger));
        _diag = diag ?? throw new ArgumentNullException(nameof(diag));
    }

    protected static (string runId, string correlationId, string sourceSystem) ReadContext(HttpRequestData req)
    {
        static string Get(HttpRequestData r, string name)
            => r.Headers.TryGetValues(name, out var v) ? v.FirstOrDefault() ?? string.Empty : string.Empty;

        // Match existing behavior: allow multiple possible header names, fall back to empty.
        var runId = Get(req, "x-run-id");
        if (string.IsNullOrWhiteSpace(runId)) runId = Get(req, "RunId");

        var correlationId = Get(req, "x-correlation-id");
        if (string.IsNullOrWhiteSpace(correlationId)) correlationId = Get(req, "CorrelationId");

        var sourceSystem = Get(req, "x-source-system");
        if (string.IsNullOrWhiteSpace(sourceSystem)) sourceSystem = Get(req, "SourceSystem");

        return (runId, correlationId, sourceSystem);
    }

    protected static async Task<string> ReadBodyAsync(HttpRequestData req)
    {
        if (req?.Body is null) return string.Empty;
        using var sr = new StreamReader(req.Body);
        return await sr.ReadToEndAsync().ConfigureAwait(false);
    }

    protected static string? TryGetHeader(HttpRequestData req, string name)
        => req.Headers.TryGetValues(name, out var values) ? values.FirstOrDefault() : null;

    protected sealed record ParsedFsJobOpsRequest(
        string? RunId,
        string? CorrelationId,
        string? Company,
        Guid WorkOrderGuid,
        string? SubProjectId);

    protected static bool TryParseFsJobOpsRequest(
        string json,
        out ParsedFsJobOpsRequest parsed,
        out string? error)
    {
        parsed = new ParsedFsJobOpsRequest(null, null, null, Guid.Empty, null);
        error = null;

        try
        {
            using var doc = JsonDocument.Parse(json);
            if (!doc.RootElement.TryGetProperty("_request", out var req) || req.ValueKind != JsonValueKind.Object)
            {
                error = "Request must contain '_request' object.";
                return false;
            }

            string? runId = null;
            if (req.TryGetProperty("RunId", out var runProp) && runProp.ValueKind == JsonValueKind.String)
                runId = runProp.GetString();

            string? correlationId = null;
            if (req.TryGetProperty("CorrelationId", out var corrProp) && corrProp.ValueKind == JsonValueKind.String)
                correlationId = corrProp.GetString();

            string? company = null;
            if (req.TryGetProperty("Company", out var compProp) && compProp.ValueKind == JsonValueKind.String)
                company = compProp.GetString();

            // WorkOrder GUID can be at root or inside WOList[0]
            Guid woGuid = Guid.Empty;

            if (req.TryGetProperty("WorkOrderGuid", out var wog1) && TryReadGuidFromElement(wog1, out woGuid)) { }
            else if (req.TryGetProperty("WorkOrderGUID", out var wog2) && TryReadGuidFromElement(wog2, out woGuid)) { }
            else if (req.TryGetProperty("workOrderGuid", out var wog3) && TryReadGuidFromElement(wog3, out woGuid)) { }
            else if (req.TryGetProperty("WOList", out var woList) && woList.ValueKind == JsonValueKind.Array && woList.GetArrayLength() > 0)
            {
                var first = woList[0];
                if (first.ValueKind == JsonValueKind.Object)
                {
                    if (first.TryGetProperty("WorkOrderGuid", out var w1) && TryReadGuidFromElement(w1, out woGuid)) { }
                    else if (first.TryGetProperty("WorkOrderGUID", out var w2) && TryReadGuidFromElement(w2, out woGuid)) { }
                    else if (first.TryGetProperty("workOrderGuid", out var w3) && TryReadGuidFromElement(w3, out woGuid)) { }
                }
            }

            if (woGuid == Guid.Empty)
            {
                error = "Request body is required and must contain workOrderGuid.";
                return false;
            }

            string? subProjectId = null;
            if (req.TryGetProperty("SubProjectId", out var sp1) && sp1.ValueKind == JsonValueKind.String) subProjectId = sp1.GetString();
            if (string.IsNullOrWhiteSpace(subProjectId) && req.TryGetProperty("SubprojectId", out var sp2) && sp2.ValueKind == JsonValueKind.String) subProjectId = sp2.GetString();

            parsed = new ParsedFsJobOpsRequest(runId, correlationId, company, woGuid, subProjectId);
            return true;
        }
        catch (Exception ex)
        {
            error = "Invalid request body. " + ex.Message;
            return false;
        }
    }

    // Test hook (keeps the real parser logic private/protected to the use case layer).
    // This avoids reflection in tests while keeping production behavior identical.
    //internal static bool TryParseFsJobOpsRequest_ForTests(
    //    string json,
    //    out ParsedFsJobOpsRequest parsed,
    //    out string? error)
    //    => TryParseFsJobOpsRequest(json, out parsed, out error);

    private static bool TryReadGuidFromElement(JsonElement e, out Guid guid)
    {
        guid = Guid.Empty;

        if (e.ValueKind == JsonValueKind.String)
        {
            var s = e.GetString();
            return TryReadGuid(s, out guid);
        }

        return false;
    }

    protected static bool TryReadGuid(string? s, out Guid guid)
    {
        guid = Guid.Empty;
        if (string.IsNullOrWhiteSpace(s)) return false;

        s = s.Trim();
        if (s.StartsWith("{", StringComparison.Ordinal)) s = s.Trim('{', '}');

        return Guid.TryParse(s, out guid);
    }

    protected async Task LogInboundPayloadAsync(string runId, string correlationId, string operation, string? body)
    {
        if (string.IsNullOrWhiteSpace(body))
            return;

        var (woGuid, woId, woCount) = TryGetFirstWorkOrderIdentity(body);

        await _aisLogger.LogJsonPayloadAsync(
            runId: runId,
            step: $"{operation}_INBOUND",
            message: "Inbound request payload received",
            payloadType: "FS_REQUEST",
            workOrderGuid: woGuid,
            workOrderNumber: woId,
            json: body,
            logBody: _diag.LogPayloadBodies && (_diag.LogMultiWoPayloadBody || woCount <= 1),
            snippetChars: _diag.PayloadSnippetChars,
            chunkChars: _diag.PayloadChunkChars,
            ct: default).ConfigureAwait(false);
    }

    private static (string WorkOrderGuid, string? WorkOrderId, int WoCount) TryGetFirstWorkOrderIdentity(string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            if (!doc.RootElement.TryGetProperty("_request", out var req) || req.ValueKind != JsonValueKind.Object)
                return ("MULTI", null, 0);
            if (!req.TryGetProperty("WOList", out var list) || list.ValueKind != JsonValueKind.Array)
                return ("MULTI", null, 0);

            var count = list.GetArrayLength();
            using var e = list.EnumerateArray();
            if (!e.MoveNext()) return ("MULTI", null, count);
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

            if (string.IsNullOrWhiteSpace(guid)) return (count <= 1 ? "UNKNOWN" : "MULTI", id, count);
            return (guid.Trim(), id, count);
        }
        catch
        {
            return ("MULTI", null, 0);
        }
    }

    protected static async Task<HttpResponseData> OkAsync(HttpRequestData req, string correlationId, string runId, object payload)
    {
        var resp = req.CreateResponse(HttpStatusCode.OK);
        resp.Headers.Add("Content-Type", "application/json; charset=utf-8");
        resp.Headers.Add("x-correlation-id", correlationId);
        resp.Headers.Add("x-run-id", runId);
        await resp.WriteStringAsync(JsonSerializer.Serialize(payload));
        return resp;
    }

    protected static async Task<HttpResponseData> AcceptedAsync(HttpRequestData req, string correlationId, string runId, object payload)
    {
        var resp = req.CreateResponse(HttpStatusCode.Accepted);
        resp.Headers.Add("Content-Type", "application/json; charset=utf-8");
        resp.Headers.Add("x-correlation-id", correlationId);
        resp.Headers.Add("x-run-id", runId);
        await resp.WriteStringAsync(JsonSerializer.Serialize(payload));
        return resp;
    }

    protected static async Task<HttpResponseData> NotFoundAsync(HttpRequestData req, string correlationId, string runId, object payload)
    {
        var resp = req.CreateResponse(HttpStatusCode.NotFound);
        resp.Headers.Add("Content-Type", "application/json; charset=utf-8");
        resp.Headers.Add("x-correlation-id", correlationId);
        resp.Headers.Add("x-run-id", runId);
        await resp.WriteStringAsync(JsonSerializer.Serialize(payload));
        return resp;
    }

    protected static async Task<HttpResponseData> BadRequestAsync(HttpRequestData req, string correlationId, string runId, string message)
    {
        var resp = req.CreateResponse(HttpStatusCode.BadRequest);
        resp.Headers.Add("Content-Type", "application/json; charset=utf-8");
        resp.Headers.Add("x-correlation-id", correlationId);
        resp.Headers.Add("x-run-id", runId);
        await resp.WriteStringAsync(JsonSerializer.Serialize(new { runId, correlationId, message }));
        return resp;
    }

    protected static async Task<HttpResponseData> BadGatewayAsync(HttpRequestData req, string correlationId, string runId, string message)
    {
        var resp = req.CreateResponse(HttpStatusCode.BadGateway);
        resp.Headers.Add("Content-Type", "application/json; charset=utf-8");
        resp.Headers.Add("x-correlation-id", correlationId);
        resp.Headers.Add("x-run-id", runId);
        await resp.WriteStringAsync(JsonSerializer.Serialize(new { runId, correlationId, message }));
        return resp;
    }

    protected static async Task<HttpResponseData> ServerErrorAsync(HttpRequestData req, string correlationId, string runId, string message)
    {
        var resp = req.CreateResponse(HttpStatusCode.InternalServerError);
        resp.Headers.Add("Content-Type", "application/json; charset=utf-8");
        resp.Headers.Add("x-correlation-id", correlationId);
        resp.Headers.Add("x-run-id", runId);
        await resp.WriteStringAsync(JsonSerializer.Serialize(new { runId, correlationId, message }));
        return resp;
    }

    // NOTE: The remaining JSON stamping/shape methods are kept in the concrete use cases,
    // because they depend on domain collaborators (FsOptions, delta services, etc.).
}
