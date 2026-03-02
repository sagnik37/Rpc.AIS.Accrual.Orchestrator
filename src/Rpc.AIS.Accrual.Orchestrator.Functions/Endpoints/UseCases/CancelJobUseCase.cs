using System;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Core.Services;
using Rpc.AIS.Accrual.Orchestrator.Functions.Services;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Clients.Posting;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Logging;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Options;

namespace Rpc.AIS.Accrual.Orchestrator.Functions.Functions;

/// <summary>
/// Cancel Job use case.
/// Fully extracted (no shared endpoint handler dependency).
/// </summary>
public sealed class CancelJobUseCase : JobOperationsUseCaseBase, ICancelJobUseCase
{
    private readonly IFsaDeltaPayloadOrchestrator _payloadOrch;
    private readonly FsOptions _fsOpt;
    private readonly IPostingClient _posting;
    private readonly IWoDeltaPayloadServiceV2 _deltaV2;
    private readonly IFscmProjectStatusClient _projectStatus;

    public CancelJobUseCase(
        ILogger<CancelJobUseCase> log,
        IAisLogger aisLogger,
        IAisDiagnosticsOptions diag,
        IFsaDeltaPayloadOrchestrator payloadOrch,
        FsOptions fsOpt,
        IPostingClient posting,
        IWoDeltaPayloadServiceV2 deltaV2,
        IFscmProjectStatusClient projectStatus)
        : base(log, aisLogger, diag)
    {
        _payloadOrch = payloadOrch ?? throw new ArgumentNullException(nameof(payloadOrch));
        _fsOpt = fsOpt ?? throw new ArgumentNullException(nameof(fsOpt));
        _posting = posting ?? throw new ArgumentNullException(nameof(posting));
        _deltaV2 = deltaV2 ?? throw new ArgumentNullException(nameof(deltaV2));
        _projectStatus = projectStatus ?? throw new ArgumentNullException(nameof(projectStatus));
    }

    public async Task<HttpResponseData> ExecuteAsync(HttpRequestData req, FunctionContext ctx)
    {
                var (runId, correlationId, sourceSystem) = ReadContext(req);

                using var scope = _log.BeginScope(new Dictionary<string, object?>
                {
                    ["RunId"] = runId,
                    ["CorrelationId"] = correlationId,
                    ["SourceSystem"] = sourceSystem,
                    ["Function"] = "CancelJob"
                });

                var body = await ReadBodyAsync(req);

                await LogInboundPayloadAsync(runId, correlationId, "CancelJob", body).ConfigureAwait(false);

                if (string.IsNullOrWhiteSpace(body))
                    return await BadRequestAsync(req, correlationId, runId, "Request body is required and must contain workOrderGuid.");

                if (!TryParseFsJobOpsRequest(body, out var parsed, out var parseError))
                    return await BadRequestAsync(req, correlationId, runId, parseError ?? "Invalid request body.");

                // Prefer envelope-provided RunId/CorrelationId when present.
                runId = string.IsNullOrWhiteSpace(parsed.RunId) ? runId : parsed.RunId!;
                correlationId = string.IsNullOrWhiteSpace(parsed.CorrelationId) ? correlationId : parsed.CorrelationId!;
                var woGuid = parsed.WorkOrderGuid;

                using var woScope = LogScopes.BeginFunctionScope(_log, new LogScopeContext
                {
                    Function = "CancelJob",
                    Operation = "CancelJob",
                    Trigger = "Http",
                    RunId = runId,
                    CorrelationId = correlationId,
                    SourceSystem = sourceSystem,
                    WorkOrderGuid = woGuid
                });

                // Build the FULL WO payload (single WO) even if not OPEN.
                var fsaPayload = await _payloadOrch.BuildSingleWorkOrderAnyStatusAsync(
                    new GetFsaDeltaPayloadInputDto(runId, correlationId, "CancelJob", woGuid.ToString()),
                    _fsOpt,
                    ctx.CancellationToken);

                //  NEW: handle empty/ignored payload → update status as Cancelled anyway.
                var payloadEmpty =
                    fsaPayload is null ||
                    string.IsNullOrWhiteSpace(fsaPayload.PayloadJson) ||
                    fsaPayload.WorkOrderNumbers.Count == 0;

                if (payloadEmpty)
                {
                    var companyFallback = parsed.Company;
                    var subProjectFallback = parsed.SubProjectId;

                    if (string.IsNullOrWhiteSpace(companyFallback) || string.IsNullOrWhiteSpace(subProjectFallback))
                    {
                        return await BadRequestAsync(
                            req,
                            correlationId,
                            runId,
                            "FSA payload was empty/ignored (no lines). To cancel status anyway, provide Company and SubProjectId in the request envelope (_request.WOList[0]).");
                    }

                    var runCtx = new RunContext(runId, DateTimeOffset.UtcNow, "CancelJob", correlationId, sourceSystem, companyFallback);

                    var woIdForStatus = woGuid.ToString("D"); // best available when FSA WO numbers are missing

                    var statusCancelUpdate = await _projectStatus.UpdateAsync(
                        runCtx,
                        companyFallback!,
                        subProjectFallback!,
                        woGuid,
                        woIdForStatus,
                        status: 6,
                        ctx.CancellationToken);

                    return await OkAsync(req, correlationId, runId, new
                    {
                        runId,
                        correlationId,
                        sourceSystem,
                        operation = "CancelJob",
                        workOrderGuid = woGuid,
                        workOrderNumbers = Array.Empty<string>(),
                        subProjectId = subProjectFallback,
                        message = "FSA payload was empty/ignored (no lines). Skipped delta/post, but ProjectStatusUpdate was executed (Cancelled).",
                        projectStatusUpdate = new
                        {
                            success = statusCancelUpdate.IsSuccess,
                            httpStatus = statusCancelUpdate.HttpStatus
                        }
                    });
                }

                // Determine Company + SubProjectId.
                // Prefer FS envelope values when provided; otherwise fall back to the FSA payload.
                var company = parsed.Company;
                var subProjectId = parsed.SubProjectId;

                if (string.IsNullOrWhiteSpace(company) || string.IsNullOrWhiteSpace(subProjectId))
                {
                    TryExtractCompanyAndSubProjectIdString(fsaPayload!.PayloadJson, out var c2, out var sp2);
                    company ??= c2;
                    subProjectId ??= sp2;
                }

                if (string.IsNullOrWhiteSpace(company))
                    return await BadRequestAsync(req, correlationId, runId, "Company is missing (provide Company in request envelope or ensure FSA payload contains Company).");

                if (string.IsNullOrWhiteSpace(subProjectId))
                    return await BadRequestAsync(req, correlationId, runId, "SubProjectId is missing (provide SubProjectId in request envelope or ensure FSA payload contains SubProjectId).");

                var runCtx2 = new RunContext(runId, DateTimeOffset.UtcNow, "CancelJob", correlationId, sourceSystem, company);

                // Build delta payload (compare FSA snapshot vs FSCM history) then validate/post.
                var deltaResult = await _deltaV2.BuildDeltaPayloadAsync(
                    runCtx2,
                    fsaPayload.PayloadJson,
                    DateTime.UtcNow.Date,
                    new WoDeltaBuildOptions(BaselineSubProjectId: subProjectId, TargetMode: WoDeltaTargetMode.CancelToZero),
                    ctx.CancellationToken);

                if (string.IsNullOrWhiteSpace(deltaResult.DeltaPayloadJson))
                {
                    var woIdForStatusCancel = fsaPayload.WorkOrderNumbers.FirstOrDefault() ?? "UNKNOWN";
                    var statusCancelUpdate = await _projectStatus.UpdateAsync(runCtx2, company!, subProjectId!, woGuid, woIdForStatusCancel, status: 6, ctx.CancellationToken);

                    return await OkAsync(req, correlationId, runId, new
                    {
                        runId,
                        correlationId,
                        sourceSystem,
                        workOrderGuid = woGuid,
                        subProjectId = subProjectId,
                        message = "Delta payload is empty; nothing to post.",
                        delta = new
                        {
                            workOrdersInInput = deltaResult.WorkOrdersInInput,
                            workOrdersInOutput = deltaResult.WorkOrdersInOutput,
                            totalDeltaLines = deltaResult.TotalDeltaLines,
                            totalReverseLines = deltaResult.TotalReverseLines,
                            totalRecreateLines = deltaResult.TotalRecreateLines
                        },
                        projectStatusUpdate = new
                        {
                            success = statusCancelUpdate.IsSuccess,
                            httpStatus = statusCancelUpdate.HttpStatus
                        }
                    });
                }

                // Stamp JournalDescription + JournalLineDescription for cancellation
                var jobIdForDesc = fsaPayload.WorkOrderNumbers.FirstOrDefault()
                                   ?? woGuid.ToString("D");

                var cancelDeltaPayloadJson = StampJournalDescriptions(
                    deltaResult.DeltaPayloadJson,
                    jobIdForDesc,
                    subProjectId!,
                    action: "Cancel");

                var postResults = await _posting.ValidateOnceAndPostAllJournalTypesAsync(
                    runCtx2,
                    cancelDeltaPayloadJson,
                    ctx.CancellationToken);

                // Update FSCM project stage/status to Cancelled.
                var woIdForStatusCancel2 = fsaPayload.WorkOrderNumbers.FirstOrDefault() ?? "UNKNOWN";
                var statusCancelUpdate2 = await _projectStatus.UpdateAsync(runCtx2, company!, subProjectId!, woGuid, woIdForStatusCancel2, status: 6, ctx.CancellationToken);

                return await OkAsync(req, correlationId, runId, new
                {
                    runId,
                    correlationId,
                    sourceSystem,
                    operation = "CancelJob",
                    workOrderGuid = woGuid,
                    workOrderNumbers = fsaPayload.WorkOrderNumbers,
                    subProjectId = subProjectId,
                    delta = new
                    {
                        workOrdersInInput = deltaResult.WorkOrdersInInput,
                        workOrdersInOutput = deltaResult.WorkOrdersInOutput,
                        totalDeltaLines = deltaResult.TotalDeltaLines,
                        totalReverseLines = deltaResult.TotalReverseLines,
                        totalRecreateLines = deltaResult.TotalRecreateLines
                    },
                    postResults = postResults.Select(r => new
                    {
                        journalType = r.JournalType.ToString(),
                        success = r.IsSuccess,
                        posted = r.WorkOrdersPosted,
                        errors = r.Errors?.Count ?? 0
                    }),
                    projectStatusUpdate = new
                    {
                        success = statusCancelUpdate2.IsSuccess,
                        httpStatus = statusCancelUpdate2.HttpStatus
                    }
                });

    }

    private static bool TryExtractCompanyAndSubProjectIdString(string woPayloadJson, out string? company, out string? subProjectId)
        {
            company = null;
            subProjectId = null;

            try
            {
                using var doc = JsonDocument.Parse(woPayloadJson);
                if (!doc.RootElement.TryGetProperty("_request", out var req) || req.ValueKind != JsonValueKind.Object)
                    return false;
                if (!req.TryGetProperty("WOList", out var woList) || woList.ValueKind != JsonValueKind.Array)
                    return false;

                var first = woList.EnumerateArray().FirstOrDefault();
                if (first.ValueKind != JsonValueKind.Object)
                    return false;

                if (first.TryGetProperty("Company", out var c))
                    company = c.ValueKind == JsonValueKind.String ? c.GetString() : c.ToString();

                if (first.TryGetProperty("SubProjectId", out var sp))
                    subProjectId = sp.ValueKind == JsonValueKind.String ? sp.GetString() : sp.ToString();

                return !string.IsNullOrWhiteSpace(company) || !string.IsNullOrWhiteSpace(subProjectId);
            }
            catch
            {
                return false;
            }
        }

    private static string StampJournalDescriptions(
            string payloadJson,
            string jobId,
            string subProjectId,
            string action)
        {
            if (string.IsNullOrWhiteSpace(payloadJson))
                return payloadJson;

            JsonNode? node;
            try { node = JsonNode.Parse(payloadJson); }
            catch { return payloadJson; }

            if (node is not JsonObject root)
                return payloadJson;

            if (!TryGetWoList(root, out var woList))
                return payloadJson;

            foreach (var woNode in woList.OfType<JsonObject>())
            {
                var woJobId = GetStringLoose(woNode, "WorkOrderID") ?? jobId;
                var woSubProjectId = GetStringLoose(woNode, "SubProjectId") ?? subProjectId;

                var desc = $"{woJobId} - {woSubProjectId} - {action}";

                StampJournal(woNode, "WOItemLines", desc);
                StampJournal(woNode, "WOExpLines", desc);
                StampJournal(woNode, "WOHourLines", desc);
            }

            // Keep payload compact (no indentation).
            return root.ToJsonString(new JsonSerializerOptions
            {
                WriteIndented = false,
                PropertyNamingPolicy = null
            });
        }

    private static void StampJournal(JsonObject woNode, string journalKey, string desc)
        {
            if (!woNode.TryGetPropertyValue(journalKey, out var jNode) || jNode is not JsonObject journal)
                return;

            journal["JournalDescription"] = desc;

            if (!journal.TryGetPropertyValue("JournalLines", out var linesNode) || linesNode is not JsonArray lines)
                return;

            foreach (var ln in lines.OfType<JsonObject>())
            {
                ln["JournalDescription"] = desc;
                ln["JournalLineDescription"] = desc;
            }
        }

    private static bool TryGetWoList(JsonObject root, out JsonArray woList)
        {
            woList = new JsonArray();

            // Expect: { "_request": { "WOList": [ ... ] } }
            if (!TryGetNodeLoose(root, "_request", out var reqNode) || reqNode is not JsonObject reqObj)
                return false;

            if (!TryGetNodeLoose(reqObj, "WOList", out var listNode) || listNode is not JsonArray arr)
                return false;

            woList = arr;
            return true;
        }

    private static string? GetStringLoose(JsonObject obj, string key)
        {
            if (!TryGetNodeLoose(obj, key, out var n) || n is null)
                return null;

            var s = n.ToString();
            return string.IsNullOrWhiteSpace(s) ? null : s;
        }

    private static bool TryGetNodeLoose(JsonObject obj, string key, out JsonNode? node)
        {
            node = null;

            if (obj.TryGetPropertyValue(key, out node))
                return true;

            foreach (var kv in obj)
            {
                if (string.Equals(kv.Key, key, StringComparison.OrdinalIgnoreCase))
                {
                    node = kv.Value;
                    return true;
                }
            }

            return false;
        }

}
