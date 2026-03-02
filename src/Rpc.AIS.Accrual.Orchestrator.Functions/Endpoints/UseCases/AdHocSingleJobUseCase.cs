using System;
using System.Text.Json;
using System.Threading;
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
/// AdHoc Batch - Single Job use case.
/// Fully extracted (no shared endpoint handler dependency).
/// </summary>
public sealed class AdHocSingleJobUseCase : JobOperationsUseCaseBase, IAdHocSingleJobUseCase
{
    private readonly IFsaDeltaPayloadOrchestrator _payloadOrch;
    private readonly FsOptions _fsOpt;
    private readonly IPostingClient _posting;
    private readonly IWoDeltaPayloadServiceV2 _deltaV2;
    private readonly InvoiceAttributeSyncRunner _invoiceSync;
    private readonly InvoiceAttributesUpdateRunner _invoiceUpdate;

    public AdHocSingleJobUseCase(
        ILogger<AdHocSingleJobUseCase> log,
        IAisLogger aisLogger,
        IAisDiagnosticsOptions diag,
        IFsaDeltaPayloadOrchestrator payloadOrch,
        FsOptions fsOpt,
        IPostingClient posting,
        IWoDeltaPayloadServiceV2 deltaV2,
        InvoiceAttributeSyncRunner invoiceSync,
        InvoiceAttributesUpdateRunner invoiceUpdate)
        : base(log, aisLogger, diag)
    {
        _payloadOrch = payloadOrch ?? throw new ArgumentNullException(nameof(payloadOrch));
        _fsOpt = fsOpt ?? throw new ArgumentNullException(nameof(fsOpt));
        _posting = posting ?? throw new ArgumentNullException(nameof(posting));
        _deltaV2 = deltaV2 ?? throw new ArgumentNullException(nameof(deltaV2));
        _invoiceSync = invoiceSync ?? throw new ArgumentNullException(nameof(invoiceSync));
        _invoiceUpdate = invoiceUpdate ?? throw new ArgumentNullException(nameof(invoiceUpdate));
    }

    public async Task<HttpResponseData> ExecuteAsync(HttpRequestData req, FunctionContext ctx)
    {
                var (runId, correlationId, sourceSystem) = ReadContext(req);

                using var scope = LogScopes.BeginFunctionScope(_log, new LogScopeContext
                {
                    Function = "AdHocBatch_SingleJob",
                    Operation = "AdHocBatch_SingleJob",
                    Trigger = "Http",
                    RunId = runId,
                    CorrelationId = correlationId,
                    SourceSystem = sourceSystem
                });

                var body = await ReadBodyAsync(req);

                await LogInboundPayloadAsync(runId, correlationId, "AdHocBatch_SingleJob", body).ConfigureAwait(false);

                if (string.IsNullOrWhiteSpace(body))
                    return await BadRequestAsync(req, correlationId, runId, "Request body is required and must contain workOrderGuid.");

                if (!TryParseFsJobOpsRequest(body, out var parsed, out var parseError))
                    return await BadRequestAsync(req, correlationId, runId, parseError ?? "Invalid request body.");

                // Prefer envelope-provided RunId/CorrelationId when present.
                runId = string.IsNullOrWhiteSpace(parsed.RunId) ? runId : parsed.RunId!;
                correlationId = string.IsNullOrWhiteSpace(parsed.CorrelationId) ? correlationId : parsed.CorrelationId!;
                var woGuid = parsed.WorkOrderGuid;

                // PURE SYNC: build payload for that WO and post in-line.
                using var woScope = LogScopes.BeginFunctionScope(_log, new LogScopeContext
                {
                    Function = "AdHocBatch_SingleJob",
                    Operation = "AdHocBatch_SingleJob",
                    Trigger = "Http",
                    RunId = runId,
                    CorrelationId = correlationId,
                    SourceSystem = sourceSystem,
                    WorkOrderGuid = woGuid
                });

                var payload = await _payloadOrch.BuildSingleWorkOrderAnyStatusAsync(
                    new GetFsaDeltaPayloadInputDto(runId, correlationId, "AdHocSingle", woGuid.ToString()),
                    _fsOpt,
                    ctx.CancellationToken);

                // If payload is empty (WO not in open set), return 404-ish business result.
                if (payload is null || string.IsNullOrWhiteSpace(payload.PayloadJson) || payload.WorkOrderNumbers.Count == 0)
                    return await NotFoundAsync(req, correlationId, runId, new
                    {
                        runId,
                        correlationId,
                        sourceSystem,
                        workOrderGuid = woGuid,
                        message = "Work order not found in OPEN set (or was skipped due to missing SubProject)."
                    });

                // Build delta payload (V2) (compare FSA snapshot vs FSCM history) then optionally post.
                if (!TryExtractCompanyAndSubProjectIdString(payload.PayloadJson, out var company, out var subProjectId) ||
                    string.IsNullOrWhiteSpace(subProjectId))
                    return await BadRequestAsync(req, correlationId, runId, "SubProjectId is missing (ensure FSA payload contains SubProjectId).");

                var runCtx = new RunContext(runId, DateTimeOffset.UtcNow, "AdHocSingle", correlationId, sourceSystem, company);

                var delta = await _deltaV2.BuildDeltaPayloadAsync(
                    runCtx,
                    payload.PayloadJson,
                    DateTime.UtcNow.Date,
                    new WoDeltaBuildOptions(BaselineSubProjectId: subProjectId!, TargetMode: WoDeltaTargetMode.Normal),
                    ctx.CancellationToken);

                List<PostResult> postResults = new();

                if (!string.IsNullOrWhiteSpace(delta.DeltaPayloadJson) && delta.TotalDeltaLines > 0)
                    postResults = await _posting.ValidateOnceAndPostAllJournalTypesAsync(runCtx, delta.DeltaPayloadJson!, ctx.CancellationToken);

                var allOk = postResults.Count == 0 || postResults.All(r => r.IsSuccess);

                // Invoice attributes update (sync logic + update endpoint) - runs even when there are no journal deltas.
                object? invoiceAttributesUpdate = null;
                if (allOk)
                {
                    var enrich = await _invoiceSync.EnrichPostingPayloadAsync(runCtx, payload.PayloadJson, ctx.CancellationToken);
                    var upd = await _invoiceUpdate.UpdateFromPostingPayloadAsync(runCtx, enrich.PostingPayloadJson, ctx.CancellationToken);

                    invoiceAttributesUpdate = new
                    {
                        attempted = enrich.Attempted,
                        success = enrich.Success,
                        workOrdersWithInvoiceAttributes = enrich.WorkOrdersWithInvoiceAttributes,
                        totalAttributePairs = enrich.TotalAttributePairs,
                        note = enrich.Note,
                        update = new
                        {
                            upd.WorkOrdersConsidered,
                            upd.WorkOrdersWithUpdates,
                            upd.UpdatePairs,
                            upd.SuccessCount,
                            upd.FailureCount
                        }
                    };
                }

                if (delta.TotalDeltaLines == 0 || string.IsNullOrWhiteSpace(delta.DeltaPayloadJson))
                {
                    return await OkAsync(req, correlationId, runId, new
                    {
                        runId,
                        correlationId,
                        sourceSystem,
                        workOrderGuid = woGuid,
                        workOrderNumbers = payload.WorkOrderNumbers,
                        message = "No deltas detected; nothing to post.",
                        delta = new
                        {
                            delta.WorkOrdersInInput,
                            delta.WorkOrdersInOutput,
                            delta.TotalDeltaLines,
                            delta.TotalReverseLines,
                            delta.TotalRecreateLines
                        },
                        invoiceAttributesUpdate
                    });
                }

                return await OkAsync(req, correlationId, runId, new
                {
                    runId,
                    correlationId,
                    sourceSystem,
                    workOrderGuid = woGuid,
                    workOrderNumbers = payload.WorkOrderNumbers,
                    delta = new
                    {
                        delta.WorkOrdersInInput,
                        delta.WorkOrdersInOutput,
                        delta.TotalDeltaLines,
                        delta.TotalReverseLines,
                        delta.TotalRecreateLines
                    },
                    postResults = postResults.Select(r => new
                    {
                        journalType = r.JournalType.ToString(),
                        success = r.IsSuccess,
                        posted = r.WorkOrdersPosted,
                        errors = r.Errors?.Count ?? 0
                    }),
                    invoiceAttributesUpdate
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

}
