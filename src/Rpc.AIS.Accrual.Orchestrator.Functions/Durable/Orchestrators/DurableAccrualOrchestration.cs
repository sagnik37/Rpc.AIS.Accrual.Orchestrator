using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Options;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Rpc.AIS.Accrual.Orchestrator.Functions.Functions;

/// <summary>
/// Provides durable accrual orchestration behavior.
/// </summary>
public sealed class DurableAccrualOrchestration
{
    private readonly ILogger<DurableAccrualOrchestration> _logger;

    // Opt-in flag to preserve existing behavior by default.
    private readonly bool _applyFscmDeltaBeforePosting;

    public DurableAccrualOrchestration(ILogger<DurableAccrualOrchestration> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _applyFscmDeltaBeforePosting = false;
    }

    // DI-preferred ctor (keeps old ctor for tests/back-compat)
    public DurableAccrualOrchestration(
        ILogger<DurableAccrualOrchestration> logger,
        IOptions<FsOptions> ingestion)
        : this(logger)
    {
        _applyFscmDeltaBeforePosting = ingestion?.Value?.ApplyFscmDeltaBeforePosting == true;
    }

    // ==============================
    // DTOs used by Activities 
    // ==============================
    /// <summary>
    /// Carries run input dto data.
    /// </summary>
    public sealed record RunInputDto(string RunId, string CorrelationId, string TriggeredBy, string? SourceSystem = null, string? WorkOrderGuid = null);

    /// <summary>
    /// Carries wo payload posting input dto data.
    /// </summary>
    public sealed record WoPayloadPostingInputDto(string RunId, string CorrelationId, string WoPayloadJson, string? DurableInstanceId = null);

    /// <summary>
    /// Carries retryable payload posting input dto data.
    /// </summary>
    public sealed record RetryableWoPayloadPostingInputDto(
        string RunId,
        string CorrelationId,
        string WoPayloadJson,
        JournalType JournalType,
        int Attempt,
        string? DurableInstanceId = null);

    /// <summary>
    /// Carries finalize wo payload input dto data.
    /// </summary>
    public sealed record FinalizeWoPayloadInputDto(
        string RunId,
        string CorrelationId,
        string WoPayloadJson,
        List<PostResult> PostResults,
        string[]? GeneralErrors,
        string? DurableInstanceId = null);

    // REQUIRED BY Activities.cs 
    /// <summary>
    /// Carries single wo posting input dto data.
    /// </summary>
    public sealed record SingleWoPostingInputDto(
        string RunId,
        string CorrelationId,
        string TriggeredBy,
        string RawJsonBody,
        string? DurableInstanceId = null);

    // REQUIRED BY Activities.cs 
    /// <summary>
    /// Carries work order status update input dto data.
    /// </summary>
    public sealed record WorkOrderStatusUpdateInputDto(
        string RunId,
        string CorrelationId,
        string TriggeredBy,
        string RawJsonBody,
        string? DurableInstanceId = null);


    /// <summary>
    /// Carries invoice attributes synchronization input dto data (best-effort enrichment/update).
    /// </summary>
    public sealed record InvoiceAttributesSyncInputDto(
        string RunId,
        string CorrelationId,
        string WoPayloadJson,
        string? DurableInstanceId = null);

    /// <summary>
    /// Outcome for invoice attributes sync.
    /// </summary>
    public sealed record InvoiceAttributesSyncResultDto(
        bool Attempted,
        bool Success,
        int WorkOrdersWithInvoiceAttributes,
        int TotalAttributePairs,
        string Note,
        int UpdateSuccessCount,
        int UpdateFailureCount);

    /// <summary>
    /// Carries run outcome dto data.
    /// </summary>
    public sealed record RunOutcomeDto(
        string RunId,
        string CorrelationId,
        int WorkOrdersConsidered,
        int WorkOrdersValid,
        int WorkOrdersInvalid,
        int PostFailureGroups,
        bool HasAnyErrors,
        List<string> GeneralErrors)
    {
        /// <summary>
        /// Executes success.
        /// </summary>
        public static RunOutcomeDto Success(string runId, string correlationId)
            => new(runId, correlationId, 0, 0, 0, 0, false, new List<string>());
    }

    [Function(nameof(AccrualOrchestrator))]
    // <summary>
    // Executes accrual orchestrator.
    // </summary>
    public async Task<RunOutcomeDto> AccrualOrchestrator([OrchestrationTrigger] TaskOrchestrationContext context)
    {
        var input = context.GetInput<RunInputDto>()
            ?? throw new InvalidOperationException("Orchestration input is required.");

        var instanceId = context.InstanceId;

        var log = context.CreateReplaySafeLogger(nameof(DurableAccrualOrchestration));
        log.LogInformation("Orchestrator.State.Begin RunId={RunId} CorrelationId={CorrelationId} InstanceId={InstanceId} TriggeredBy={TriggeredBy} SourceSystem={SourceSystem} WorkOrderGuid={WorkOrderGuid}", input.RunId, input.CorrelationId, instanceId, input.TriggeredBy, input.SourceSystem, input.WorkOrderGuid);


        log.LogInformation("Orchestrator.State.FetchFromFsa.Begin RunId={RunId}", input.RunId);

        // 1) Get payload from Dataverse change tracking / full fetch pipeline
        var delta = await context.CallActivityAsync<GetFsaDeltaPayloadResultDto>(
            nameof(FsaDeltaActivities.GetFsaDeltaPayload),
            new GetFsaDeltaPayloadInputDto(input.RunId, input.CorrelationId, input.TriggeredBy, input.WorkOrderGuid, instanceId));

        log.LogInformation("Orchestrator.State.FetchFromFsa.End RunId={RunId} PayloadBytes={Bytes}", input.RunId, (delta.PayloadJson ?? string.Empty).Length);

        var woPayloadJson = delta.PayloadJson ?? string.Empty;
        var originalFsPayloadJson = woPayloadJson;

        // 1b)  build delta-only payload by comparing FS payload to FSCM journal history.
        if (_applyFscmDeltaBeforePosting)
        {
            log.LogInformation("Orchestrator.State.FetchFromFscmAndDelta.Begin RunId={RunId}", input.RunId);
            var deltaResult = await context.CallActivityAsync<BuildDeltaPayloadFromFscmHistoryResultDto>(
                nameof(DeltaActivities.BuildDeltaPayloadFromFscmHistory),
                new BuildDeltaPayloadFromFscmHistoryInputDto(
                    RunId: input.RunId,
                    CorrelationId: input.CorrelationId,
                    TriggeredBy: input.TriggeredBy,
                    FsaPayloadJson: woPayloadJson,
                    DurableInstanceId: instanceId));

            woPayloadJson = deltaResult.DeltaPayloadJson ?? string.Empty;
            log.LogInformation("Orchestrator.State.FetchFromFscmAndDelta.End RunId={RunId} PayloadBytes={Bytes}", input.RunId, woPayloadJson.Length);
        }

        log.LogInformation("Orchestrator.State.ValidateAndPost.Begin RunId={RunId}", input.RunId);

        // 2) Validate once + post all journal types (existing activity)
        var postResults = await context.CallActivityAsync<List<PostResult>>(
            nameof(Activities.ValidateAndPostWoPayload),
            new WoPayloadPostingInputDto(
                RunId: input.RunId,
                CorrelationId: input.CorrelationId,
                WoPayloadJson: woPayloadJson,
                DurableInstanceId: instanceId)) ?? new List<PostResult>();

        log.LogInformation("Orchestrator.State.ValidateAndPost.End RunId={RunId} ResultGroups={Groups}", input.RunId, postResults.Count);

        // 2b) Retry only retryable lines (lookup unavailable, transient dependencies)
        postResults = await RetryRetryableGroupsAsync(context, input.RunId, input.CorrelationId, instanceId, postResults);


        log.LogInformation("Orchestrator.State.FinalizeAndNotify.Begin RunId={RunId}", input.RunId);

        // 3) Finalize + notify (existing activity)
        var outcome = await context.CallActivityAsync<RunOutcomeDto>(
            nameof(Activities.FinalizeAndNotifyWoPayload),
            new FinalizeWoPayloadInputDto(
                RunId: input.RunId,
                CorrelationId: input.CorrelationId,
                WoPayloadJson: woPayloadJson,
                PostResults: postResults,
                GeneralErrors: Array.Empty<string>(),
                DurableInstanceId: instanceId));

        // Invoice attributes update: orchestration-level only (best-effort).
        // Runs ONLY when all journal posts succeeded.
        if (postResults.Count == 0 || postResults.All(r => r.IsSuccess))
        {
            await context.CallActivityAsync<InvoiceAttributesSyncResultDto>(
                nameof(Activities.SyncInvoiceAttributes),
                new InvoiceAttributesSyncInputDto(input.RunId, input.CorrelationId, originalFsPayloadJson, instanceId));
        }



        return outcome;
    }

    /// <summary>
    /// Retries posting for retryable-only groups returned by validation/posting.
    /// Retries are executed per journal type and only for the retryable subset payload.
    /// </summary>
    private static async Task<List<PostResult>> RetryRetryableGroupsAsync(
        TaskOrchestrationContext context,
        string runId,
        string correlationId,
        string instanceId,
        List<PostResult> postResults)
    {
        if (postResults is null || postResults.Count == 0)
            return postResults ?? new List<PostResult>();

        // Hard rule: never retry HTTP POST operations.
        // Posting retryable groups ultimately performs FSCM POSTs; disabling orchestration-level retries
        // avoids duplicate side effects when the first attempt succeeded server-side.
        const int MaxAttempts = 1;

        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            var retryGroups = postResults
                .Where(r => r.RetryableWorkOrders > 0 && !string.IsNullOrWhiteSpace(r.RetryablePayloadJson))
                .ToList();

            if (retryGroups.Count == 0)
                break;

            foreach (var g in retryGroups)
            {
                var retryResult = await context.CallActivityAsync<PostResult>(
                    nameof(Activities.PostRetryableWoPayload),
                    new RetryableWoPayloadPostingInputDto(
                        RunId: runId,
                        CorrelationId: correlationId,
                        WoPayloadJson: g.RetryablePayloadJson!,
                        JournalType: g.JournalType,
                        Attempt: attempt,
                        DurableInstanceId: instanceId));

                // Replace the corresponding result by journal type.
                var idx = postResults.FindIndex(x => x.JournalType == g.JournalType);
                if (idx >= 0)
                    postResults[idx] = retryResult;
            }
        }

        return postResults;
    }
}
