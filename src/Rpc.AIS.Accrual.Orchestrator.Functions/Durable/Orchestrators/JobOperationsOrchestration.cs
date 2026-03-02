using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Functions.Services;

namespace Rpc.AIS.Accrual.Orchestrator.Functions.Functions;

/// <summary>
/// Durable orchestrations for job operations (Post / Customer Change).
/// These orchestrations intentionally reuse the existing "Fetch from FSA -> optional delta -> validate/post" pipeline,
/// then execute operation-specific FSCM project lifecycle steps.
/// </summary>
public sealed class JobOperationsOrchestration
{
    private readonly bool _applyFscmDeltaBeforePosting;

    public JobOperationsOrchestration(IOptions<Rpc.AIS.Accrual.Orchestrator.Infrastructure.Options.FsOptions> fsOptions)
    {
        _applyFscmDeltaBeforePosting =
            fsOptions?.Value?.ApplyFscmDeltaBeforePosting == true;
    }


    public sealed record JobOperationInputDto(
        string RunId,
        string CorrelationId,
        string TriggeredBy,
        string? SourceSystem,
        Guid WorkOrderGuid,
        string? RawRequestJson);

    public sealed record JobOperationOutcomeDto(
        bool IsSuccess,
        string RunId,
        string CorrelationId,
        string TriggeredBy,
        string? SourceSystem,
        Guid WorkOrderGuid,
        string? DurableInstanceId,
        string? Notes);

    [Function(nameof(PostJobOrchestrator))]
    public async Task<JobOperationOutcomeDto> PostJobOrchestrator([OrchestrationTrigger] TaskOrchestrationContext context)
    {
        var input = context.GetInput<JobOperationInputDto>()
            ?? throw new InvalidOperationException("Orchestration input is required.");

        var log = context.CreateReplaySafeLogger(nameof(JobOperationsOrchestration));
        log.LogInformation(
            "JobOp.Begin Operation=Post RunId={RunId} CorrelationId={CorrelationId} SourceSystem={SourceSystem} WorkOrderGuid={WorkOrderGuid}",
            input.RunId, input.CorrelationId, input.SourceSystem, input.WorkOrderGuid);

        // Reuse existing durable pipeline, but scoped to one Work Order.
        await RunAccrualPipelineForSingleWoAsync(context, input, log);

        // Operation-specific FSCM project lifecycle steps (placeholders).
        await context.CallActivityAsync(
            nameof(JobOperationsActivities.UpdateProjectStage),
            new JobOperationsActivities.UpdateProjectStageInputDto(
                input.RunId, input.CorrelationId, input.SourceSystem, input.WorkOrderGuid, stage: "Ready for invoicing"));

        await context.CallActivityAsync(
            nameof(JobOperationsActivities.SyncJobAttributesToProject),
            new JobOperationsActivities.SyncJobAttributesInputDto(
                input.RunId, input.CorrelationId, input.SourceSystem, input.WorkOrderGuid));

        log.LogInformation("JobOp.End Operation=Post RunId={RunId} WorkOrderGuid={WorkOrderGuid}", input.RunId, input.WorkOrderGuid);

        return new JobOperationOutcomeDto(
            IsSuccess: true,
            RunId: input.RunId,
            CorrelationId: input.CorrelationId,
            TriggeredBy: input.TriggeredBy,
            SourceSystem: input.SourceSystem,
            WorkOrderGuid: input.WorkOrderGuid,
            DurableInstanceId: context.InstanceId,
            Notes: "Post completed (journals + stage + attributes)." );
    }

    // ------------------------------------------------------------------
    // V2 orchestrators (additive)
    // - designed for synchronous FS-triggered flows
    // - adds runtime invoice attribute mapping + compare + update
    // ------------------------------------------------------------------

    [Function(nameof(PostJobOrchestratorV2))]
    public async Task<JobOperationOutcomeDto> PostJobOrchestratorV2([OrchestrationTrigger] TaskOrchestrationContext context)
    {
        var input = context.GetInput<JobOperationInputDto>()
            ?? throw new InvalidOperationException("Orchestration input is required.");

        var log = context.CreateReplaySafeLogger(nameof(JobOperationsOrchestration));
        log.LogInformation(
            "JobOpV2.Begin Operation=Post RunId={RunId} CorrelationId={CorrelationId} SourceSystem={SourceSystem} WorkOrderGuid={WorkOrderGuid}",
            input.RunId, input.CorrelationId, input.SourceSystem, input.WorkOrderGuid);

        await RunAccrualPipelineForSingleWoAsync(context, input, log);

        // Extract subprojectGuid from the raw FS request payload.
        var subprojectGuid = await context.CallActivityAsync<Guid?>(
            nameof(JobOperationsV2ParsingActivities.TryExtractSubprojectGuid),
            new JobOperationsV2ParsingActivities.ExtractSubprojectGuidInputDto(
                input.RunId, input.CorrelationId, input.SourceSystem, input.WorkOrderGuid, input.RawRequestJson ?? string.Empty));

        if (subprojectGuid is null || subprojectGuid == Guid.Empty)
        {
            log.LogWarning("JobOpV2.Post Missing subprojectGuid in request. Skipping invoice attributes + project status updates.");
            return new JobOperationOutcomeDto(true, input.RunId, input.CorrelationId, input.TriggeredBy, input.SourceSystem, input.WorkOrderGuid, context.InstanceId,
                "Posted journals; subprojectGuid missing so invoice attributes/project status were skipped.");
        }

        //await context.CallActivityAsync<JobOperationsV2Activities.UpdateInvoiceAttributesResultDto>(
        //    nameof(JobOperationsV2Activities.UpdateInvoiceAttributesRuntime),
        //    new JobOperationsV2Activities.UpdateInvoiceAttributesInputDto(
        //        input.RunId, input.CorrelationId, input.SourceSystem, input.WorkOrderGuid, subprojectGuid.Value, input.RawRequestJson ?? string.Empty));
        log.LogInformation("JobOpV2.End Operation=Post RunId={RunId} WorkOrderGuid={WorkOrderGuid}", input.RunId, input.WorkOrderGuid);

        return new JobOperationOutcomeDto(true, input.RunId, input.CorrelationId, input.TriggeredBy, input.SourceSystem, input.WorkOrderGuid, context.InstanceId,
            "V2 post completed (journals + invoice attributes + project status).");
    }



    [Function(nameof(CustomerChangeOrchestrator))]
    public async Task<JobOperationOutcomeDto> CustomerChangeOrchestrator([OrchestrationTrigger] TaskOrchestrationContext context)
    {
        var input = context.GetInput<JobOperationInputDto>()
            ?? throw new InvalidOperationException("Orchestration input is required.");

        var log = context.CreateReplaySafeLogger(nameof(JobOperationsOrchestration));
        log.LogInformation(
            "JobOp.Begin Operation=CustomerChange RunId={RunId} CorrelationId={CorrelationId} SourceSystem={SourceSystem} WorkOrderGuid={WorkOrderGuid}",
            input.RunId, input.CorrelationId, input.SourceSystem, input.WorkOrderGuid);

        // Customer change is an end-to-end business process:
        // 1) Create new subproject, 2) post old lines into new subproject, 3) reverse old lines, 4) cancel old subproject.
        var ccResult = await context.CallActivityAsync<CustomerChangeResultDto>(
            nameof(JobOperationsActivities.CustomerChangeExecute),
            new JobOperationsActivities.CustomerChangeInputDto(
                input.RunId, input.CorrelationId, input.SourceSystem, input.WorkOrderGuid, input.RawRequestJson));

        log.LogInformation("CustomerChange.NewSubProjectId={NewSubProjectId}", ccResult.NewSubProjectId);
log.LogInformation("JobOp.End Operation=CustomerChange RunId={RunId} WorkOrderGuid={WorkOrderGuid}", input.RunId, input.WorkOrderGuid);

        return new JobOperationOutcomeDto(
            IsSuccess: true,
            RunId: input.RunId,
            CorrelationId: input.CorrelationId,
            TriggeredBy: input.TriggeredBy,
            SourceSystem: input.SourceSystem,
            WorkOrderGuid: input.WorkOrderGuid,
            DurableInstanceId: context.InstanceId,
            Notes: "Customer change completed. NewSubProjectId=" + ccResult.NewSubProjectId);
    }

    private async Task RunAccrualPipelineForSingleWoAsync(TaskOrchestrationContext context, JobOperationInputDto input, ILogger log)
    {
        // 1) Get payload from Dataverse full fetch pipeline, scoped to WorkOrderGuid.
        var delta = await context.CallActivityAsync<GetFsaDeltaPayloadResultDto>(
            nameof(FsaDeltaActivities.GetFsaDeltaPayload),
            new GetFsaDeltaPayloadInputDto(
                RunId: input.RunId,
                CorrelationId: input.CorrelationId,
                TriggeredBy: input.TriggeredBy,
                WorkOrderGuid: input.WorkOrderGuid.ToString()));

        var woPayloadJson = delta.PayloadJson ?? string.Empty;

        // 1b) build delta-only payload by comparing FS payload to FSCM journal history.
        if (_applyFscmDeltaBeforePosting)
        {
            var deltaResult = await context.CallActivityAsync<BuildDeltaPayloadFromFscmHistoryResultDto>(
                nameof(DeltaActivities.BuildDeltaPayloadFromFscmHistory),
                new BuildDeltaPayloadFromFscmHistoryInputDto(
                    RunId: input.RunId,
                    CorrelationId: input.CorrelationId,
                    TriggeredBy: input.TriggeredBy,
                    FsaPayloadJson: woPayloadJson));

            woPayloadJson = deltaResult.DeltaPayloadJson ?? string.Empty;
        }

        // 2) Validate + post (existing activity)
        var postResults = await context.CallActivityAsync<List<PostResult>>(
            nameof(Activities.ValidateAndPostWoPayload),
            new DurableAccrualOrchestration.WoPayloadPostingInputDto(
                RunId: input.RunId,
                CorrelationId: input.CorrelationId,
                WoPayloadJson: woPayloadJson)) ?? new List<PostResult>();

        log.LogInformation(
            "JobOp.Pipeline.Completed RunId={RunId} WorkOrderGuid={WorkOrderGuid} ResultGroups={Groups}",
            input.RunId, input.WorkOrderGuid, postResults.Count);
    }
}
