using Microsoft.Azure.Functions.Worker;
using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Functions.Services;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Rpc.AIS.Accrual.Orchestrator.Functions.Functions;

/// <summary>
/// Durable activity adapter only. All business logic lives in <see cref="ActivitiesUseCase"/>.
/// </summary>
public sealed class Activities
{
    private readonly IActivitiesUseCase _useCase;

    public Activities(IActivitiesUseCase useCase)
        => _useCase = useCase ?? throw new ArgumentNullException(nameof(useCase));

    [Function(nameof(ValidateAndPostWoPayload))]
    public Task<List<PostResult>> ValidateAndPostWoPayload(
        [ActivityTrigger] DurableAccrualOrchestration.WoPayloadPostingInputDto input,
        FunctionContext ctx)
    {
        var runCtx = new RunContext(input.RunId, DateTimeOffset.UtcNow, "Durable", input.CorrelationId);
        return _useCase.ValidateAndPostWoPayloadAsync(input, runCtx, ctx.CancellationToken);
    }

    [Function(nameof(PostSingleWorkOrder))]
    public Task<PostSingleWorkOrderResponse> PostSingleWorkOrder(
        [ActivityTrigger] DurableAccrualOrchestration.SingleWoPostingInputDto input,
        FunctionContext ctx)
    {
        var runCtx = new RunContext(input.RunId, DateTimeOffset.UtcNow, "Durable", input.CorrelationId);
        return _useCase.PostSingleWorkOrderAsync(input, runCtx, ctx.CancellationToken);
    }

    [Function(nameof(UpdateWorkOrderStatus))]
    public Task<WorkOrderStatusUpdateResponse> UpdateWorkOrderStatus(
        [ActivityTrigger] DurableAccrualOrchestration.WorkOrderStatusUpdateInputDto input,
        FunctionContext ctx)
    {
        var runCtx = new RunContext(input.RunId, DateTimeOffset.UtcNow, "Durable", input.CorrelationId);
        return _useCase.UpdateWorkOrderStatusAsync(input, runCtx, ctx.CancellationToken);
    }

    [Function(nameof(PostRetryableWoPayload))]
    public Task<PostResult> PostRetryableWoPayload(
        [ActivityTrigger] DurableAccrualOrchestration.RetryableWoPayloadPostingInputDto input,
        FunctionContext ctx)
    {
        var runCtx = new RunContext(input.RunId, DateTimeOffset.UtcNow, "Durable", input.CorrelationId);
        return _useCase.PostRetryableWoPayloadAsync(input, runCtx, ctx.CancellationToken);
    }

    [Function(nameof(SyncInvoiceAttributes))]
    public Task<DurableAccrualOrchestration.InvoiceAttributesSyncResultDto> SyncInvoiceAttributes(
        [ActivityTrigger] DurableAccrualOrchestration.InvoiceAttributesSyncInputDto input,
        FunctionContext ctx)
    {
        var runCtx = new RunContext(input.RunId, DateTimeOffset.UtcNow, "Durable", input.CorrelationId);
        return _useCase.SyncInvoiceAttributesAsync(input, runCtx, ctx.CancellationToken);
    }

    [Function(nameof(FinalizeAndNotifyWoPayload))]
    public Task<DurableAccrualOrchestration.RunOutcomeDto> FinalizeAndNotifyWoPayload(
        [ActivityTrigger] DurableAccrualOrchestration.FinalizeWoPayloadInputDto input,
        FunctionContext ctx)
    {
        var runCtx = new RunContext(input.RunId, DateTimeOffset.UtcNow, "Durable", input.CorrelationId);
        return _useCase.FinalizeAndNotifyWoPayloadAsync(input, runCtx, ctx.CancellationToken);
    }
}
