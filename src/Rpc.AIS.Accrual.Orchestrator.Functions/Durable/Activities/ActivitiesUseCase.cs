using Microsoft.Extensions.Logging;

using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Core.Services;
using Rpc.AIS.Accrual.Orchestrator.Functions.Functions;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Options;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Rpc.AIS.Accrual.Orchestrator.Functions.Services.Handlers;

namespace Rpc.AIS.Accrual.Orchestrator.Functions.Services;

/// <summary>
/// Implements activity business logic (Activities.cs becomes an orchestration adapter only).
/// </summary>
public sealed class ActivitiesUseCase : IActivitiesUseCase
{
    private readonly ILogger<ActivitiesUseCase> _logger;

    private readonly ValidateAndPostWoPayloadHandler _validateAndPost;
    private readonly PostSingleWorkOrderHandler _postSingle;
    private readonly UpdateWorkOrderStatusHandler _updateStatus;
    private readonly PostRetryableWoPayloadHandler _postRetryable;
    private readonly SyncInvoiceAttributesHandler _syncInvoice;
    private readonly FinalizeAndNotifyWoPayloadHandler _finalizeAndNotify;

    public ActivitiesUseCase(
        ValidateAndPostWoPayloadHandler validateAndPost,
        PostSingleWorkOrderHandler postSingle,
        UpdateWorkOrderStatusHandler updateStatus,
        PostRetryableWoPayloadHandler postRetryable,
        SyncInvoiceAttributesHandler syncInvoice,
        FinalizeAndNotifyWoPayloadHandler finalizeAndNotify,
        ILogger<ActivitiesUseCase> logger)
    {
        _validateAndPost = validateAndPost ?? throw new ArgumentNullException(nameof(validateAndPost));
        _postSingle = postSingle ?? throw new ArgumentNullException(nameof(postSingle));
        _updateStatus = updateStatus ?? throw new ArgumentNullException(nameof(updateStatus));
        _postRetryable = postRetryable ?? throw new ArgumentNullException(nameof(postRetryable));
        _syncInvoice = syncInvoice ?? throw new ArgumentNullException(nameof(syncInvoice));
        _finalizeAndNotify = finalizeAndNotify ?? throw new ArgumentNullException(nameof(finalizeAndNotify));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<List<PostResult>> ValidateAndPostWoPayloadAsync(
        DurableAccrualOrchestration.WoPayloadPostingInputDto input,
        RunContext runCtx,
        CancellationToken ct)
    {
        return await _validateAndPost.HandleAsync(input, runCtx, ct);
    }

    public async Task<PostSingleWorkOrderResponse> PostSingleWorkOrderAsync(
        DurableAccrualOrchestration.SingleWoPostingInputDto input,
        RunContext runCtx,
        CancellationToken ct)
    {
        return await _postSingle.HandleAsync(input, runCtx, ct);
    }

    public async Task<WorkOrderStatusUpdateResponse> UpdateWorkOrderStatusAsync(
        DurableAccrualOrchestration.WorkOrderStatusUpdateInputDto input,
        RunContext runCtx,
        CancellationToken ct)
    {
        return await _updateStatus.HandleAsync(input, runCtx, ct);
    }

    public async Task<PostResult> PostRetryableWoPayloadAsync(
        DurableAccrualOrchestration.RetryableWoPayloadPostingInputDto input,
        RunContext runCtx,
        CancellationToken ct)
    {
        return await _postRetryable.HandleAsync(input, runCtx, ct);
    }

    public async Task<DurableAccrualOrchestration.InvoiceAttributesSyncResultDto> SyncInvoiceAttributesAsync(
        DurableAccrualOrchestration.InvoiceAttributesSyncInputDto input,
        RunContext runCtx,
        CancellationToken ct)
    {
        return await _syncInvoice.HandleAsync(input, runCtx, ct);
    }

    public async Task<DurableAccrualOrchestration.RunOutcomeDto> FinalizeAndNotifyWoPayloadAsync(
        DurableAccrualOrchestration.FinalizeWoPayloadInputDto input,
        RunContext runCtx,
        CancellationToken ct)
    {
        return await _finalizeAndNotify.HandleAsync(input, runCtx, ct);
    }

    internal static PostResult AggregateForEmail(IReadOnlyList<PostResult> postResults)
    {
        if (postResults is null || postResults.Count == 0)
        {
            return new PostResult(
                JournalType: JournalType.Expense,
                IsSuccess: true,
                JournalId: null,
                SuccessMessage: "No posting results were produced.",
                Errors: Array.Empty<PostError>(),
                WorkOrdersBefore: 0,
                WorkOrdersPosted: 0,
                WorkOrdersFiltered: 0,
                ValidationResponseRaw: null);
        }

        var anyFailure = postResults.Any(r => !r.IsSuccess);
        var maxBefore = postResults.Max(r => r.WorkOrdersBefore);
        var maxFiltered = postResults.Max(r => r.WorkOrdersFiltered);

        var postedNonZero = postResults.Select(r => r.WorkOrdersPosted).Where(x => x > 0).ToList();
        var conservativePosted = postedNonZero.Count > 0 ? postedNonZero.Min() : 0;

        var allErrors = postResults
            .SelectMany(r => r.Errors ?? Array.Empty<PostError>())
            .ToList();

        var rawValidation = postResults
            .Select(r => r.ValidationResponseRaw)
            .FirstOrDefault(s => !string.IsNullOrWhiteSpace(s));

        var msg = anyFailure
            ? "One or more journal types reported errors during validation/posting."
            : "All journal types completed successfully.";

        return new PostResult(
            JournalType: JournalType.Expense,
            IsSuccess: !anyFailure,
            JournalId: null,
            SuccessMessage: msg,
            Errors: allErrors,
            WorkOrdersBefore: maxBefore,
            WorkOrdersPosted: conservativePosted,
            WorkOrdersFiltered: maxFiltered,
            ValidationResponseRaw: rawValidation);
    }

    internal static int TryGetWorkOrderCount(string woPayloadJson)
    {
        if (string.IsNullOrWhiteSpace(woPayloadJson)) return 0;

        try
        {
            using var doc = JsonDocument.Parse(woPayloadJson);

            if (!doc.RootElement.TryGetProperty("_request", out var reqObj) || reqObj.ValueKind != JsonValueKind.Object)
                return 0;

            if (reqObj.TryGetProperty("WOList", out var woList1) && woList1.ValueKind == JsonValueKind.Array)
                return woList1.GetArrayLength();

            if (reqObj.TryGetProperty("wo list", out var woList2) && woList2.ValueKind == JsonValueKind.Array)
                return woList2.GetArrayLength();

            return 0;
        }
        catch
        {
            return 0;
        }
    }

    private IDisposable BeginScope(RunContext ctx, string activityName, string? durableInstanceId)
        => _logger.BeginScope(new Dictionary<string, object?>
        {
            ["RunId"] = ctx.RunId,
            ["CorrelationId"] = ctx.CorrelationId,
            ["DurableInstanceId"] = durableInstanceId,
            ["Activity"] = activityName
        }) ?? NoopScope.Instance;

    private sealed class NoopScope : IDisposable
    {
        public static readonly NoopScope Instance = new();
        private NoopScope() { }
        public void Dispose() { }
    }
}
