using Microsoft.Extensions.Logging;

using System.Threading;
using System.Threading.Tasks;
using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Functions.Functions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;

namespace Rpc.AIS.Accrual.Orchestrator.Functions.Services.Handlers;

public sealed class PostRetryableWoPayloadHandler : ActivitiesHandlerBase
{
    private readonly IPostingClient _posting;
    private readonly IAisLogger _ais;
    private readonly ILogger<PostRetryableWoPayloadHandler> _logger;

    public PostRetryableWoPayloadHandler(
        IPostingClient posting,
        IAisLogger ais,
        ILogger<PostRetryableWoPayloadHandler> logger)
    {
        _posting = posting ?? throw new ArgumentNullException(nameof(posting));
        _ais = ais ?? throw new ArgumentNullException(nameof(ais));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<PostResult> HandleAsync(
        DurableAccrualOrchestration.RetryableWoPayloadPostingInputDto input,
        RunContext runCtx,
        CancellationToken ct)
    {
        using var scope = BeginScope(_logger, runCtx, "PostRetryableWoPayload", input.DurableInstanceId);

        _logger.LogInformation(
            "Activity PostRetryableWoPayload: Begin. RunId={RunId} CorrelationId={CorrelationId} JournalType={JournalType} Attempt={Attempt}",
            runCtx.RunId, runCtx.CorrelationId, input.JournalType, input.Attempt);

        try
        {
            // Re-run validation for this subset; any remaining retryables will be carried forward again.
            var result = await _posting.PostFromWoPayloadAsync(
                runCtx,
                input.JournalType,
                input.WoPayloadJson,
                ct).ConfigureAwait(false);

            _logger.LogInformation(
                "Activity PostRetryableWoPayload: Completed. RunId={RunId} CorrelationId={CorrelationId} JournalType={JournalType} Attempt={Attempt} Success={Success} PostedWO={PostedWO} RetryableWO={RetryWO}",
                runCtx.RunId, runCtx.CorrelationId, input.JournalType, input.Attempt, result.IsSuccess, result.WorkOrdersPosted, result.RetryableWorkOrders);

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Activity PostRetryableWoPayload failed. RunId={RunId} CorrelationId={CorrelationId} JournalType={JournalType} Attempt={Attempt}",
                runCtx.RunId, runCtx.CorrelationId, input.JournalType, input.Attempt);

            throw;
        }
    }
}
