using Microsoft.Extensions.Logging;

using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Generic;

using Rpc.AIS.Accrual.Orchestrator.Core.Services;
using Rpc.AIS.Accrual.Orchestrator.Functions.Services;
using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Functions.Functions;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Options;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;

namespace Rpc.AIS.Accrual.Orchestrator.Functions.Services.Handlers;

public sealed class FinalizeAndNotifyWoPayloadHandler : ActivitiesHandlerBase
{
    private readonly IAisLogger _ais;
    private readonly IEmailSender _email;
    private readonly NotificationOptions _notifications;
    private readonly ILogger<FinalizeAndNotifyWoPayloadHandler> _logger;

    public FinalizeAndNotifyWoPayloadHandler(
        IAisLogger ais,
        IEmailSender email,
        NotificationOptions notifications,
        ILogger<FinalizeAndNotifyWoPayloadHandler> logger)
    {
        _ais = ais ?? throw new ArgumentNullException(nameof(ais));
        _email = email ?? throw new ArgumentNullException(nameof(email));
        _notifications = notifications ?? throw new ArgumentNullException(nameof(notifications));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<DurableAccrualOrchestration.RunOutcomeDto> HandleAsync(
        DurableAccrualOrchestration.FinalizeWoPayloadInputDto input,
        RunContext runCtx,
        CancellationToken ct)
    {
        using var scope = BeginScope(_logger, runCtx, "FinalizeAndNotifyWoPayload", input.DurableInstanceId);

        _logger.LogInformation(
            "Activity FinalizeAndNotifyWoPayload: Begin. RunId={RunId} CorrelationId={CorrelationId}",
            runCtx.RunId, runCtx.CorrelationId);

        var woConsidered = ActivitiesUseCase.TryGetWorkOrderCount(input.WoPayloadJson ?? string.Empty);

        var postedCounts = input.PostResults.Select(r => r.WorkOrdersPosted).Where(c => c > 0).ToList();
        var woValid = postedCounts.Count > 0 ? postedCounts.Min() : 0;
        var woInvalid = woConsidered > 0 ? Math.Max(0, woConsidered - woValid) : 0;

        var postFailureGroups = input.PostResults.Count(r => !r.IsSuccess);
        var hasGeneralErrors = (input.GeneralErrors?.Length ?? 0) > 0;
        var hasAnyErrors = postFailureGroups > 0 || hasGeneralErrors;

        await _ais.InfoAsync(runCtx.RunId, "End", "Durable accrual orchestration completed (WO payload).", new
        {
            runCtx.CorrelationId,
            WorkOrdersConsidered = woConsidered,
            WorkOrdersValid = woValid,
            WorkOrdersInvalid = woInvalid,
            PostFailureGroups = postFailureGroups,
            GeneralErrors = input.GeneralErrors ?? Array.Empty<string>()
        }, ct);

        if (hasAnyErrors)
        {
            try
            {
                var recipients = _notifications.GetRecipients();
                if (recipients.Count == 0)
                {
                    _logger.LogWarning(
                        "ErrorDistributionList is empty. Failure email will not be sent. RunId={RunId} CorrelationId={CorrelationId}",
                        runCtx.RunId, runCtx.CorrelationId);
                }
                else
                {
                    var subject = $"[AIS][Accrual][ERROR] RunId={runCtx.RunId}";
                    var aggregated = ActivitiesUseCase.AggregateForEmail(input.PostResults);
                    var body = ErrorEmailComposer.ComposeHtml(runCtx, aggregated);
                    await _email.SendAsync(subject, body, recipients, ct);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to send failure notification email. RunId={RunId}", runCtx.RunId);
            }
        }

        return new DurableAccrualOrchestration.RunOutcomeDto(
            RunId: runCtx.RunId,
            CorrelationId: runCtx.CorrelationId,
            WorkOrdersConsidered: woConsidered,
            WorkOrdersValid: woValid,
            WorkOrdersInvalid: woInvalid,
            PostFailureGroups: postFailureGroups,
            HasAnyErrors: hasAnyErrors,
            GeneralErrors: (input.GeneralErrors ?? Array.Empty<string>()).ToList());
    }
}
