using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain.Validation;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Options;

namespace Rpc.AIS.Accrual.Orchestrator.Infrastructure.Notifications;

/// <summary>
/// Sends AIS-side validation failures for delta payloads to a DL.
/// This is invoked BEFORE any posting attempt so that invalid records are never sent to FSCM.
/// </summary>
public sealed class InvalidPayloadEmailNotifier : IInvalidPayloadNotifier
{
    private readonly IEmailSender _email;
    private readonly NotificationOptions _notifications;
    private readonly ILogger<InvalidPayloadEmailNotifier> _logger;

    public InvalidPayloadEmailNotifier(IEmailSender email, NotificationOptions notifications, ILogger<InvalidPayloadEmailNotifier> logger)
    {
        _email = email ?? throw new ArgumentNullException(nameof(email));
        _notifications = notifications ?? throw new ArgumentNullException(nameof(notifications));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task NotifyAsync(
        RunContext context,
        JournalType journalType,
        IReadOnlyList<WoPayloadValidationFailure> failures,
        int workOrdersBefore,
        int workOrdersAfter,
        CancellationToken ct)
    {
        if (context is null) throw new ArgumentNullException(nameof(context));
        if (failures is null || failures.Count == 0) return;

        var to = _notifications.GetInvalidPayloadRecipients();
        if (to.Count == 0)
        {
            _logger.LogWarning(
                "Invalid payload failures exist but no DL configured. RunId={RunId} CorrelationId={CorrelationId} JournalType={JournalType} FailureCount={FailureCount}",
                context.RunId, context.CorrelationId, journalType, failures.Count);
            return;
        }

        var subject = $"AIS | INVALID Delta Payload | {journalType} | RunId={context.RunId}";
        var html = BuildHtmlBody(context, journalType, failures, workOrdersBefore, workOrdersAfter);

        _logger.LogWarning(
            "Sending invalid payload email. RunId={RunId} CorrelationId={CorrelationId} JournalType={JournalType} To={To} FailureCount={FailureCount} WorkOrdersBefore={Before} WorkOrdersAfter={After}",
            context.RunId, context.CorrelationId, journalType, string.Join(";", to), failures.Count, workOrdersBefore, workOrdersAfter);

        await _email.SendAsync(subject, html, to, ct).ConfigureAwait(false);
    }

    private static string BuildHtmlBody(
        RunContext context,
        JournalType journalType,
        IReadOnlyList<WoPayloadValidationFailure> failures,
        int workOrdersBefore,
        int workOrdersAfter)
    {
        static string E(string? s) => WebUtility.HtmlEncode(s ?? string.Empty);

        var grouped = failures
            .GroupBy(f => f.WorkOrderGuid)
            .OrderBy(g => g.Key.ToString());

        var sb = new StringBuilder();
        sb.AppendLine("<html><body style='font-family:Segoe UI,Arial,sans-serif'>");
        sb.AppendLine($"<h2>AIS Delta Payload Validation Failures</h2>");
        sb.AppendLine($"<p><b>RunId:</b> {E(context.RunId)}<br/>");
        sb.AppendLine($"<b>CorrelationId:</b> {E(context.CorrelationId)}<br/>");
        sb.AppendLine($"<b>JournalType:</b> {E(journalType.ToString())}<br/>");
        sb.AppendLine($"<b>WorkOrdersBefore:</b> {workOrdersBefore} &nbsp; <b>WorkOrdersAfter:</b> {workOrdersAfter}<br/>");
        sb.AppendLine($"<b>FailureCount:</b> {failures.Count}</p>");

        sb.AppendLine("<table border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse'>");
        sb.AppendLine("<thead><tr><th>WorkOrderGuid</th><th>WorkOrderNumber</th><th>WorkOrderLineGuid</th><th>Code</th><th>Message</th></tr></thead>");
        sb.AppendLine("<tbody>");

        foreach (var f in failures)
        {
            sb.AppendLine("<tr>");
            sb.AppendLine($"<td>{E(f.WorkOrderGuid == Guid.Empty ? "(missing)" : f.WorkOrderGuid.ToString())}</td>");
            sb.AppendLine($"<td>{E(f.WorkOrderNumber)}</td>");
            sb.AppendLine($"<td>{E(f.WorkOrderLineGuid?.ToString())}</td>");
            sb.AppendLine($"<td>{E(f.Code)}</td>");
            sb.AppendLine($"<td>{E(f.Message)}</td>");
            sb.AppendLine("</tr>");
        }

        sb.AppendLine("</tbody></table>");
        sb.AppendLine("<p>Action: Fix the source record(s) in Field Service / AIS mapping, then re-run. Invalid records were NOT sent to FSCM.</p>");
        sb.AppendLine("</body></html>");
        return sb.ToString();
    }
}
