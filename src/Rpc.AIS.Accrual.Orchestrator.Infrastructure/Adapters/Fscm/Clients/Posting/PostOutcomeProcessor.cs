using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;

namespace Rpc.AIS.Accrual.Orchestrator.Infrastructure.Clients.Posting;

/// <summary>
/// Default <see cref="IPostOutcomeProcessor"/>.
/// Owns posting execution (HTTP) and mapping into <see cref="PostResult"/> including handlers.
/// </summary>
public sealed class PostOutcomeProcessor : IPostOutcomeProcessor
{
    private static readonly HashSet<string> SkipJournalPostingTriggers = new(StringComparer.OrdinalIgnoreCase)
    {
        "Timer",
        "AdHocSingle",
        "AdHocAll",
        "AdHocBulk"
    };

    private readonly IFscmJournalPoster _poster;
    private readonly IFscmPostingResponseParser _parser;
    private readonly IPostErrorAggregator _errorAgg;
    private readonly IReadOnlyList<IPostResultHandler> _handlers;
    private readonly ILogger<PostOutcomeProcessor> _logger;

    public PostOutcomeProcessor(
        IFscmJournalPoster poster,
        IFscmPostingResponseParser parser,
        IPostErrorAggregator errorAgg,
        IEnumerable<IPostResultHandler> handlers,
        ILogger<PostOutcomeProcessor> logger)
    {
        _poster = poster ?? throw new ArgumentNullException(nameof(poster));
        _parser = parser ?? throw new ArgumentNullException(nameof(parser));
        _errorAgg = errorAgg ?? throw new ArgumentNullException(nameof(errorAgg));
        _handlers = (handlers ?? Array.Empty<IPostResultHandler>()).ToList();
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<PostResult> PostAndProcessAsync(RunContext ctx, PreparedWoPosting prepared, CancellationToken ct)
    {
        if (ctx is null) throw new ArgumentNullException(nameof(ctx));
        if (prepared is null) throw new ArgumentNullException(nameof(prepared));

        // 1) Post
        var outcome = await _poster.PostAsync(ctx, prepared.JournalType, prepared.ProjectedJournalPayloadJson, ct).ConfigureAwait(false);

        // 2) Start from validation/pre errors + section pruning
        var errors = _errorAgg.Build(prepared.PreErrors, prepared.RemovedDueToMissingOrEmptySection, prepared.JournalType);

        // 3) HTTP errors
        if ((int)outcome.StatusCode < 200 || (int)outcome.StatusCode > 299)
        {
            var all = _errorAgg.AddHttpError(errors, outcome);
            var fail = new PostResult(
                prepared.JournalType,
                false,
                null,
                "Posting failed.",
                all,
                WorkOrdersBefore: prepared.WorkOrdersBefore,
                WorkOrdersPosted: 0,
                WorkOrdersFiltered: prepared.WorkOrdersBefore - prepared.WorkOrdersAfter,
                ValidationResponseRaw: prepared.ValidationResponseRaw,
                RetryableWorkOrders: prepared.RetryableWorkOrders,
                RetryableLines: prepared.RetryableLines,
                RetryablePayloadJson: prepared.RetryablePayloadJson);

            await RunHandlersAsync(ctx, fail, ct).ConfigureAwait(false);
            return fail;
        }

        // 4) Parse success response
        var parsed = _parser.Parse(outcome.Body ?? string.Empty);
        if (!parsed.Ok)
        {
            var all = _errorAgg.AddParseErrors(errors, parsed.ParseErrors);
            var fail = new PostResult(
                prepared.JournalType,
                false,
                null,
                parsed.Message,
                all,
                WorkOrdersBefore: prepared.WorkOrdersBefore,
                WorkOrdersPosted: 0,
                WorkOrdersFiltered: prepared.WorkOrdersBefore - prepared.WorkOrdersAfter,
                ValidationResponseRaw: prepared.ValidationResponseRaw,
                RetryableWorkOrders: prepared.RetryableWorkOrders,
                RetryableLines: prepared.RetryableLines,
                RetryablePayloadJson: prepared.RetryablePayloadJson);

            await RunHandlersAsync(ctx, fail, ct).ConfigureAwait(false);
            return fail;
        }

        var skipPosting = !string.IsNullOrWhiteSpace(ctx.TriggeredBy) && SkipJournalPostingTriggers.Contains(ctx.TriggeredBy!);
        var okMessage = skipPosting
            ? "Create completed; journal posting skipped by trigger policy."
            : parsed.Message;

        var ok = new PostResult(
            prepared.JournalType,
            true,
            parsed.JournalId,
            okMessage,
            errors,
            WorkOrdersBefore: prepared.WorkOrdersBefore,
            WorkOrdersPosted: skipPosting ? 0 : prepared.WorkOrdersAfter,
            WorkOrdersFiltered: prepared.WorkOrdersBefore - prepared.WorkOrdersAfter,
            ValidationResponseRaw: prepared.ValidationResponseRaw,
            RetryableWorkOrders: prepared.RetryableWorkOrders,
            RetryableLines: prepared.RetryableLines,
            RetryablePayloadJson: prepared.RetryablePayloadJson);

        await RunHandlersAsync(ctx, ok, ct).ConfigureAwait(false);
        return ok;
    }

    private async Task RunHandlersAsync(RunContext ctx, PostResult result, CancellationToken ct)
    {
        foreach (var h in _handlers)
        {
            try
            {
                if (h.CanHandle(result))
                    await h.HandleAsync(ctx, result, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "PostResultHandler failed. Handler={Handler} JournalType={JournalType} RunId={RunId}",
                    h.GetType().Name, result.JournalType, ctx.RunId);
            }
        }
    }
}
