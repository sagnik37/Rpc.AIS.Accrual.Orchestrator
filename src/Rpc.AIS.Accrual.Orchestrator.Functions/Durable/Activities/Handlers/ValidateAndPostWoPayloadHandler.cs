using Microsoft.Extensions.Logging;

using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Functions.Functions;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Options;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using System.Text;
using System.Diagnostics;

namespace Rpc.AIS.Accrual.Orchestrator.Functions.Services.Handlers;

public sealed class ValidateAndPostWoPayloadHandler : ActivitiesHandlerBase
{
    private readonly IPostingClient _posting;
    private readonly FscmODataStagingOptions _odataOpt;
    private readonly IAisLogger _ais;
    private readonly ILogger<ValidateAndPostWoPayloadHandler> _logger;

    public ValidateAndPostWoPayloadHandler(
        IPostingClient posting,
        FscmODataStagingOptions odataOpt,
        IAisLogger ais,
        ILogger<ValidateAndPostWoPayloadHandler> logger)
    {
        _posting = posting ?? throw new ArgumentNullException(nameof(posting));
        _odataOpt = odataOpt ?? throw new ArgumentNullException(nameof(odataOpt));
        _ais = ais ?? throw new ArgumentNullException(nameof(ais));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<List<PostResult>> HandleAsync(
        DurableAccrualOrchestration.WoPayloadPostingInputDto input,
        RunContext runCtx,
        CancellationToken ct)
    {
        using var scope = BeginScope(_logger, runCtx, "ValidateAndPostWoPayload", input.DurableInstanceId);

        var woPayloadJson = input.WoPayloadJson ?? string.Empty;

        _logger.LogInformation(
            "Activity ValidateAndPostWoPayload. RunId={RunId} CorrelationId={CorrelationId} PayloadBytes={Bytes}",
            input.RunId, input.CorrelationId, Encoding.UTF8.GetByteCount(woPayloadJson));

        try
        {
            var sw = Stopwatch.StartNew();

            var mode = _odataOpt.Enabled ? "ODataStaging" : "JournalAsync";
            _logger.LogInformation(
                "Activity ValidateAndPostWoPayload: Begin single-validation. Mode={Mode} RunId={RunId} CorrelationId={CorrelationId}",
                mode, runCtx.RunId, runCtx.CorrelationId);

            var results = await _posting.ValidateOnceAndPostAllJournalTypesAsync(runCtx, woPayloadJson, ct);

            sw.Stop();

            var safeResults = results ?? new List<PostResult>();
            var failures = safeResults.Count(r => !r.IsSuccess);

            _logger.LogInformation(
                "Activity ValidateAndPostWoPayload: End single-validation. Mode={Mode} ElapsedMs={ElapsedMs} FailureGroups={FailureGroups} RunId={RunId} CorrelationId={CorrelationId}",
                mode, sw.ElapsedMilliseconds, failures, runCtx.RunId, runCtx.CorrelationId);

            return safeResults;
        }
        catch (Exception ex)
        {
            var err = new PostError(
                Code: "POST_EXCEPTION",
                Message: $"Posting exception (single-validation): {ex.Message}",
                StagingId: null,
                JournalId: null,
                JournalDeleted: false,
                DeleteMessage: ex.ToString());

            return new List<PostResult>
            {
                new(JournalType.Item, false, null, "Posting threw exception (single-validation).", new[] { err }),
                new(JournalType.Expense, false, null, "Posting threw exception (single-validation).", new[] { err }),
                new(JournalType.Hour, false, null, "Posting threw exception (single-validation).", new[] { err })
            };
        }
    }
}
