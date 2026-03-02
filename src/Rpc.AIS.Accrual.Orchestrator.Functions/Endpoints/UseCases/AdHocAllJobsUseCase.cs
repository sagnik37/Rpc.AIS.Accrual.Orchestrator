using System;
using System.Threading.Tasks;

using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;

using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Services;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Logging;

namespace Rpc.AIS.Accrual.Orchestrator.Functions.Functions;

/// <summary>
/// AdHoc Batch - All Jobs use case.
/// Fully extracted (no shared endpoint handler dependency).
/// </summary>
public sealed class AdHocAllJobsUseCase : JobOperationsUseCaseBase, IAdHocAllJobsUseCase
{
    public AdHocAllJobsUseCase(
        ILogger<AdHocAllJobsUseCase> log,
        IAisLogger aisLogger,
        IAisDiagnosticsOptions diag)
        : base(log, aisLogger, diag)
    {
    }

    public async Task<HttpResponseData> ExecuteAsync(HttpRequestData req, DurableTaskClient client, FunctionContext ctx)
    {
                var (runId, correlationId, sourceSystem) = ReadContext(req);

                using var scope = LogScopes.BeginFunctionScope(_log, new LogScopeContext
                {
                    Function = "AdHocBatch_AllJobs",
                    Operation = "AdHocBatch_AllJobs",
                    Trigger = "Http",
                    RunId = runId,
                    CorrelationId = correlationId,
                    SourceSystem = sourceSystem
                });

                var instanceId = $"{runId}-adhoc-all";

                // Ensure Step + DurableInstanceId land in customDimensions (scope), not only in message text.
                using var scheduleScope = LogScopes.BeginFunctionScope(_log, new LogScopeContext
                {
                    Function = "AdHocBatch_AllJobs",
                    Operation = "AdHocBatch_AllJobs",
                    Trigger = "Http",
                    Step = "ScheduleDurableOrchestrator",
                    RunId = runId,
                    CorrelationId = correlationId,
                    SourceSystem = sourceSystem,
                    DurableInstanceId = instanceId
                });

                var input = new DurableAccrualOrchestration.RunInputDto(
                    RunId: runId,
                    CorrelationId: correlationId,
                    TriggeredBy: "AdHocAll",
                    SourceSystem: sourceSystem,
                    WorkOrderGuid: null);

                await client.ScheduleNewOrchestrationInstanceAsync(
                    nameof(DurableAccrualOrchestration.AccrualOrchestrator),
                    input,
                    new StartOrchestrationOptions { InstanceId = instanceId },
                    ctx.CancellationToken);

                _log.LogInformation("Scheduled orchestration. Orchestrator={Orchestrator} InstanceId={InstanceId}", nameof(DurableAccrualOrchestration.AccrualOrchestrator), instanceId);

                return await AcceptedAsync(req, correlationId, runId, new
                {
                    instanceId,
                    runId,
                    correlationId,
                    sourceSystem,
                    trigger = "AdHocAll"
                });

    }
}
