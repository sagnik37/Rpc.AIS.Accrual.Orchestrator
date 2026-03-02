using System;
using System.Collections.Generic;

using Microsoft.Extensions.Logging;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;

namespace Rpc.AIS.Accrual.Orchestrator.Functions.Services.Handlers;

public abstract class ActivitiesHandlerBase
{
    private static readonly IDisposable NoopScope = new NoopDisposable();
    private sealed class NoopDisposable : IDisposable { public void Dispose() { } }

    protected static IDisposable BeginScope(ILogger logger, RunContext ctx, string activityName, string? durableInstanceId)
        => logger.BeginScope(new Dictionary<string, object?>
        {
            ["RunId"] = ctx.RunId,
            ["CorrelationId"] = ctx.CorrelationId,
            ["DurableInstanceId"] = durableInstanceId,
            ["Activity"] = activityName
        }) ?? NoopScope;
}
