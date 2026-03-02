using System;

using Rpc.AIS.Accrual.Orchestrator.Core.Domain;

namespace Rpc.AIS.Accrual.Orchestrator.Infrastructure.Logging;

/// <summary>
/// Normalized logging scope fields for all Functions/Activities/Clients.
/// Keep it small and stable; add fields only when they are broadly useful.
/// </summary>
public readonly record struct LogScopeContext
{
    // Required-ish (most scopes have these)
    public string? Function { get; init; }
    public string? Activity { get; init; }
    public string? Operation { get; init; }   // e.g., "PostJob", "CustomerChange"
    public string? Trigger { get; init; }     // e.g., "Timer", "AdHocAll"

    // Optional pipeline step name (e.g., "ScheduleDurableOrchestrator", "FetchFromFsa", "ValidateAndPost").
    public string? Step { get; init; }

    public string? RunId { get; init; }
    public string? CorrelationId { get; init; }
    public string? SourceSystem { get; init; }

    // Optional identifiers
    public Guid? WorkOrderGuid { get; init; }
    public string? WorkOrderId { get; init; }
    public string? SubProjectId { get; init; }
    public string? DurableInstanceId { get; init; }

    // Optional domain signal
    public JournalType? JournalType { get; init; }


    // Convenience factories
    public static LogScopeContext ForHttp(string function, string runId, string correlationId, string sourceSystem) =>
        new()
        {
            Function = function,
            Operation = function,
            Trigger = "Http",
            RunId = runId,
            CorrelationId = correlationId,
            SourceSystem = sourceSystem
        };

    public static LogScopeContext ForTimer(string function, string runId, string correlationId, string mode) =>
        new()
        {
            Function = function,
            Operation = function,
            Trigger = "Timer",
            RunId = runId,
            CorrelationId = correlationId,
            SourceSystem = "AIS"
        };

    public LogScopeContext WithWorkOrder(Guid woGuid) => this with { WorkOrderGuid = woGuid };
    public LogScopeContext WithSubProject(string subProjectId) => this with { SubProjectId = subProjectId };
    public LogScopeContext WithDurableInstance(string instanceId) => this with { DurableInstanceId = instanceId };
    public LogScopeContext WithStep(string step) => this with { Step = step };
    public LogScopeContext WithJournal(JournalType jt) => this with { JournalType = jt };
}
