namespace Rpc.AIS.Accrual.Orchestrator.Core.Domain;

/// <summary>
/// Carries status change request data.
/// </summary>
public sealed record StatusChangeRequest(
    string EntityName,
    string RecordId,
    string OldStatus,
    string NewStatus,
    string? Message,
    string? RunId,
    string? CorrelationId,
    object? Payload);
