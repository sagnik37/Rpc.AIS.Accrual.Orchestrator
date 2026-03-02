using System;
using System.Threading;
using System.Threading.Tasks;

namespace Rpc.AIS.Accrual.Orchestrator.Core.Domain.Delta;

/// <summary>
/// Represents the accounting period context required for reversal dating rules.
/// </summary>
public sealed record AccountingPeriodSnapshot
(
    DateTime CurrentOpenPeriodStartDate,
    string ClosedReversalDateStrategy,
    DateTime SnapshotMinDate,
    DateTime SnapshotMaxDate,
    Func<DateTime, CancellationToken, ValueTask<bool>> IsDateInClosedPeriodAsync,
    Func<DateTime, CancellationToken, ValueTask<DateTime>> ResolveTransactionDateUtcAsync
)
{
    public bool IsDateInClosedPeriod(DateTime dateUtc)
        => IsDateInClosedPeriodAsync(dateUtc, CancellationToken.None).AsTask().GetAwaiter().GetResult();

    public DateTime ResolveTransactionDateUtc(DateTime opsDateUtc)
        => ResolveTransactionDateUtcAsync(opsDateUtc, CancellationToken.None).AsTask().GetAwaiter().GetResult();
}
