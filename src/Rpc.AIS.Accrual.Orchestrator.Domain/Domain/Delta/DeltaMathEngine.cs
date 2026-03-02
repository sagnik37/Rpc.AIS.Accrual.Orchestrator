// File: .../Core/Domain/Delta/DeltaMathEngine.cs
// Extracted orchestration logic from DeltaCalculationEngine.cs to improve SRP.

namespace Rpc.AIS.Accrual.Orchestrator.Core.Domain.Delta;

using System;
using System.Threading;
using System.Threading.Tasks;

internal static class DeltaMathEngine
{
    internal static async Task<DeltaCalculationResult> CalculateAsync(
            JournalReversalPlanner reversalPlanner,

            FsaWorkOrderLineSnapshot fsa,
            FscmWorkOrderLineAggregation? fscmAgg,
            AccountingPeriodSnapshot period,
            DateTime today,
            CancellationToken ct,
            string? reasonPrefix = null)
    {
        if (fsa is null) throw new ArgumentNullException(nameof(fsa));
        if (period is null) throw new ArgumentNullException(nameof(period));

        var reasonHead = string.IsNullOrWhiteSpace(reasonPrefix) ? "Delta" : reasonPrefix.Trim();
        var woId = fsa.WorkOrderId;
        var lineId = fsa.WorkOrderLineId;
        var opsDate = (fsa.OperationsDateUtc?.Date) ?? today.Date;
        var txDate = await period.ResolveTransactionDateUtcAsync(opsDate, ct).ConfigureAwait(false);

        var fscmTotalQty = fscmAgg?.TotalQuantity ?? 0m;

        // Business rule (OR):
        // Reverse+Recreate when:
        //   - any of the 5 fields changed (Dept/Warehouse/CalcUnitPrice/ProductLine/LineProperty)
        //     where CalcUnitPrice counts ONLY when explicitly supplied (Intent-2), OR
        //   - line is inactive AND ops date is in a CLOSED period.
        var hasFieldChange = DeltaEdgeCaseRules.RequiresReversalDueToFieldChange(fsa, fscmAgg);
        var inactiveAndClosed = !fsa.IsActive && period.IsDateInClosedPeriod(opsDate);
        var shouldReverseAndRecreate = hasFieldChange || inactiveAndClosed;

        // -------------------- 1) Inactive but NOT (inactive+closed) => reverse all history only --------------------
        if (!fsa.IsActive && !inactiveAndClosed)
        {
            if (fscmAgg is null || fscmAgg.TotalQuantity == 0m)
            {
                return new DeltaCalculationResult(
                    woId, lineId,
                    DeltaDecision.NoChange,
                    Array.Empty<DeltaPlannedLine>(),
                    $"{reasonHead}: LineInactive but no FSCM history");
            }

            if (DeltaEdgeCaseRules.HasReversalInEffectiveReversalPeriod(fscmAgg, period, opsDate))
            {
                return new DeltaCalculationResult(
                    woId, lineId,
                    DeltaDecision.NoChange,
                    Array.Empty<DeltaPlannedLine>(),
                    $"{reasonHead}: LineInactive but reversal already exists in effective period => NoChange");
            }

            var plan = await JournalReversalPlanner.PlanFullReversalAsync(
                workOrderLineId: lineId,
                dateBuckets: fscmAgg.DateBuckets,
                period: period,
                openPeriodReversalDate: txDate.Date,
                reason: $"{reasonHead}: LineInactive",
                ct: ct).ConfigureAwait(false);

            var lines = DeltaBucketBuilder.BuildReversalLinesOnly(fsa, fscmAgg, plan);

            return new DeltaCalculationResult(
                woId, lineId,
                DeltaDecision.ReverseOnly,
                lines,
                $"{reasonHead}: LineInactive => ReverseOnly (TotalQty={plan.TotalQuantityToReverse})");
        }

        // -------------------- 2) Field change OR (inactive+closed) => reverse + recreate --------------------
        if (shouldReverseAndRecreate)
        {
            // No history => just create current FSA qty
            if (fscmAgg is null || fscmAgg.TotalQuantity == 0m)
            {
                // If the line is inactive and there is no history, there's nothing to reverse.
                if (!fsa.IsActive)
                {
                    return new DeltaCalculationResult(
                        woId, lineId,
                        DeltaDecision.NoChange,
                        Array.Empty<DeltaPlannedLine>(),
                        $"{reasonHead}: ReverseAndRecreate condition met but no FSCM history and line inactive => NoChange");
                }

                var create = DeltaBucketBuilder.BuildPositiveLine(
                    fsa: fsa,
                    transactionDate: txDate.Date,
                    quantity: fsa.Quantity,
                    isReversal: false,
                    fromClosedSplit: false,
                    lineReason: $"{reasonHead}: ReverseAndRecreate condition met but no FSCM history => Create");

                return new DeltaCalculationResult(
                    woId, lineId,
                    DeltaDecision.ReverseAndRecreate,
                    new[] { create },
                    $"{reasonHead}: ReverseAndRecreate => Create only (no history)");
            }

            // Idempotency guard: if a reversal already exists in effective period, do not reverse again.
            // With FieldChange, the safest move is to post ONLY the remaining quantity delta (if any) using current FSA attributes.
            if (DeltaEdgeCaseRules.HasReversalInEffectiveReversalPeriod(fscmAgg, period, opsDate))
            {
                var deltaQtyExisting = fsa.Quantity - fscmTotalQty;
                if (deltaQtyExisting == 0m)
                {
                    return new DeltaCalculationResult(
                        woId, lineId,
                        DeltaDecision.NoChange,
                        Array.Empty<DeltaPlannedLine>(),
                        $"{reasonHead}: ReverseAndRecreate but reversal already exists in effective period => NoChange");
                }

                var deltaExisting = DeltaBucketBuilder.BuildPositiveLine(
                    fsa: fsa,
                    transactionDate: txDate.Date,
                    quantity: deltaQtyExisting,
                    isReversal: false,
                    fromClosedSplit: false,
                    lineReason: $"{reasonHead}: QuantityDelta (idempotency-guard) (FSA={fsa.Quantity}, FSCM={fscmTotalQty})");

                return new DeltaCalculationResult(
                    woId, lineId,
                    DeltaDecision.QuantityDelta,
                    new[] { deltaExisting },
                    $"{reasonHead}: ReverseAndRecreate idempotency-guard => QuantityDelta Qty={deltaQtyExisting}");
            }

            var plan = await JournalReversalPlanner.PlanFullReversalAsync(
                workOrderLineId: lineId,
                dateBuckets: fscmAgg.DateBuckets,
                period: period,
                openPeriodReversalDate: txDate.Date,
                reason: $"{reasonHead}: ReverseAndRecreate",
                ct: ct).ConfigureAwait(false);

            var outLines = new List<DeltaPlannedLine>(capacity: plan.Reversals.Count + 1);

            // Reversal lines MUST use FSCM history attributes (old values)
            foreach (var r in plan.Reversals)
            {
                outLines.Add(DeltaBucketBuilder.BuildReversalLine(
                    fsa: fsa,
                    fscmAgg: fscmAgg,
                    transactionDate: txDate.Date,
                    quantity: r.Quantity,
                    fromClosedSplit: r.FromClosedPeriod,
                    lineReason: r.Reason));
            }

            // Recreate MUST reflect CURRENT FSA quantity with UPDATED attributes
            if (fsa.Quantity != 0m)
            {
                outLines.Add(DeltaBucketBuilder.BuildPositiveLine(
                    fsa: fsa,
                    transactionDate: txDate.Date,
                    quantity: fsa.Quantity,
                    isReversal: false,
                    fromClosedSplit: false,
                    lineReason: $"{reasonHead}: RecreateWithUpdatedAttributes (FSAQty={fsa.Quantity})"));
            }

            return new DeltaCalculationResult(
                woId, lineId,
                DeltaDecision.ReverseAndRecreate,
                outLines,
                $"{reasonHead}: ReverseAndRecreate (FieldChange={hasFieldChange}, InactiveClosed={inactiveAndClosed}) => ReversalLines={plan.Reversals.Count}, Recreate={(fsa.Quantity != 0m ? "Yes" : "No")}");
        }

        // -------------------- 3) Quantity delta only --------------------
        var deltaQty = fsa.Quantity - fscmTotalQty;
        if (deltaQty == 0m)
        {
            return new DeltaCalculationResult(
                woId, lineId,
                DeltaDecision.NoChange,
                Array.Empty<DeltaPlannedLine>(),
                $"{reasonHead}: NoChange (Qty equal)");
        }

        var deltaLine = DeltaBucketBuilder.BuildPositiveLine(
            fsa: fsa,
            transactionDate: txDate.Date,
            quantity: deltaQty,
            isReversal: false,
            fromClosedSplit: false,
            lineReason: $"{reasonHead}: QuantityDelta (FSA={fsa.Quantity}, FSCM={fscmTotalQty})");

        return new DeltaCalculationResult(
            woId, lineId,
            DeltaDecision.QuantityDelta,
            new[] { deltaLine },
            $"{reasonHead}: QuantityDelta => Qty={deltaQty}");
    }
}
