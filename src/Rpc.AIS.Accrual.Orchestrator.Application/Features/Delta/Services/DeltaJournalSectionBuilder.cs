// File: Rpc.AIS.Accrual.Orchestrator/src/Rpc.AIS.Accrual.Orchestrator.Core/Services/DeltaJournalSectionBuilder.cs

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain.Delta;
using Rpc.AIS.Accrual.Orchestrator.Core.Utilities;

namespace Rpc.AIS.Accrual.Orchestrator.Core.Services;

internal sealed class DeltaJournalSectionBuilder
{
    private readonly DeltaCalculationEngine _deltaEngine;
    private readonly IAisLogger _aisLogger;

    internal DeltaJournalSectionBuilder(DeltaCalculationEngine deltaEngine, IAisLogger aisLogger)
    {
        _deltaEngine = deltaEngine ?? throw new ArgumentNullException(nameof(deltaEngine));
        _aisLogger = aisLogger ?? throw new ArgumentNullException(nameof(aisLogger));
    }

    internal async Task<JsonObject?> BuildAsync(
        RunContext context,
        JsonObject inputWo,
        string[] journalKeyCandidates,
        string qtyKey,
        JournalType jt,
        Guid woGuid,
        string? woNumber,
        IReadOnlyDictionary<Guid, FscmWorkOrderLineAggregation> aggDict,
        AccountingPeriodSnapshot period,
        DateTime todayUtc,
        Func<int> totalDeltaLines,
        Action incDelta,
        Action incReverse,
        Action incRecreate,
        CancellationToken ct)
    {
        var section = FindFirstObjectLoose(inputWo, journalKeyCandidates);
        if (section is null) return null;

        var journalLines = FindJournalLinesArrayLoose(section);
        if (journalLines is null || journalLines.Count == 0) return null;

        var outLines = new JsonArray();

        foreach (var ln in journalLines)
        {
            if (ln is not JsonObject lineObj) continue;

            var lineId = GetWorkOrderLineGuid(lineObj);
            if (lineId == Guid.Empty) continue;

            var fsaQty = GetDecimalLoose(lineObj, qtyKey) ?? 0m;

            // Intent-2 (price drift counts only when explicitly supplied):
            // - Do NOT treat UnitCost/UnitAmount as an explicit price signal (those often drift due to upstream calc).
            // - Treat FSAUnitPrice as the explicit signal.
            // - Additionally, treat 0 as "not explicitly supplied" to avoid false reversals.
            var hasFsaUnitPriceNode = HasAnyNodeLoose(lineObj, Keys.FsaUnitPrice, "FSAUnitPrice", "FsaUnitPrice", "fsaunitprice");
            var fsaExplicitUnitPrice = GetDecimalLooseAny(lineObj, "FSAUnitPrice", "FsaUnitPrice");
            var unitPriceProvided = fsaExplicitUnitPrice.HasValue && fsaExplicitUnitPrice.Value != 0m;
            var fsaUnitCost = unitPriceProvided ? fsaExplicitUnitPrice : null;

            var linePropertyProvided = HasAnyNodeLoose(lineObj, Keys.LineProperty, "Line property", "LineProperty");
            var fsaLineProperty =
                GetStringLooseAny(lineObj, Keys.LineProperty, "Line property", "LineProperty");

            var deptProvided = HasAnyNodeLoose(lineObj, Keys.DimDepartment, "Dimension department", "DimensionDepartment");
            var fsaDept =
                GetStringLooseAny(lineObj, Keys.DimDepartment, "Dimension department", "DimensionDepartment");

            var prodProvided = HasAnyNodeLoose(lineObj, Keys.DimProduct, "Dimension product", "DimensionProduct");
            var fsaProd =
                GetStringLooseAny(lineObj, Keys.DimProduct, "Dimension product", "DimensionProduct");

            // If FSA provided DimensionDisplayValue (as in  real payloads),
            // treat it as an explicit Department/ProductLine signal.
            if (!deptProvided || !prodProvided)
            {
                if (JsonLooseKey.TryGetStringLoose(lineObj, "DimensionDisplayValue", out var ddv) &&
                    TryParseDeptProdFromDimensionDisplayValue(ddv, out var ddDept, out var ddProd))
                {
                    if (!deptProvided)
                    {
                        fsaDept = ddDept;
                        deptProvided = true;
                    }

                    if (!prodProvided)
                    {
                        fsaProd = ddProd;
                        prodProvided = true;
                    }
                }
            }

            var warehouseProvided = HasAnyNodeLoose(lineObj, Keys.Warehouse, "Warehouse");
            var fsaWarehouse = GetStringLooseAny(lineObj, Keys.Warehouse, "Warehouse");

            // IMPORTANT: Warehouse is a reversal-triggering dimension ONLY for Item.
            // For Expense/Hour, ignore it even if FS sent it.
            // (We are still in the FSA snapshot-building stage; no "planned" line exists yet.)
            if (jt != JournalType.Item)
            {
                warehouseProvided = false;
                fsaWarehouse = null;
            }

            var opsDateUtc = ResolveOperationsDateUtc(lineObj, todayUtc);

            aggDict.TryGetValue(lineId, out var fscmAgg);

            // If FSA sends a partial update (e.g., only Quantity), treat missing attributes as "unchanged"
            // by falling back to FSCM's current effective attributes for delta evaluation + payload stamping.
            if (fscmAgg is not null && fscmAgg.DimensionBuckets is { Count: > 0 })
            {
                var b = fscmAgg.DimensionBuckets[0];

                if (!deptProvided) fsaDept = b.Department;
                if (!prodProvided) fsaProd = b.ProductLine;
                if (jt == JournalType.Item && !warehouseProvided)
                    fsaWarehouse = b.Warehouse;
                if (!linePropertyProvided) fsaLineProperty = b.LineProperty;

                if (!unitPriceProvided)
                    fsaUnitCost = b.CalculatedUnitPrice ?? fscmAgg.EffectiveUnitPrice;
                else
                    fsaUnitCost = fsaExplicitUnitPrice; // explicit always wins
            }

            var fsa = new FsaWorkOrderLineSnapshot(
                WorkOrderId: woGuid,
                WorkOrderLineId: lineId,
                JournalType: jt,
                IsActive: GetIsActive(lineObj),
                Quantity: fsaQty,
                CalculatedUnitPrice: fsaUnitCost,
                LineProperty: fsaLineProperty,
                Department: fsaDept,
                ProductLine: fsaProd,
                Warehouse: fsaWarehouse,
                OperationsDateUtc: opsDateUtc,

                DepartmentProvided: deptProvided,
                ProductLineProvided: prodProvided,
                WarehouseProvided: warehouseProvided,
                LinePropertyProvided: linePropertyProvided,
                UnitPriceProvided: unitPriceProvided
            );

            var res = await _deltaEngine.CalculateAsync(
                fsa,
                fscmAgg,
                period,
                todayUtc.Date,
                ct,
                reasonPrefix: $"WO:{jt}");

            await _aisLogger.InfoAsync(
                context.RunId,
                "Delta",
                "Delta decision.",
                new
                {
                    context.CorrelationId,
                    WorkOrderGuid = woGuid,
                    WorkOrderNumber = woNumber,
                    JournalType = jt.ToString(),
                    WorkOrderLineGuid = lineId,
                    Decision = res.Decision.ToString(),
                    Fsa = new
                    {
                        fsa.IsActive,
                        fsa.Quantity,
                        fsa.CalculatedUnitPrice,
                        fsa.LineProperty,
                        fsa.Department,
                        fsa.ProductLine,
                        fsa.Warehouse
                    },
                    Fscm = fscmAgg,
                    Planned = res.Lines
                },
                ct).ConfigureAwait(false);

            if (res.Decision == DeltaDecision.NoChange || res.Lines.Count == 0)
                continue;

            foreach (var planned in res.Lines)
            {
                JsonObject cloned;

                // REVERSAL lines must be built from FSCM snapshot mapping (Item/Expense only).
                if (planned.IsReversal
                    && (jt == JournalType.Item || jt == JournalType.Expense)
                    && fscmAgg?.RepresentativeSnapshot is not null)
                {
                    cloned = BuildReversalLineFromFscmSnapshot(
                        snap: fscmAgg.RepresentativeSnapshot,
                        jt: jt,
                        plannedQuantity: planned.Quantity);
                }
                else
                {
                    cloned = (JsonObject)lineObj.DeepClone();
                }

                // JournalDescription must exist ONLY at section/header level, never on lines
                RemoveLoose(cloned, "JournalDescription");
                RemoveLoose(cloned, Keys.JournalDescription);

                // NEVER include modifiedon/rpc_accrualfieldmodifiedon in delta payload
                RemoveLoose(cloned, Keys.ModifiedOn);
                RemoveLoose(cloned, Keys.RpcAccrualFieldModifiedOn);

                // PAYLOAD HYGIENE:
                RemoveLoose(cloned, Keys.LineNum);
                RemoveLoose(cloned, "LineNum");
                RemoveLoose(cloned, "LineNumber");

                // Remove section/header-only fields that sometimes leak into lines
                RemoveLoose(cloned, "JournalDescription");
                RemoveLoose(cloned, "JournalName");

                // FSACustomerProductDesc: fallback to JournalLineDescription (present in input payload)
                var desc =
                    GetStringLooseAny(cloned, "FSACustomerProductDesc", "msdyn_description", "JournalLineDescription");
                if (!string.IsNullOrWhiteSpace(desc))
                    SetOrAddLoose(cloned, "FSACustomerProductDesc", desc);

                // FSACustomerProductID: MUST remain an ID (do NOT take line descriptions).
                // Prefer the existing ID fields only.
                var custProdId =
                    GetStringLooseAny(cloned, "FSACustomerProductID", "rpc_customerproductid", "RPCCustomerProductReference");
                if (!string.IsNullOrWhiteSpace(custProdId))
                    SetOrAddLoose(cloned, "FSACustomerProductID", custProdId);

                // Duration must ONLY exist for Hour journals (never for Item/Expense)
                if (jt == JournalType.Item || jt == JournalType.Expense)
                    RemoveLoose(cloned, Keys.Duration);

                // Quantity / Duration override from planned line
                cloned[qtyKey] = planned.Quantity;

                // -----------------------------
                // NEW REQUIREMENT (Ops date driven):
                // - RPCWorkingDate must be original FS rpc_operationsdate (per line).
                // - TransactionDate must initially equal RPCWorkingDate; posting layer will adjust if period is closed.
                // - Both must be FSCM /Date(ms)/ literal format.
                // -----------------------------
                // Determine working/ops date for diagnostics and (non-reversal) stamping.
                // For reversal lines (which may be built from FSCM snapshot), we first try to read
                // OperationDate/TransactionDate already present on the payload line.
                var workingUtc = ResolveWorkingDateUtcFromLineOrFallback(cloned, planned.TransactionDate.Date);

                if (!planned.IsReversal || fscmAgg?.RepresentativeSnapshot is null)
                {
                    // IMPORTANT:
                    // OperationDate MUST reflect the original operations date (FS working date), even if that date
                    // falls in a CLOSED/ON-HOLD period. The posting layer (PayloadPostingDateAdjuster) will resolve
                    // TransactionDate for closed periods.
                    cloned["OperationDate"] = ToFscmDateLiteral(workingUtc.Date);
                    cloned[Keys.TransactionDate] = ToFscmDateLiteral(workingUtc.Date);
                }

                // For diagnostics: classify based on working date (ops date), not planned.TransactionDate.
                var isClosed = period.IsDateInClosedPeriod(workingUtc);
                await _aisLogger.InfoAsync(
                    context.RunId,
                    "Delta",
                    "Accounting period classification (OpsDate-based).",
                    new
                    {
                        context.CorrelationId,
                        WorkOrderGuid = woGuid,
                        WorkOrderNumber = woNumber,
                        JournalType = jt.ToString(),
                        WorkOrderLineGuid = lineId,
                        RPCWorkingDate = workingUtc.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                        IsClosedOrOnHold = isClosed,
                        CurrentOpenPeriodStartDate = period.CurrentOpenPeriodStartDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
                    },
                    ct).ConfigureAwait(false);

                // -----------------------------
                // CRITICAL FIX (Dept/Product updates for Expense/Hour):
                // FSCM consumes DimensionDisplayValue. Ensure we stamp it from the PLANNED dimensions.
                // - Prefer planned.Department/ProductLine.
                // - Fall back to existing DimensionDisplayValue parsing.
                // - Finally fall back to fsa snapshot values (already resolved w/ FSCM fallback earlier).
                // -----------------------------
                var plannedDept = planned.Department;
                var plannedProd = planned.ProductLine;

                if (string.IsNullOrWhiteSpace(plannedDept) || string.IsNullOrWhiteSpace(plannedProd))
                {
                    if (JsonLooseKey.TryGetStringLoose(cloned, "DimensionDisplayValue", out var existingDdv) &&
                        TryParseDeptProdFromDimensionDisplayValue(existingDdv, out var ddDept2, out var ddProd2))
                    {
                        if (string.IsNullOrWhiteSpace(plannedDept)) plannedDept = ddDept2;
                        if (string.IsNullOrWhiteSpace(plannedProd)) plannedProd = ddProd2;
                    }

                    if (string.IsNullOrWhiteSpace(plannedDept)) plannedDept = fsa.Department;
                    if (string.IsNullOrWhiteSpace(plannedProd)) plannedProd = fsa.ProductLine;
                }

                var newDdv = BuildDefaultDimensionDisplayValue(plannedDept, plannedProd);
                if (!string.IsNullOrWhiteSpace(newDdv))
                    SetOrAddLoose(cloned, "DimensionDisplayValue", newDdv);

                // Always stamp LineProperty from planned line
                SetOrAddLoose(cloned, Keys.LineProperty, planned.LineProperty);

                // DO NOT send these to FSCM (validation/postJournal)
                RemoveLoose(cloned, Keys.DimDepartment);
                RemoveLoose(cloned, Keys.DimProduct);

                if (jt == JournalType.Item)
                {
                    // Only Item journals are allowed to carry Warehouse
                    if (!string.IsNullOrWhiteSpace(planned.Warehouse))
                        SetOrAddLoose(cloned, Keys.Warehouse, planned.Warehouse);
                }
                else
                {
                    // Ensure Warehouse never leaks into Expense/Hour payload
                    RemoveLoose(cloned, Keys.Warehouse);
                    RemoveLoose(cloned, "Warehouse");
                }

                // For REVERSALS we must always take amounts from FSCM history (not FS).
                // This ensures we reverse exactly what FSCM has on record.
                if (planned.IsReversal && fscmAgg is not null)
                {
                    var fscmPrice = ResolveFscmUnitPriceForReversal(fscmAgg);
                    if (fscmPrice.HasValue)
                        SetOrAddLoose(cloned, Keys.UnitCost, fscmPrice.Value);
                }
                else if (planned.CalculatedUnitPrice.HasValue)
                {
                    SetOrAddLoose(cloned, Keys.UnitCost, planned.CalculatedUnitPrice.Value);
                }

                ComputeAndSetUnitAmount(cloned, qtyKey);

                outLines.Add(cloned);

                incDelta();
                if (planned.IsReversal) incReverse();
                if (!planned.IsReversal && res.Decision == DeltaDecision.ReverseAndRecreate) incRecreate();
            }
        }

        if (outLines.Count == 0)
            return null;

        var outSection = new JsonObject();

        // 1) JournalDescription FIRST
        CopyIfPresentLoose(section, outSection, "JournalDescription");

        // 2) JournalName SECOND (ensure key exists even if empty)
        CopyIfPresentLoose(section, outSection, "JournalName");
        if (!outSection.ContainsKey("JournalName"))
            outSection["JournalName"] = "";

        // 3) LineType THIRD
        CopyIfPresentLoose(section, outSection, Keys.LineType);

        // 4) JournalLines LAST
        outSection[Keys.JournalLines] = outLines;

        return outSection;
    }

    // -------------------------
    // JSON helpers
    // -------------------------

    private static bool TryGetYesNoFromDataverseBoolean(JsonObject obj, string logicalName, out string yesNo)
    {
        yesNo = "No";

        // If formatted value exists, trust it (e.g. "Yes"/"No")
        if (JsonLooseKey.TryGetStringLoose(obj, logicalName + "@OData.Community.Display.V1.FormattedValue", out var formatted)
            && !string.IsNullOrWhiteSpace(formatted))
        {
            yesNo = formatted.Trim();
            return true;
        }

        if (!JsonLooseKey.TryGetNodeLoose(obj, logicalName, out var node) || node is null)
            return false;

        if (node is JsonValue v)
        {
            if (v.TryGetValue<bool>(out var b))
            {
                yesNo = b ? "Yes" : "No";
                return true;
            }

            if (v.TryGetValue<int>(out var i))
            {
                yesNo = i != 0 ? "Yes" : "No";
                return true;
            }

            if (v.TryGetValue<string>(out var s) && !string.IsNullOrWhiteSpace(s))
            {
                if (bool.TryParse(s, out var bb))
                {
                    yesNo = bb ? "Yes" : "No";
                    return true;
                }
                if (int.TryParse(s, out var ii))
                {
                    yesNo = ii != 0 ? "Yes" : "No";
                    return true;
                }
            }
        }

        return false;
    }

    private static decimal? ResolveFscmUnitPriceForReversal(FscmWorkOrderLineAggregation fscmAgg)
    {
        if (fscmAgg is null) return null;

        // Prefer effective unit price if aggregator provides it.
        if (fscmAgg.EffectiveUnitPrice.HasValue)
            return Math.Abs(fscmAgg.EffectiveUnitPrice.Value);

        // Fall back to extended amount / quantity when available.
        if (fscmAgg.TotalExtendedAmount.HasValue && fscmAgg.TotalQuantity != 0)
        {
            var price = fscmAgg.TotalExtendedAmount.Value / fscmAgg.TotalQuantity;
            return Math.Abs(price);
        }

        // As a last resort, try the first dimension bucket price.
        if (fscmAgg.DimensionBuckets is { Count: > 0 })
        {
            var p = fscmAgg.DimensionBuckets[0].CalculatedUnitPrice;
            if (p.HasValue) return Math.Abs(p.Value);
        }

        return null;
    }

    private static class Keys
    {
        public const string JournalLines = "JournalLines";
        public const string JournalDescription = "JournalDescription";
        public const string WorkOrderLineGuid = "WorkOrderLineGuid";

        public const string Quantity = "Quantity";
        public const string Duration = "Duration";

        public const string UnitCost = "UnitCost";
        public const string UnitAmount = "UnitAmount";

        // Explicit unit price signal from FSA (Intent-2). Do NOT infer explicitness from UnitCost/UnitAmount.
        public const string FsaUnitPrice = "FSAUnitPrice";

        public const string LineProperty = "LineProperty";
        public const string Warehouse = "Warehouse";
        public const string DimDepartment = "DimensionDepartment";
        public const string DimProduct = "DimensionProduct";

        public const string LineType = "LineType";

        public const string TransactionDate = "TransactionDate";

        // NEW canonical field for ops date (working date)
        public const string RpcWorkingDate = "RPCWorkingDate";

        // Source field name coming from Dataverse payload (if present)
        public const string RpcOperationsDate = "rpc_operationsdate";

        // Do not emit these in DELTA payload
        public const string ModifiedOn = "modifiedon";
        public const string RpcAccrualFieldModifiedOn = "rpc_accrualfieldmodifiedon";

        // Remove from payload (hygiene)
        public const string LineNum = "Line num";
    }

    private static DateTime ResolveWorkingDateUtcFromLineOrFallback(JsonObject lineObj, DateTime fallbackUtcDate)
    {
        // Prefer existing RPCWorkingDate if upstream already set it as FSCM literal
        if (JsonLooseKey.TryGetStringLoose(lineObj, Keys.RpcWorkingDate, out var rpcWorkingLiteral) &&
            !string.IsNullOrWhiteSpace(rpcWorkingLiteral) &&
            TryParseFscmDateLiteral(rpcWorkingLiteral!, out var rpcWorkingUtc))
        {
            return rpcWorkingUtc.Date;
        }

        // Prefer OperationDate if present (FS sends /Date(ms)/)
        if (JsonLooseKey.TryGetStringLoose(lineObj, "OperationDate", out var opLiteral) &&
            !string.IsNullOrWhiteSpace(opLiteral) &&
            TryParseFscmDateLiteral(opLiteral!, out var opUtc))
        {
            return opUtc.Date;
        }

        // Else try TransactionDate if present (/Date(ms)/)
        if (JsonLooseKey.TryGetStringLoose(lineObj, Keys.TransactionDate, out var txLiteral) &&
            !string.IsNullOrWhiteSpace(txLiteral) &&
            TryParseFscmDateLiteral(txLiteral!, out var txUtc))
        {
            return txUtc.Date;
        }

        // Else try Dataverse ISO string in rpc_operationsdate
        if (JsonLooseKey.TryGetNodeLoose(lineObj, Keys.RpcOperationsDate, out var opsNode) && opsNode is not null)
        {
            // try ISO first
            var parsed = TryParseIsoUtc(opsNode.ToString());
            if (parsed.HasValue) return parsed.Value.Date;

            // then /Date(ms)/
            var s = opsNode.ToString();
            if (!string.IsNullOrWhiteSpace(s) && TryParseFscmDateLiteral(s, out var dt))
                return dt.Date;
        }

        // Fail-open: fallback to planned date
        return fallbackUtcDate.Date;
    }

    private static DateTime? TryParseIsoUtc(string? s)
    {
        if (string.IsNullOrWhiteSpace(s)) return null;

        if (DateTime.TryParse(
                s,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal,
                out var dt))
        {
            return DateTime.SpecifyKind(dt, DateTimeKind.Utc);
        }

        return null;
    }

    private static bool TryParseFscmDateLiteral(string literal, out DateTime utc)
    {
        // Expected: /Date(1700000000000)/
        utc = default;
        if (string.IsNullOrWhiteSpace(literal)) return false;

        var s = literal.Trim();
        if (!s.StartsWith("/Date(", StringComparison.OrdinalIgnoreCase)) return false;
        var end = s.IndexOf(")/", StringComparison.OrdinalIgnoreCase);
        if (end < 0) return false;

        var numPart = s.Substring(6, end - 6);
        if (!long.TryParse(numPart, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ms))
            return false;

        utc = DateTimeOffset.FromUnixTimeMilliseconds(ms).UtcDateTime;
        return true;
    }

    private static string ToFscmDateLiteral(DateTime utcDate)
    {
        var dt = new DateTime(utcDate.Year, utcDate.Month, utcDate.Day, 0, 0, 0, DateTimeKind.Utc);
        var ms = new DateTimeOffset(dt).ToUnixTimeMilliseconds();
        return $"/Date({ms})/";
    }

    private static bool HasAnyNodeLoose(JsonObject obj, params string[] keys)
        => keys.Any(k => JsonLooseKey.TryGetNodeLoose(obj, k, out var n) && n is not null);

    private static JsonObject? FindFirstObjectLoose(JsonObject root, params string[] keys)
    {
        foreach (var k in keys)
        {
            if (JsonLooseKey.TryGetNodeLoose(root, k, out var node) && node is JsonObject o)
                return o;
        }

        return null;
    }

    private static JsonArray? FindJournalLinesArrayLoose(JsonObject section)
    {
        if (JsonLooseKey.TryGetNodeLoose(section, Keys.JournalLines, out var node) && node is JsonArray a)
            return a;

        // sometimes: "Journal lines"
        if (JsonLooseKey.TryGetNodeLoose(section, "Journal lines", out var node2) && node2 is JsonArray a2)
            return a2;

        return null;
    }

    private static Guid GetWorkOrderLineGuid(JsonObject lineObj)
    {
        if (!JsonLooseKey.TryGetStringLoose(lineObj, Keys.WorkOrderLineGuid, out var s) || string.IsNullOrWhiteSpace(s))
            return Guid.Empty;

        // handles "{GUID}" format
        s = s.Trim();
        if (s.StartsWith("{", StringComparison.Ordinal) && s.EndsWith("}", StringComparison.Ordinal))
            s = s.Substring(1, s.Length - 2);

        return Guid.TryParse(s, out var g) ? g : Guid.Empty;
    }

    private static decimal? GetDecimalLoose(JsonObject lineObj, string key)
    {
        if (JsonLooseKey.TryGetNodeLoose(lineObj, key, out var node) && node is not null)
            return TryParseDecimal(node);

        return null;
    }

    private static decimal? GetDecimalLooseAny(JsonObject lineObj, params string[] keys)
    {
        foreach (var k in keys)
        {
            if (JsonLooseKey.TryGetNodeLoose(lineObj, k, out var node) && node is not null)
            {
                var d = TryParseDecimal(node);
                if (d.HasValue) return d;
            }
        }

        return null;
    }

    private static string? GetStringLooseAny(JsonObject lineObj, params string[] keys)
    {
        foreach (var k in keys)
        {
            if (JsonLooseKey.TryGetStringLoose(lineObj, k, out var s) && !string.IsNullOrWhiteSpace(s))
                return s;
        }

        return null;
    }

    private static bool GetIsActive(JsonObject lineObj)
    {
        // Many payloads use IsActive / isactive / "Status" etc.
        // Try the known key first.
        if (JsonLooseKey.TryGetNodeLoose(lineObj, "IsActive", out var node) && node is not null)
        {
            if (node is JsonValue v)
            {
                if (v.TryGetValue<bool>(out var b)) return b;
                if (v.TryGetValue<int>(out var i)) return i != 0;
                if (v.TryGetValue<string>(out var s) && bool.TryParse(s, out var bb)) return bb;
            }
        }

        // Dataverse boolean pattern
        if (TryGetYesNoFromDataverseBoolean(lineObj, "isactive", out var yn))
            return string.Equals(yn, "Yes", StringComparison.OrdinalIgnoreCase);

        // fail-open: assume active
        return true;
    }

    private static decimal? TryParseDecimal(JsonNode node)
    {
        try
        {
            var s = node.ToString();
            if (string.IsNullOrWhiteSpace(s)) return null;

            if (decimal.TryParse(
                    s,
                    NumberStyles.Any,
                    CultureInfo.InvariantCulture,
                    out var d))
            {
                return d;
            }
        }
        catch
        {
            // ignore
        }

        return null;
    }

    private static void CopyIfPresentLoose(JsonObject src, JsonObject dst, string key)
    {
        if (JsonLooseKey.TryGetNodeLoose(src, key, out var node) && node is not null)
            dst[key] = node.DeepClone();
    }

    private static void RemoveLoose(JsonObject obj, string key)
    {
        if (obj.ContainsKey(key))
            obj.Remove(key);

        // also remove common casing variants
        var match = obj.FirstOrDefault(kvp => string.Equals(kvp.Key, key, StringComparison.OrdinalIgnoreCase));
        if (!string.IsNullOrWhiteSpace(match.Key) && obj.ContainsKey(match.Key))
            obj.Remove(match.Key);
    }

    private static void SetOrAddLoose(JsonObject obj, string key, JsonNode value)
    {
        obj[key] = value;
    }

    private static void SetOrAddLoose(JsonObject obj, string key, string value)
    {
        obj[key] = value;
    }

    private static void SetOrAddLoose(JsonObject obj, string key, decimal value)
    {
        obj[key] = value;
    }

    private static DateTime ResolveOperationsDateUtc(JsonObject lineObj, DateTime todayUtc)
    {
        // Priority order (most reliable first):
        // 1) OperationDate (FS sends this as /Date(ms)/ in your payloads)
        // 2) RPCWorkingDate (if already stamped upstream as /Date(ms)/)
        // 3) TransactionDate (FS sends this as /Date(ms)/)
        // 4) rpc_operationsdate (Dataverse ISO or /Date(ms)/)
        // 5) fallback = todayUtc.Date

        // 1) OperationDate
        if (JsonLooseKey.TryGetStringLoose(lineObj, "OperationDate", out var opLiteral) &&
            !string.IsNullOrWhiteSpace(opLiteral) &&
            TryParseFscmDateLiteral(opLiteral!, out var opUtc))
        {
            return opUtc.Date;
        }

        // 2) RPCWorkingDate
        if (JsonLooseKey.TryGetStringLoose(lineObj, Keys.RpcWorkingDate, out var rpcWorkingLiteral) &&
            !string.IsNullOrWhiteSpace(rpcWorkingLiteral) &&
            TryParseFscmDateLiteral(rpcWorkingLiteral!, out var rpcWorkingUtc))
        {
            return rpcWorkingUtc.Date;
        }

        // 3) TransactionDate
        if (JsonLooseKey.TryGetStringLoose(lineObj, Keys.TransactionDate, out var txLiteral) &&
            !string.IsNullOrWhiteSpace(txLiteral) &&
            TryParseFscmDateLiteral(txLiteral!, out var txUtc))
        {
            return txUtc.Date;
        }

        // 4) rpc_operationsdate (ISO or /Date(ms)/)
        if (JsonLooseKey.TryGetNodeLoose(lineObj, Keys.RpcOperationsDate, out var node) && node is not null)
        {
            var s = node.ToString();

            if (TryParseIsoUtc(s) is DateTime iso)
                return iso.Date;

            if (!string.IsNullOrWhiteSpace(s) && TryParseFscmDateLiteral(s, out var dt))
                return dt.Date;
        }

        // 5) fallback
        return todayUtc.Date;
    }

    private static JsonObject BuildReversalLineFromFscmSnapshot(FscmReversalPayloadSnapshot snap, JournalType jt, decimal plannedQuantity)
    {
        var o = new JsonObject();

        // Identifiers
        o[Keys.WorkOrderLineGuid] = "{" + snap.WorkOrderLineId.ToString("D").ToUpperInvariant() + "}";

        // Core attributes
        if (!string.IsNullOrWhiteSpace(snap.Currency)) o["Currency"] = snap.Currency;
        if (!string.IsNullOrWhiteSpace(snap.DimensionDisplayValue)) o["DimensionDisplayValue"] = snap.DimensionDisplayValue;

        if (snap.FsaUnitPrice.HasValue) o["FSAUnitPrice"] = snap.FsaUnitPrice.Value;
        if (!string.IsNullOrWhiteSpace(snap.ItemId)) o["ItemId"] = snap.ItemId;
        if (!string.IsNullOrWhiteSpace(snap.ProjectCategory)) o["ProjectCategory"] = snap.ProjectCategory;
        if (!string.IsNullOrWhiteSpace(snap.JournalLineDescription)) o["JournalLineDescription"] = snap.JournalLineDescription;
        if (!string.IsNullOrWhiteSpace(snap.LineProperty)) o["LineProperty"] = snap.LineProperty;

        // Quantity: use planned quantity (computed from FSCM aggregation) to avoid partial-reversal drift.
        o[Keys.Quantity] = plannedQuantity;

        // Optional unmapped per provided sheet (no FSCM mapping supplied)
        o["RPCCustomerProductReference"] = string.Empty;

        // Discounts / markups / surcharges
        if (snap.RpcDiscountAmount.HasValue) o["RPCDiscountAmount"] = snap.RpcDiscountAmount.Value;
        if (snap.RpcDiscountPercent.HasValue) o["RPCDiscountPercent"] = snap.RpcDiscountPercent.Value;
        if (snap.RpcMarkupPercent.HasValue) o["RPCMarkupPercent"] = snap.RpcMarkupPercent.Value;
        if (snap.RpcOverallDiscountAmount.HasValue) o["RPCOverallDiscountAmount"] = snap.RpcOverallDiscountAmount.Value;
        if (snap.RpcOverallDiscountPercent.HasValue) o["RPCOverallDiscountPercent"] = snap.RpcOverallDiscountPercent.Value;
        if (snap.RpcSurchargeAmount.HasValue) o["RPCSurchargeAmount"] = snap.RpcSurchargeAmount.Value;
        if (snap.RpcSurchargePercent.HasValue) o["RPCSurchargePercent"] = snap.RpcSurchargePercent.Value;
        if (snap.RpMarkUpAmount.HasValue) o["RPMarkUpAmount"] = snap.RpMarkUpAmount.Value;

        // Dates
        if (snap.TransactionDate.HasValue) o[Keys.TransactionDate] = ToFscmDateLiteral(snap.TransactionDate.Value.Date);
        if (snap.OperationDate.HasValue) o["OperationDate"] = ToFscmDateLiteral(snap.OperationDate.Value.Date);

        // Amounts
        if (snap.UnitAmount.HasValue) o[Keys.UnitAmount] = snap.UnitAmount.Value;
        if (snap.UnitCost.HasValue) o[Keys.UnitCost] = snap.UnitCost.Value;

        // Print & UOM
        if (snap.IsPrintable.HasValue) o["IsPrintable"] = snap.IsPrintable.Value;
        if (!string.IsNullOrWhiteSpace(snap.UnitId)) o["UnitId"] = snap.UnitId;

        // Inventory dims
        if (!string.IsNullOrWhiteSpace(snap.Warehouse)) o["Warehouse"] = snap.Warehouse;
        if (!string.IsNullOrWhiteSpace(snap.Site)) o["Site"] = snap.Site;

        // Descriptions
        if (!string.IsNullOrWhiteSpace(snap.FsaCustomerProductDesc)) o["FSACustomerProductDesc"] = snap.FsaCustomerProductDesc;

        // Reversal-only additional header mapping
        if (!string.IsNullOrWhiteSpace(snap.FsaTaxabilityType)) o["FSATaxabilityType"] = snap.FsaTaxabilityType;

        return o;
    }

    static void ComputeAndSetUnitAmount(JsonObject lineObj, string qtyKey)
    {
        // Only infer UnitAmount from FSAUnitPrice when UnitAmount is not explicitly set.
        var existing = GetDecimalLooseAny(lineObj, Keys.UnitAmount, "UnitAmount");
        if (existing.HasValue) return;

        var fsaUnitPrice = GetDecimalLooseAny(lineObj, "FSAUnitPrice", "FsaUnitPrice");
        if (fsaUnitPrice.HasValue)
            SetOrAddLoose(lineObj, Keys.UnitAmount, fsaUnitPrice.Value);
    }

    private static bool TryParseDeptProdFromDimensionDisplayValue(string? ddv, out string? dept, out string? prod)
    {
        dept = null;
        prod = null;

        if (string.IsNullOrWhiteSpace(ddv))
            return false;

        // Expected: "-DEPT-PROD-----"
        // Example: "-0006-119-----"
        var s = ddv.Trim();

        if (!s.StartsWith("-", StringComparison.Ordinal))
            return false;

        var parts = s.Split('-', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2)
            return false;

        dept = parts[0].Trim();
        prod = parts[1].Trim();

        return !(string.IsNullOrWhiteSpace(dept) || string.IsNullOrWhiteSpace(prod));
    }

    // Builds FSCM-friendly default dimension display value from dept+prod.
    // Format: "-DEPT-PROD-----"
    private static string BuildDefaultDimensionDisplayValue(string? dept, string? prod)
    {
        var d = (dept ?? string.Empty).Trim();
        var p = (prod ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(d) && string.IsNullOrWhiteSpace(p)) return string.Empty;
        return "-" + d + "-" + p + "-----";
    }
}
