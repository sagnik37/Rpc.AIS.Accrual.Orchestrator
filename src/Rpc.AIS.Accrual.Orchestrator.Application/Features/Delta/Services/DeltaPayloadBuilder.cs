// File: Rpc.AIS.Accrual.Orchestrator/src/Rpc.AIS.Accrual.Orchestrator.Core/Services/DeltaPayloadBuilder.cs
//
// (FULL FILE CONTENT)
// Behavioral changes applied:
// - FSAUnitPrice comes ONLY from l.FsaUnitPrice (msdyn_unitamount). No rpc_calculatedunitprice fallback.
// - UnitAmount remains l.UnitAmount (now also msdyn_unitamount via mapper).
//
// Critical fix (empty payload bug):
// - When caller passes FsaDeltaSnapshot, product lines live under:
//      InventoryProducts  -> Item journal lines
//      NonInventoryProducts -> Expense journal lines
//   So we must include those property names in reflection lookup.
//
// Compile/architecture fixes applied:
// - No dependency on missing DeltaWorkOrderSnapshot: Build<TWorkOrder>() + reflection helpers.
// - Added BuildWoListPayload(...) (string) for existing callers.
// - Fixed CS8116 by removing illegal nullable-pattern matching for decimal? and DateTime?.
//
// NEW FIX (Warehouse rules):
// - Warehouse is only applicable for Item journal lines.
// - Expense journal payload MUST NOT include Warehouse at all.
// - Hour journal payload MUST NOT include Warehouse at all (already the case).

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;

using Rpc.AIS.Accrual.Orchestrator.Core.Domain;

namespace Rpc.AIS.Accrual.Orchestrator.Core.Services;

public static class DeltaPayloadBuilder
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = null,
        WriteIndented = false
    };

    /// <summary>
    /// Existing use-case contract: build outbound payload JSON as string.
    /// </summary>
    public static string BuildWoListPayload(
        IReadOnlyList<FsaDeltaSnapshot> snapshots,
        string correlationId,
        string runId,
        string system = "FieldService",
        string? triggeredByOverride = null)
    {
        var json = Build(system, runId, correlationId, snapshots, triggeredByOverride);
        return json.ToJsonString(JsonOpts);
    }

    /// <summary>
    /// Generic to avoid a compile-time dependency on a specific snapshot type name.
    /// Caller can pass IReadOnlyList&lt;AnySnapshotType&gt; and this will reflect required properties.
    /// </summary>
    public static JsonObject Build<TWorkOrder>(
        string system,
        string runId,
        string correlationId,
        IReadOnlyList<TWorkOrder> workOrders,
        string? triggeredByOverride = null)
        where TWorkOrder : class
    {
        var woList = new JsonArray();

        foreach (var wo in workOrders)
        {
            woList.Add(BuildWorkOrder(system, runId, correlationId, wo, triggeredByOverride));
        }

        return new JsonObject
        {
            ["_request"] = new JsonObject
            {
                ["System"] = system,
                ["RunId"] = runId,
                ["CorrelationId"] = correlationId,
                ["WOList"] = woList
            }
        };
    }

    private static JsonObject BuildWorkOrder<TWorkOrder>(string system, string runId, string correlationId, TWorkOrder wo, string? triggeredByOverride)
        where TWorkOrder : class
    {
        var jobId = GetStringProp(wo,
                        "WorkOrderNumber", "WorkOrderNo", "WorkOrderID", "WorkOrderIdText", "msdyn_name")
                    ?? string.Empty;

        var subProjectId = GetStringProp(wo, "SubProjectId", "SubProjectID", "rpc_subprojectid", "SubProject")
                           ?? string.Empty;

        var woGuid = GetGuidProp(wo, "WorkOrderId", "WorkOrderGuid", "WorkOrderGUID", "WorkOrderIDGuid", "msdyn_workorderid");

        var triggeredBy = GetStringProp(wo, "TriggeredBy", "Trigger", "Triggered", "Source", "InvocationSource")
                         ?? triggeredByOverride
                         ?? string.Empty;
        var action = ResolveJournalActionSuffixForTriggeredBy(triggeredBy);
        var journalDescription = BuildJournalDescription(jobId, subProjectId, action);

        var obj = new JsonObject
        {
            ["WorkOrderGUID"] = ToBracedUpperGuidString(woGuid),
            ["WorkOrderID"] = jobId,
            ["Company"] = GetStringProp(wo, "Company", "LegalEntity", "DataAreaId", "dataAreaId") ?? string.Empty,
            ["SubProjectId"] = subProjectId,
            ["CountryRegionId"] = GetStringProp(wo, "CountryRegionId", "Country", "CountryRegion") ?? string.Empty,
            ["County"] = GetStringProp(wo, "County") ?? string.Empty,
            ["State"] = GetStringProp(wo, "State") ?? string.Empty,
            ["DimensionDisplayValue"] = GetStringProp(wo, "DimensionDisplayValue", "DefaultDimensionDisplayValue") ?? string.Empty,
            ["FSATaxabilityType"] = GetStringProp(wo, "FSATaxabilityType", "TaxabilityType") ?? string.Empty,
            ["FSAWellAge"] = GetStringProp(wo, "FSAWellAge", "WellAge") ?? string.Empty,
            ["FSAWorkType"] = GetStringProp(wo, "FSAWorkType", "WorkType") ?? string.Empty
        };

        var itemLines = GetLines<FsaProductLine>(
            wo,
            "ItemLines", "WOItemLines", "ProductLines", "ItemJournalLines",
            "InventoryProducts", "InventoryLines", "InventoryProductLines");

        if (itemLines is { Count: > 0 })
        {
            obj["WOItemLines"] = BuildItemJournal(itemLines, journalDescription);
        }

        var expenseLines = GetLines<FsaProductLine>(
            wo,
            "ExpenseLines", "WOExpLines", "ExpenseJournalLines",
            "NonInventoryProducts", "NonInventoryLines", "NonInventoryProductLines");

        if (expenseLines is { Count: > 0 })
        {
            obj["WOExpLines"] = BuildExpenseJournal(expenseLines, journalDescription);
        }

        var hourLines = GetLines<FsaServiceLine>(
            wo,
            "HourLines", "WOHourLines", "ServiceLines", "HourJournalLines");

        if (hourLines is { Count: > 0 })
        {
            obj["WOHourLines"] = BuildHourJournal(hourLines, journalDescription);
        }

        return obj;
    }

    private static JsonObject BuildItemJournal(IReadOnlyList<FsaProductLine> lines, string journalDescription)
    {
        return new JsonObject
        {
            ["JournalDescription"] = journalDescription,
            ["LineType"] = "Item",
            ["JournalLines"] = JsonSerializer.SerializeToNode(
                lines.OrderBy(l => l.WorkOrderNumber ?? string.Empty)
                     .ThenBy(l => l.LineId)
                     .Select(l => BuildProductJournalLine(l, journalDescription, includeWarehouse: true))
                     .ToList(),
                JsonOpts)
        };
    }

    private static JsonObject BuildExpenseJournal(IReadOnlyList<FsaProductLine> lines, string journalDescription)
    {
        var list = lines
            .OrderBy(l => l.WorkOrderNumber ?? string.Empty)
            .ThenBy(l => l.LineId)
            .Select(l =>
            {
                var o = BuildProductJournalLine(l, journalDescription, includeWarehouse: false);
                o.Remove("Warehouse");
                o.Remove("Site");
                return o;
            })
            .ToList();

        return new JsonObject
        {
            ["JournalDescription"] = journalDescription,
            ["LineType"] = "Expense",
            ["JournalLines"] = JsonSerializer.SerializeToNode(list, JsonOpts)
        };
    }

    private static JsonObject BuildHourJournal(IReadOnlyList<FsaServiceLine> lines, string journalDescription)
    {
        return new JsonObject
        {
            ["JournalDescription"] = journalDescription,
            ["LineType"] = "Hour",
            ["JournalLines"] = JsonSerializer.SerializeToNode(
                lines.OrderBy(l => l.WorkOrderNumber ?? string.Empty)
                     .ThenBy(l => l.LineId)
                     .Select(l => BuildServiceJournalLine(l, journalDescription))
                     .ToList(),
                JsonOpts)
        };
    }

    private static JsonObject BuildProductJournalLine(FsaProductLine l, string journalDescription, bool includeWarehouse)
    {
        var unitCost = l.CalculatedUnitPrice ?? l.UnitCost;
        var quantity = l.Quantity ?? 0m;

        var fsaUnitPrice = l.FsaUnitPrice ?? 0m;

        var txDate = l.OperationsDateUtc
                     ?? GetDateTimeProp(l, "TransactionDate", "Transactiondate", "rpc_transactiondate", "Date", "PostedDate");

        var txDateFscm = txDate is null ? string.Empty : ToFscmDateLiteral(txDate.Value);

        var obj = new JsonObject
        {
            ["WorkOrderLineGuid"] = ToBracedUpperGuidString(l.LineId),
            ["Currency"] = S(l.Currency),

            ["DimensionDisplayValue"] = BuildDefaultDimensionDisplayValue(l.Department, l.ProductLine),

            ["FSAUnitPrice"] = fsaUnitPrice,

            ["ItemId"] = S(l.ItemNumber),
            ["ProjectCategory"] = S(l.ProjectCategory),

            ["JournalDescription"] = journalDescription,
            ["JournalLineDescription"] = l.JournalDescription,

            ["LineProperty"] = S(l.LineProperty),

            ["Quantity"] = quantity,

            ["RPCCustomerProductReference"] = S(l.CustomerProductReference),

            ["RPCDiscountAmount"] = l.DiscountAmount ?? 0m,
            ["RPCDiscountPercent"] = l.DiscountPercent ?? 0m,
            ["RPCMarkupPercent"] = GetDecimalProp(l, "MarkupPercent", "RPCMarkupPercent") ?? 0m,
            ["RPCOverallDiscountAmount"] = GetDecimalProp(l, "OverallDiscountAmount", "RPCOverallDiscountAmount") ?? 0m,
            ["RPCOverallDiscountPercent"] = GetDecimalProp(l, "OverallDiscountPercent", "RPCOverallDiscountPercent") ?? 0m,
            ["RPCSurchargeAmount"] = l.SurchargeAmount ?? 0m,
            ["RPCSurchargePercent"] = l.SurchargePercent ?? 0m,
            ["RPMarkUpAmount"] = GetDecimalProp(l, "MarkupAmount", "RPMarkUpAmount") ?? 0m,

            ["TransactionDate"] = txDateFscm,
            ["OperationDate"] = txDateFscm,

            ["UnitAmount"] = l.UnitAmount ?? 0m,
            ["UnitCost"] = unitCost ?? 0m,

            ["IsPrintable"] = (l.Printable ?? false) ? "Yes" : "No",
            ["UnitId"] = S(l.Unit)
        };

        if (includeWarehouse)
        {
            var wh = l.Warehouse;
            if (!string.IsNullOrWhiteSpace(wh))
            {
                obj["Warehouse"] = wh.Trim();
            }
        }

        obj.Remove("Site");
        obj.Remove("ResourceCompany");
        obj.Remove("ResourceId");
        obj.Remove("AccrualLineVersionNumber");
        obj.Remove("ProductColourId");
        obj.Remove("ProductConfigurationId");
        obj.Remove("ProductSizeId");
        obj.Remove("ProductStyleId");

        if (!string.IsNullOrWhiteSpace(l.TaxabilityType))
        {
            obj["FSATaxabilityType"] = S(l.TaxabilityType);
        }

        return obj;
    }

    private static JsonObject BuildServiceJournalLine(FsaServiceLine l, string journalDescription)
    {
        var unitCost = l.CalculatedUnitPrice ?? l.UnitCost;
        var fsaUnitPrice = l.FsaUnitPrice ?? 0m;

        var txDate = l.OperationsDateUtc
                     ?? GetDateTimeProp(l, "TransactionDate", "Transactiondate", "rpc_transactiondate", "Date", "PostedDate");

        var txDateFscm = txDate is null ? string.Empty : ToFscmDateLiteral(txDate.Value);

        var obj = new JsonObject
        {
            ["WorkOrderLineGuid"] = ToBracedUpperGuidString(l.LineId),
            ["Currency"] = S(l.Currency),

            ["DimensionDisplayValue"] = BuildDefaultDimensionDisplayValue(l.Department, l.ProductLine),

            ["Duration"] = l.Duration ?? 0m,

            ["FSAUnitPrice"] = fsaUnitPrice,

            ["ItemId"] = S(GetStringProp(l, "ItemId", "ItemNumber", "Item", "msdyn_name")),

            ["JournalDescription"] = journalDescription,
            ["JournalLineDescription"] = journalDescription,

            ["LineProperty"] = S(l.LineProperty),

            ["TransactionDate"] = txDateFscm,
            ["OperationDate"] = txDateFscm,

            ["IsPrintable"] = (l.Printable ?? false) ? "Yes" : "No",

            ["UnitAmount"] = l.UnitAmount ?? 0m,
            ["UnitCost"] = unitCost ?? 0m,
            ["UnitId"] = S(l.Unit)
        };

        obj.Remove("Site");
        obj.Remove("ResourceCompany");
        obj.Remove("ResourceId");
        obj.Remove("AccrualLineVersionNumber");
        obj.Remove("ProductColourId");
        obj.Remove("ProductConfigurationId");
        obj.Remove("ProductSizeId");
        obj.Remove("ProductStyleId");

        if (!string.IsNullOrWhiteSpace(l.TaxabilityType))
        {
            obj["FSATaxabilityType"] = S(l.TaxabilityType);
        }

        return obj;
    }

    private static string BuildJournalDescription(string jobId, string subProjectId, string action)
        => $"{S(jobId)} - {S(subProjectId)} - {S(action)}";

    /// <summary>
    /// Resolve the journal action suffix used in JournalDescription based on trigger source.
    /// This is intentionally tolerant of casing and minor variants (e.g., AdhocBulk vs AdHocBulk).
    /// </summary>
    public static string ResolveJournalActionSuffixForTriggeredBy(string? triggeredBy)
    {
        var t = triggeredBy?.Trim();
        if (string.IsNullOrWhiteSpace(t)) return "Post";

        if (t.Equals("Timer", StringComparison.OrdinalIgnoreCase)) return "Create";
        if (t.Equals("AdHocSingle", StringComparison.OrdinalIgnoreCase)) return "Create";
        if (t.Equals("AdHocBulk", StringComparison.OrdinalIgnoreCase) || t.Equals("AdhocBulk", StringComparison.OrdinalIgnoreCase)) return "Create";
        if (t.Equals("AdHocAll", StringComparison.OrdinalIgnoreCase) || t.Equals("AdhocAll", StringComparison.OrdinalIgnoreCase)) return "Create";
        if (t.Equals("CustomerChange", StringComparison.OrdinalIgnoreCase)) return "Billing Location Change";
        if (t.Equals("Cancel", StringComparison.OrdinalIgnoreCase)) return "Cancel";

        return "Post";
    }

    private static string S(string? s) => s ?? string.Empty;

    private static string ToBracedUpperGuidString(Guid id)
        => "{" + id.ToString().ToUpperInvariant() + "}";

    private static string BuildDefaultDimensionDisplayValue(string? dept, string? prod)
    {
        var d = (dept ?? string.Empty).Trim();
        var p = (prod ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(d) && string.IsNullOrWhiteSpace(p)) return string.Empty;
        return "-" + d + "-" + p + "-----";
    }

    private static string ToFscmDateLiteral(DateTime dtUtc)
    {
        var utc = DateTime.SpecifyKind(dtUtc, DateTimeKind.Utc);
        var ms = new DateTimeOffset(utc).ToUnixTimeMilliseconds();
        return $"/Date({ms})/";
    }

    private static IReadOnlyList<TLine>? GetLines<TLine>(object obj, params string[] names) where TLine : class
    {
        foreach (var n in names)
        {
            var p = obj.GetType().GetProperty(n, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            if (p is null) continue;

            var v = p.GetValue(obj);
            if (v is null) continue;

            if (v is IReadOnlyList<TLine> ro) return ro;
            if (v is List<TLine> l) return l;
            if (v is IEnumerable<TLine> e) return e.ToList();
        }

        return null;
    }

    private static decimal? GetDecimalProp(object obj, params string[] names)
    {
        foreach (var n in names)
        {
            var p = obj.GetType().GetProperty(n, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            if (p is null) continue;

            var v = p.GetValue(obj);
            if (v is null) continue;

            if (v is decimal d) return d;

            if (decimal.TryParse(
                    v.ToString(),
                    NumberStyles.Any,
                    CultureInfo.InvariantCulture,
                    out var parsed))
            {
                return parsed;
            }
        }

        return null;
    }

    private static string? GetStringProp(object obj, params string[] names)
    {
        foreach (var n in names)
        {
            var p = obj.GetType().GetProperty(n, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            if (p is null) continue;

            var v = p.GetValue(obj);
            if (v is null) continue;

            return v.ToString();
        }

        return null;
    }

    private static Guid GetGuidProp(object obj, params string[] names)
    {
        foreach (var n in names)
        {
            var p = obj.GetType().GetProperty(n, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            if (p is null) continue;

            var v = p.GetValue(obj);
            if (v is null) continue;

            if (v is Guid g && g != Guid.Empty) return g;

            if (Guid.TryParse(v.ToString(), out var parsed) && parsed != Guid.Empty)
                return parsed;
        }

        return Guid.Empty;
    }

    private static DateTime? GetDateTimeProp(object obj, params string[] names)
    {
        foreach (var n in names)
        {
            var p = obj.GetType().GetProperty(n, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            if (p is null) continue;

            var v = p.GetValue(obj);
            if (v is null) continue;

            if (v is DateTime dt) return dt;

            if (DateTime.TryParse(
                    v.ToString(),
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                    out var parsed))
            {
                return DateTime.SpecifyKind(parsed, DateTimeKind.Utc);
            }
        }

        return null;
    }
}
