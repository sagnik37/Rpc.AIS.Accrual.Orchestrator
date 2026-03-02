//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text.Json;
//using System.Text.Json.Nodes;
//using System.Threading;
//using System.Threading.Tasks;
//using FluentAssertions;
//using Moq;
//using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
//using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
//using Rpc.AIS.Accrual.Orchestrator.Core.Domain.Delta;
//using Rpc.AIS.Accrual.Orchestrator.Core.Services;
//using Xunit;
//using Xunit.Abstractions;

//namespace Rpc.AIS.Accrual.Orchestrator.Tests.Delta;

///// <summary>
///// Provides delta payload generation tests behavior.
///// </summary>
//public sealed class DeltaPayloadGenerationTests
//{
//    private readonly ITestOutputHelper _output;

//    public DeltaPayloadGenerationTests(ITestOutputHelper output) => _output = output;

//    [Fact]
//    // <summary>
//    // Executes quantity changed item line produces single delta line.
//    // </summary>
//    public async Task QuantityChanged_ItemLine_ProducesSingleDeltaLine()
//    {
//        var today = new DateTime(2026, 01, 30);
//        var openStart = new DateTime(2026, 01, 01);

//        var (woId, itemLineId, expLineId, hourLineId, fsaJson) = BuildBasePayload();

//        // Change FSA Quantity on Item line: 3 -> 5
//        fsaJson = MutateFsaLine(fsaJson, sectionKey: "WO Item Lines", accrualLineGuid: itemLineId, mutate: line =>
//        {
//            line["Quantity"] = 5;
//        });

//        // : Build history that matches BASE FSA payload attributes for each line type.
//        // (So only the mutated item quantity triggers delta)
//        var history = BuildBaselineHistory(
//            woId, itemLineId, expLineId, hourLineId,
//            itemQty: 3m, expQty: 2m, hourDuration: 2m,
//            itemUnitCost: 10m, expUnitCost: 10m, hourUnitCost: 10m);

//        var svc = BuildService(history, openStart, isClosed: _ => false);

//        var ctx = NewContext();

//        WoDeltaPayloadBuildResult result;
//        try
//        {
//            result = await svc.BuildDeltaPayloadAsync(ctx, fsaJson, today, CancellationToken.None);
//        }
//        catch (Exception ex)
//        {
//            _output.WriteLine(ex.ToString());
//            throw;
//        }

//        //var result = await svc.BuildDeltaPayloadAsync(ctx, fsaJson, today, CancellationToken.None);

//        result.TotalDeltaLines.Should().Be(1);
//        result.TotalReverseLines.Should().Be(0);
//        result.TotalRecreateLines.Should().Be(0);

//        var delta = Parse(result.DeltaPayloadJson);

//        // Item journal should have exactly 1 delta line
//        var itemLines = GetJournalLines(delta, woId, "WO Item Lines");
//        itemLines.Should().HaveCount(1);
//        GetDecimal(itemLines[0], "Quantity").Should().Be(3m - 5m); // (history - current) => -2

//        // Expense/Hour should NOT produce any lines
//        TryGetJournalLines(delta, woId, "WO Exp Lines", out var expLines).Should().BeTrue();
//        expLines.Should().BeEmpty();

//        TryGetJournalLines(delta, woId, "WO Hour Lines", out var hourLines).Should().BeTrue();
//        hourLines.Should().BeEmpty();

//        _output.WriteLine(result.DeltaPayloadJson);
//    }

//    [Fact]
//    // <summary>
//    // Executes unit cost changed item line produces reverse and recreate.
//    // </summary>
//    public async Task UnitCostChanged_ItemLine_ProducesReverseAndRecreate()
//    {
//        var today = new DateTime(2026, 01, 30);
//        var openStart = new DateTime(2026, 01, 01);

//        var (woId, itemLineId, expLineId, hourLineId, fsaJson) = BuildBasePayload();

//        // Change "UnitCost" (used as CalculatedUnitPrice in delta logic): 10 -> 12
//        fsaJson = MutateFsaLine(fsaJson, "WO Item Lines", itemLineId, line =>
//        {
//            line["UnitCost"] = 12;
//        });

//        var history = BuildBaselineHistory(
//            woId, itemLineId, expLineId, hourLineId,
//            itemQty: 3m, expQty: 2m, hourDuration: 2m,
//            itemUnitCost: 10m, expUnitCost: 10m, hourUnitCost: 10m);

//        var svc = BuildService(history, openStart, isClosed: _ => false);

//        var result = await svc.BuildDeltaPayloadAsync(NewContext(), fsaJson, today, CancellationToken.None);

//        result.TotalDeltaLines.Should().Be(2); // reverse + recreate
//        result.TotalReverseLines.Should().Be(1);
//        result.TotalRecreateLines.Should().Be(1);

//        var delta = Parse(result.DeltaPayloadJson);
//        var lines = GetJournalLines(delta, woId, "WO Item Lines");
//        lines.Should().HaveCount(2);

//        lines.Select(l => GetDecimal(l, "Quantity")).Should().BeEquivalentTo(new[] { -3m, 3m });

//        // Recreate line must carry updated unit cost = 12
//        lines.Any(l => GetDecimal(l, "Quantity") == 3m && GetDecimal(l, "UnitCost") == 12m).Should().BeTrue();

//        _output.WriteLine(result.DeltaPayloadJson);
//    }

//    [Fact]
//    // <summary>
//    // Executes department changed expense line produces reverse and recreate.
//    // </summary>
//    public async Task DepartmentChanged_ExpenseLine_ProducesReverseAndRecreate()
//    {
//        var today = new DateTime(2026, 01, 30);
//        var openStart = new DateTime(2026, 01, 01);

//        var (woId, itemLineId, expLineId, hourLineId, fsaJson) = BuildBasePayload();

//        // Change Department on Expense line: 0344 -> 9999
//        fsaJson = MutateFsaLine(fsaJson, "WO Exp Lines", expLineId, line =>
//        {
//            line["DimensionDepartment"] = "9999";
//        });

//        // History that matches base FSA for item/hour, and matches base FSA for expense too
//        // (so ONLY the dept change triggers field change)
//        var history = BuildBaselineHistory(
//            woId, itemLineId, expLineId, hourLineId,
//            itemQty: 3m, expQty: 2m, hourDuration: 2m,
//            itemUnitCost: 10m, expUnitCost: 10m, hourUnitCost: 10m,
//            expDept: "0344", expProd: "003", expLineProp: "NonBill");

//        var svc = BuildService(history, openStart, isClosed: _ => false);

//        var result = await svc.BuildDeltaPayloadAsync(NewContext(), fsaJson, today, CancellationToken.None);

//        var delta = Parse(result.DeltaPayloadJson);
//        var lines = GetJournalLines(delta, woId, "WO Exp Lines");
//        lines.Should().HaveCount(2);

//        // Recreate line must carry updated dept
//        lines.Any(l => GetDecimal(l, "Quantity") == 2m && GetString(l, "DimensionDepartment") == "9999").Should().BeTrue();
//    }

//    [Fact]
//    // <summary>
//    // Executes line property changed hour line produces reverse and recreate.
//    // </summary>
//    public async Task LinePropertyChanged_HourLine_ProducesReverseAndRecreate()
//    {
//        var today = new DateTime(2026, 01, 30);
//        var openStart = new DateTime(2026, 01, 01);

//        var (woId, itemLineId, expLineId, hourLineId, fsaJson) = BuildBasePayload();

//        // Change LineProperty on Hour line: Bill -> NonBill
//        fsaJson = MutateFsaLine(fsaJson, "WO Hour Lines", hourLineId, line =>
//        {
//            line["LineProperty"] = "NonBill";
//        });

//        var history = BuildBaselineHistory(
//            woId, itemLineId, expLineId, hourLineId,
//            itemQty: 3m, expQty: 2m, hourDuration: 2m,
//            itemUnitCost: 10m, expUnitCost: 10m, hourUnitCost: 10m);

//        var svc = BuildService(history, openStart, isClosed: _ => false);

//        var result = await svc.BuildDeltaPayloadAsync(NewContext(), fsaJson, today, CancellationToken.None);

//        var delta = Parse(result.DeltaPayloadJson);
//        var lines = GetJournalLines(delta, woId, "WO Hour Lines");
//        lines.Should().HaveCount(2);

//        // Recreate duration stays 2 (hour uses Duration as qtyKey)
//        lines.Any(l => GetDecimal(l, "Duration") == 2m && GetString(l, "LineProperty") == "NonBill").Should().BeTrue();
//    }

//    [Fact]
//    // <summary>
//    // Executes field change with closed open split produces two reversals and one recreate.
//    // </summary>
//    public async Task FieldChangeWithClosedOpenSplit_ProducesTwoReversalsAndOneRecreate()
//    {
//        var today = new DateTime(2026, 01, 30);
//        var openStart = new DateTime(2026, 01, 01);

//        var (woId, itemLineId, _, _, fsaJson) = BuildBasePayload();

//        // Trigger reversal by changing UnitCost 10 -> 12
//        fsaJson = MutateFsaLine(fsaJson, "WO Item Lines", itemLineId, line =>
//        {
//            line["UnitCost"] = 12;
//        });

//        // History split:
//        // - closed: qty 1 on 2025-12-15
//        // - open:   qty 2 on 2026-01-10
//        var itemHistory = new List<FscmJournalLine>
//        {
//            new(JournalType.Item, woId, itemLineId, Quantity: 1m, CalculatedUnitPrice: 10m, ExtendedAmount: null,
//                Department: "0344", ProductLine: "119", Warehouse: null, LineProperty: "Bill",
//                TransactionDate: new DateTime(2025,12,15), DataAreaId: "425", SourceJournalNumber: "J1"),

//            new(JournalType.Item, woId, itemLineId, Quantity: 2m, CalculatedUnitPrice: 10m, ExtendedAmount: null,
//                Department: "0344", ProductLine: "119", Warehouse: null, LineProperty: "Bill",
//                TransactionDate: new DateTime(2026,01,10), DataAreaId: "425", SourceJournalNumber: "J2"),
//        };

//        var history = new HistoryBundle(
//            Item: itemHistory,
//            Expense: Array.Empty<FscmJournalLine>(),
//            Hour: Array.Empty<FscmJournalLine>());

//        // Closed determination: anything before openStart is closed
//        var svc = BuildService(history, openStart, isClosed: d => d.Date < openStart.Date);

//        var result = await svc.BuildDeltaPayloadAsync(NewContext(), fsaJson, today, CancellationToken.None);

//        var delta = Parse(result.DeltaPayloadJson);
//        var lines = GetJournalLines(delta, woId, "WO Item Lines");
//        lines.Should().HaveCount(3);

//        lines.Select(l => GetDecimal(l, "Quantity")).Should().BeEquivalentTo(new[] { -1m, -2m, 3m });

//        // Closed reversal should be dated at openStart
//        lines.Any(l => GetDecimal(l, "Quantity") == -1m && GetString(l, "TransactionDate") == openStart.ToString("yyyy-MM-dd")).Should().BeTrue();

//        // Open reversal should be dated today
//        lines.Any(l => GetDecimal(l, "Quantity") == -2m && GetString(l, "TransactionDate") == today.ToString("yyyy-MM-dd")).Should().BeTrue();

//        // Recreate line should be dated today
//        lines.Any(l => GetDecimal(l, "Quantity") == 3m && GetString(l, "TransactionDate") == today.ToString("yyyy-MM-dd")).Should().BeTrue();
//    }

//    // ---------------- helpers ----------------

//    /// <summary>
//    /// Carries history bundle data.
//    /// </summary>
//    private sealed record HistoryBundle(
//        IReadOnlyList<FscmJournalLine> Item,
//        IReadOnlyList<FscmJournalLine> Expense,
//        IReadOnlyList<FscmJournalLine> Hour);

//    /// <summary>
//    /// Executes new context.
//    /// </summary>
//    private static RunContext NewContext()
//        => new("RUN-TEST", DateTimeOffset.UtcNow, "UNIT_TEST", "CORR-TEST");

//    /// <summary>
//    /// Executes build service.
//    /// </summary>
//    private static WoDeltaPayloadService BuildService(HistoryBundle history, DateTime openStart, Func<DateTime, bool> isClosed)
//    {
//        var fscmFetch = new Mock<IFscmJournalFetchClient>(MockBehavior.Strict);
//        fscmFetch.Setup(m => m.FetchByWorkOrderAsync(It.IsAny<RunContext>(), JournalType.Item, It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
//            .ReturnsAsync((RunContext _, JournalType __, Guid ___, CancellationToken ____) => history.Item);
//        fscmFetch.Setup(m => m.FetchByWorkOrderAsync(It.IsAny<RunContext>(), JournalType.Expense, It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
//            .ReturnsAsync((RunContext _, JournalType __, Guid ___, CancellationToken ____) => history.Expense);
//        fscmFetch.Setup(m => m.FetchByWorkOrderAsync(It.IsAny<RunContext>(), JournalType.Hour, It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
//            .ReturnsAsync((RunContext _, JournalType __, Guid ___, CancellationToken ____) => history.Hour);

//        var periodClient = new Mock<IFscmAccountingPeriodClient>(MockBehavior.Strict);
//        periodClient.Setup(m => m.GetSnapshotAsync(It.IsAny<RunContext>(), It.IsAny<CancellationToken>()))
//            .ReturnsAsync(new AccountingPeriodSnapshot(
//                CurrentOpenPeriodStartDate: openStart.Date,
//                ClosedReversalDateStrategy: "CurrentOpenPeriodStart",
//                SnapshotMinDate: openStart.AddMonths(-18),
//                SnapshotMaxDate: openStart.AddMonths(18),
//                IsDateInClosedPeriodAsync: (d, _) => new ValueTask<bool>(isClosed(d))));

//        var aggregator = new FscmJournalAggregator();
//        var planner = new JournalReversalPlanner();
//        var engine = new DeltaCalculationEngine(planner);

//        var logger = new TestAisLogger();
//        var diag = new TestDiagOptions();

//        return new WoDeltaPayloadService(
//            fscmFetch.Object,
//            periodClient.Object,
//            aggregator,
//            engine,
//            logger,
//            diag);
//    }

//    /// <summary>
//    /// Builds FSCM history that matches the BASE FSA payload by default.
//    /// This is critical: if history doesn't match expense/hour base attributes,
//    /// the engine will correctly detect FieldChange and will emit reverse+recreate.
//    /// </summary>
//    private static HistoryBundle BuildBaselineHistory(
//        Guid woId,
//        Guid itemLineId,
//        Guid expLineId,
//        Guid hourLineId,
//        decimal itemQty,
//        decimal expQty,
//        decimal hourDuration,
//        decimal itemUnitCost,
//        decimal expUnitCost,
//        decimal hourUnitCost,
//        // ---- Item attributes (base FSA defaults) ----
//        string itemDept = "0344",
//        string itemProd = "119",
//        string itemLineProp = "Bill",
//        // ---- Expense attributes (base FSA defaults) ----
//        string expDept = "0344",
//        string expProd = "003",
//        string expLineProp = "NonBill",
//        // ---- Hour attributes (base FSA defaults) ----
//        string hourDept = "0344",
//        string hourProd = "119",
//        string hourLineProp = "Bill")
//    {
//        var date = new DateTime(2026, 01, 10);

//        var item = new List<FscmJournalLine>
//        {
//            new(JournalType.Item, woId, itemLineId, Quantity: itemQty, CalculatedUnitPrice: itemUnitCost, ExtendedAmount: null,
//                Department: itemDept, ProductLine: itemProd, Warehouse: "344-01", LineProperty: itemLineProp,
//                TransactionDate: date, DataAreaId: "425", SourceJournalNumber: "ITEM-BASE")
//        };

//        var exp = new List<FscmJournalLine>
//        {
//            new(JournalType.Expense, woId, expLineId, Quantity: expQty, CalculatedUnitPrice: expUnitCost, ExtendedAmount: null,
//                Department: expDept, ProductLine: expProd, Warehouse: null, LineProperty: expLineProp,
//                TransactionDate: date, DataAreaId: "425", SourceJournalNumber: "EXP-BASE")
//        };

//        var hour = new List<FscmJournalLine>
//        {
//            // In  normalized history model, hour still uses Quantity for aggregation;
//            // engine maps this against FSA Duration.
//            new(JournalType.Hour, woId, hourLineId, Quantity: hourDuration, CalculatedUnitPrice: hourUnitCost, ExtendedAmount: null,
//                Department: hourDept, ProductLine: hourProd, Warehouse: null, LineProperty: hourLineProp,
//                TransactionDate: date, DataAreaId: "425", SourceJournalNumber: "HOUR-BASE")
//        };

//        return new HistoryBundle(item, exp, hour);
//    }

//    private static (Guid WoId, Guid ItemLineId, Guid ExpLineId, Guid HourLineId, string Json) BuildBasePayload()
//    {
//        var woId = Guid.Parse("ACB75B69-586B-4C56-8544-FE0C39A617B3");
//        var itemLineId = Guid.Parse("6362226B-ACBC-43B8-8ABA-F3D7C9A2FFC2");
//        var expLineId = Guid.Parse("387D4E8C-56C5-4B3E-AC9E-F338FBEE423B");
//        var hourLineId = Guid.Parse("DC11B08B-E309-46E2-862D-E753F883F556");

//        var json = @"
//{
//  ""_request"": {
//    ""WOList"": [
//      {
//        ""Company"": ""425"",
//        ""IsStatusPosted"": ""False"",
//        ""Sub Project Id"": ""425-P0000001-00001"",
//        ""WO Exp Lines"": {
//          ""JournalDescription"": ""WO Expense Jour - 1"",
//          ""JournalLines"": [
//            {
//              ""AccrualLineGUID"": ""{387D4E8C-56C5-4B3E-AC9E-F338FBEE423B}"",
//              ""AccrualLineVersionNumber"": 1,
//              ""Currency"": ""USD"",
//              ""DimensionDepartment"": ""0344"",
//              ""DimensionProduct"": ""003"",
//              ""FSAUnitPrice"": 10,
//              ""JournalLineDescription"": ""WO Expense Jour Line 1"",
//              ""LineNum"": 1,
//              ""LineProperty"": ""NonBill"",
//              ""ProjectCategory"": ""30201(EXPENSE)SERVICES"",
//              ""Quantity"": 2,
//              ""ResourceCompany"": ""425"",
//              ""ResourceId"": ""12045"",
//              ""RPCCustomerProductReference"": ""Testing"",
//              ""RPCDiscountAmount"": 0,
//              ""RPMarkUpAmount"": 0,
//              ""RPCDiscountPercent"": 0,
//              ""RPCMarkupPercent"": 0,
//              ""RPCOverallDiscountAmount"": 0,
//              ""RPCOverallDiscountPercent"": 0,
//              ""RPCSurchargeAmount"": 0,
//              ""RPCSurchargePercent"": 0,
//              ""TransactionDate"": ""/Date(1767052800000)/"",
//              ""UnitAmount"": 30,
//              ""UnitCost"": 10,
//              ""UnitId"": ""ea""
//            }
//          ],
//          ""LineType"": ""Expense""
//        },
//        ""WO Item Lines"": {
//          ""JournalDescription"": ""WO Item Jour - 1"",
//          ""JournalLines"": [
//            {
//              ""AccrualLineGUID"": ""{6362226B-ACBC-43B8-8ABA-F3D7C9A2FFC2}"",
//              ""AccrualLineVersionNumber"": 1,
//              ""Currency"": ""USD"",
//              ""DimensionDepartment"": ""0344"",
//              ""DimensionProduct"": ""119"",
//              ""FSAUnitPrice"": 10,
//              ""ItemId"": ""10002"",
//              ""JournalLineDescription"": ""WO Item Jour Line 1"",
//              ""LineNum"": 1,
//              ""LineProperty"": ""Bill"",
//              ""Location"": """",
//              ""ProductColourId"": """",
//              ""ProductConfigurationId"": """",
//              ""ProductSizeId"": """",
//              ""ProductStyleId"": """",
//              ""ProjectCategory"": ""GENERIC M&R ITEM"",
//              ""Quantity"": 3,
//              ""RPCCustomerProductReference"": ""Testing"",
//              ""RPCDiscountAmount"": 0,
//              ""RPMarkUpAmount"": 0,
//              ""RPCDiscountPercent"": 0,
//              ""RPCMarkupPercent"": 0,
//              ""RPCOverallDiscountAmount"": 0,
//              ""RPCOverallDiscountPercent"": 0,
//              ""RPCSurchargeAmount"": 0,
//              ""RPCSurchargePercent"": 0,
//              ""Site"": ""344"",
//              ""TransactionDate"": ""/Date(1767052800000)/"",
//              ""UnitAmount"": 30,
//              ""UnitCost"": 10,
//              ""UnitId"": ""ea"",
//              ""Warehouse"": ""344-01""
//            }
//          ],
//          ""LineType"": ""Item""
//        },
//        ""WO Hour Lines"": {
//          ""JournalDescription"": ""WO Hour Jour - 1"",
//          ""JournalLines"": [
//            {
//              ""AccrualLineGUID"": ""{DC11B08B-E309-46E2-862D-E753F883F556}"",
//              ""AccrualLineVersionNumber"": 1,
//              ""Currency"": ""USD"",
//              ""DimensionDepartment"": ""0344"",
//              ""DimensionProduct"": ""119"",
//              ""Duration"": 2,
//              ""FSAUnitPrice"": 10,
//              ""JournalLineDescription"": ""WO Hour Jour Line 1"",
//              ""LineNum"": 1,
//              ""LineProperty"": ""Bill"",
//              ""ProjectCategory"": ""30201(SERVICE)SERVICES"",
//              ""ResourceCompany"": ""425"",
//              ""ResourceId"": ""12045"",
//              ""RPCCustomerProductReference"": ""Testing"",
//              ""RPCDiscountAmount"": 0,
//              ""RPMarkUpAmount"": 0,
//              ""RPCDiscountPercent"": 0,
//              ""RPCMarkupPercent"": 0,
//              ""RPCOverallDiscountAmount"": 0,
//              ""RPCOverallDiscountPercent"": 0,
//              ""RPCSurchargeAmount"": 0,
//              ""RPCSurchargePercent"": 0,
//              ""TransactionDate"": ""/Date(1767052800000)/"",
//              ""UnitAmount"": 30,
//              ""UnitCost"": 10
//            }
//          ],
//          ""LineType"": ""Hour""
//        },
//        ""Work order GUID"": ""{ACB75B69-586B-4C56-8544-FE0C39A617B3}"",
//        ""Work order ID"": ""Work order 1""
//      }
//    ]
//  }
//}";
//        return (woId, itemLineId, expLineId, hourLineId, json);
//    }

//    /// <summary>
//    /// Executes mutate fsa line.
//    /// </summary>
//    private static string MutateFsaLine(string json, string sectionKey, Guid accrualLineGuid, Action<JsonObject> mutate)
//    {
//        var root = JsonNode.Parse(json)!.AsObject();
//        var wo = root["_request"]!["WOList"]![0]!.AsObject();
//        var section = wo[sectionKey]!.AsObject();
//        var lines = section["JournalLines"]!.AsArray();

//        foreach (var n in lines)
//        {
//            var obj = n!.AsObject();
//            var s = (obj["WorkOrderLineGuid"] ?? obj["Accrual line GUID"])!.ToString();
//            s = s.Trim().Trim('{', '}');
//            if (Guid.TryParse(s, out var g) && g == accrualLineGuid)
//            {
//                mutate(obj);
//                break;
//            }
//        }

//        return root.ToJsonString(new JsonSerializerOptions { WriteIndented = false });
//    }

//    /// <summary>
//    /// Executes parse.
//    /// </summary>
//    private static JsonObject Parse(string json)
//        => JsonNode.Parse(json)!.AsObject();

//    /// <summary>
//    /// Executes get journal lines.
//    /// </summary>
//    private static List<JsonObject> GetJournalLines(JsonObject delta, Guid woId, string sectionKey)
//    {
//        if (!TryGetJournalLines(delta, woId, sectionKey, out var lines))
//            throw new InvalidOperationException($"WO '{woId}' or section '{sectionKey}' not found in delta payload.");

//        return lines;
//    }

//    /// <summary>
//    /// Executes try get journal lines.
//    /// </summary>
//    private static bool TryGetJournalLines(JsonObject delta, Guid woId, string sectionKey, out List<JsonObject> lines)
//    {
//        lines = new List<JsonObject>();

//        var woArr = delta["_request"]?["WOList"]?.AsArray();
//        if (woArr is null) return false;

//        foreach (var woNode in woArr)
//        {
//            var wo = woNode!.AsObject();

//            var id = GetGuid(wo["WorkorderGUID"] ?? wo["Work order GUID"]);
//            if (id != woId) continue;

//            // If builder omitted the section for unchanged journals, treat as "exists but empty"
//            if (!wo.TryGetPropertyValue(sectionKey, out var secNode) || secNode is null)
//                return true;

//            var section = secNode.AsObject();
//            var arr = section["JournalLines"]?.AsArray();
//            if (arr is null) return true;

//            lines = arr.Select(x => x!.AsObject()).ToList();
//            return true;
//        }

//        return false;
//    }

//    /// <summary>
//    /// Executes get guid.
//    /// </summary>
//    private static Guid GetGuid(JsonNode? n)
//    {
//        if (n is null) return Guid.Empty;
//        var s = n.ToString().Trim().Trim('{', '}');
//        return Guid.TryParse(s, out var g) ? g : Guid.Empty;
//    }

//    /// <summary>
//    /// Executes get decimal.
//    /// </summary>
//    private static decimal GetDecimal(JsonObject o, string key)
//    {
//        var n = o[key] ?? o[key.Replace(" ", "")];
//        n.Should().NotBeNull($"Expected key '{key}' in JSON line.");
//        return decimal.Parse(n!.ToString());
//    }

//    /// <summary>
//    /// Executes get string.
//    /// </summary>
//    private static string GetString(JsonObject o, string key)
//    {
//        var n = o[key] ?? o[key.Replace(" ", "")];
//        n.Should().NotBeNull($"Expected key '{key}' in JSON line.");
//        return n!.ToString();
//    }

//    /// <summary>
//    /// Provides test diag options behavior.
//    /// </summary>
//    private sealed class TestDiagOptions : IAisDiagnosticsOptions
//    {
//        public bool LogPayloadBodies => false;
//        public bool LogMultiWoPayloadBody => false;
//        public bool IncludeDeltaReasonKey => true;
//        public int PayloadSnippetChars => 0;
//        public int PayloadChunkChars => 0;
//    }

//    /// <summary>
//    /// Provides test ais logger behavior.
//    /// </summary>
//    private sealed class TestAisLogger : IAisLogger
//    {
//        /// <summary>
//        /// Executes info async.
//        /// </summary>
//        public Task InfoAsync(string runId, string step, string message, object? data, CancellationToken ct)
//            => Task.CompletedTask;

//        /// <summary>
//        /// Executes warn async.
//        /// </summary>
//        public Task WarnAsync(string runId, string step, string message, object? data, CancellationToken ct)
//            => Task.CompletedTask;

//        /// <summary>
//        /// Executes error async.
//        /// </summary>
//        public Task ErrorAsync(string runId, string step, string message, Exception? ex, object? data, CancellationToken ct)
//            => Task.CompletedTask;
//    }
//}
