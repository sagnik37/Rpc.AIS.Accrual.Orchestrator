using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;

using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain.Validation;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Options;

namespace Rpc.AIS.Accrual.Orchestrator.Infrastructure.Clients.Posting;

public sealed class WoPostingPreparationPipeline : IWoPostingPreparationPipeline
{
    private readonly FscmOptions _fscm;
    private readonly IFsaLineFetcher? _fsaLineFetcher;
    private readonly IWoPayloadNormalizer _normalizer;
    private readonly IWoPayloadShapeGuard _shapeGuard;
    private readonly IWoPayloadValidationEngine _validationEngine;
    private readonly Rpc.AIS.Accrual.Orchestrator.Core.Abstractions.IFscmWoPayloadValidationClient _fscmValidator;
    private readonly IInvalidPayloadNotifier _invalidPayloadNotifier;
    private readonly IWoJournalProjector _projector;
    private readonly PayloadPostingDateAdjuster _dateAdjuster;
    private readonly ILogger<WoPostingPreparationPipeline> _logger;
    private readonly IFscmLegalEntityIntegrationParametersClient _leParams;


    public WoPostingPreparationPipeline(
        FscmOptions fscm,
        IWoPayloadNormalizer normalizer,
        IWoPayloadShapeGuard shapeGuard,
        IWoPayloadValidationEngine validationEngine,
        Rpc.AIS.Accrual.Orchestrator.Core.Abstractions.IFscmWoPayloadValidationClient fscmValidator,
        IInvalidPayloadNotifier invalidPayloadNotifier,
        IWoJournalProjector projector,
        PayloadPostingDateAdjuster dateAdjuster,
        IFsaLineFetcher? fsaLineFetcher,
         IFscmLegalEntityIntegrationParametersClient leParams,
        ILogger<WoPostingPreparationPipeline> logger)
    {
        _fscm = fscm;
        _normalizer = normalizer;
        _shapeGuard = shapeGuard;
        _validationEngine = validationEngine;
        _fscmValidator = fscmValidator;
        _invalidPayloadNotifier = invalidPayloadNotifier;
        _projector = projector;
        _dateAdjuster = dateAdjuster;
        _leParams = leParams;
        _logger = logger;
        _fsaLineFetcher = fsaLineFetcher;
    }

    public Task<PreparedWoPosting> PrepareValidatedAsync(
        RunContext ctx,
        JournalType journalType,
        string woPayloadJson,
        string? validationResponseRaw,
        CancellationToken ct)
    {
        // Simply delegate to PrepareAsync (keeps compatibility with  interface)
        return PrepareAsync(ctx, journalType, woPayloadJson, ct);
    }

    public async Task<PreparedWoPosting> PrepareAsync(
        RunContext ctx,
        JournalType journalType,
        string woPayloadJson,
        CancellationToken ct)
    {
        var normalized = _normalizer.NormalizeToWoListKey(woPayloadJson);
        _shapeGuard.EnsureValidShapeOrThrow(normalized);

        // IMPORTANT:
        // All downstream work (validation, projection, date adjustment) operates on the normalized payload.
        // Therefore journal name injection must be applied to the normalized JSON, not the original raw input.
        normalized = await InjectJournalNamesIfMissingAsync(ctx, journalType, normalized, ct);

        if (_fsaLineFetcher is not null)
            normalized = await EnrichMissingOperationsDatesFromFsaAsync(ctx, normalized, ct);

        var local = await _validationEngine.ValidateAndFilterAsync(ctx, journalType, normalized, ct);

        if (local.Failures.Count > 0)
        {
            await _invalidPayloadNotifier.NotifyAsync(
                ctx,
                journalType,
                local.Failures,
                local.WorkOrdersBefore,
                local.WorkOrdersAfter,
                ct);
        }

        var filtered = local.FilteredPayloadJson;

        var proj = _projector.Project(filtered, journalType);

        var adjustedProjected =
            await _dateAdjuster.AdjustAsync(ctx, proj.PayloadJson, ct);

        proj = proj with { PayloadJson = adjustedProjected };

        var (retryWo, retryLines) = CountRetryable(local.RetryableFailures);

        return new PreparedWoPosting(
            journalType,
            normalized,
            proj.PayloadJson,
            proj.WorkOrdersBefore,
            proj.WorkOrdersAfter,
            proj.RemovedDueToMissingOrEmptySection,
            ToPostErrors(local.Failures),
            null,
            retryWo,
            retryLines,
            local.RetryablePayloadJson);
    }

    // -----------------------------------------------------
    //  FS OPS DATE ENRICHMENT (NO rpc_operationsdate output)
    // -----------------------------------------------------
    private async Task<string> InjectJournalNamesIfMissingAsync(
    RunContext ctx,
    JournalType journalType,
    string payloadJson,
    CancellationToken ct)
    {
        var mutableDoc = JsonNode.Parse(payloadJson);
        if (mutableDoc is null)
            return payloadJson;

        var woArray = mutableDoc["_request"]?["WOList"]?.AsArray();
        if (woArray is null || woArray.Count == 0)
            return payloadJson;

        // Collect distinct companies
        var companies = woArray
            .Select(wo => wo?["Company"]?.GetValue<string>())
            .Where(c => !string.IsNullOrWhiteSpace(c))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (companies.Count == 0)
            return payloadJson;

        // Fetch journal name params per company once
        var journalMap = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        foreach (var company in companies)
        {
            var result = await _leParams.GetJournalNamesAsync(ctx, company!, ct).ConfigureAwait(false);
            if (result is not null)
                journalMap[company!] = result;
        }

        static bool HasLines(JsonObject wo, string sectionKey)
        {
            var section = wo[sectionKey] as JsonObject;
            var lines = section?["JournalLines"] as JsonArray;
            return lines is not null && lines.Count > 0;
        }

        static bool IsBlank(JsonObject wo, string sectionKey)
        {
            var section = wo[sectionKey] as JsonObject;
            var val = section?["JournalName"]?.GetValue<string>();
            return string.IsNullOrWhiteSpace(val);
        }

        foreach (var woNode in woArray.OfType<JsonObject>())
        {
            var company = woNode["Company"]?.GetValue<string>();
            if (string.IsNullOrWhiteSpace(company))
                continue;

            if (!journalMap.TryGetValue(company!, out var resultObj))
                continue;

            dynamic result = resultObj;

            switch (journalType)
            {
                case JournalType.Item:
                    if (HasLines(woNode, "WOItemLines") && IsBlank(woNode, "WOItemLines"))
                    {
                        woNode["WOItemLines"]!["JournalName"] = (string?)result.InventJournalNameId ?? string.Empty;
                    }
                    break;

                case JournalType.Expense:
                    if (HasLines(woNode, "WOExpLines") && IsBlank(woNode, "WOExpLines"))
                    {
                        woNode["WOExpLines"]!["JournalName"] = (string?)result.ExpenseJournalNameId ?? string.Empty;
                    }
                    break;

                case JournalType.Hour:
                    if (HasLines(woNode, "WOHourLines") && IsBlank(woNode, "WOHourLines"))
                    {
                        woNode["WOHourLines"]!["JournalName"] = (string?)result.HourJournalNameId ?? string.Empty;
                    }
                    break;
            }
        }

        return mutableDoc.ToJsonString(new JsonSerializerOptions { WriteIndented = false });
    }
    private async Task<string> EnrichMissingOperationsDatesFromFsaAsync(
        RunContext ctx,
        string woPayloadJson,
        CancellationToken ct)
    {
        using var doc = JsonDocument.Parse(woPayloadJson);

        if (!doc.RootElement.TryGetProperty("_request", out var req) ||
            !req.TryGetProperty("WOList", out var woList))
            return woPayloadJson;

        var workOrderIds = woList.EnumerateArray()
            .Select(x => x.GetProperty("WorkOrderGUID").GetString())
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct()
            .ToList();

        if (workOrderIds.Count == 0)
            return woPayloadJson;

        var products = await _fsaLineFetcher!
            .GetWorkOrderProductsAsync(ctx, workOrderIds!, ct);

        var services = await _fsaLineFetcher!
            .GetWorkOrderServicesAsync(ctx, workOrderIds!, ct);

        var map = BuildOpsMap(products, "msdyn_workorderproductid");
        foreach (var kv in BuildOpsMap(services, "msdyn_workorderserviceid"))
            map[kv.Key] = kv.Value;

        var rootNode = JsonNode.Parse(woPayloadJson);
        var nodeWoList = rootNode?["_request"]?["WOList"]?.AsArray();
        if (nodeWoList is null) return woPayloadJson;

        foreach (var wo in nodeWoList)
        {
            InjectOpsDate(wo, "WOItemLines", map);
            InjectOpsDate(wo, "WOHourLines", map);
            InjectOpsDate(wo, "WOExpLines", map);
        }

        return rootNode!.ToJsonString();
    }

    private static Dictionary<Guid, string> BuildOpsMap(JsonDocument doc, string idField)
    {
        var dict = new Dictionary<Guid, string>();

        if (!doc.RootElement.TryGetProperty("value", out var arr))
            return dict;

        foreach (var row in arr.EnumerateArray())
        {
            if (!row.TryGetProperty(idField, out var idEl)) continue;
            if (!Guid.TryParse(idEl.GetString()?.Trim('{', '}'), out var id)) continue;

            if (row.TryGetProperty("rpc_operationsdate", out var dateEl))
            {
                var date = dateEl.GetString();
                if (!string.IsNullOrWhiteSpace(date))
                    dict[id] = date!;
            }
        }

        return dict;
    }

    private static void InjectOpsDate(
        JsonNode? woNode,
        string sectionName,
        Dictionary<Guid, string> map)
    {
        var lines = woNode?[sectionName]?["JournalLines"]?.AsArray();
        if (lines is null) return;

        foreach (var ln in lines)
        {
            var obj = ln?.AsObject();
            if (obj is null) continue;

            var rawGuid = obj["WorkOrderLineGuid"]?.GetValue<string>();
            if (!Guid.TryParse(rawGuid?.Trim('{', '}'), out var id)) continue;

            if (!map.TryGetValue(id, out var rawDate)) continue;

            var literal = NormalizeDate(rawDate);
            if (literal is null) continue;

            // REMOVE rpc_operationsdate if exists
            obj.Remove("rpc_operationsdate");
            obj.Remove("rpc_OperationsDate");

            if (string.IsNullOrWhiteSpace(obj["OperationDate"]?.GetValue<string>()))
                obj["OperationDate"] = literal;

            obj.Remove("RPCWorkingDate");

            if (string.IsNullOrWhiteSpace(obj["TransactionDate"]?.GetValue<string>()))
                obj["TransactionDate"] = literal;
        }
    }

    private static string? NormalizeDate(string raw)
    {
        if (!DateTimeOffset.TryParse(raw, out var dto))
            return null;

        var utc = new DateTime(dto.Year, dto.Month, dto.Day, 0, 0, 0, DateTimeKind.Utc);
        var ms = new DateTimeOffset(utc).ToUnixTimeMilliseconds();
        return $"/Date({ms})/";
    }

    // Replace these helpers at the bottom of WoPostingPreparationPipeline.cs

    private static (int RetryableWorkOrders, int RetryableLines) CountRetryable(
    IReadOnlyList<WoPayloadValidationFailure>? failures)
    {
        if (failures is null || failures.Count == 0)
            return (0, 0);

        // local.RetryableFailures already contains only retryables in  design.
        // Count distinct WorkOrderNumber as "work orders", and total failures as "lines".
        var retryableWorkOrders = failures
            .Select(f => f.WorkOrderNumber)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Count();

        var retryableLines = failures.Count;
        return (retryableWorkOrders, retryableLines);
    }

    private static List<PostError> ToPostErrors(IReadOnlyList<WoPayloadValidationFailure>? failures)
    {
        if (failures is null || failures.Count == 0)
            return new List<PostError>(0);

        var list = new List<PostError>(failures.Count);

        foreach (var f in failures)
        {
            var woId = f.WorkOrderNumber ?? string.Empty;

            string? lineGuid = f.WorkOrderLineGuid.HasValue
                ? $"{{{f.WorkOrderLineGuid.Value}}}"
                : null;

            // : use positional arguments ONLY
            list.Add(new PostError(
                woId,                     // string WorkOrderId
                f.Message ?? "Validation failure",  // string Message
                lineGuid,                 // string? WorkOrderLineGuid
                null,                     // string? SubProjectId
                false,                    // bool IsRetryable
                null                      // string? StagingId
            ));
        }

        return list;
    }



}
