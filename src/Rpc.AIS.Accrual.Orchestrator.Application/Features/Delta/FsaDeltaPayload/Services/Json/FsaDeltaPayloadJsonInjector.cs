// File: src/Rpc.AIS.Accrual.Orchestrator.Core/Services/FsaDeltaPayload/Json/FsaDeltaPayloadJsonInjector.cs

// File: .../Core/UseCases/FsaDeltaPayload/*
//
// SOLID refactor:
// - Moves delta payload orchestration into Core (UseCase layer) and splits the orchestrator into partials.
// - Functions layer becomes a thin adapter.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using static Rpc.AIS.Accrual.Orchestrator.Core.Services.FsaDeltaPayload.FsaDeltaPayloadJsonUtil;
using Microsoft.Extensions.Logging;
using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Core.Options;
using Rpc.AIS.Accrual.Orchestrator.Core.Services;

namespace Rpc.AIS.Accrual.Orchestrator.Core.Services.FsaDeltaPayload;

internal static class FsaDeltaPayloadJsonInjector
{
    internal static void CopyRootWithInjectionAndStats(
        JsonElement root,
        Utf8JsonWriter w,
        Dictionary<Guid, FsLineExtras> extrasByLineGuid,
        List<WoEnrichmentStats> stats)
    {
        if (root.ValueKind != JsonValueKind.Object)
        {
            root.WriteTo(w);
            return;
        }

        w.WriteStartObject();

        foreach (var p in root.EnumerateObject())
        {
            if (p.NameEquals("_request") && p.Value.ValueKind == JsonValueKind.Object)
            {
                w.WritePropertyName(p.Name);
                CopyRequestWithInjectionAndStats(p.Value, w, extrasByLineGuid, stats);
            }
            else
            {
                w.WritePropertyName(p.Name);
                p.Value.WriteTo(w);
            }
        }

        w.WriteEndObject();
    }

    private static void CopyRequestWithInjectionAndStats(
        JsonElement req,
        Utf8JsonWriter w,
        Dictionary<Guid, FsLineExtras> extrasByLineGuid,
        List<WoEnrichmentStats> stats)
    {
        w.WriteStartObject();

        foreach (var p in req.EnumerateObject())
        {
            if (p.NameEquals("WOList") && p.Value.ValueKind == JsonValueKind.Array)
            {
                w.WritePropertyName("WOList");
                w.WriteStartArray();

                foreach (var wo in p.Value.EnumerateArray())
                {
                    var s = new WoEnrichmentStats
                    {
                        WorkorderId = ReadWoIdText(wo),
                        WorkorderGuidRaw = ReadWoGuidText(wo),
                        Company = wo.TryGetProperty("Company", out var comp) && comp.ValueKind == JsonValueKind.String ? comp.GetString() : null
                    };

                    CopyWoWithInjectionAndStats(wo, w, extrasByLineGuid, s);
                    stats.Add(s);
                }

                w.WriteEndArray();
            }
            else
            {
                w.WritePropertyName(p.Name);
                p.Value.WriteTo(w);
            }
        }

        w.WriteEndObject();
    }

    private static void CopyWoWithInjectionAndStats(
        JsonElement wo,
        Utf8JsonWriter w,
        Dictionary<Guid, FsLineExtras> extrasByLineGuid,
        WoEnrichmentStats s)
    {
        w.WriteStartObject();

        foreach (var p in wo.EnumerateObject())
        {
            if (p.NameEquals("WOExpLines") || p.NameEquals("WOItemLines") || p.NameEquals("WOHourLines"))
            {
                w.WritePropertyName(p.Name);

                if (p.Value.ValueKind == JsonValueKind.Object)
                {
                    var group = p.Name;
                    CopyWoLinesBlockWithInjectionAndStats(p.Value, w, extrasByLineGuid, s, group);
                }
                else
                {
                    p.Value.WriteTo(w);
                }

                continue;
            }

            w.WritePropertyName(p.Name);
            p.Value.WriteTo(w);
        }

        w.WriteEndObject();
    }

    private static void CopyWoLinesBlockWithInjectionAndStats(
        JsonElement block,
        Utf8JsonWriter w,
        Dictionary<Guid, FsLineExtras> extrasByLineGuid,
        WoEnrichmentStats s,
        string groupName)
    {
        w.WriteStartObject();

        foreach (var p in block.EnumerateObject())
        {
            if (p.NameEquals("JournalLines") && p.Value.ValueKind == JsonValueKind.Array)
            {
                w.WritePropertyName("JournalLines");
                w.WriteStartArray();

                foreach (var line in p.Value.EnumerateArray())
                {
                    var enrichedThisLine = CopyJournalLineWithInjectionAndStats(line, w, extrasByLineGuid, s);

                    if (enrichedThisLine)
                    {
                        s.EnrichedLinesTotal++;

                        if (groupName == "WO Hour Lines") s.EnrichedHourLines++;
                        else if (groupName == "WO Exp Lines") s.EnrichedExpLines++;
                        else if (groupName == "WO Item Lines") s.EnrichedItemLines++;
                    }
                }

                w.WriteEndArray();
                continue;
            }

            w.WritePropertyName(p.Name);
            p.Value.WriteTo(w);
        }

        w.WriteEndObject();
    }

    private static bool CopyJournalLineWithInjectionAndStats(
        JsonElement line,
        Utf8JsonWriter w,
        Dictionary<Guid, FsLineExtras> extrasByLineGuid,
        WoEnrichmentStats s)
    {
        Guid? lineGuid = null;

        // Support common variants
        if (line.TryGetProperty("WorkOrderLineGuid", out var g1) && g1.ValueKind == JsonValueKind.String)
            lineGuid = ParseGuidLoose(g1.GetString());
        else if (line.TryGetProperty("WorkOrderLineGUID", out var g2) && g2.ValueKind == JsonValueKind.String)
            lineGuid = ParseGuidLoose(g2.GetString());
        else if (line.TryGetProperty("WorkOrderLineId", out var g3) && g3.ValueKind == JsonValueKind.String)
            lineGuid = ParseGuidLoose(g3.GetString());

        var hasExtras = false;
        FsLineExtras extras = default;

        if (lineGuid.HasValue && extrasByLineGuid.TryGetValue(lineGuid.Value, out var found))
        {
            extras = found;
            hasExtras = true;
        }

        // : rpc_operationsdate is an FS ingestion detail. It must NOT be emitted in the outbound payload.
        // We only use it (or the enriched extras) to stamp the canonical fields:
        //   - RPCWorkingDate (original operations date)
        //   - TransactionDate (effective posting date; adjuster may shift if CLOSED)
        var opsRaw = TryGetString(line, "rpc_operationsdate") ?? TryGetString(line, "rpc_OperationsDate");

        var anyFilled = false;

        if (string.IsNullOrWhiteSpace(opsRaw) && hasExtras && !string.IsNullOrWhiteSpace(extras.OperationsDate))
        {
            opsRaw = extras.OperationsDate;
            anyFilled = true;
            s.MarkFilledOperationsDate();
        }

        var opsLiteral = NormalizeToFscmDateLiteralOrNull(opsRaw);

        var hasRpcWorkingDate = false;
        var hasTransactionDate = false;

        w.WriteStartObject();

        foreach (var p in line.EnumerateObject())
        {
            // : Company must NOT exist in JournalLines
            if (p.NameEquals("Company"))
                continue;

            // : Sub Project Id must NOT exist in JournalLines (handle common spellings)
            if (p.NameEquals("SubProjectId") || p.NameEquals("Sub Project Id") || p.NameEquals("SubProjectID"))
                continue;

            // Do not emit FS-only operations date field
            if (p.NameEquals("rpc_operationsdate") || p.NameEquals("rpc_OperationsDate"))
                continue;

            // Normalize legacy transactionDate casing into TransactionDate
            if (p.NameEquals("transactionDate"))
            {
                var existing = p.Value.ValueKind == JsonValueKind.String ? p.Value.GetString() : null;
                if (!string.IsNullOrWhiteSpace(existing))
                {
                    w.WriteString("TransactionDate", existing);
                    hasTransactionDate = true;
                }

                continue;
            }

            if (p.NameEquals("RPCWorkingDate"))
            {
                var existing = p.Value.ValueKind == JsonValueKind.String ? p.Value.GetString() : null;

                w.WritePropertyName("RPCWorkingDate");
                p.Value.WriteTo(w);

                if (!string.IsNullOrWhiteSpace(existing))
                    hasRpcWorkingDate = true;

                continue;
            }

            if (p.NameEquals("TransactionDate"))
            {
                var existing = p.Value.ValueKind == JsonValueKind.String ? p.Value.GetString() : null;

                w.WritePropertyName("TransactionDate");
                p.Value.WriteTo(w);

                if (!string.IsNullOrWhiteSpace(existing))
                    hasTransactionDate = true;

                continue;
            }

            if (p.NameEquals("Currency"))
            {
                var existing = p.Value.ValueKind == JsonValueKind.String ? p.Value.GetString() : null;
                var shouldFill = string.IsNullOrWhiteSpace(existing) && hasExtras && !string.IsNullOrWhiteSpace(extras.Currency);
                var final = shouldFill ? extras.Currency : existing;

                w.WritePropertyName("Currency");
                if (final is null) w.WriteNullValue(); else w.WriteStringValue(final);

                if (shouldFill) { anyFilled = true; s.FilledCurrency++; }
                continue;
            }

            if (p.NameEquals("ResourceId"))
            {
                var existing = p.Value.ValueKind == JsonValueKind.String ? p.Value.GetString() : null;
                var shouldFill = string.IsNullOrWhiteSpace(existing) && hasExtras && !string.IsNullOrWhiteSpace(extras.WorkerNumber);
                var final = shouldFill ? extras.WorkerNumber : existing;

                w.WritePropertyName("ResourceId");
                if (final is null) w.WriteNullValue(); else w.WriteStringValue(final);

                if (shouldFill) { anyFilled = true; s.FilledResourceId++; }
                continue;
            }

            if (p.NameEquals("Warehouse"))
            {
                var existing = p.Value.ValueKind == JsonValueKind.String ? p.Value.GetString() : null;

                var shouldFill =
                    hasExtras
                    && !string.IsNullOrWhiteSpace(extras.WarehouseIdentifier)
                    && (string.IsNullOrWhiteSpace(existing)
                        || !string.Equals(existing, extras.WarehouseIdentifier, StringComparison.Ordinal));

                var final = shouldFill ? extras.WarehouseIdentifier : existing;

                w.WritePropertyName("Warehouse");
                if (final is null) w.WriteNullValue(); else w.WriteStringValue(final);

                if (shouldFill) { anyFilled = true; s.FilledWarehouse++; }
                continue;
            }

            if (p.NameEquals("Site"))
            {
                var existing = p.Value.ValueKind == JsonValueKind.String ? p.Value.GetString() : null;
                var shouldFill = string.IsNullOrWhiteSpace(existing) && hasExtras && !string.IsNullOrWhiteSpace(extras.SiteId);
                var final = shouldFill ? extras.SiteId : existing;

                w.WritePropertyName("Site");
                if (final is null) w.WriteNullValue(); else w.WriteStringValue(final);

                if (shouldFill) { anyFilled = true; s.FilledSite++; }
                continue;
            }

            if (p.NameEquals("Line num"))
            {
                int? existingNum = null;

                if (p.Value.ValueKind == JsonValueKind.Number && p.Value.TryGetInt32(out var n))
                    existingNum = n;
                else if (p.Value.ValueKind == JsonValueKind.String && int.TryParse(p.Value.GetString(), out var n2))
                    existingNum = n2;

                var shouldFill = (!existingNum.HasValue) && hasExtras && extras.LineNum.HasValue;
                var final = shouldFill ? extras.LineNum : existingNum;

                w.WritePropertyName("Line num");
                if (final.HasValue) w.WriteNumberValue(final.Value);
                else w.WriteNullValue();

                if (shouldFill) { anyFilled = true; s.FilledLineNum++; }
                continue;
            }

            w.WritePropertyName(p.Name);
            p.Value.WriteTo(w);
        }

        if (hasExtras)
        {
            if (!HasProp(line, "Currency") && !string.IsNullOrWhiteSpace(extras.Currency))
            {
                w.WriteString("Currency", extras.Currency);
                anyFilled = true; s.FilledCurrency++;
            }

            if (!HasProp(line, "ResourceId") && !string.IsNullOrWhiteSpace(extras.WorkerNumber))
            {
                w.WriteString("ResourceId", extras.WorkerNumber);
                anyFilled = true; s.FilledResourceId++;
            }

            if (!HasProp(line, "Warehouse") && !string.IsNullOrWhiteSpace(extras.WarehouseIdentifier))
            {
                w.WriteString("Warehouse", extras.WarehouseIdentifier);
                anyFilled = true; s.FilledWarehouse++;
            }

            if (!HasProp(line, "Site") && !string.IsNullOrWhiteSpace(extras.SiteId))
            {
                w.WriteString("Site", extras.SiteId);
                anyFilled = true; s.FilledSite++;
            }

            if (!HasProp(line, "Line num") && extras.LineNum.HasValue)
            {
                w.WriteNumber("Line num", extras.LineNum.Value);
                anyFilled = true; s.FilledLineNum++;
            }
        }

        // Stamp canonical dates from ops date (without emitting rpc_operationsdate)
        if (!string.IsNullOrWhiteSpace(opsLiteral))
        {
            if (!hasRpcWorkingDate && !HasProp(line, "OperationDate"))
            {
                w.WriteString("OperationDate", opsLiteral);
                anyFilled = true;
            }

            if (!hasTransactionDate && !HasProp(line, "TransactionDate") && !HasProp(line, "transactionDate"))
            {
                w.WriteString("TransactionDate", opsLiteral);
                anyFilled = true;
            }
        }

        w.WriteEndObject();
        return anyFilled;
    }

    private static string? NormalizeToFscmDateLiteralOrNull(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        var s = raw.Trim();

        if (s.StartsWith("/Date(", StringComparison.OrdinalIgnoreCase))
            return s;

        if (!DateTimeOffset.TryParse(
                s,
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal,
                out var dto))
        {
            return null;
        }

        var dt = new DateTime(dto.UtcDateTime.Year, dto.UtcDateTime.Month, dto.UtcDateTime.Day, 0, 0, 0, DateTimeKind.Utc);
        var ms = new DateTimeOffset(dt).ToUnixTimeMilliseconds();
        return $"/Date({ms})/";
    }

    private static string? TryGetString(JsonElement obj, string prop)
    {
        if (!obj.TryGetProperty(prop, out var el)) return null;
        if (el.ValueKind == JsonValueKind.String) return el.GetString();
        return null;
    }

    private static string? ReadWoIdText(JsonElement wo)
    {
        if (wo.TryGetProperty("Work order ID", out var id1) && id1.ValueKind == JsonValueKind.String)
            return id1.GetString();

        if (wo.TryGetProperty("WorkOrderID", out var id2) && id2.ValueKind == JsonValueKind.String)
            return id2.GetString();

        return null;
    }

    private static string? ReadWoGuidText(JsonElement wo)
    {
        if (wo.TryGetProperty("WorkOrderGUID", out var id1) && id1.ValueKind == JsonValueKind.String)
            return id1.GetString();

        if (wo.TryGetProperty("WorkOrderGuid", out var id2) && id2.ValueKind == JsonValueKind.String)
            return id2.GetString();

        return null;
    }

    private static bool HasProp(JsonElement obj, string name)
    {
        if (obj.ValueKind != JsonValueKind.Object) return false;
        foreach (var p in obj.EnumerateObject())
            if (string.Equals(p.Name, name, StringComparison.OrdinalIgnoreCase))
                return true;
        return false;
    }
}
