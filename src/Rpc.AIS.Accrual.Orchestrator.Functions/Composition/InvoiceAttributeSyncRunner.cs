using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain.InvoiceAttributes;
using Rpc.AIS.Accrual.Orchestrator.Core.Services.InvoiceAttributes;

namespace Rpc.AIS.Accrual.Orchestrator.Functions.Services;

/// <summary>
/// Builds invoice attribute delta (FS -> FSCM) using Work Order header fields and FSCM snapshots,
/// and injects the computed InvoiceAttributes into the posting payload.
/// 
/// :
/// - This component MUST NOT call FSCM "update" endpoints directly.
/// - Posting pipeline (FscmJournalPoster) is the single place that performs the actual update,
///   and it must happen AFTER successful journal posting (hard dependent).
///
/// Rules:
/// - FS is system-of-record: FS overrides FSCM.
/// - If FS value is null but FSCM has value, AIS clears FSCM (sets null).
/// - Outbound payload uses FSCM attribute names (via Fs->Fscm mapping) OR fixed FSCM names for derived comparisons.
///
/// Mapping source of truth (for standard fields):
/// - FSCM AttributeTypeGlobalAttributes (Name, FSA)
///
/// Option B behavior:
/// - For each (Company, SubProjectId), FSCM definitions + current values are fetched ONCE and cached in-memory for the run.
/// - Comparisons/delta are performed against the in-memory snapshot.
/// </summary>
public sealed class InvoiceAttributeSyncRunner
{
    private const string ODataFormattedSuffix = "@OData.Community.Display.V1.FormattedValue";

    // Standard FS keys mapped via IFscmGlobalAttributeMappingClient (FSA -> FSCM schema name).
    // : these are logical FS names (non-underscore), because the mapping table uses those.
    private static readonly string[] FsInvoiceKeys =
    [
        "rpc_wellnametext",
        "rpc_wellnumber",
        "rpc_area",
        "rpc_representativeid",
        "rpc_welllocale",
        "rpc_manufacturingplant",
        "rpc_afe_wbsnumber",
        "rpc_customersignature",
        "rpc_leasename",
        "rpc_ocsgnumber",
        "rpc_rig",
        "rpc_pricelist",
        "rpc_welllatitude",
        "rpc_welllongitude",
        "rpc_countrylookup",
        "rpc_countylookup",
        "rpc_statelookup",
        "rpc_invoicenotesinternal",
        "rpc_declinedtosignreason",
        "rpc_invoicenotesexternal",

        // Used to derive the two fixed FSCM comparisons below.
        "rpc_worktypelookup"
    ];

    // Derived comparisons (fixed FSCM schema names).
    private const string FscmAttr_WorkType = "FSA Work Type";
    private const string FscmAttr_WellAge = "FSA Well Age";
    private const string FscmAttr_TaxabilityType = "FSA Taxability Type";

    private readonly ILogger<InvoiceAttributeSyncRunner> _log;
    private readonly IFsaLineFetcher _fsa;
    private readonly IFscmInvoiceAttributesClient _fscmReadOnly; // read-only usage: definitions + snapshot
    private readonly IFscmGlobalAttributeMappingClient _attrMap;

    public InvoiceAttributeSyncRunner(
        ILogger<InvoiceAttributeSyncRunner> log,
        IFsaLineFetcher fsa,
        IFscmInvoiceAttributesClient fscmReadOnly,
        IFscmGlobalAttributeMappingClient attrMap)
    {
        _log = log ?? throw new ArgumentNullException(nameof(log));
        _fsa = fsa ?? throw new ArgumentNullException(nameof(fsa));
        _fscmReadOnly = fscmReadOnly ?? throw new ArgumentNullException(nameof(fscmReadOnly));
        _attrMap = attrMap ?? throw new ArgumentNullException(nameof(attrMap));
    }

    public sealed record EnrichResult(
        bool Attempted,
        bool Success,
        int WorkOrdersWithInvoiceAttributes,
        int TotalAttributePairs,
        string Note,
        string PostingPayloadJson);

    private sealed record WoCtx(Guid WoGuid, string WorkOrderId, string Company, string SubProjectId);

    private sealed record SubProjectKey(string Company, string SubProjectId);

    /// <summary>
    /// Enriches the given posting payload JSON by injecting computed InvoiceAttributes onto each WO element
    /// (only for WOs that have a delta vs FSCM snapshot).
    ///
    /// Orchestrators/endpoints must call FSCM update endpoint AFTER successful journal post using InvoiceAttributesUpdateRunner.
    /// </summary>
    public async Task<EnrichResult> EnrichPostingPayloadAsync(RunContext ctx, string postingPayloadJson, CancellationToken ct)
    {
        if (ctx is null) throw new ArgumentNullException(nameof(ctx));

        if (string.IsNullOrWhiteSpace(postingPayloadJson))
            return new EnrichResult(false, true, 0, 0, "Empty payload; invoice attribute enrichment skipped.", postingPayloadJson);

        if (!TryReadWorkOrders(postingPayloadJson, out var workOrders) || workOrders.Count == 0)
            return new EnrichResult(false, true, 0, 0, "Could not read WorkOrders (Company/SubProjectId/WorkOrderGUID) from payload; invoice attribute enrichment skipped.", postingPayloadJson);

        // Fetch WO headers from Dataverse (once).
        var woGuidStrings = workOrders.Select(w => w.WoGuid.ToString("D")).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        using var woHeadersDoc = await _fsa.GetWorkOrdersAsync(ctx, woGuidStrings, ct).ConfigureAwait(false);

        var woHeaderById = IndexWorkOrderHeaders(woHeadersDoc);
        if (woHeaderById.Count == 0)
            return new EnrichResult(false, true, 0, 0, "No work order headers returned by Dataverse; invoice attribute enrichment skipped.", postingPayloadJson);

        // Fetch WOP/WOS ONCE (taxability logic).
        using var wopDoc = await _fsa.GetWorkOrderProductsAsync(ctx, woGuidStrings, ct).ConfigureAwait(false);
        using var wosDoc = await _fsa.GetWorkOrderServicesAsync(ctx, woGuidStrings, ct).ConfigureAwait(false);

        var taxabilityByWo = BuildTaxabilityTypeByWorkOrder(wopDoc, wosDoc);

        // Mapping source of truth for standard keys.
        var mapping = await _attrMap.GetFsToFscmNameMapAsync(ctx, ct).ConfigureAwait(false);
        if (mapping is null || mapping.Count == 0)
        {
            // Keep run alive; we can still proceed with derived fixed FSCM names.
            _log.LogWarning(
                "InvoiceAttributes.Enrich: Fs->Fscm mapping from FSCM is empty. Proceeding with derived-only attributes. RunId={RunId} CorrelationId={CorrelationId}",
                ctx.RunId, ctx.CorrelationId);
            mapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        // Option B cache: fetch FSCM data once per subproject for this run.
        var defsCache = new Dictionary<SubProjectKey, HashSet<string>>(capacity: 8);
        var currentCache = new Dictionary<SubProjectKey, Dictionary<string, string?>>(capacity: 8);

        // For each subproject: compute delta once (using first WO as source), then apply to all WOs in that group.
        var groups = workOrders
            .GroupBy(w => new SubProjectKey(w.Company, w.SubProjectId))
            .ToList();

        // Computed invoice updates per WO GUID (in posting payload) => list of pairs to inject.
        var updatesByWoGuid = new Dictionary<Guid, IReadOnlyList<InvoiceAttributePair>>();

        var totalPairs = 0;
        var woWithAttrs = 0;

        foreach (var g in groups)
        {
            ct.ThrowIfCancellationRequested();

            var key = g.Key;
            var source = g.First(); // existing behavior preserved

            if (!woHeaderById.TryGetValue(source.WoGuid, out var woHeader))
            {
                _log.LogWarning(
                    "InvoiceAttributes.Enrich: WO header missing for WorkOrderId={WorkOrderId} WorkOrderGuid={WorkOrderGuid}.",
                    source.WorkOrderId, source.WoGuid);
                continue;
            }

            // 1) Extract FS attributes (raw FS logical keys).
            var fsAttrsRaw = ExtractFsAttributes(woHeader);

            // 2) Derived comparisons: Work Type + Well Age.
            AddWorkTypeAndWellAgeDerived(woHeader, fsAttrsRaw);

            // 3) Derived comparison: Taxability type (from WOP/WOS).
            if (taxabilityByWo.TryGetValue(source.WoGuid, out var taxability))
                fsAttrsRaw["rpc_taxabilitytype"] = taxability;

            if (fsAttrsRaw.Count == 0)
            {
                _log.LogInformation("InvoiceAttributes.Enrich: No invoice attributes present for WorkOrderId={WorkOrderId}; skipped.", source.WorkOrderId);
                continue;
            }

            // 4) Map FS keys to FSCM schema names.
            var fsAttrsMapped = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);

            foreach (var kvp in fsAttrsRaw)
            {
                if (kvp.Key.Equals("rpc_taxabilitytype", StringComparison.OrdinalIgnoreCase))
                {
                    fsAttrsMapped[FscmAttr_TaxabilityType] = kvp.Value;
                    continue;
                }

                if (kvp.Key.Equals(FscmAttr_WorkType, StringComparison.OrdinalIgnoreCase) ||
                    kvp.Key.Equals(FscmAttr_WellAge, StringComparison.OrdinalIgnoreCase))
                {
                    fsAttrsMapped[kvp.Key] = kvp.Value;
                    continue;
                }

                if (!mapping.TryGetValue(kvp.Key, out var mappedName) || string.IsNullOrWhiteSpace(mappedName))
                    continue;

                fsAttrsMapped[mappedName] = kvp.Value;
            }

            if (fsAttrsMapped.Count == 0)
            {
                _log.LogInformation(
                    "InvoiceAttributes.Enrich: After mapping, no attributes remain for SubProjectId={SubProjectId} WorkOrderId={WorkOrderId}; skipped.",
                    key.SubProjectId, source.WorkOrderId);
                continue;
            }

            // 5) FSCM definitions cached per subproject.
            var active = await GetActiveDefinitionsAsync(ctx, key, defsCache, ct).ConfigureAwait(false);

            // 6) Filter only active attrs (if definitions empty => allow-all).
            var allowed = (active.Count == 0)
                ? new Dictionary<string, string?>(fsAttrsMapped, StringComparer.OrdinalIgnoreCase)
                : fsAttrsMapped
                    .Where(kvp => active.Contains(kvp.Key))
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value, StringComparer.OrdinalIgnoreCase);

            if (allowed.Count == 0)
            {
                _log.LogInformation("InvoiceAttributes.Enrich: No mapped attributes are active in FSCM for SubProjectId={SubProjectId}; skipped.", key.SubProjectId);
                continue;
            }

            // 7) FSCM current snapshot cached per subproject.
            var currentDict = await GetCurrentSnapshotAsync(ctx, key, allowed.Keys, currentCache, ct).ConfigureAwait(false);

            // 8) Build delta list.
            var identityMap = allowed.Keys.ToDictionary(k => k, k => k, StringComparer.OrdinalIgnoreCase);
            var delta = InvoiceAttributeDeltaBuilder.BuildDelta(allowed, identityMap, currentDict);

            if (delta.Updates.Count == 0)
            {
                _log.LogInformation("InvoiceAttributes.Enrich: No changes for SubProjectId={SubProjectId}.", key.SubProjectId);
                continue;
            }

            // : Do NOT call FSCM update here.
            // Instead: inject delta.Updates into posting payload (InvoiceAttributes array) for all WOs in this subproject group.
            ApplyUpdatesToCurrentSnapshot(key, delta.Updates, currentCache);

            foreach (var wo in g)
                updatesByWoGuid[wo.WoGuid] = delta.Updates;

            woWithAttrs += g.Count();
            totalPairs += delta.Updates.Count;
        }

        if (updatesByWoGuid.Count == 0)
            return new EnrichResult(true, true, 0, 0, "No invoice attribute changes detected; no enrichment applied.", postingPayloadJson);

        var enrichedJson = InjectInvoiceAttributesIntoPostingPayload(postingPayloadJson, updatesByWoGuid);

        return new EnrichResult(
            Attempted: true,
            Success: true,
            WorkOrdersWithInvoiceAttributes: woWithAttrs,
            TotalAttributePairs: totalPairs,
             "Invoice attributes enriched into posting payload. FSCM update will occur after journal post.",
            PostingPayloadJson: enrichedJson);
    }

    private async Task<HashSet<string>> GetActiveDefinitionsAsync(
        RunContext ctx,
        SubProjectKey key,
        Dictionary<SubProjectKey, HashSet<string>> defsCache,
        CancellationToken ct)
    {
        if (defsCache.TryGetValue(key, out var cached))
            return cached;

        var defs = await _fscmReadOnly.GetDefinitionsAsync(ctx, key.Company, key.SubProjectId, ct).ConfigureAwait(false);

        var active = new HashSet<string>(
            defs.Where(d => d is not null && d.Active && !string.IsNullOrWhiteSpace(d.AttributeName))
                .Select(d => d.AttributeName!),
            StringComparer.OrdinalIgnoreCase);

        defsCache[key] = active;
        return active;
    }

    private async Task<Dictionary<string, string?>> GetCurrentSnapshotAsync(
        RunContext ctx,
        SubProjectKey key,
        IEnumerable<string> requiredNames,
        Dictionary<SubProjectKey, Dictionary<string, string?>> currentCache,
        CancellationToken ct)
    {
        if (currentCache.TryGetValue(key, out var cached))
            return cached;

        var names = requiredNames?.Where(n => !string.IsNullOrWhiteSpace(n)).Distinct(StringComparer.OrdinalIgnoreCase).ToArray() ?? Array.Empty<string>();
        var current = await _fscmReadOnly.GetCurrentValuesAsync(ctx, key.Company, key.SubProjectId, names, ct).ConfigureAwait(false);

        var dict = current
            .Where(p => !string.IsNullOrWhiteSpace(p.AttributeName))
            .GroupBy(p => p.AttributeName!, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(g2 => g2.Key, g2 => g2.Last().AttributeValue, StringComparer.OrdinalIgnoreCase);

        currentCache[key] = dict;
        return dict;
    }

    private static void ApplyUpdatesToCurrentSnapshot(
        SubProjectKey key,
        IReadOnlyList<InvoiceAttributePair> updates,
        Dictionary<SubProjectKey, Dictionary<string, string?>> currentCache)
    {
        if (!currentCache.TryGetValue(key, out var dict) || dict is null)
            return;

        foreach (var u in updates)
        {
            if (string.IsNullOrWhiteSpace(u.AttributeName)) continue;
            dict[u.AttributeName] = u.AttributeValue;
        }
    }

    private static string InjectInvoiceAttributesIntoPostingPayload(
        string postingPayloadJson,
        IReadOnlyDictionary<Guid, IReadOnlyList<InvoiceAttributePair>> updatesByWoGuid)
    {
        using var doc = JsonDocument.Parse(postingPayloadJson);

        using var ms = new MemoryStream();
        using var writer = new Utf8JsonWriter(ms, new JsonWriterOptions { Indented = false });

        var root = doc.RootElement;

        writer.WriteStartObject();

        foreach (var prop in root.EnumerateObject())
        {
            if (!prop.NameEquals("_request"))
            {
                prop.WriteTo(writer);
                continue;
            }

            // _request
            writer.WritePropertyName("_request");
            writer.WriteStartObject();

            if (prop.Value.ValueKind != JsonValueKind.Object)
            {
                writer.WriteEndObject();
                continue;
            }

            foreach (var reqProp in prop.Value.EnumerateObject())
            {
                if (!reqProp.NameEquals("WOList"))
                {
                    reqProp.WriteTo(writer);
                    continue;
                }

                // WOList
                writer.WritePropertyName("WOList");
                writer.WriteStartArray();

                if (reqProp.Value.ValueKind == JsonValueKind.Array)
                {
                    foreach (var wo in reqProp.Value.EnumerateArray())
                    {
                        if (wo.ValueKind != JsonValueKind.Object)
                        {
                            wo.WriteTo(writer);
                            continue;
                        }

                        if (!TryReadWorkOrderGuid(wo, out var woGuid) || !updatesByWoGuid.TryGetValue(woGuid, out var updates) || updates.Count == 0)
                        {
                            // No enrichment for this WO; copy as-is.
                            wo.WriteTo(writer);
                            continue;
                        }

                        writer.WriteStartObject();

                        // Copy all existing properties EXCEPT any existing InvoiceAttributes (we replace).
                        foreach (var woProp in wo.EnumerateObject())
                        {
                            if (woProp.NameEquals("InvoiceAttributes"))
                                continue;

                            woProp.WriteTo(writer);
                        }

                        // Inject InvoiceAttributes array
                        writer.WritePropertyName("InvoiceAttributes");
                        writer.WriteStartArray();
                        foreach (var u in updates)
                        {
                            if (string.IsNullOrWhiteSpace(u.AttributeName)) continue;

                            writer.WriteStartObject();
                            writer.WriteString("AttributeName", u.AttributeName);
                            if (u.AttributeValue is null)
                                writer.WriteNull("AttributeValue");
                            else
                                writer.WriteString("AttributeValue", u.AttributeValue);
                            writer.WriteEndObject();
                        }
                        writer.WriteEndArray();

                        writer.WriteEndObject();
                    }
                }

                writer.WriteEndArray();
            }

            writer.WriteEndObject(); // _request object
        }

        writer.WriteEndObject(); // root
        writer.Flush();

        return System.Text.Encoding.UTF8.GetString(ms.ToArray());

        static bool TryReadWorkOrderGuid(JsonElement wo, out Guid woGuid)
        {
            woGuid = Guid.Empty;
            if (!wo.TryGetProperty("WorkOrderGUID", out var p)) return false;

            var s = p.ValueKind == JsonValueKind.String ? p.GetString() : p.ToString();
            if (string.IsNullOrWhiteSpace(s)) return false;

            s = s.Trim();
            if (s.StartsWith("{") && s.EndsWith("}"))
                s = s.Trim('{', '}');

            return Guid.TryParse(s, out woGuid);
        }
    }

    private static bool TryReadWorkOrders(string payloadJson, out List<WoCtx> workOrders)
    {
        workOrders = new List<WoCtx>();

        try
        {
            using var doc = JsonDocument.Parse(payloadJson);
            if (!doc.RootElement.TryGetProperty("_request", out var req) || req.ValueKind != JsonValueKind.Object)
                return false;

            if (!req.TryGetProperty("WOList", out var list) || list.ValueKind != JsonValueKind.Array)
                return false;

            foreach (var wo in list.EnumerateArray())
            {
                if (wo.ValueKind != JsonValueKind.Object) continue;

                var company = TryReadString(wo, "Company") ?? TryReadString(wo, "company");
                var subProjectId = TryReadString(wo, "SubProjectId") ?? TryReadString(wo, "subProjectId");
                var woId = TryReadString(wo, "WorkOrderID") ?? TryReadString(wo, "WorkOrderId") ?? TryReadString(wo, "WONumber") ?? string.Empty;

                if (string.IsNullOrWhiteSpace(company) || string.IsNullOrWhiteSpace(subProjectId))
                    continue;

                if (!TryReadGuid(wo, "WorkOrderGUID", out var woGuid))
                    continue;

                workOrders.Add(new WoCtx(woGuid, woId, company.Trim(), subProjectId.Trim()));
            }

            return true;
        }
        catch
        {
            return false;
        }
    }

    private static bool TryReadGuid(JsonElement obj, string prop, out Guid guid)
    {
        guid = Guid.Empty;
        if (!obj.TryGetProperty(prop, out var p)) return false;

        var s = p.ValueKind == JsonValueKind.String ? p.GetString() : p.ToString();
        if (string.IsNullOrWhiteSpace(s)) return false;

        s = s.Trim();
        if (s.StartsWith("{") && s.EndsWith("}"))
            s = s.Trim('{', '}');

        return Guid.TryParse(s, out guid);
    }

    private static string? TryReadString(JsonElement obj, string prop)
    {
        if (!obj.TryGetProperty(prop, out var p)) return null;
        if (p.ValueKind == JsonValueKind.String) return p.GetString();
        if (p.ValueKind == JsonValueKind.Null) return null;
        return p.ToString();
    }

    private static Dictionary<Guid, JsonElement> IndexWorkOrderHeaders(JsonDocument woHeadersDoc)
    {
        var dict = new Dictionary<Guid, JsonElement>();
        try
        {
            if (!woHeadersDoc.RootElement.TryGetProperty("value", out var value) || value.ValueKind != JsonValueKind.Array)
                return dict;

            foreach (var el in value.EnumerateArray())
            {
                if (!el.TryGetProperty("msdyn_workorderid", out var idProp)) continue;
                var idStr = idProp.ValueKind == JsonValueKind.String ? idProp.GetString() : idProp.ToString();
                if (!Guid.TryParse(idStr, out var id)) continue;
                dict[id] = el;
            }
        }
        catch { }

        return dict;
    }

    private static Dictionary<string, string?> ExtractFsAttributes(JsonElement woHeader)
    {
        var dict = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);

        foreach (var key in FsInvoiceKeys)
        {
            var val = TryReadValueOrLookupFormatted(woHeader, key);
            if (!val.IsPresent)
                continue;

            dict[key] = val.Value;
        }

        return dict;
    }

    private static void AddWorkTypeAndWellAgeDerived(JsonElement woHeader, Dictionary<string, string?> fsAttrsRaw)
    {
        const string lookup = "_rpc_worktypelookup_value";
        var formatted = TryGetFormattedValue(woHeader, lookup);
        if (string.IsNullOrWhiteSpace(formatted))
            return;

        var parts = formatted.Split(new[] { "--" }, StringSplitOptions.None);
        var workType = parts.Length >= 1 ? parts[0].Trim() : string.Empty;
        var wellAge = parts.Length >= 2 ? parts[1].Trim() : string.Empty;

        if (!string.IsNullOrWhiteSpace(workType))
            fsAttrsRaw[FscmAttr_WorkType] = workType;

        if (!string.IsNullOrWhiteSpace(wellAge))
            fsAttrsRaw[FscmAttr_WellAge] = wellAge;
    }

    private static Dictionary<Guid, string> BuildTaxabilityTypeByWorkOrder(JsonDocument wopDoc, JsonDocument wosDoc)
    {
        var result = new Dictionary<Guid, string>();
        AddFromLines(wopDoc, result);
        AddFromLines(wosDoc, result);
        return result;

        static void AddFromLines(JsonDocument doc, Dictionary<Guid, string> map)
        {
            try
            {
                if (!doc.RootElement.TryGetProperty("value", out var value) || value.ValueKind != JsonValueKind.Array)
                    return;

                foreach (var row in value.EnumerateArray())
                {
                    if (row.ValueKind != JsonValueKind.Object) continue;

                    if (!TryReadGuidLookup(row, "_msdyn_workorder_value", out var woGuid))
                        continue;

                    if (map.ContainsKey(woGuid))
                        continue;

                    if (!row.TryGetProperty("_rpc_operationtype_value", out var op) || op.ValueKind == JsonValueKind.Null)
                        continue;

                    var taxFormatted = TryGetFormattedValue(row, "_rpc_taxabilitytype_value");
                    if (string.IsNullOrWhiteSpace(taxFormatted))
                        continue;

                    map[woGuid] = taxFormatted.Trim();
                }
            }
            catch { }
        }
    }

    private static bool TryReadGuidLookup(JsonElement obj, string lookupValueProp, out Guid guid)
    {
        guid = Guid.Empty;
        if (!obj.TryGetProperty(lookupValueProp, out var p)) return false;
        var s = p.ValueKind == JsonValueKind.String ? p.GetString() : p.ToString();
        return Guid.TryParse(s, out guid);
    }

    private static string? TryGetFormattedValue(JsonElement obj, string prop)
    {
        var formattedKey = prop + ODataFormattedSuffix;
        if (obj.TryGetProperty(formattedKey, out var fv) && fv.ValueKind == JsonValueKind.String)
            return fv.GetString();
        return null;
    }

    private static PresentOrValue TryReadValueOrLookupFormatted(JsonElement obj, string logicalName)
    {
        if (obj.TryGetProperty(logicalName, out var p))
        {
            if (p.ValueKind == JsonValueKind.Null)
                return new PresentOrValue(true, null);

            return new PresentOrValue(true, p.ValueKind == JsonValueKind.String ? p.GetString() : p.ToString());
        }

        var lookupValueProp = "_" + logicalName + "_value";
        if (obj.TryGetProperty(lookupValueProp, out var lp))
        {
            var formatted = TryGetFormattedValue(obj, lookupValueProp);
            if (!string.IsNullOrWhiteSpace(formatted))
                return new PresentOrValue(true, formatted);

            if (lp.ValueKind == JsonValueKind.Null)
                return new PresentOrValue(true, null);

            return new PresentOrValue(true, lp.ValueKind == JsonValueKind.String ? lp.GetString() : lp.ToString());
        }

        return new PresentOrValue(false, null);
    }

    private readonly struct PresentOrValue
    {
        public bool IsPresent { get; }
        public string? Value { get; }

        public PresentOrValue(bool isPresent, string? value)
        {
            IsPresent = isPresent;
            Value = value;
        }
    }
}
