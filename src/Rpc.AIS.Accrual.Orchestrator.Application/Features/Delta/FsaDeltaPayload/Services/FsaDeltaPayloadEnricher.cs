// File: .../Core/UseCases/FsaDeltaPayload/*
//
// - Moves delta payload orchestration into Core (UseCase layer) and splits the orchestrator into partials.
// - Functions layer becomes a thin adapter.


using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Core.Options;
using Rpc.AIS.Accrual.Orchestrator.Core.Services;
using Rpc.AIS.Accrual.Orchestrator.Core.Services.FsaDeltaPayload.Enrichment;

using static Rpc.AIS.Accrual.Orchestrator.Core.Services.FsaDeltaPayload.FsaDeltaPayloadJsonUtil;

namespace Rpc.AIS.Accrual.Orchestrator.Core.Services.FsaDeltaPayload;

public sealed class FsaDeltaPayloadEnricher : IFsaDeltaPayloadEnricher
{
    private readonly ILogger<FsaDeltaPayloadEnricher> _log;
    private readonly IFsExtrasInjector _fsExtras;
    private readonly ISubProjectIdInjector _subProjectId;
    private readonly IWorkOrderHeaderFieldsInjector _woHeader;
    private readonly IJournalNamesInjector _journalNames;
    private readonly IJournalDescriptionsStamper _journalDescriptions;
    private readonly ICompanyInjector _company;

    public FsaDeltaPayloadEnricher(ILogger<FsaDeltaPayloadEnricher> log)
    {
        _log = log ?? throw new ArgumentNullException(nameof(log));

        // Composition: keep injectors cohesive and independently testable.
        _fsExtras = new FsExtrasInjector(_log);
        _subProjectId = new SubProjectIdInjector(_log);
        _woHeader = new WorkOrderHeaderFieldsInjector(_log);
        _journalNames = new JournalNamesInjector(_log);
        _journalDescriptions = new JournalDescriptionsStamper(_log);
        _company = new CompanyInjector(_log);
    }
public string InjectFsExtrasAndLogPerWoSummary(
        string payloadJson,
        Dictionary<Guid, FsLineExtras> extrasByLineGuid,
        string runId,
        string corr)
    {
        return _fsExtras.InjectFsExtrasAndLogPerWoSummary(payloadJson, extrasByLineGuid, runId, corr);
    }

    // =====================================================================
    // Payload header enrichment (Company) – WO level only
    // =====================================================================

    /// <summary>
    /// Executes build work order company name map.
    /// </summary>
    private static Dictionary<Guid, string> BuildWorkOrderCompanyNameMap(JsonDocument woHeaders)
    {
        var map = new Dictionary<Guid, string>();

        if (woHeaders is null)
            return map;

        if (!woHeaders.RootElement.TryGetProperty("value", out var arr) || arr.ValueKind != JsonValueKind.Array)
            return map;

        foreach (var row in arr.EnumerateArray())
        {
            if (!TryGuid(row, "msdyn_workorderid", out var woId))
                continue;

            // Priority order:
            //  1) Flattened by fetcher enrichment: cdm_companycode (string)
            //  2) Nested expand: msdyn_serviceaccount._msdyn_company_value@FormattedValue
            //  3) Legacy fallbacks if present
            var company =
                TryGetString(row, "cdm_companycode")
                ?? TryGetNestedFormattedOrRawNonGuid(
                    row,
                    nestedObjProp: "msdyn_serviceaccount",
                    preferredFormattedProp: "_msdyn_company_value@OData.Community.Display.V1.FormattedValue",
                    preferredRawProp: "_msdyn_company_value")
                ?? TryGetString(row, "msdyn_companyname")
                ?? TryGetString(row, "msdyn_company");

            if (!string.IsNullOrWhiteSpace(company))
                map[woId] = company!;
        }

        return map;

        static string? TryGetString(JsonElement obj, string prop)
            => obj.TryGetProperty(prop, out var p) && p.ValueKind == JsonValueKind.String ? p.GetString() : null;

        static string? TryGetNestedFormattedOrRawNonGuid(
            JsonElement root,
            string nestedObjProp,
            string preferredFormattedProp,
            string preferredRawProp)
        {
            if (!root.TryGetProperty(nestedObjProp, out var nested) || nested.ValueKind != JsonValueKind.Object)
                return null;

            // Prefer formatted label
            if (nested.TryGetProperty(preferredFormattedProp, out var f) && f.ValueKind == JsonValueKind.String)
            {
                var label = f.GetString();
                if (!string.IsNullOrWhiteSpace(label))
                    return label;
            }

            // Fallback to raw (but ignore GUID-looking values)
            if (nested.TryGetProperty(preferredRawProp, out var r))
            {
                var raw = r.ValueKind == JsonValueKind.String ? r.GetString() : r.ToString();
                if (LooksLikeGuid(raw))
                    return null;

                return string.IsNullOrWhiteSpace(raw) ? null : raw;
            }

            return null;
        }
    }

    /// <summary>
    /// Executes build work order sub project id map.
    /// </summary>
    private static Dictionary<Guid, string> BuildWorkOrderSubProjectIdMap(JsonDocument woHeaders)
    {
        var map = new Dictionary<Guid, string>();

        if (woHeaders is null)
            return map;

        if (!woHeaders.RootElement.TryGetProperty("value", out var arr) || arr.ValueKind != JsonValueKind.Array)
            return map;

        foreach (var row in arr.EnumerateArray())
        {
            if (!TryGuid(row, "msdyn_workorderid", out var woId))
                continue;

            // SubProject lookup is selected as "_rpc_subproject_value".
            // Requirement: use @FormattedValue only (do NOT resolve GUIDs).
            string? subProjectId = null;
            TryFormattedOnly(row, "_rpc_subproject_value", out subProjectId);

            if (!string.IsNullOrWhiteSpace(subProjectId))
                map[woId] = subProjectId!;
        }

        return map;

        //static string? TryGetString(JsonElement obj, string prop)
        //    => obj.TryGetProperty(prop, out var p) && p.ValueKind == JsonValueKind.String ? p.GetString() : null;

        //static string? TryGetNestedString(JsonElement root, string nestedObjProp, string nestedStringProp)
        //{
        //    if (!root.TryGetProperty(nestedObjProp, out var nested) || nested.ValueKind != JsonValueKind.Object)
        //        return null;

        //    return nested.TryGetProperty(nestedStringProp, out var p) && p.ValueKind == JsonValueKind.String
        //        ? p.GetString()
        //        : null;
        //}
    }

    public string InjectSubProjectIdIntoPayload(
        string payloadJson,
        IReadOnlyDictionary<Guid, string> woIdToSubProjectId)
    {
        return _subProjectId.InjectSubProjectIdIntoPayload(payloadJson, woIdToSubProjectId);
    }



    // =====================================================================
    // Payload WO header mapping-only enrichment
    // =====================================================================

    public string InjectWorkOrderHeaderFieldsIntoPayload(
        string payloadJson,
        IReadOnlyDictionary<Guid, WoHeaderMappingFields> woIdToHeaderFields)
    {
        return _woHeader.InjectWorkOrderHeaderFieldsIntoPayload(payloadJson, woIdToHeaderFields);
    }

    internal static void CopyRootWithWoHeaderFieldsInjection(
        JsonElement root,
        Utf8JsonWriter w,
        IReadOnlyDictionary<Guid, WoHeaderMappingFields> woIdToHeaderFields)
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
                CopyRequestWithWoHeaderFieldsInjection(p.Value, w, woIdToHeaderFields);
            }
            else
            {
                w.WritePropertyName(p.Name);
                p.Value.WriteTo(w);
            }
        }

        w.WriteEndObject();
    }

    private static void CopyRequestWithWoHeaderFieldsInjection(
        JsonElement req,
        Utf8JsonWriter w,
        IReadOnlyDictionary<Guid, WoHeaderMappingFields> woIdToHeaderFields)
    {
        w.WriteStartObject();

        foreach (var p in req.EnumerateObject())
        {
            if (p.NameEquals("WOList") && p.Value.ValueKind == JsonValueKind.Array)
            {
                w.WritePropertyName("WOList");
                w.WriteStartArray();

                foreach (var wo in p.Value.EnumerateArray())
                    CopyWoWithWoHeaderFieldsInjection(wo, w, woIdToHeaderFields);

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

    private static void CopyWoWithWoHeaderFieldsInjection(
        JsonElement wo,
        Utf8JsonWriter w,
        IReadOnlyDictionary<Guid, WoHeaderMappingFields> woIdToHeaderFields)
    {
        Guid? woId = null;

        if (wo.ValueKind == JsonValueKind.Object)
        {
            if (wo.TryGetProperty("WorkOrderGUID", out var g1) && g1.ValueKind == JsonValueKind.String)
                woId = ParseGuidLoose(g1.GetString());
            else if (wo.TryGetProperty("WorkorderGUID", out var g2) && g2.ValueKind == JsonValueKind.String)
                woId = ParseGuidLoose(g2.GetString());
        }

        WoHeaderMappingFields? header = null;
        var hasHeader = woId.HasValue && woIdToHeaderFields.TryGetValue(woId.Value, out header);

        // For *newly introduced* header fields, enforce: if source payload has no value, do not emit the field at all.
        // This prevents noisy blank fields in outbound payloads and avoids downstream duplicate-key issues.
        static bool IsBlankNewFieldValue(JsonElement v)
        {
            return v.ValueKind switch
            {
                JsonValueKind.Null => true,
                JsonValueKind.Undefined => true,
                JsonValueKind.String => string.IsNullOrWhiteSpace(v.GetString()),
                _ => false
            };
        }

        var newHeaderKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "ActualStartDate",
            "ActualEndDate",
            "CountryRegionId",
            "County",
            "State",
            "FSAWorkType",
            "FSAWellAge",
            "FSATaxabilityType"
        };

        w.WriteStartObject();

        foreach (var p in wo.EnumerateObject())
        {
            // Strip blank values for newly introduced header keys.
            if (newHeaderKeys.Contains(p.Name) && IsBlankNewFieldValue(p.Value))
                continue;

            // Pass-through existing properties, but avoid duplicate keys when we are about to inject
            // non-empty header fields (duplicates break System.Text.Json.Nodes.JsonObject later).
            if (hasHeader && header is not null)
            {
                var ddv = BuildDefaultDimensionDisplayValue(header.Department, header.ProductLine);

                // Overwrite semantics: if header has a non-empty value for a field, skip any existing
                // property with the same name so it appears only once in the final JSON.
                if ((p.NameEquals("CountryRegionId") && !string.IsNullOrWhiteSpace(header.Coountry)) ||
                    (p.NameEquals("County") && !string.IsNullOrWhiteSpace(header.County)) ||
                    (p.NameEquals("State") && !string.IsNullOrWhiteSpace(header.State)) ||
                    (p.NameEquals("FSATaxabilityType") && !string.IsNullOrWhiteSpace(header.FSATaxabilityType)) ||
                    (p.NameEquals("FSAWellAge") && !string.IsNullOrWhiteSpace(header.FSAWellAge)) ||
                    (p.NameEquals("FSAWorkType") && !string.IsNullOrWhiteSpace(header.FSAWorkType)) ||
                    (p.NameEquals("Latitude") && header.WellLatitude.HasValue) ||
                    (p.NameEquals("Longitude") && header.WellLongitude.HasValue) ||
                    (p.NameEquals("InvoiceNotesInternal") && !string.IsNullOrWhiteSpace(header.InvoiceNotesInternal)) ||
                    (p.NameEquals("FSACustomerReference") && !string.IsNullOrWhiteSpace(header.PONumber)) ||
                    (p.NameEquals("FSADeclinedToSign") && !string.IsNullOrWhiteSpace(header.DeclinedToSignReason)) ||
                    (p.NameEquals("ActualStartDate") && header.ActualStartDateUtc.HasValue) ||
                    (p.NameEquals("ActualEndDate") && header.ActualEndDateUtc.HasValue) ||
                    (p.NameEquals("ProjectedStartDate") && header.ProjectedStartDateUtc.HasValue) ||
                    (p.NameEquals("ProjectedEndDate") && header.ProjectedEndDateUtc.HasValue) ||
                    (p.NameEquals("DimensionDisplayValue") && !string.IsNullOrWhiteSpace(ddv)))
                {
                    continue; // will be injected below
                }
            }

            w.WritePropertyName(p.Name);
            p.Value.WriteTo(w);
        }

        if (hasHeader && header is not null)
        {
            // Dates -> ISO yyyy-MM-dd (mapping contract)
            WriteIsoDateIfPresent(w, "ActualStartDate", header.ActualStartDateUtc);
            WriteIsoDateIfPresent(w, "ActualEndDate", header.ActualEndDateUtc);
            WriteIsoDateIfPresent(w, "ProjectedStartDate", header.ProjectedStartDateUtc);
            WriteIsoDateIfPresent(w, "ProjectedEndDate", header.ProjectedEndDateUtc);

            if (header.WellLatitude.HasValue)
                w.WriteNumber("Latitude", header.WellLatitude.Value);

            if (header.WellLongitude.HasValue)
                w.WriteNumber("Longitude", header.WellLongitude.Value);

            if (!string.IsNullOrWhiteSpace(header.InvoiceNotesInternal))
                w.WriteString("InvoiceNotesInternal", header.InvoiceNotesInternal);

            if (!string.IsNullOrWhiteSpace(header.PONumber))
                w.WriteString("FSACustomerReference", header.PONumber);

            if (!string.IsNullOrWhiteSpace(header.DeclinedToSignReason))
                w.WriteString("FSADeclinedToSign", header.DeclinedToSignReason);

            // Location header fields (used by FSCM validation/creation)
            if (!string.IsNullOrWhiteSpace(header.Coountry))
                w.WriteString("CountryRegionId", header.Coountry);

            if (!string.IsNullOrWhiteSpace(header.County))
                w.WriteString("County", header.County);

            if (!string.IsNullOrWhiteSpace(header.State))
                w.WriteString("State", header.State);

            // Optional canonical WO-level fields (if present in org)
            if (!string.IsNullOrWhiteSpace(header.FSATaxabilityType))
                w.WriteString("FSATaxabilityType", header.FSATaxabilityType);

            if (!string.IsNullOrWhiteSpace(header.FSAWellAge))
                w.WriteString("FSAWellAge", header.FSAWellAge);

            if (!string.IsNullOrWhiteSpace(header.FSAWorkType))
                w.WriteString("FSAWorkType", header.FSAWorkType);

            // DimensionDisplayValue follows same rules as line-level (dept/product only)
            var ddv = BuildDefaultDimensionDisplayValue(header.Department, header.ProductLine);
            if (!string.IsNullOrWhiteSpace(ddv))
                w.WriteString("DimensionDisplayValue", ddv);
        }

        w.WriteEndObject();
    }

    private static void WriteIsoDateIfPresent(Utf8JsonWriter w, string propName, DateTime? dtUtc)
    {
        if (!dtUtc.HasValue) return;

        var dt = dtUtc.Value;
        if (dt.Kind == DateTimeKind.Local)
            dt = dt.ToUniversalTime();
        else if (dt.Kind == DateTimeKind.Unspecified)
            dt = DateTime.SpecifyKind(dt, DateTimeKind.Utc);

        // Output yyyy-MM-dd
        w.WriteString(propName, dt.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture));
    }

    private static string BuildDefaultDimensionDisplayValue(string? department, string? productLine)
    {
        static string? Clean(string? s)
        {
            if (string.IsNullOrWhiteSpace(s)) return null;
            return s.Trim();
        }

        var dept = Clean(department) ?? string.Empty;
        var prod = Clean(productLine) ?? string.Empty;

        var segments = new[]
        {
            string.Empty,
            dept,
            prod,
            string.Empty,
            string.Empty,
            string.Empty,
            string.Empty,
            string.Empty
        };

        return string.Join("-", segments);
    }
    // =====================================================================
    // Payload journal header enrichment (JournalName)
    // =====================================================================

    public string InjectJournalNamesIntoPayload(
        string payloadJson,
        IReadOnlyDictionary<string, LegalEntityJournalNames> journalNamesByCompany)
    {
        return _journalNames.InjectJournalNamesIntoPayload(payloadJson, journalNamesByCompany);
    }

    internal static void CopyRootWithJournalNamesInjection(
        JsonElement root,
        Utf8JsonWriter w,
        IReadOnlyDictionary<string, LegalEntityJournalNames> journalNamesByCompany)
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
                CopyRequestWithJournalNamesInjection(p.Value, w, journalNamesByCompany);
            }
            else
            {
                w.WritePropertyName(p.Name);
                p.Value.WriteTo(w);
            }
        }

        w.WriteEndObject();
    }

    private static void CopyRequestWithJournalNamesInjection(
        JsonElement req,
        Utf8JsonWriter w,
        IReadOnlyDictionary<string, LegalEntityJournalNames> journalNamesByCompany)
    {
        w.WriteStartObject();

        foreach (var p in req.EnumerateObject())
        {
            if (p.NameEquals("WOList") && p.Value.ValueKind == JsonValueKind.Array)
            {
                w.WritePropertyName("WOList");
                w.WriteStartArray();

                foreach (var wo in p.Value.EnumerateArray())
                    CopyWoWithJournalNamesInjection(wo, w, journalNamesByCompany);

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

    private static void CopyWoWithJournalNamesInjection(
        JsonElement wo,
        Utf8JsonWriter w,
        IReadOnlyDictionary<string, LegalEntityJournalNames> journalNamesByCompany)
    {
        string? company = null;
        if (wo.ValueKind == JsonValueKind.Object && wo.TryGetProperty("Company", out var c) && c.ValueKind == JsonValueKind.String)
            company = c.GetString();

        LegalEntityJournalNames? names = null;
        if (!string.IsNullOrWhiteSpace(company) && journalNamesByCompany.TryGetValue(company!.Trim(), out var v))
            names = v;

        w.WriteStartObject();

        foreach (var p in wo.EnumerateObject())
        {
            if ((p.NameEquals("WOItemLines") || p.NameEquals("WOExpLines") || p.NameEquals("WOHourLines")) &&
                p.Value.ValueKind == JsonValueKind.Object)
            {
                w.WritePropertyName(p.Name);
                CopyJournalHeaderWithName(p.Name, p.Value, w, names);
                continue;
            }

            w.WritePropertyName(p.Name);
            p.Value.WriteTo(w);
        }

        w.WriteEndObject();
    }

    private static void CopyJournalHeaderWithName(
        string sectionKey,
        JsonElement journal,
        Utf8JsonWriter w,
        LegalEntityJournalNames? names)
    {
        // Determine journal name per section.
        var journalName = sectionKey switch
        {
            "WOItemLines" => names?.InventJournalNameId,
            "WOExpLines" => names?.ExpenseJournalNameId,
            "WOHourLines" => names?.HourJournalNameId,
            _ => null
        };

        w.WriteStartObject();

        var wroteJournalName = false;

        foreach (var p in journal.EnumerateObject())
        {
            if (p.NameEquals("JournalName"))
            {
                wroteJournalName = true;
                w.WritePropertyName("JournalName");
                w.WriteStringValue(journalName ?? (p.Value.ValueKind == JsonValueKind.String ? p.Value.GetString() ?? string.Empty : string.Empty));
                continue;
            }

            w.WritePropertyName(p.Name);
            p.Value.WriteTo(w);
        }

        if (!wroteJournalName)
        {
            w.WritePropertyName("JournalName");
            w.WriteStringValue(journalName ?? string.Empty);
        }

        w.WriteEndObject();
    }


    // =====================================================================
    // Payload journal description stamping (JournalDescription / JournalLineDescription)
    // =====================================================================

    public string StampJournalDescriptionsIntoPayload(string payloadJson, string action)
    {
        return _journalDescriptions.StampJournalDescriptionsIntoPayload(payloadJson, action);
    }

    internal static void CopyRootWithJournalDescriptionStamp(JsonElement root, Utf8JsonWriter w, string action)
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
                CopyRequestWithJournalDescriptionStamp(p.Value, w, action);
            }
            else
            {
                w.WritePropertyName(p.Name);
                p.Value.WriteTo(w);
            }
        }

        w.WriteEndObject();
    }

    private static void CopyRequestWithJournalDescriptionStamp(JsonElement req, Utf8JsonWriter w, string action)
    {
        w.WriteStartObject();

        foreach (var p in req.EnumerateObject())
        {
            if (p.NameEquals("WOList") && p.Value.ValueKind == JsonValueKind.Array)
            {
                w.WritePropertyName("WOList");
                w.WriteStartArray();

                foreach (var wo in p.Value.EnumerateArray())
                    CopyWoWithJournalDescriptionStamp(wo, w, action);

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

    private static void CopyWoWithJournalDescriptionStamp(JsonElement wo, Utf8JsonWriter w, string action)
    {
        // Read final header values (after any previous enrichment).
        string jobId = string.Empty;
        string subProjectId = string.Empty;

        if (wo.ValueKind == JsonValueKind.Object)
        {
            if (wo.TryGetProperty("WorkOrderID", out var j) && j.ValueKind == JsonValueKind.String)
                jobId = j.GetString() ?? string.Empty;

            if (wo.TryGetProperty("SubProjectId", out var sp) && sp.ValueKind == JsonValueKind.String)
                subProjectId = sp.GetString() ?? string.Empty;
        }

        var desc = $"{jobId} - {subProjectId} - {action}";

        w.WriteStartObject();

        foreach (var p in wo.EnumerateObject())
        {
            if ((p.NameEquals("WOItemLines") || p.NameEquals("WOExpLines") || p.NameEquals("WOHourLines")) &&
                p.Value.ValueKind == JsonValueKind.Object)
            {
                w.WritePropertyName(p.Name);
                CopyJournalWithDescriptionStamp(p.Value, w, desc);
                continue;
            }

            w.WritePropertyName(p.Name);
            p.Value.WriteTo(w);
        }

        w.WriteEndObject();
    }

    private static void CopyJournalWithDescriptionStamp(JsonElement journal, Utf8JsonWriter w, string desc)
    {
        w.WriteStartObject();

        foreach (var p in journal.EnumerateObject())
        {
            if (p.NameEquals("JournalDescription"))
            {
                w.WritePropertyName("JournalDescription");
                w.WriteStringValue(desc);
                continue;
            }

            if (p.NameEquals("JournalLines") && p.Value.ValueKind == JsonValueKind.Array)
            {
                w.WritePropertyName("JournalLines");
                w.WriteStartArray();

                foreach (var ln in p.Value.EnumerateArray())
                    CopyLineWithDescriptionStamp(ln, w, desc);

                w.WriteEndArray();
                continue;
            }

            w.WritePropertyName(p.Name);
            p.Value.WriteTo(w);
        }

        // Ensure JournalDescription exists even if it was missing.
        if (!journal.TryGetProperty("JournalDescription", out _))
        {
            w.WritePropertyName("JournalDescription");
            w.WriteStringValue(desc);
        }

        w.WriteEndObject();
    }

    private static void CopyLineWithDescriptionStamp(JsonElement line, Utf8JsonWriter w, string desc)
    {
        if (line.ValueKind != JsonValueKind.Object)
        {
            line.WriteTo(w);
            return;
        }

        w.WriteStartObject();

        var wroteJld = false;
        var hadNonEmptyJld = false;

        foreach (var p in line.EnumerateObject())
        {
            if (p.NameEquals("JournalLineDescription"))
            {
                wroteJld = true;

                // Preserve non-empty existing value; stamp only if blank.
                string? existing = null;
                if (p.Value.ValueKind == JsonValueKind.String)
                    existing = p.Value.GetString();
                else if (p.Value.ValueKind != JsonValueKind.Null && p.Value.ValueKind != JsonValueKind.Undefined)
                    existing = p.Value.ToString();

                if (!string.IsNullOrWhiteSpace(existing))
                {
                    hadNonEmptyJld = true;
                    w.WritePropertyName("JournalLineDescription");
                    p.Value.WriteTo(w); // keep existing
                }
                else
                {
                    w.WritePropertyName("JournalLineDescription");
                    w.WriteStringValue(desc); // stamp
                }

                continue;
            }

            w.WritePropertyName(p.Name);
            p.Value.WriteTo(w);
        }

        // If the property didn't exist at all, add it.
        if (!wroteJld)
        {
            w.WritePropertyName("JournalLineDescription");
            w.WriteStringValue(desc);
        }

        w.WriteEndObject();
    }

    internal static void CopyRootWithSubProjectIdInjection(
            JsonElement root,
            Utf8JsonWriter w,
            IReadOnlyDictionary<Guid, string> woIdToSubProjectId)
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
                CopyRequestWithSubProjectIdInjection(p.Value, w, woIdToSubProjectId);
            }
            else
            {
                w.WritePropertyName(p.Name);
                p.Value.WriteTo(w);
            }
        }

        w.WriteEndObject();
    }

    private static void CopyRequestWithSubProjectIdInjection(
        JsonElement req,
        Utf8JsonWriter w,
        IReadOnlyDictionary<Guid, string> woIdToSubProjectId)
    {
        w.WriteStartObject();

        foreach (var p in req.EnumerateObject())
        {
            if (p.NameEquals("WOList") && p.Value.ValueKind == JsonValueKind.Array)
            {
                w.WritePropertyName("WOList");
                w.WriteStartArray();

                foreach (var wo in p.Value.EnumerateArray())
                    CopyWoWithSubProjectIdInjection(wo, w, woIdToSubProjectId);

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

    private static void CopyWoWithSubProjectIdInjection(
        JsonElement wo,
        Utf8JsonWriter w,
        IReadOnlyDictionary<Guid, string> woIdToSubProjectId)
    {
        Guid? woId = null;

        if (wo.ValueKind == JsonValueKind.Object)
        {
            if (wo.TryGetProperty("WorkOrderGUID", out var g1) && g1.ValueKind == JsonValueKind.String)
                woId = ParseGuidLoose(g1.GetString());
            else if (wo.TryGetProperty("WorkorderGUID", out var g2) && g2.ValueKind == JsonValueKind.String)
                woId = ParseGuidLoose(g2.GetString());
        }

        string? subProjectId = null;
        var hasSubProject = woId.HasValue && woIdToSubProjectId.TryGetValue(woId.Value, out subProjectId);

        w.WriteStartObject();

        var wrote = false;

        foreach (var p in wo.EnumerateObject())
        {
            // Header-only field name:
            if (p.NameEquals("SubProjectId"))
            {
                var existing = p.Value.ValueKind == JsonValueKind.String ? p.Value.GetString() : null;
                var final = (string.IsNullOrWhiteSpace(existing) && hasSubProject) ? subProjectId : existing;

                w.WritePropertyName("SubProjectId");
                if (final is null) w.WriteNullValue();
                else w.WriteStringValue(final);

                wrote = true;
                continue;
            }

            w.WritePropertyName(p.Name);
            p.Value.WriteTo(w);
        }

        if (!wrote && hasSubProject && !string.IsNullOrWhiteSpace(subProjectId))
            w.WriteString("SubProjectId", subProjectId);

        w.WriteEndObject();
    }

    public string InjectCompanyIntoPayload(
        string payloadJson,
        IReadOnlyDictionary<Guid, string> woIdToCompanyName)
    {
        return _company.InjectCompanyIntoPayload(payloadJson, woIdToCompanyName);
    }

    internal static void CopyRootWithCompanyInjection(
        JsonElement root,
        Utf8JsonWriter w,
        IReadOnlyDictionary<Guid, string> woIdToCompanyName)
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
                CopyRequestWithCompanyInjection(p.Value, w, woIdToCompanyName);
            }
            else
            {
                w.WritePropertyName(p.Name);
                p.Value.WriteTo(w);
            }
        }

        w.WriteEndObject();
    }

    private static void CopyRequestWithCompanyInjection(
        JsonElement req,
        Utf8JsonWriter w,
        IReadOnlyDictionary<Guid, string> woIdToCompanyName)
    {
        w.WriteStartObject();

        foreach (var p in req.EnumerateObject())
        {
            if (p.NameEquals("WOList") && p.Value.ValueKind == JsonValueKind.Array)
            {
                w.WritePropertyName("WOList");
                w.WriteStartArray();

                foreach (var wo in p.Value.EnumerateArray())
                    CopyWoWithCompanyInjection(wo, w, woIdToCompanyName);

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

    private static void CopyWoWithCompanyInjection(
        JsonElement wo,
        Utf8JsonWriter w,
        IReadOnlyDictionary<Guid, string> woIdToCompanyName)
    {
        Guid? woId = null;

        if (wo.ValueKind == JsonValueKind.Object)
        {
            if (wo.TryGetProperty("WorkOrderGUID", out var g1) && g1.ValueKind == JsonValueKind.String)
                woId = ParseGuidLoose(g1.GetString());
            else if (wo.TryGetProperty("WorkorderGUID", out var g2) && g2.ValueKind == JsonValueKind.String)
                woId = ParseGuidLoose(g2.GetString());
        }

        string? companyName = null;
        var hasCompany = woId.HasValue && woIdToCompanyName.TryGetValue(woId.Value, out companyName);

        w.WriteStartObject();

        var wroteCompany = false;

        foreach (var p in wo.EnumerateObject())
        {
            if (p.NameEquals("Company"))
            {
                var existing = p.Value.ValueKind == JsonValueKind.String ? p.Value.GetString() : null;
                var final = (string.IsNullOrWhiteSpace(existing) && hasCompany) ? companyName : existing;

                w.WritePropertyName("Company");
                if (final is null) w.WriteNullValue();
                else w.WriteStringValue(final);

                wroteCompany = true;
                continue;
            }

            w.WritePropertyName(p.Name);
            p.Value.WriteTo(w);
        }

        if (!wroteCompany && hasCompany && !string.IsNullOrWhiteSpace(companyName))
            w.WriteString("Company", companyName);

        w.WriteEndObject();
    }

    // <summary>
    // Executes build work order number map.
    // </summary>
}
