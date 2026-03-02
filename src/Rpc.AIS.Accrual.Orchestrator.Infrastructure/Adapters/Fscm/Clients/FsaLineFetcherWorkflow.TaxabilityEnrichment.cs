// File: FsaLineFetcherWorkflow.TaxabilityEnrichment.cs

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Rpc.AIS.Accrual.Orchestrator.Infrastructure.Clients;

public sealed partial class FsaLineFetcherWorkflow
{
    private const string OperationTypeLookupField = "_rpc_operationtype_value";
    private const string IncidentTypeEntitySet = "msdyn_incidenttypes";
    private const string IncidentTypeIdField = "msdyn_incidenttypeid";
    private const string IncidentTypeTaxabilityLookupField = "_rpc_taxabilitytype_value";

    private const string TaxabilityEntitySet = "rpc_taxabilitytypes";
    private const string TaxabilityIdField = "rpc_taxabilitytypeid";
    private const string TaxabilityNameField = "rpc_name";

    private const string PayloadTaxabilityKey = "Taxability Type";
    private const string PayloadWellNameKey = "Well Name";
    private const string PayloadWellAgeKey = "Well Age";

    internal JsonDocument EnrichWorkOrdersWithWorkTypeSplit(JsonDocument doc)
    {
        if (doc is null) return doc;
        if (!doc.RootElement.TryGetProperty("value", out var arr) || arr.ValueKind != JsonValueKind.Array)
            return doc;

        var sep = string.IsNullOrWhiteSpace(_opt.WorkTypeSeparator) ? " " : _opt.WorkTypeSeparator;
        using var stream = new MemoryStream();
        using (var w = new Utf8JsonWriter(stream))
        {
            w.WriteStartObject();
            foreach (var prop in doc.RootElement.EnumerateObject())
            {
                if (prop.NameEquals("value"))
                {
                    w.WritePropertyName("value");
                    w.WriteStartArray();
                    foreach (var row in arr.EnumerateArray())
                    {
                        w.WriteStartObject();
                        foreach (var c in row.EnumerateObject())
                            c.WriteTo(w);

                        // Work type formatted value -> split
                        var workTypeFormattedKey = "_rpc_worktypelookup_value" + DataverseSchema.ODataFormattedSuffix;
                        if (row.TryGetProperty(workTypeFormattedKey, out var fv) && fv.ValueKind == JsonValueKind.String)
                        {
                            var display = fv.GetString() ?? string.Empty;
                            if (!string.IsNullOrWhiteSpace(display))
                            {
                                var parts = display.Split(new[] { sep }, StringSplitOptions.None)
                                                   .Select(p => p.Trim())
                                                   .Where(p => !string.IsNullOrWhiteSpace(p))
                                                   .ToArray();
                                if (parts.Length >= 1)
                                    w.WriteString(PayloadWellNameKey, parts[0]);
                                if (parts.Length >= 2)
                                    w.WriteString(PayloadWellAgeKey, parts[1]);
                            }
                        }

                        w.WriteEndObject();
                    }
                    w.WriteEndArray();
                }
                else
                {
                    prop.WriteTo(w);
                }
            }
            w.WriteEndObject();
        }

        stream.Position = 0;
        return JsonDocument.Parse(stream);
    }

    private async Task<JsonDocument> EnrichWithTaxabilityTypeAsync(JsonDocument doc, CancellationToken ct)
    {
        if (doc is null) return doc;
        if (!doc.RootElement.TryGetProperty("value", out var arr) || arr.ValueKind != JsonValueKind.Array)
            return doc;

        // Gather distinct operation type (incident type) ids from the rows.
        var opTypeIds = new HashSet<Guid>();
        foreach (var row in arr.EnumerateArray())
        {
            if (TryGuid(row, OperationTypeLookupField, out var opId) && opId != Guid.Empty)
                opTypeIds.Add(opId);
        }

        if (opTypeIds.Count == 0)
            return doc;

        // 1) IncidentType -> TaxabilityTypeId
        var incidentDoc = await AggregateByIdsAsync(
            entitySetName: IncidentTypeEntitySet,
            idProperty: IncidentTypeIdField,
            ids: opTypeIds.ToList(),
            select: string.Join(",", IncidentTypeIdField, IncidentTypeTaxabilityLookupField),
            expand: null,
            orderBy: null,
            chunkSize: (_opt.OrFilterChunkSize > 0 ? _opt.OrFilterChunkSize : DefaultChunkSize),
            ct: ct).ConfigureAwait(false);

        var taxIdByIncidentId = new Dictionary<Guid, Guid>();
        if (incidentDoc.RootElement.TryGetProperty("value", out var incArr) && incArr.ValueKind == JsonValueKind.Array)
        {
            foreach (var row in incArr.EnumerateArray())
            {
                if (!TryGuid(row, IncidentTypeIdField, out var incidentId) || incidentId == Guid.Empty)
                    continue;
                if (TryGuid(row, IncidentTypeTaxabilityLookupField, out var taxId) && taxId != Guid.Empty)
                {
                    taxIdByIncidentId[incidentId] = taxId;
                }
            }
        }

        if (taxIdByIncidentId.Count == 0)
            return doc; // nothing to enrich

        // 2) TaxabilityTypeId -> rpc_name
        var taxIds = taxIdByIncidentId.Values.Distinct().ToList();
        var taxDoc = await AggregateByIdsAsync(
            entitySetName: TaxabilityEntitySet,
            idProperty: TaxabilityIdField,
            ids: taxIds,
            select: string.Join(",", TaxabilityIdField, TaxabilityNameField),
            expand: null,
            orderBy: null,
            chunkSize: (_opt.OrFilterChunkSize > 0 ? _opt.OrFilterChunkSize : DefaultChunkSize),
            ct: ct).ConfigureAwait(false);

        var taxNameById = new Dictionary<Guid, string>();
        if (taxDoc.RootElement.TryGetProperty("value", out var taxArr) && taxArr.ValueKind == JsonValueKind.Array)
        {
            foreach (var row in taxArr.EnumerateArray())
            {
                if (!TryGuid(row, TaxabilityIdField, out var taxId) || taxId == Guid.Empty)
                    continue;
                if (row.TryGetProperty(TaxabilityNameField, out var nameEl) && nameEl.ValueKind == JsonValueKind.String)
                {
                    var name = nameEl.GetString() ?? string.Empty;
                    if (!string.IsNullOrWhiteSpace(name))
                        taxNameById[taxId] = name;
                }
            }
        }

        if (taxNameById.Count == 0)
            return doc;

        using var stream = new MemoryStream();
        using (var w = new Utf8JsonWriter(stream))
        {
            w.WriteStartObject();
            foreach (var prop in doc.RootElement.EnumerateObject())
            {
                if (prop.NameEquals("value"))
                {
                    w.WritePropertyName("value");
                    w.WriteStartArray();
                    foreach (var row in arr.EnumerateArray())
                    {
                        w.WriteStartObject();
                        foreach (var c in row.EnumerateObject())
                            c.WriteTo(w);

                        if (TryGuid(row, OperationTypeLookupField, out var opId) && opId != Guid.Empty &&
                            taxIdByIncidentId.TryGetValue(opId, out var taxId) &&
                            taxNameById.TryGetValue(taxId, out var taxName) &&
                            !string.IsNullOrWhiteSpace(taxName))
                        {
                            w.WriteString(PayloadTaxabilityKey, taxName);
                            w.WriteString("FSATaxabilityType", taxName);
                        }

                        w.WriteEndObject();
                    }
                    w.WriteEndArray();
                }
                else
                {
                    prop.WriteTo(w);
                }
            }
            w.WriteEndObject();
        }

        stream.Position = 0;
        return JsonDocument.Parse(stream);
    }
}
