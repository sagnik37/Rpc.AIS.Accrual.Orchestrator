using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain.InvoiceAttributes;
using Rpc.AIS.Accrual.Orchestrator.Core.Utilities;

namespace Rpc.AIS.Accrual.Orchestrator.Functions.Services;

/// <summary>
/// Orchestration-level runner that reads InvoiceAttributes from the posting payload
/// and calls the FSCM UpdateInvoiceAttributes endpoint.
///
/// :
/// - This must be called explicitly by orchestrators/endpoints.
/// - Posting (journal validate/create/post) must NOT call invoice update implicitly.
/// </summary>
public sealed class InvoiceAttributesUpdateRunner
{
    private readonly IFscmInvoiceAttributesClient _fscm;
    private readonly ILogger<InvoiceAttributesUpdateRunner> _log;

    public InvoiceAttributesUpdateRunner(IFscmInvoiceAttributesClient fscm, ILogger<InvoiceAttributesUpdateRunner> log)
    {
        _fscm = fscm ?? throw new ArgumentNullException(nameof(fscm));
        _log = log ?? throw new ArgumentNullException(nameof(log));
    }

    public sealed record UpdateSummary(int WorkOrdersConsidered, int WorkOrdersWithUpdates, int UpdatePairs, int SuccessCount, int FailureCount);

    public async Task<UpdateSummary> UpdateFromPostingPayloadAsync(RunContext ctx, string postingPayloadJson, CancellationToken ct)
    {
        if (ctx is null) throw new ArgumentNullException(nameof(ctx));

        if (string.IsNullOrWhiteSpace(postingPayloadJson))
            return new UpdateSummary(0, 0, 0, 0, 0);

        if (!TryReadWorkOrders(postingPayloadJson, out var workOrders) || workOrders.Count == 0)
            return new UpdateSummary(0, 0, 0, 0, 0);

        var considered = workOrders.Count;
        var withUpdates = 0;
        var pairs = 0;
        var ok = 0;
        var fail = 0;

        foreach (var wo in workOrders)
        {
            ct.ThrowIfCancellationRequested();

            if (wo.Updates is null || wo.Updates.Count == 0)
                continue;

            withUpdates++;
            pairs += wo.Updates.Count;

            var res = await _fscm.UpdateAsync(
                ctx,
                company: wo.Company,
                subProjectId: wo.SubProjectId,
                workOrderGuid: wo.WorkOrderGuid,
                workOrderId: wo.WorkOrderId,
                countryRegionId: wo.CountryRegionId,
                county: wo.County,
                state: wo.State,
                dimensionDisplayValue: wo.DimensionDisplayValue,
                fsaTaxabilityType: wo.FSATaxabilityType,
                fsaWellAge: wo.FSAWellAge,
                fsaWorkType: wo.FSAWorkType,
                updates: wo.Updates,
                ct).ConfigureAwait(false);

            if (res.IsSuccess)
            {
                ok++;
                _log.LogInformation(
                    "InvoiceAttributes.Update OK Company={Company} SubProjectId={SubProjectId} WorkOrderId={WorkOrderId} WorkOrderGuid={WorkOrderGuid} Pairs={Pairs} HttpStatus={HttpStatus}",
                    wo.Company, wo.SubProjectId, wo.WorkOrderId, wo.WorkOrderGuid, wo.Updates.Count, res.HttpStatus);
            }
            else
            {
                fail++;
                _log.LogWarning(
                    "InvoiceAttributes.Update FAILED Company={Company} SubProjectId={SubProjectId} WorkOrderId={WorkOrderId} WorkOrderGuid={WorkOrderGuid} Pairs={Pairs} HttpStatus={HttpStatus} Body={Body}",
                    wo.Company, wo.SubProjectId, wo.WorkOrderId, wo.WorkOrderGuid, wo.Updates.Count, res.HttpStatus, LogText.TrimForLog(res.Body ?? string.Empty));
            }
        }

        return new UpdateSummary(considered, withUpdates, pairs, ok, fail);
    }

    private sealed record WorkOrderInvoiceUpdates(
        string Company,
        string SubProjectId,
        Guid WorkOrderGuid,
        string WorkOrderId,
        string? CountryRegionId,
        string? County,
        string? State,
        string? DimensionDisplayValue,
        string? FSATaxabilityType,
        string? FSAWellAge,
        string? FSAWorkType,
        IReadOnlyList<InvoiceAttributePair> Updates);

    private static bool TryReadWorkOrders(string json, out List<WorkOrderInvoiceUpdates> workOrders)
    {
        workOrders = new List<WorkOrderInvoiceUpdates>();

        try
        {
            using var doc = JsonDocument.Parse(json);
            if (!doc.RootElement.TryGetProperty("_request", out var req) || req.ValueKind != JsonValueKind.Object)
                return false;

            if (!req.TryGetProperty("WOList", out var list) || list.ValueKind != JsonValueKind.Array)
                return false;

            foreach (var wo in list.EnumerateArray())
            {
                if (wo.ValueKind != JsonValueKind.Object)
                    continue;

                var company = ReadString(wo, "Company") ?? ReadString(wo, "company");
                var subProjectId = ReadString(wo, "SubProjectId") ?? ReadString(wo, "subProjectId");
                var woGuidStr = ReadString(wo, "WorkOrderGUID") ?? ReadString(wo, "WorkOrderGuid") ?? ReadString(wo, "workOrderGuid");
                var woId = ReadString(wo, "WorkOrderID") ?? ReadString(wo, "WorkOrderId") ?? ReadString(wo, "workOrderId") ?? ReadString(wo, "WONumber") ?? "";

                if (string.IsNullOrWhiteSpace(company) || string.IsNullOrWhiteSpace(subProjectId) || string.IsNullOrWhiteSpace(woGuidStr) || string.IsNullOrWhiteSpace(woId))
                    continue;

                if (!Guid.TryParse(woGuidStr.Trim().TrimStart('{').TrimEnd('}'), out var woGuid) || woGuid == Guid.Empty)
                    continue;

                if (!wo.TryGetProperty("InvoiceAttributes", out var attrs) || attrs.ValueKind != JsonValueKind.Array)
                    continue;

                var updates = new List<InvoiceAttributePair>();

                foreach (var a in attrs.EnumerateArray())
                {
                    if (a.ValueKind != JsonValueKind.Object)
                        continue;

                    var name = ReadString(a, "AttributeName") ?? ReadString(a, "name");
                    if (string.IsNullOrWhiteSpace(name))
                        continue;

                    string? val = null;
                    if (a.TryGetProperty("AttributeValue", out var v))
                    {
                        val = v.ValueKind == JsonValueKind.Null ? null : v.ToString();
                    }
                    else if (a.TryGetProperty("value", out var v2))
                    {
                        val = v2.ValueKind == JsonValueKind.Null ? null : v2.ToString();
                    }

                    updates.Add(new InvoiceAttributePair(name!, val));
                }

                if (updates.Count == 0)
                    continue;

                // These header fields are required by FSCM InvoiceAttributes update envelope.
                // Keep them present in the outbound payload even when blank.
                var countryRegionId = ReadString(wo, "CountryRegionId") ?? ReadString(wo, "CountryRegionID") ?? ReadString(wo, "Country");
                var county = ReadString(wo, "County");
                var state = ReadString(wo, "State");
                var ddv = ReadString(wo, "DimensionDisplayValue");
                var tax = ReadString(wo, "FSATaxabilityType");
                var wellAge = ReadString(wo, "FSAWellAge");
                var workType = ReadString(wo, "FSAWorkType");

                workOrders.Add(new WorkOrderInvoiceUpdates(
                    Company: company.Trim(),
                    SubProjectId: subProjectId.Trim(),
                    WorkOrderGuid: woGuid,
                    WorkOrderId: woId.Trim(),
                    CountryRegionId: countryRegionId,
                    County: county,
                    State: state,
                    DimensionDisplayValue: ddv,
                    FSATaxabilityType: tax,
                    FSAWellAge: wellAge,
                    FSAWorkType: workType,
                    Updates: updates));
            }

            return workOrders.Count > 0;
        }
        catch
        {
            return false;
        }

        static string? ReadString(JsonElement obj, string prop)
        {
            if (!obj.TryGetProperty(prop, out var p)) return null;
            if (p.ValueKind == JsonValueKind.String) return p.GetString();
            if (p.ValueKind == JsonValueKind.Null) return null;
            return p.ToString();
        }
    }
}
