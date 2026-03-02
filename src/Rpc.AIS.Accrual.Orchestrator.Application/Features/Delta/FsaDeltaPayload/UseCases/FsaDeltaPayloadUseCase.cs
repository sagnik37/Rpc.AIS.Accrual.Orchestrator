// File: .../Core/UseCases/FsaDeltaPayload/FsaDeltaPayloadUseCase.cs
//
// (FULL FILE CONTENT)
// Changes applied:
// - Removed DeltaPayloadBuilder injection + field (it is static).
// - Replaced _builder.BuildWoListPayload(...) with DeltaPayloadBuilder.BuildWoListPayload(...).
// - Leave the rest as-is (Core use-case orchestration + thin Functions adapter).

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
using Rpc.AIS.Accrual.Orchestrator.Core.Services.FsaDeltaPayload;
using Rpc.AIS.Accrual.Orchestrator.Application.Features.Delta.FsaDeltaPayload.Services.EnrichmentPipeline;

namespace Rpc.AIS.Accrual.Orchestrator.Core.UseCases.FsaDeltaPayload;

public sealed partial class FsaDeltaPayloadUseCase : IFsaDeltaPayloadUseCase
{
    private readonly ILogger<FsaDeltaPayloadUseCase> _log;
    private readonly ITelemetry _telemetry;
    private readonly IFsaLineFetcher _fetcher;
    private readonly DeltaComparer _comparer;
    private readonly IFscmBaselineFetcher _baseline;
    private readonly IFsaSnapshotBuilder _snapshotBuilder;
    private readonly IFsaDeltaPayloadEnricher _enricher;
    private readonly IFsaDeltaPayloadEnrichmentPipeline _enrichmentPipeline;
    private readonly IFscmReleasedDistinctProductsClient _releasedDistinctProducts;
    private readonly IFscmLegalEntityIntegrationParametersClient _leParams;
    private readonly IEmailSender _email;
    private readonly NotificationOptions _notifications;

    public FsaDeltaPayloadUseCase(
        ILogger<FsaDeltaPayloadUseCase> log,
        ITelemetry telemetry,
        IFsaLineFetcher fetcher,
        DeltaComparer comparer,
        IFscmBaselineFetcher baseline,
        IFsaSnapshotBuilder snapshotBuilder,
        IFsaDeltaPayloadEnricher enricher,
        IFsaDeltaPayloadEnrichmentPipeline enrichmentPipeline,
        IFscmReleasedDistinctProductsClient releasedDistinctProducts,
        IFscmLegalEntityIntegrationParametersClient leParams,
        IEmailSender email,
        NotificationOptions notifications)
    {
        _log = log ?? throw new ArgumentNullException(nameof(log));
        _telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
        _fetcher = fetcher ?? throw new ArgumentNullException(nameof(fetcher));
        _comparer = comparer ?? throw new ArgumentNullException(nameof(comparer));
        _baseline = baseline ?? throw new ArgumentNullException(nameof(baseline));
        _snapshotBuilder = snapshotBuilder ?? throw new ArgumentNullException(nameof(snapshotBuilder));
        _enricher = enricher ?? throw new ArgumentNullException(nameof(enricher));
        _enrichmentPipeline = enrichmentPipeline ?? throw new ArgumentNullException(nameof(enrichmentPipeline));
        _releasedDistinctProducts = releasedDistinctProducts ?? throw new ArgumentNullException(nameof(releasedDistinctProducts));
        _leParams = leParams ?? throw new ArgumentNullException(nameof(leParams));
        _email = email ?? throw new ArgumentNullException(nameof(email));
        _notifications = notifications ?? throw new ArgumentNullException(nameof(notifications));
    }

    /// <summary>
    /// Executes build full fetch async.
    /// </summary>
    public Task<GetFsaDeltaPayloadResultDto> BuildFullFetchAsync(GetFsaDeltaPayloadInputDto input, FsaDeltaPayloadRunOptions opt, CancellationToken ct)
        => GetFullFetchPayloadAsync(input, opt, ct);

    private async Task<GetFsaDeltaPayloadResultDto> GetFullFetchPayloadAsync(
            GetFsaDeltaPayloadInputDto input,
            FsaDeltaPayloadRunOptions opt,
            CancellationToken ct)
    {
        var runId = input.RunId;
        var corr = input.CorrelationId;

        var runContext = new RunContext(runId, DateTimeOffset.UtcNow, input.TriggeredBy, corr);

        if (string.IsNullOrWhiteSpace(opt.WorkOrderFilter))
            throw new InvalidOperationException("FsaIngestion:WorkOrderFilter is required for FullFetch mode.");

        _log.LogInformation("FullFetch START. WorkOrderFilter={Filter}", opt.WorkOrderFilter);

        // 1) Fetch OPEN work orders (headers)
        var openWoHeaders = await _fetcher.GetOpenWorkOrdersAsync(runContext, ct);
        _telemetry.LogJson("Dataverse.OpenWorkOrders", runId, corr, null, openWoHeaders.RootElement.GetRawText());

        var woIdToNumber = BuildWorkOrderNumberMap(openWoHeaders);
        var woIdToCompanyName = FsaDeltaPayloadWorkOrderHeaderMaps.BuildWorkOrderCompanyNameMap(openWoHeaders);
        var woIdToSubProjectId = FsaDeltaPayloadWorkOrderHeaderMaps.BuildWorkOrderSubProjectIdMap(openWoHeaders);
        var woIdToHeaderFields = FsaDeltaPayloadWorkOrderHeaderMaps.BuildWorkOrderHeaderFieldsMap(openWoHeaders);

        var openWoIds = woIdToNumber.Keys.ToList();

        // If caller requested a single Work Order, narrow to that WO (must be OPEN to be processed).
        if (!string.IsNullOrWhiteSpace(input.WorkOrderGuid))
        {
            if (Guid.TryParse(input.WorkOrderGuid, out var woGuid))
            {
                if (openWoIds.Contains(woGuid))
                {
                    openWoIds = new List<Guid> { woGuid };
                    _log.LogInformation("FullFetch SINGLE work order requested. WorkOrderGuid={WorkOrderGuid}", woGuid);
                }
                else
                {
                    _log.LogWarning(
                        "FullFetch SINGLE work order requested but not found in OPEN set. WorkOrderGuid={WorkOrderGuid} OpenCount={OpenCount}",
                        woGuid, openWoIds.Count);

                    openWoIds = new List<Guid>(); // results in empty payload downstream
                }
            }
            else
            {
                _log.LogWarning("FullFetch SINGLE work order requested but invalid GUID supplied. WorkOrderGuid={WorkOrderGuid}", input.WorkOrderGuid);
                openWoIds = new List<Guid>();
            }
        }

        _log.LogInformation(
            "FullFetch OPEN work orders fetched. Count={Count} WithCompany={WithCompany} WithSubProject={WithSubProject}",
            openWoIds.Count,
            woIdToCompanyName.Count,
            woIdToSubProjectId.Count);

        // -----------------------------------------------------------------
        // Extra validation: only process work orders that have a SubProject.
        // Work orders missing SubProject are marked invalid for this run,
        // logged, and sent to the notification DL.
        // -----------------------------------------------------------------
        var invalidNoSubProject = openWoIds
            .Where(id => !woIdToSubProjectId.ContainsKey(id))
            .ToList();

        if (invalidNoSubProject.Count > 0)
        {
            _log.LogWarning(
                "OPEN work orders missing SubProject will be skipped. InvalidCount={InvalidCount} TotalOpen={TotalOpen} RunId={RunId} CorrelationId={CorrelationId}",
                invalidNoSubProject.Count, openWoIds.Count, runId, corr);

            // Remove invalids from processing list
            openWoIds = openWoIds.Where(id => woIdToSubProjectId.ContainsKey(id)).ToList();

            try
            {
                var recipients = _notifications.GetRecipients();
                if (recipients.Count > 0)
                {
                    var subject = $"[AIS][Accrual][INVALID] Missing SubProject (skipped) RunId={runId}";
                    var sb = new System.Text.StringBuilder();
                    sb.Append("<p>The following open work orders were skipped because SubProject was missing.</p>");
                    sb.Append("<table border='1' cellpadding='4' cellspacing='0'>");
                    sb.Append("<tr><th>WorkOrderGuid</th><th>WorkOrderNumber</th><th>Company</th></tr>");

                    foreach (var id in invalidNoSubProject)
                    {
                        woIdToNumber.TryGetValue(id, out var woNo);
                        woIdToCompanyName.TryGetValue(id, out var company);
                        sb.Append("<tr>");
                        sb.Append("<td>").Append(id).Append("</td>");
                        sb.Append("<td>").Append(System.Net.WebUtility.HtmlEncode(woNo ?? string.Empty)).Append("</td>");
                        sb.Append("<td>").Append(System.Net.WebUtility.HtmlEncode(company ?? string.Empty)).Append("</td>");
                        sb.Append("</tr>");
                    }

                    sb.Append("</table>");
                    sb.Append("<p>These work orders were not included in payload build for this run.</p>");

                    await _email.SendAsync(subject, sb.ToString(), recipients, ct);
                }
                else
                {
                    _log.LogWarning("Notifications ErrorDistributionList is empty; invalid SubProject email not sent.");
                }
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Failed to send invalid SubProject email. RunId={RunId} CorrelationId={CorrelationId}", runId, corr);
            }
        }

        if (openWoIds.Count == 0)
        {
            var emptyPayload = DeltaPayloadBuilder.BuildWoListPayload(Array.Empty<FsaDeltaSnapshot>(), corr, runId);
            _telemetry.LogJson("Delta.Payload.Outbound", runId, corr, null, emptyPayload);

            _log.LogInformation("FullFetch END. No open work orders.");

            return new GetFsaDeltaPayloadResultDto(
                PayloadJson: emptyPayload,
                ProductDeltaLinkAfter: null,
                ServiceDeltaLinkAfter: null,
                WorkOrderNumbers: Array.Empty<string>());
        }

        // 2) Presence checks first (cheap)
        var openWoIdStrings = openWoIds.Select(x => x.ToString()).ToList();

        var woIdsWithProducts = await _fetcher.GetWorkOrderIdsWithProductsAsync(runContext, openWoIdStrings, ct);
        var woIdsWithServices = await _fetcher.GetWorkOrderIdsWithServicesAsync(runContext, openWoIdStrings, ct);
        var eligibleWoIds = new List<Guid>();
        var ignoredNoLines = new List<Guid>();

        foreach (var woId in openWoIds)
        {
            var hasP = woIdsWithProducts.Contains(woId.ToString());
            var hasS = woIdsWithServices.Contains(woId.ToString());

            if (!hasP && !hasS)
            {
                ignoredNoLines.Add(woId);
                continue;
            }

            eligibleWoIds.Add(woId);
        }

        _log.LogWarning(
            "FullFetch classification summary: TotalOpen={TotalOpen} IgnoredNoLines={IgnoredNoLines} Eligible={Eligible}",
            openWoIds.Count, ignoredNoLines.Count, eligibleWoIds.Count);

        if (eligibleWoIds.Count == 0)
        {
            var emptyPayload = DeltaPayloadBuilder.BuildWoListPayload(Array.Empty<FsaDeltaSnapshot>(), corr, runId);
            _telemetry.LogJson("Delta.Payload.Outbound", runId, corr, null, emptyPayload);

            _log.LogWarning("FullFetch END. All open work orders were ignored due to no line items.");

            return new GetFsaDeltaPayloadResultDto(
                PayloadJson: emptyPayload,
                ProductDeltaLinkAfter: null,
                ServiceDeltaLinkAfter: null,
                WorkOrderNumbers: Array.Empty<string>());
        }

        // 3) Fetch only the required line types.
        JsonDocument woProducts;
        if (woIdsWithProducts.Count > 0)
        {
            woProducts = await _fetcher.GetWorkOrderProductsAsync(runContext, woIdsWithProducts.ToList(), ct);
            _telemetry.LogJson("Dataverse.WO.Products", runId, corr, null, woProducts.RootElement.GetRawText());
        }
        else
        {
            _log.LogWarning("No eligible WOs for product fetch. Skipping products call.");
            woProducts = EmptyValueDocument();
        }

        JsonDocument woServices;
        if (woIdsWithServices.Count > 0)
        {
            woServices = await _fetcher.GetWorkOrderServicesAsync(runContext, woIdsWithServices.ToList(), ct);
            _telemetry.LogJson("Dataverse.WO.Services", runId, corr, null, woServices.RootElement.GetRawText());
        }
        else
        {
            _log.LogWarning("No eligible WOs for service fetch. Skipping services call.");
            woServices = EmptyValueDocument();
        }

        // 4) Two-phase product enrichment
        var productIds = new HashSet<Guid>();
        CollectLookupIds(woProducts, productIds, "_msdyn_product_value", "_productid_value");
        CollectLookupIds(woServices, productIds, "_msdyn_service_value", "_msdyn_product_value", "_productid_value");

        var products = await _fetcher.GetProductsAsync(runContext, productIds.ToList(), ct);
        _telemetry.LogJson("Dataverse.Products", runId, corr, null, products.RootElement.GetRawText());

        var (productTypeById, itemNumberById) = BuildProductEnrichmentMaps(products);

        // 5) Build snapshots (only for eligible WOs)
        var snapshots = _snapshotBuilder.BuildSnapshots(
            impactedWoIds: eligibleWoIds,
            woNumberById: woIdToNumber,
            woProducts: woProducts,
            woServices: woServices,
            productTypeById: productTypeById,
            itemNumberById: itemNumberById);

        snapshots = snapshots
            .Select(s => woIdToHeaderFields.TryGetValue(s.WorkOrderId, out var h) ? s with { Header = h } : s)
            .ToList();

        // 5a) Enrich Project Categories from FSCM CDSReleasedDistinctProducts (Dataverse category not used)
        snapshots = await EnrichReleasedDistinctProductCategoriesAsync(runContext, snapshots, ct).ConfigureAwait(false);

        // 6) Outbound payload
        // IMPORTANT: TriggeredBy is not carried on snapshots; pass override so JournalDescription suffix is correct.
        var payloadJson = DeltaPayloadBuilder.BuildWoListPayload(
            snapshots,
            corr,
            runId,
            system: "FieldService",
            triggeredByOverride: input.TriggeredBy);

        // 6a) Enrich payload via step-per-concern pipeline (OCP-friendly)
        var extrasByLineGuid = FsaDeltaPayloadLookupMaps.BuildLineExtrasMapForFinalPayload(woProducts, woServices);
        var journalNamesByCompany = await FetchJournalNamesByCompanyAsync(runContext, woIdToCompanyName, ct).ConfigureAwait(false);

        var actionSuffix = DeltaPayloadBuilder.ResolveJournalActionSuffixForTriggeredBy(input.TriggeredBy);

        var enrichmentCtx = new EnrichmentContext(
            PayloadJson: payloadJson,
            RunId: runId,
            CorrelationId: corr,
            Action: actionSuffix,
            ExtrasByLineGuid: extrasByLineGuid,
            WoIdToCompanyName: woIdToCompanyName,
            JournalNamesByCompany: journalNamesByCompany,
            WoIdToSubProjectId: woIdToSubProjectId,
            WoIdToHeaderFields: woIdToHeaderFields);

        payloadJson = await _enrichmentPipeline.ApplyAsync(enrichmentCtx, ct).ConfigureAwait(false);

        _telemetry.LogJson("Delta.Payload.Outbound", runId, corr, null, payloadJson);

        // 7) FSCM baseline scaffold (unchanged)
        var baselineRecords = await _baseline.FetchBaselineAsync(ct);
        _log.LogInformation("FSCM baseline fetched (scaffold only). RecordCount={Count}", baselineRecords?.Count ?? 0);

        var woNumbers = snapshots.Select(s => s.WorkOrderNumber).Distinct().ToList();
        _log.LogInformation("FullFetch END WorkOrders={Count}", woNumbers.Count);

        return new GetFsaDeltaPayloadResultDto(
            PayloadJson: payloadJson,
            ProductDeltaLinkAfter: null,
            ServiceDeltaLinkAfter: null,
            WorkOrderNumbers: woNumbers);
    }

    private async Task<IReadOnlyDictionary<string, LegalEntityJournalNames>> FetchJournalNamesByCompanyAsync(
        RunContext ctx,
        IReadOnlyDictionary<Guid, string> woIdToCompanyName,
        CancellationToken ct)
    {
        var result = new Dictionary<string, LegalEntityJournalNames>(StringComparer.OrdinalIgnoreCase);

        if (woIdToCompanyName is null || woIdToCompanyName.Count == 0)
            return result;

        foreach (var company in woIdToCompanyName.Values
                     .Where(c => !string.IsNullOrWhiteSpace(c))
                     .Select(c => c.Trim())
                     .Distinct(StringComparer.OrdinalIgnoreCase))
        {
            if (result.ContainsKey(company))
                continue;

            try
            {
                var names = await _leParams.GetJournalNamesAsync(ctx, company, ct).ConfigureAwait(false);
                result[company] = names;
            }
            catch (Exception ex)
            {
                // Non-fatal: payload will contain empty JournalName, and posting will decide requiredness.
                _log.LogWarning(ex, "Failed to fetch FSCM journal names for Company={Company}. Proceeding with empty JournalName.", company);
                result[company] = new LegalEntityJournalNames(null, null, null);
            }
        }

        return result;
    }

    // =====================================================================
    // Payload enrichment + per-WO summary logging (Currency/Worker/Warehouse/Site/LineNum)
    // =====================================================================

    private static JsonDocument EmptyValueDocument()
    {
        return JsonDocument.Parse("{\"value\":[]}");
    }
}
