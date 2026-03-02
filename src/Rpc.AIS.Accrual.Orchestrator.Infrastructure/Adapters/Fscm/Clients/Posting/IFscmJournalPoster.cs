using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Core.Services.Validation;
using Rpc.AIS.Accrual.Orchestrator.Core.Utilities;

using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Options;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Resilience;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Utilities;

namespace Rpc.AIS.Accrual.Orchestrator.Infrastructure.Clients.Posting;

public interface IFscmJournalPoster
{
    Task<HttpPostOutcome> PostAsync(RunContext ctx, JournalType journalType, string payloadJson, CancellationToken ct);
}

public sealed record HttpPostOutcome(HttpStatusCode StatusCode, string Body, long ElapsedMs, string Url)
{
    public bool IsSuccessStatusCode => (int)StatusCode >= 200 && (int)StatusCode <= 299;
}

public sealed class FscmJournalPoster : IFscmJournalPoster
{
    private static readonly HashSet<string> SkipJournalPostingTriggers = new(StringComparer.OrdinalIgnoreCase)
    {
        // Accrual/Batch triggers: create journals but do not post them.
        "Timer",
        "AdHocSingle",
        "AdHocAll",
        "AdHocBulk"
    };

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        WriteIndented = false
        // :
        // - Do NOT set PropertyNamingPolicy here.
        // - The exact payload keys (company/journalId/journalType) should be enforced
        //   via [JsonPropertyName] on DTO properties (FscmJournalPostItem).
    };

    private readonly HttpClient _http;
    private readonly FscmOptions _endpoints;
    private readonly IFscmPostRequestFactory _reqFactory;
    private readonly IResilientHttpExecutor _executor;
    private readonly ILogger<FscmJournalPoster> _logger;
    private readonly PayloadPostingDateAdjuster _dateAdjuster;
    private readonly IAisLogger _aisLogger;
    private readonly IAisDiagnosticsOptions _diag;

    public FscmJournalPoster(
        HttpClient http,
        FscmOptions endpoints,
        IFscmPostRequestFactory reqFactory,
        IResilientHttpExecutor executor,
        PayloadPostingDateAdjuster dateAdjuster,
        IAisLogger aisLogger,
        IAisDiagnosticsOptions diag,
        ILogger<FscmJournalPoster> logger)
    {
        _http = http ?? throw new ArgumentNullException(nameof(http));
        _endpoints = endpoints ?? throw new ArgumentNullException(nameof(endpoints));
        _reqFactory = reqFactory ?? throw new ArgumentNullException(nameof(reqFactory));
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        _dateAdjuster = dateAdjuster ?? throw new ArgumentNullException(nameof(dateAdjuster));
        _aisLogger = aisLogger ?? throw new ArgumentNullException(nameof(aisLogger));
        _diag = diag ?? throw new ArgumentNullException(nameof(diag));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<HttpPostOutcome> PostAsync(RunContext ctx, JournalType journalType, string payloadJson, CancellationToken ct)
    {
        if (ctx is null) throw new ArgumentNullException(nameof(ctx));
        if (string.IsNullOrWhiteSpace(payloadJson)) throw new ArgumentException("Payload is empty.", nameof(payloadJson));

        var baseUrl = FscmUrlBuilder.ResolveFscmBaseUrl(_endpoints, _endpoints.PostingBaseUrlOverride, legacyName: "FscmPostingBaseUrl");

        // 0) Existing behavior: adjust PostingDate based on open/closed fiscal period
        payloadJson = await _dateAdjuster.AdjustAsync(ctx, payloadJson, ct).ConfigureAwait(false);

        // === NEW: Extract context once, validate per endpoint before calling FSCM ===
        var contextReq = ExtractPostingContext(payloadJson);

        // 1) Journal Validate
        if (!string.IsNullOrWhiteSpace(_endpoints.JournalValidatePath))
        {
            var fail = TryValidateEndpoint(FscmEndpointType.JournalValidate, contextReq);
            if (fail is not null) return fail;
        }

        // 2) Journal Create
        if (!string.IsNullOrWhiteSpace(_endpoints.JournalCreatePath))
        {
            var fail = TryValidateEndpoint(FscmEndpointType.JournalCreate, contextReq);
            if (fail is not null) return fail;
        }

        // 3) Journal Post (supports either new or legacy config key)
        var postPath = !string.IsNullOrWhiteSpace(_endpoints.JournalPostPath)
            ? _endpoints.JournalPostPath
            : _endpoints.JournalPostCustomPath;

        if (!string.IsNullOrWhiteSpace(postPath))
        {
            var fail = TryValidateEndpoint(FscmEndpointType.JournalPost, contextReq);
            if (fail is not null) return fail;
        }

        // === Existing workflow continues unchanged ===

        // Validate
        var validate = await CallStepAsync(ctx, "FSCM_JOURNAL_VALIDATE", baseUrl, _endpoints.JournalValidatePath, payloadJson, ct);

        if ((int)validate.StatusCode >= 500)
        {
            _logger.LogError(
                "FSCM validate returned {Status}. Blocking journal CREATE to prevent duplicates. RunId={RunId} CorrelationId={CorrelationId} JournalType={JournalType}",
                (int)validate.StatusCode, ctx.RunId, ctx.CorrelationId, journalType);

            return validate; // <- ensures CREATE never runs for 5xx
        }

        if (!validate.IsSuccessStatusCode)
            return validate;


        // Create
        var create = await CallStepAsync(ctx, "FSCM_JOURNAL_CREATE", baseUrl, _endpoints.JournalCreatePath, payloadJson, ct)
            .ConfigureAwait(false);

        if (!create.IsSuccessStatusCode)
            return create;

        // Policy: for some triggers we must NOT post journals (side-effectful POST).
        // In these flows we stop after CREATE and return the create response as the outcome.
        // The caller will treat this as success (HTTP 2xx), but WorkOrdersPosted will be forced to 0 by the outcome layer.
        if (ShouldSkipJournalPosting(ctx))
        {
            _logger.LogInformation(
                "FSCM journal POST step skipped by trigger policy. TriggeredBy={TriggeredBy} JournalType={JournalType} RunId={RunId} CorrelationId={CorrelationId}",
                ctx.TriggeredBy, journalType, ctx.RunId, ctx.CorrelationId);

            return create;
        }

        // Mandatory fields already validated above, safe to use.
        var company = contextReq.Company ?? string.Empty;

        // NEW: Extract ALL journal ids returned by create response (Item/Expense/Hour per WO)
        if (!TryExtractJournalPostsFromCreateResponse(create.Body ?? string.Empty, company, out var journalPosts) ||
            journalPosts.Count == 0)
        {
            _logger.LogError(
                "FSCM create step succeeded but did not return any JournalIds. Step={Step} RunId={RunId} CorrelationId={CorrelationId}. CreateBody={Body}",
                "FSCM_JOURNAL_CREATE", ctx.RunId, ctx.CorrelationId, LogText.TrimForLog(create.Body ?? string.Empty));

            return new HttpPostOutcome(
                HttpStatusCode.InternalServerError,
                @"{""error"":""FSCM create did not return any JournalIds; cannot post journals.""}",
                create.ElapsedMs,
                create.Url);
        }

        // NEW: Post ALL created journals in ONE call as JournalList[]
        var postPayload = JsonSerializer.Serialize(
            new FscmPostJournalRequest(
                new FscmPostJournalEnvelope(
                    journalPosts.Select(j => new FscmJournalPostItem(
                        Company: j.Company,
                        JournalId: j.JournalId,
                        JournalType: j.JournalType
                    )).ToArray())),
            JsonOpts);

        var post = await CallStepAsync(ctx, "FSCM_JOURNAL_POST", baseUrl, postPath, postPayload, ct)
            .ConfigureAwait(false);

        if (!post.IsSuccessStatusCode)
            return post;

        return post;
    }

    private static bool ShouldSkipJournalPosting(RunContext ctx)
        => !string.IsNullOrWhiteSpace(ctx.TriggeredBy) && SkipJournalPostingTriggers.Contains(ctx.TriggeredBy!);
    private sealed record JournalPostRow(string Company, string JournalId, string JournalType);

    private static bool TryExtractJournalPostsFromCreateResponse(
        string createResponseJson,
        string company,
        out List<JournalPostRow> journalPosts)
    {
        journalPosts = new List<JournalPostRow>();
        if (string.IsNullOrWhiteSpace(createResponseJson))
            return false;

        try
        {
            using var doc = JsonDocument.Parse(createResponseJson);
            var root = doc.RootElement;

            if (!root.TryGetProperty("WOList", out var woListEl) || woListEl.ValueKind != JsonValueKind.Array)
                return false;

            foreach (var woEl in woListEl.EnumerateArray())
            {
                // Item
                TryAddSectionJournal(woEl, "WOItemLines", "Item", company, journalPosts);

                // Expense
                TryAddSectionJournal(woEl, "WOExpLines", "Expense", company, journalPosts);

                // Hour (may be null)
                TryAddSectionJournal(woEl, "WOHourLines", "Hour", company, journalPosts);
            }

            // De-dup journal ids defensively (if FSCM repeats)
            journalPosts = journalPosts
                .GroupBy(j => $"{j.Company}||{j.JournalType}||{j.JournalId}", StringComparer.OrdinalIgnoreCase)
                .Select(g => g.First())
                .ToList();

            return true;
        }
        catch
        {
            journalPosts = new List<JournalPostRow>();
            return false;
        }
    }

    private static void TryAddSectionJournal(
        JsonElement woEl,
        string sectionName,
        string journalType,
        string company,
        List<JournalPostRow> target)
    {
        if (!woEl.TryGetProperty(sectionName, out var secEl))
            return;

        if (secEl.ValueKind == JsonValueKind.Null || secEl.ValueKind == JsonValueKind.Undefined)
            return;

        if (secEl.ValueKind != JsonValueKind.Object)
            return;

        // Only post when Status == "Success" AND JournalId is present
        var status = secEl.TryGetProperty("Status", out var stEl) && stEl.ValueKind == JsonValueKind.String
            ? stEl.GetString()
            : null;

        //if (!string.Equals(status, "Success", StringComparison.OrdinalIgnoreCase))
        //    return;

        var journalId = secEl.TryGetProperty("JournalId", out var idEl) && idEl.ValueKind == JsonValueKind.String
            ? idEl.GetString()
            : null;

        if (string.IsNullOrWhiteSpace(journalId))
            return;

        target.Add(new JournalPostRow(company, journalId!.Trim(), journalType));
    }

    private static PostingContextRequest ExtractPostingContext(string payloadJson)
    {
        try
        {
            using var doc = JsonDocument.Parse(payloadJson);
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
                return new PostingContextRequest(null, null);

            if (!doc.RootElement.TryGetProperty("_request", out var req) || req.ValueKind != JsonValueKind.Object)
                return new PostingContextRequest(null, null);

            if (!req.TryGetProperty("WOList", out var woList) || woList.ValueKind != JsonValueKind.Array)
                return new PostingContextRequest(null, null);

            foreach (var wo in woList.EnumerateArray())
            {
                if (wo.ValueKind != JsonValueKind.Object) continue;

                string? company = null;
                string? subProjectId = null;

                if (wo.TryGetProperty("Company", out var c) && c.ValueKind == JsonValueKind.String)
                    company = (c.GetString() ?? string.Empty).Trim();

                if (wo.TryGetProperty("SubProjectId", out var sp) && sp.ValueKind == JsonValueKind.String)
                    subProjectId = (sp.GetString() ?? string.Empty).Trim();
                else if (wo.TryGetProperty("SubProject", out var sp2) && sp2.ValueKind == JsonValueKind.String)
                    subProjectId = (sp2.GetString() ?? string.Empty).Trim();

                //  named args removed; positional ctor used
                return new PostingContextRequest(
                    string.IsNullOrWhiteSpace(company) ? null : company,
                    string.IsNullOrWhiteSpace(subProjectId) ? null : subProjectId);
            }

            return new PostingContextRequest(null, null);
        }
        catch
        {
            return new PostingContextRequest(null, null);
        }
    }

    private sealed record PostingContextRequest(string? Company, string? SubProjectId);


    private HttpPostOutcome? TryValidateEndpoint(FscmEndpointType endpoint, PostingContextRequest contextReq)
    {
        // Company is required for ALL endpoints.
        var companyMissing = string.IsNullOrWhiteSpace(contextReq.Company);

        // SubProjectId is required ONLY for Validate/Create (JournalPost works on JournalList (Company + JournalId)).
        var subProjectRequired = endpoint == FscmEndpointType.JournalValidate || endpoint == FscmEndpointType.JournalCreate;
        var subProjectMissing = subProjectRequired && string.IsNullOrWhiteSpace(contextReq.SubProjectId);

        if (!companyMissing && !subProjectMissing)
            return null;

        var errors = new System.Collections.Generic.List<string>(capacity: 2);
        if (companyMissing) errors.Add($"AIS_{endpoint}_MISSING_COMPANY");
        if (subProjectMissing) errors.Add($"AIS_{endpoint}_MISSING_SUBPROJECTID");

        _logger.LogError(
            "FSCM endpoint pre-validation failed. Endpoint={Endpoint} Errors={Errors}",
            endpoint,
            JsonSerializer.Serialize(errors, JsonOpts));

        var body = JsonSerializer.Serialize(new
        {
            error = "Endpoint request validation failed.",
            endpoint = endpoint.ToString(),
            errors
        }, JsonOpts);

        return new HttpPostOutcome(HttpStatusCode.BadRequest, body, 0, string.Empty);
    }

    // =========================
    // Existing helpers (unchanged)
    // =========================

    private static bool TryExtractJournalIdFromResponse(string responseBody, out string journalId)
    {
        journalId = string.Empty;

        if (string.IsNullOrWhiteSpace(responseBody))
            return false;

        try
        {
            using var doc = JsonDocument.Parse(responseBody);
            var root = doc.RootElement;

            if (!root.TryGetProperty("WOList", out var woList) ||
                woList.ValueKind != JsonValueKind.Array)
                return false;

            foreach (var wo in woList.EnumerateArray())
            {
                // Check all journal sections
                if (TryExtractFromSection(wo, "WOItemLines", out journalId)) return true;
                if (TryExtractFromSection(wo, "WOExpLines", out journalId)) return true;
                if (TryExtractFromSection(wo, "WOHourLines", out journalId)) return true;
            }

            return false;

            static bool TryExtractFromSection(JsonElement wo, string sectionName, out string id)
            {
                id = string.Empty;

                if (!wo.TryGetProperty(sectionName, out var section) ||
                    section.ValueKind != JsonValueKind.Object)
                    return false;

                if (!section.TryGetProperty("JournalId", out var jProp))
                    return false;

                id = jProp.ValueKind switch
                {
                    JsonValueKind.String => jProp.GetString() ?? string.Empty,
                    JsonValueKind.Number => jProp.GetRawText(),
                    _ => string.Empty
                };

                id = id.Trim();
                return id.Length > 0;
            }
        }
        catch
        {
            return false;
        }
    }

    private async Task<HttpPostOutcome> CallStepAsync(
        RunContext ctx,
        string stepName,
        string baseUrl,
        string pathOrEntitySet,
        string payloadJson,
        CancellationToken ct,
        bool isOData = false)
    {
        if (string.IsNullOrWhiteSpace(pathOrEntitySet))
        {
            _logger.LogWarning("FSCM step skipped because endpoint is empty. Step={Step} RunId={RunId} CorrelationId={CorrelationId}",
                stepName, ctx.RunId, ctx.CorrelationId);
            return new HttpPostOutcome(HttpStatusCode.OK, "", 0, "");
        }

        var url = isOData
            ? FscmUrlBuilder.BuildUrl(baseUrl, $"/data/{pathOrEntitySet}")
            : FscmUrlBuilder.BuildUrl(baseUrl, pathOrEntitySet);

        _logger.LogInformation(
            "FSCM step START. Step={Step} Url={Url} RunId={RunId} CorrelationId={CorrelationId} PayloadBytes={Bytes}",
            stepName, url, ctx.RunId, ctx.CorrelationId, Encoding.UTF8.GetByteCount(payloadJson));

        var (woGuid, woId) = TryGetFirstWorkOrderIdentity(payloadJson);
        await _aisLogger.LogJsonPayloadAsync(
            runId: ctx.RunId,
            step: stepName,
            message: "Outbound payload to FSCM",
            payloadType: "FSCM_REQUEST",
            workOrderGuid: woGuid,
            workOrderNumber: woId,
            json: payloadJson,
            logBody: _diag.LogPayloadBodies && (_diag.LogMultiWoPayloadBody || !string.Equals(woGuid, "MULTI", StringComparison.OrdinalIgnoreCase)),
            snippetChars: _diag.PayloadSnippetChars,
            chunkChars: _diag.PayloadChunkChars,
            ct: ct).ConfigureAwait(false);

        var sw = Stopwatch.StartNew();

        HttpResponseMessage resp = await _executor.SendAsync(
            _http,
            () => _reqFactory.CreateJsonPost(ctx, url, payloadJson),
            ctx,
            operationName: stepName,
            ct).ConfigureAwait(false);

        var body = (await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false)) ?? string.Empty;
        sw.Stop();

        await _aisLogger.LogJsonPayloadAsync(
            runId: ctx.RunId,
            step: stepName,
            message: "Inbound response from FSCM",
            payloadType: "FSCM_RESPONSE",
            workOrderGuid: woGuid,
            workOrderNumber: woId,
            json: body,
            logBody: _diag.LogPayloadBodies,
            snippetChars: _diag.PayloadSnippetChars,
            chunkChars: _diag.PayloadChunkChars,
            ct: ct).ConfigureAwait(false);

        _logger.LogInformation(
            "FSCM step END. Step={Step} Status={Status} Url={Url} RunId={RunId} CorrelationId={CorrelationId} ElapsedMs={ElapsedMs} ResponseBytes={ResponseBytes}",
            stepName, (int)resp.StatusCode, url, ctx.RunId, ctx.CorrelationId, sw.ElapsedMilliseconds, Encoding.UTF8.GetByteCount(body));

        if (resp.StatusCode == HttpStatusCode.Unauthorized || resp.StatusCode == HttpStatusCode.Forbidden)
        {
            _logger.LogError(
                "FSCM auth failure. Step={Step} Status={Status} RunId={RunId} CorrelationId={CorrelationId}. Body={Body}",
                stepName, (int)resp.StatusCode, ctx.RunId, ctx.CorrelationId, LogText.TrimForLog(body));

            throw new HttpRequestException(
                $"FSCM auth failed at {stepName} ({(int)resp.StatusCode} {resp.ReasonPhrase}). Body: {LogText.TrimForLog(body)}",
                null,
                resp.StatusCode);
        }

        return new HttpPostOutcome(resp.StatusCode, body, sw.ElapsedMilliseconds, url);
    }

    private static (string WorkOrderGuid, string? WorkOrderId) TryGetFirstWorkOrderIdentity(string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            if (!doc.RootElement.TryGetProperty("_request", out var req) || req.ValueKind != JsonValueKind.Object)
                return ("MULTI", null);
            if (!req.TryGetProperty("WOList", out var list) || list.ValueKind != JsonValueKind.Array)
                return ("MULTI", null);

            using var e = list.EnumerateArray();
            if (!e.MoveNext()) return ("MULTI", null);
            var wo = e.Current;

            static string? ReadString(JsonElement obj, string prop)
            {
                if (obj.ValueKind != JsonValueKind.Object) return null;
                if (obj.TryGetProperty(prop, out var v))
                {
                    if (v.ValueKind == JsonValueKind.String) return v.GetString();
                    if (v.ValueKind == JsonValueKind.Number || v.ValueKind == JsonValueKind.True || v.ValueKind == JsonValueKind.False) return v.ToString();
                }
                return null;
            }

            var guid = ReadString(wo, "WorkOrderGUID") ?? ReadString(wo, "WorkOrderGuid") ?? ReadString(wo, "workOrderGuid");
            var id = ReadString(wo, "WorkOrderID") ?? ReadString(wo, "WorkOrderId") ?? ReadString(wo, "workOrderId") ?? ReadString(wo, "WO Number");
            if (string.IsNullOrWhiteSpace(guid)) return ("MULTI", id);
            return (guid.Trim(), id);
        }
        catch
        {
            return ("MULTI", null);
        }
    }
}
