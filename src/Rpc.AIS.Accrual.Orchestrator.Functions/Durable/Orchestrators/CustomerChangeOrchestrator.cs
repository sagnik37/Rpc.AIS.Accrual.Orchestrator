using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Core.Services;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Options;

namespace Rpc.AIS.Accrual.Orchestrator.Functions.Services;

public sealed class CustomerChangeOrchestrator : ICustomerChangeOrchestrator
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        WriteIndented = false,
        PropertyNamingPolicy = null
    };

    private readonly ILogger<CustomerChangeOrchestrator> _log;
    private readonly SubProjectProvisioningService _subProjectSvc;
    private readonly IFsaDeltaPayloadOrchestrator _fsaPayloadOrch;
    private readonly IPostingClient _posting;
    private readonly IWoDeltaPayloadServiceV2 _deltaV2;
    private readonly IFscmProjectStatusClient _projectStatus;
    private readonly FsOptions _fsOpt;
    private readonly InvoiceAttributeSyncRunner _invoiceSync;
    private readonly InvoiceAttributesUpdateRunner _invoiceUpdate;

    public CustomerChangeOrchestrator(
        ILogger<CustomerChangeOrchestrator> log,
        SubProjectProvisioningService subProjectSvc,
        IFsaDeltaPayloadOrchestrator fsaPayloadOrch,
        IPostingClient posting,
        IWoDeltaPayloadServiceV2 deltaV2,
        IFscmProjectStatusClient projectStatus,
        InvoiceAttributeSyncRunner invoiceSync,
        InvoiceAttributesUpdateRunner invoiceUpdate,
        IOptions<FsOptions> fsOpt)
    {
        _log = log ?? throw new ArgumentNullException(nameof(log));
        _subProjectSvc = subProjectSvc ?? throw new ArgumentNullException(nameof(subProjectSvc));
        _fsaPayloadOrch = fsaPayloadOrch ?? throw new ArgumentNullException(nameof(fsaPayloadOrch));
        _posting = posting ?? throw new ArgumentNullException(nameof(posting));
        _deltaV2 = deltaV2 ?? throw new ArgumentNullException(nameof(deltaV2));
        _projectStatus = projectStatus ?? throw new ArgumentNullException(nameof(projectStatus));
        _invoiceSync = invoiceSync ?? throw new ArgumentNullException(nameof(invoiceSync));
        _invoiceUpdate = invoiceUpdate ?? throw new ArgumentNullException(nameof(invoiceUpdate));
        _fsOpt = fsOpt?.Value ?? new FsOptions();
    }

    public async Task<CustomerChangeResultDto> ExecuteAsync(
        RunContext ctx,
        Guid workOrderGuid,
        string rawRequestJson,
        CancellationToken ct)
    {
        if (ctx is null) throw new ArgumentNullException(nameof(ctx));

        var req = CustomerChangeRequest.TryParse(rawRequestJson)
                  ?? throw new InvalidOperationException("CustomerChange request payload is missing or invalid.");

        if (req.WorkOrderGuid != Guid.Empty && req.WorkOrderGuid != workOrderGuid)
            throw new InvalidOperationException("WorkOrderGuid mismatch between orchestration input and request body.");

        if (string.IsNullOrWhiteSpace(req.OldSubProjectId))
            throw new InvalidOperationException("OldSubProjectId is required for Customer Change.");

        // ---- Always extract from RAW FS request (authoritative if FullFetch is empty) ----
        var company = req.LegalEntity; // parsed from FS payload: Company
        if (string.IsNullOrWhiteSpace(company))
            throw new InvalidOperationException("Company/DataAreaId could not be resolved (missing in request payload).");

        var resolvedWorkOrderId = req.WorkOrderId ?? workOrderGuid.ToString("D");

        var parentProjectId = req.ParentProjectId;
        if (string.IsNullOrWhiteSpace(parentProjectId))
            throw new InvalidOperationException("ParentProjectId is required (provide it at root or inside NewSubProjectOverrides; TryParse flattens it).");

        var projectName = string.IsNullOrWhiteSpace(req.ProjectName)
            ? $"WO-SubProject-{DateTimeOffset.UtcNow:yyyyMMddHHmmss}"
            : req.ProjectName!;

        var isFsaProject = req.IsFsaProject;          // may be null
        var projectStatus = req.ProjectStatus ?? 3;   // default 3 if not provided

        // ---- 1) Fetch canonical WO payload from FSA (may be empty if WO has no lines / ignored) ----
        var fsa = await _fsaPayloadOrch.BuildFullFetchAsync(
            new GetFsaDeltaPayloadInputDto(
                ctx.RunId,
                ctx.CorrelationId,
                ctx.TriggeredBy ?? "CustomerChange",
                workOrderGuid.ToString("D")),
            _fsOpt,
            ct).ConfigureAwait(false);

        var fsaPayloadJson = fsa?.PayloadJson;
        var fsaEmpty = string.IsNullOrWhiteSpace(fsaPayloadJson);

        // If FullFetch is NOT empty, prefer canonical Company + WorkOrderId from FullFetch.
        // (Do not fail if FullFetch is missing these—fallback to request-derived values.)
        if (!fsaEmpty)
        {
            var parsed = PayloadReads.TryReadCompanyAndWorkOrderId(fsaPayloadJson!, workOrderGuid);

            if (!string.IsNullOrWhiteSpace(parsed.Company))
                company = parsed.Company!;

            if (!string.IsNullOrWhiteSpace(parsed.WorkOrderId))
                resolvedWorkOrderId = parsed.WorkOrderId!;
        }

        // Local helper: Cancel OLD subproject (status=6)
        async Task CancelOldAsync()
        {
            _log.LogInformation("CustomerChange: ProjectStatusUpdate BEGIN OldSubProjectId={Old} Status=6", req.OldSubProjectId);

            var statusRes = await _projectStatus.UpdateAsync(
                ctx,
                company!,
                req.OldSubProjectId!,
                workOrderGuid,
                resolvedWorkOrderId!,
                status: 6,
                ct).ConfigureAwait(false);

            if (!statusRes.IsSuccess)
                throw new InvalidOperationException($"ProjectStatusUpdate failed. Http={statusRes.HttpStatus} Body={statusRes.Body}");

            _log.LogInformation("CustomerChange: ProjectStatusUpdate OK OldSubProjectId={Old}", req.OldSubProjectId);
        }

        // Local helper: Create NEW subproject from flattened request fields
        async Task<string> CreateNewAsync()
        {
            var createReq = new SubProjectCreateRequest(
                DataAreaId: company!,
                ParentProjectId: parentProjectId!,
                ProjectName: projectName,
                CustomerReference: null,
                InvoiceNotes: null,
                ActualStartDate: null,
                ActualEndDate: null,
                AddressName: null,
                Street: null,
                City: null,
                State: null,
                County: null,
                CountryRegionId: null,
                WellLocale: null,
                WellName: null,
                WellNumber: null,
                ProjectStatus: projectStatus)
            {
                // : exact FSCM contract fields asked for
                WorkOrderGuid = req.WorkOrderGuid == Guid.Empty ? null : req.WorkOrderGuid.ToString("B").ToUpperInvariant(), // "{GUID}"
                IsFsaProject = isFsaProject,
                ProjectStatus = projectStatus
            };

            _log.LogInformation(
                "CustomerChange: CreateSubproject BEGIN OldSubProjectId={Old} Company={Company} ParentProjectId={Parent} ProjectName={Name} IsFSAProject={IsFSAProject} ProjectStatus={ProjectStatus}",
                req.OldSubProjectId, createReq.DataAreaId, createReq.ParentProjectId, createReq.ProjectName, createReq.IsFsaProject, createReq.ProjectStatus);

            var createRes = await _subProjectSvc.ProvisionAsync(ctx, createReq, ct).ConfigureAwait(false);

            if (!createRes.IsSuccess || string.IsNullOrWhiteSpace(createRes.parmSubProjectId))
                throw new InvalidOperationException($"Subproject creation failed. Message={createRes.Message}");

            var newSubProjectId = createRes.parmSubProjectId!;
            _log.LogInformation("CustomerChange: CreateSubproject OK NewSubProjectId={New}", newSubProjectId);

            return newSubProjectId;
        }

        // -------------------------------------------------------
        // REQUIRED BRANCH:
        // IF FullFetch has lines -> normal flow
        // ELSE -> Cancel OLD, Create NEW, invoice sync NEW, return
        // -------------------------------------------------------
        if (fsaEmpty)
        {
            _log.LogWarning(
                "CustomerChange: FSA FullFetch payload is empty/ignored (no lines). Performing project-level move only. OldSubProjectId={Old} Company={Company}",
                req.OldSubProjectId, company);

            // 1) Cancel OLD
            await CancelOldAsync().ConfigureAwait(false);

            // 2) Create NEW
            var newSubProjectIdEmpty = await CreateNewAsync().ConfigureAwait(false);

            // 3) NEW FIX: Always sync invoice attributes for NEW subproject even when FullFetch is empty.
            // Use a minimal posting-envelope payload that the invoice sync/update pipeline can enrich.
            var minimalInvoicePayload = BuildMinimalInvoicePayloadJson(
                ctx,
                workOrderGuid,
                resolvedWorkOrderId!,
                company!,
                newSubProjectIdEmpty);

            await BestEffortInvoiceAttributesAsync(ctx, minimalInvoicePayload, ct).ConfigureAwait(false);

            return new CustomerChangeResultDto(newSubProjectIdEmpty);
        }

        // ---- Normal flow (FullFetch has lines) ----

        // 2) Create NEW subproject
        var newSubProjectId = await CreateNewAsync().ConfigureAwait(false);

        // 3) Recreate into NEW subproject via DeltaV2 Normal (Baseline = New subproject)
        var fsaForNew = PayloadReads.RewriteSubProjectId(fsaPayloadJson!, newSubProjectId);
        var todayUtc = DateTime.UtcNow.Date;

        var deltaNew = await _deltaV2.BuildDeltaPayloadAsync(
            ctx,
            fsaForNew,
            todayUtc,
            new WoDeltaBuildOptions(BaselineSubProjectId: newSubProjectId, TargetMode: WoDeltaTargetMode.Normal),
            ct).ConfigureAwait(false);

        // Post NEW if there are deltas
        if (!string.IsNullOrWhiteSpace(deltaNew.DeltaPayloadJson) && deltaNew.TotalDeltaLines > 0)
        {
            _log.LogInformation("CustomerChange: Post NEW BEGIN NewSubProjectId={New} DeltaLines={Lines}", newSubProjectId, deltaNew.TotalDeltaLines);

            var postNew = await _posting.ValidateOnceAndPostAllJournalTypesAsync(ctx, deltaNew.DeltaPayloadJson!, ct).ConfigureAwait(false);
            if (postNew.Any(r => !r.IsSuccess))
                throw new InvalidOperationException("Posting to NEW subproject failed (see PostResults).");

            _log.LogInformation("CustomerChange: Post NEW OK NewSubProjectId={New}", newSubProjectId);
        }
        else
        {
            _log.LogInformation("CustomerChange: No delta lines for NEW recreate; skipping post. NewSubProjectId={New}", newSubProjectId);
        }

        //  NEW FIX: Always sync invoice attributes for NEW subproject, even when deltaNew has 0 lines.
        // Prefer delta payload if present (it matches posting envelope shape), otherwise use FullFetch rewritten to NEW.
        // As a last resort, use minimal payload.
        var invoicePayloadForNew =
            (!string.IsNullOrWhiteSpace(deltaNew.DeltaPayloadJson) ? deltaNew.DeltaPayloadJson! :
             !string.IsNullOrWhiteSpace(fsaForNew) ? fsaForNew :
             BuildMinimalInvoicePayloadJson(ctx, workOrderGuid, resolvedWorkOrderId!, company!, newSubProjectId));

        await BestEffortInvoiceAttributesAsync(ctx, invoicePayloadForNew, ct).ConfigureAwait(false);

        // 4) Reverse OLD subproject via DeltaV2 CancelToZero (Baseline = Old subproject)
        var fsaForOld = PayloadReads.RewriteSubProjectId(fsaPayloadJson!, req.OldSubProjectId!);

        var deltaOld = await _deltaV2.BuildDeltaPayloadAsync(
            ctx,
            fsaForOld,
            todayUtc,
            new WoDeltaBuildOptions(BaselineSubProjectId: req.OldSubProjectId, TargetMode: WoDeltaTargetMode.CancelToZero),
            ct).ConfigureAwait(false);

        if (!string.IsNullOrWhiteSpace(deltaOld.DeltaPayloadJson) && deltaOld.TotalDeltaLines > 0)
        {
            _log.LogInformation("CustomerChange: Reverse OLD BEGIN OldSubProjectId={Old} DeltaLines={Lines}", req.OldSubProjectId, deltaOld.TotalDeltaLines);

            var postOld = await _posting.ValidateOnceAndPostAllJournalTypesAsync(ctx, deltaOld.DeltaPayloadJson!, ct).ConfigureAwait(false);
            if (postOld.Any(r => !r.IsSuccess))
                throw new InvalidOperationException("Reversal posting to OLD subproject failed (see PostResults).");

            _log.LogInformation("CustomerChange: Reverse OLD OK OldSubProjectId={Old}", req.OldSubProjectId);
        }
        else
        {
            _log.LogInformation("CustomerChange: No delta lines for OLD reversal; skipping post. OldSubProjectId={Old}", req.OldSubProjectId);
        }

        // 5) Cancel OLD subproject (Status = 6)
        await CancelOldAsync().ConfigureAwait(false);

        return new CustomerChangeResultDto(newSubProjectId);
    }

    private async Task BestEffortInvoiceAttributesAsync(RunContext ctx, string postingPayloadJson, CancellationToken ct)
    {
        try
        {
            var enriched = await _invoiceSync.EnrichPostingPayloadAsync(ctx, postingPayloadJson, ct).ConfigureAwait(false);
            await _invoiceUpdate.UpdateFromPostingPayloadAsync(ctx, enriched.PostingPayloadJson, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "CustomerChange: InvoiceAttributesUpdate FAILED (best-effort). RunId={RunId} CorrelationId={CorrelationId}",
                ctx.RunId, ctx.CorrelationId);
        }
    }

    /// <summary>
    /// Minimal payload builder used when we have no delta payload and/or FullFetch is empty.
    /// This payload is shaped like the posting envelope and is sufficient for the invoice sync/update pipeline.
    /// </summary>
    private static string BuildMinimalInvoicePayloadJson(
        RunContext ctx,
        Guid workOrderGuid,
        string workOrderId,
        string company,
        string subProjectId)
    {
        var woGuid = "{" + workOrderGuid.ToString("D").ToUpperInvariant() + "}";

        var root = new JsonObject
        {
            ["_request"] = new JsonObject
            {
                ["System"] = "FieldService",
                ["RunId"] = ctx.RunId,
                ["CorrelationId"] = ctx.CorrelationId,
                ["WOList"] = new JsonArray
                {
                    new JsonObject
                    {
                        ["Company"] = company ?? string.Empty,
                        ["WorkOrderGUID"] = woGuid,
                        ["WorkOrderID"] = workOrderId ?? string.Empty,
                        ["SubProjectId"] = subProjectId ?? string.Empty
                    }
                }
            }
        };

        return root.ToJsonString(JsonOpts);
    }

    private sealed record CustomerChangeRequest(
        Guid WorkOrderGuid,
        string? WorkOrderId,
        string? OldSubProjectId,
        string? ParentProjectId,
        string? ProjectName,
        int? IsFsaProject,
        int? ProjectStatus,
        string? LegalEntity)
    {
        public static CustomerChangeRequest? TryParse(string? raw)
        {
            if (string.IsNullOrWhiteSpace(raw)) return null;

            try
            {
                using var doc = JsonDocument.Parse(raw);
                var root = doc.RootElement;

                // FS envelope: { "_request": { "WOList": [ { ... } ] } }
                var payloadRoot = root;
                if (root.ValueKind == JsonValueKind.Object &&
                    root.TryGetProperty("_request", out var req) && req.ValueKind == JsonValueKind.Object &&
                    req.TryGetProperty("WOList", out var woList) && woList.ValueKind == JsonValueKind.Array)
                {
                    var first = woList.EnumerateArray().FirstOrDefault();
                    if (first.ValueKind == JsonValueKind.Object)
                        payloadRoot = first;
                }

                Guid wo = Guid.Empty;
                if (TryGetString(payloadRoot, "WorkOrderGUID", out var woStr) ||
                    TryGetString(payloadRoot, "WorkOrderGuid", out woStr) ||
                    TryGetString(payloadRoot, "workOrderGuid", out woStr))
                {
                    // allow "{GUID}" format
                    var trimmed = woStr?.Trim();
                    if (!string.IsNullOrWhiteSpace(trimmed))
                        Guid.TryParse(trimmed.Trim('{', '}'), out wo);
                }

                TryGetString(payloadRoot, "WorkOrderID", out var woId);

                // NOTE: oldSubProjectId might be provided under different casing/key styles depending on caller.
                TryGetString(payloadRoot, "OldSubProjectId", out var old);
                if (string.IsNullOrWhiteSpace(old))
                    TryGetString(payloadRoot, "OldSubProjectID", out old);

                // Root-level fallback (older callers)
                TryGetString(payloadRoot, "ParentProjectId", out var parent);
                TryGetString(payloadRoot, "ProjectName", out var name);

                int? isFsaProject = null;
                int? projectStatus = null;

                // NEW contract: NewSubProjectOverrides
                if (payloadRoot.ValueKind == JsonValueKind.Object &&
                    payloadRoot.TryGetProperty("NewSubProjectOverrides", out var ov) &&
                    ov.ValueKind == JsonValueKind.Object)
                {
                    if (TryGetString(ov, "ParentProjectId", out var ovParent) && !string.IsNullOrWhiteSpace(ovParent))
                        parent = ovParent;

                    if (TryGetString(ov, "ProjectName", out var ovName) && !string.IsNullOrWhiteSpace(ovName))
                        name = ovName;

                    if (TryGetInt32(ov, "IsFSAProject", out var n1))
                        isFsaProject = n1;

                    if (TryGetInt32(ov, "ProjectStatus", out var n2))
                        projectStatus = n2;
                }

                // Legal entity from FS payload: Company
                TryGetString(payloadRoot, "Company", out var le);

                return new CustomerChangeRequest(
                    WorkOrderGuid: wo,
                    WorkOrderId: woId,
                    OldSubProjectId: old,
                    ParentProjectId: parent,
                    ProjectName: name,
                    IsFsaProject: isFsaProject,
                    ProjectStatus: projectStatus,
                    LegalEntity: le);
            }
            catch
            {
                return null;
            }
        }

        private static bool TryGetString(JsonElement root, string prop, out string? value)
        {
            value = null;
            if (root.ValueKind != JsonValueKind.Object) return false;
            if (!root.TryGetProperty(prop, out var el)) return false;

            if (el.ValueKind == JsonValueKind.String)
            {
                value = el.GetString();
                return true;
            }

            value = el.ToString();
            return true;
        }

        private static bool TryGetInt32(JsonElement root, string prop, out int value)
        {
            value = default;
            if (root.ValueKind != JsonValueKind.Object) return false;
            if (!root.TryGetProperty(prop, out var el)) return false;

            if (el.ValueKind == JsonValueKind.Number && el.TryGetInt32(out var n))
            {
                value = n;
                return true;
            }

            if (el.ValueKind == JsonValueKind.String &&
                int.TryParse(el.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var s))
            {
                value = s;
                return true;
            }

            return false;
        }
    }

    private static class PayloadReads
    {
        public static (string? Company, string? WorkOrderId) TryReadCompanyAndWorkOrderId(string woPayloadJson, Guid workOrderGuid)
        {
            try
            {
                var node = JsonNode.Parse(woPayloadJson);
                if (node is null) return (null, null);

                var req = node["_request"] as JsonObject;
                var woList = req?["WOList"] as JsonArray;
                if (woList is null) return (null, null);

                foreach (var wo in woList.OfType<JsonObject>())
                {
                    var guidStr = wo["WorkOrderGUID"]?.ToString();
                    if (!Guid.TryParse(guidStr?.Trim('{', '}'), out var g) || g != workOrderGuid)
                        continue;

                    var company = wo["Company"]?.ToString();
                    var woId = wo["WorkOrderID"]?.ToString();
                    return (company, woId);
                }
            }
            catch { }

            return (null, null);
        }

        public static string RewriteSubProjectId(string woPayloadJson, string subProjectId)
        {
            if (string.IsNullOrWhiteSpace(woPayloadJson)) return woPayloadJson;
            if (string.IsNullOrWhiteSpace(subProjectId)) return woPayloadJson;

            JsonNode? node;
            try { node = JsonNode.Parse(woPayloadJson); }
            catch { return woPayloadJson; }

            if (node is not JsonObject root) return woPayloadJson;
            if (!root.TryGetPropertyValue("_request", out var reqNode) || reqNode is not JsonObject reqObj) return woPayloadJson;
            if (!reqObj.TryGetPropertyValue("WOList", out var listNode) || listNode is not JsonArray woList) return woPayloadJson;

            foreach (var wo in woList.OfType<JsonObject>())
            {
                wo["SubProjectId"] = subProjectId;
            }

            return root.ToJsonString(JsonOpts);
        }
    }
}
