// File: Rpc.AIS.Accrual.Orchestrator/src/Rpc.AIS.Accrual.Orchestrator.Infrastructure/Clients/FscmJournalFetchHttpClient.cs
//

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Options;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Clients.FscmJournalPolicies;

namespace Rpc.AIS.Accrual.Orchestrator.Infrastructure.Clients;

/// <summary>
/// FSCM OData fetch client to retrieve journal lines by WorkOrder GUID.
/// Maps multiple entity sets (Item/Expense/Hour) into a normalized FscmJournalLine DTO.
/// </summary>
public sealed class FscmJournalFetchHttpClient : IFscmJournalFetchClient
{
    private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly HttpClient _http;
    private readonly FscmOptions _endpoints;
    private readonly ILogger<FscmJournalFetchHttpClient> _logger;
    private readonly FscmJournalFetchPolicyResolver _policyResolver;

    public FscmJournalFetchHttpClient(
        HttpClient http,
        FscmOptions endpoints,
        FscmJournalFetchPolicyResolver policyResolver,
        ILogger<FscmJournalFetchHttpClient> logger)
    {
        _http = http ?? throw new ArgumentNullException(nameof(http));
        _endpoints = endpoints ?? throw new ArgumentNullException(nameof(endpoints));
        _policyResolver = policyResolver ?? throw new ArgumentNullException(nameof(policyResolver));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    //  FIX (CS0535): This method matches the interface EXACTLY.
    public Task<IReadOnlyList<FscmJournalLine>> FetchByWorkOrdersAsync(
        RunContext context,
        JournalType journalType,
        IReadOnlyCollection<Guid> workOrderIds,
        CancellationToken ct)
    {
        return FetchByWorkOrdersInternalAsync(context, journalType, workOrderIds, ct, allowSelectFallback: true);
    }

    // Internal helper with the extra flag (not part of interface)
    private async Task<IReadOnlyList<FscmJournalLine>> FetchByWorkOrdersInternalAsync(
        RunContext context,
        JournalType journalType,
        IReadOnlyCollection<Guid> workOrderIds,
        CancellationToken ct,
        bool allowSelectFallback)
    {
        if (context is null) throw new ArgumentNullException(nameof(context));
        if (workOrderIds is null) throw new ArgumentNullException(nameof(workOrderIds));

        var cleaned = workOrderIds.Where(g => g != Guid.Empty).Distinct().ToList();
        if (cleaned.Count == 0) return Array.Empty<FscmJournalLine>();

        var baseUrl = ResolveBaseUrlOrThrow();
        var policy = _policyResolver.Resolve(journalType);

        var entitySet = policy.EntitySet;
        const string workOrderIdField = "RPCWorkOrderGuid";
        const string workOrderLineIdField = "RPCWorkOrderLineGuid";

        var select = policy.Select;

        var chunkSize = _endpoints.JournalHistoryOrFilterChunkSize <= 0 ? 25 : _endpoints.JournalHistoryOrFilterChunkSize;
        var all = new List<FscmJournalLine>(capacity: Math.Min(cleaned.Count * 8, 5000));

        foreach (var chunk in Chunk(cleaned, chunkSize))
        {
            // In  env: RPCWorkOrderGuid eq <bare-guid>
            //  Do NOT wrap in guid'...' unless  env requires it. Here we keep it bare-guid
            // to match the existing single-WO implementation.select={select}
            var filter = string.Join(" or ", chunk.Select(id => $"{workOrderIdField} eq {id:D}"));

            var url =
                $"{baseUrl.TrimEnd('/')}/data/{entitySet}" +
                $"?cross-company=true&$&$filter={filter.Replace(" ", "%20")}";

            var lines = await FetchSingleUrlAsync(
                    context,
                    journalType,
                    entitySet,
                    url,
                    policy,
                    workOrderLineIdField,
                    ct,
                    allowSelectFallback: allowSelectFallback)
                .ConfigureAwait(false);

            if (lines.Count != 0)
                all.AddRange(lines);
        }

        return all;
    }

    private async Task<IReadOnlyList<FscmJournalLine>> FetchSingleUrlAsync(
        RunContext context,
        JournalType journalType,
        string entitySet,
        string url,
        IFscmJournalFetchPolicy policy,
        string workOrderLineIdField,
        CancellationToken ct,
        bool allowSelectFallback = true)
    {
        using var req = new HttpRequestMessage(HttpMethod.Get, url);
        req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        // Propagate correlation (best effort)
        if (!string.IsNullOrWhiteSpace(context.RunId))
            req.Headers.TryAddWithoutValidation("x-run-id", context.RunId);
        if (!string.IsNullOrWhiteSpace(context.CorrelationId))
            req.Headers.TryAddWithoutValidation("x-correlation-id", context.CorrelationId);

        _logger.LogInformation(
            "FSCM OData fetch START. JournalType={JournalType} EntitySet={EntitySet} Url={Url} RunId={RunId} CorrelationId={CorrelationId}",
            journalType, entitySet, url, context.RunId, context.CorrelationId);

        var sw = Stopwatch.StartNew();
        using var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
        var body = (await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false)) ?? string.Empty;
        sw.Stop();

        _logger.LogInformation(
            "FSCM OData fetch END. Status={Status} JournalType={JournalType} EntitySet={EntitySet} ElapsedMs={ElapsedMs} Bytes={Bytes} RunId={RunId} CorrelationId={CorrelationId}",
            (int)resp.StatusCode, journalType, entitySet, sw.ElapsedMilliseconds, body.Length, context.RunId, context.CorrelationId);

        // Auth failures => fail-fast
        if (resp.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)
        {
            _logger.LogError(
                "FSCM OData fetch AUTH failure. Status={Status} JournalType={JournalType} EntitySet={EntitySet} Body={Body} RunId={RunId} CorrelationId={CorrelationId}",
                (int)resp.StatusCode, journalType, entitySet, Trim(body), context.RunId, context.CorrelationId);

            throw new UnauthorizedAccessException(
                $"FSCM OData unauthorized/forbidden for {entitySet}. HTTP {(int)resp.StatusCode}. Body: {Trim(body)}");
        }

        // Transient => durable retry
        if ((int)resp.StatusCode == 429 || (int)resp.StatusCode >= 500)
        {
            _logger.LogWarning(
                "FSCM OData fetch transient failure. Status={Status} JournalType={JournalType} EntitySet={EntitySet} Body={Body} RunId={RunId} CorrelationId={CorrelationId}",
                (int)resp.StatusCode, journalType, entitySet, Trim(body), context.RunId, context.CorrelationId);

            throw new HttpRequestException(
                $"Transient FSCM OData fetch failure {(int)resp.StatusCode} {resp.ReasonPhrase}. EntitySet={entitySet}. Body: {Trim(body)}",
                null,
                resp.StatusCode);
        }

        // Other 4xx => non-transient => treat as "no data" but log
        if ((int)resp.StatusCode >= 400 && (int)resp.StatusCode <= 499)
        {
            _logger.LogWarning(
                "FSCM OData fetch non-transient failure. Status={Status} JournalType={JournalType} EntitySet={EntitySet} Body={Body} RunId={RunId} CorrelationId={CorrelationId}",
                (int)resp.StatusCode, journalType, entitySet, Trim(body), context.RunId, context.CorrelationId);

            // Some FSCM environments don't expose all fields used in the policy's $select (e.g., optional amounts for price normalization).
            // If we detect a missing-field OData error, retry once with the policy's fallback select.
            if (allowSelectFallback && policy is FscmJournalFetchPolicyBase basePolicy)
            {
                var fallbackSelect = basePolicy.SelectFallback;
                if (!string.IsNullOrWhiteSpace(fallbackSelect)
                    && !string.Equals(fallbackSelect, policy.Select, StringComparison.Ordinal)
                    && LooksLikeMissingSelectFieldError(body))
                {
                    var fallbackUrl = ReplaceSelect(url, fallbackSelect);
                    _logger.LogWarning(
                        "Retrying FSCM OData fetch with fallback $select due to missing-field error. JournalType={JournalType} EntitySet={EntitySet} RunId={RunId} CorrelationId={CorrelationId}",
                        journalType, entitySet, context.RunId, context.CorrelationId);

                    return await FetchSingleUrlAsync(context, journalType, entitySet, fallbackUrl, policy, workOrderLineIdField, ct, allowSelectFallback: false)
                        .ConfigureAwait(false);
                }
            }

            return Array.Empty<FscmJournalLine>();
        }

        return ParseODataValueArrayToJournalLines(body, policy, workOrderLineIdField);
    }

    /// <summary>
    /// Executes resolve base url or throw.
    /// </summary>
    private string ResolveBaseUrlOrThrow()
    {
        var baseUrl = _endpoints.ResolveBaseUrl(_endpoints.BaseUrl);
        if (string.IsNullOrWhiteSpace(baseUrl))
            throw new InvalidOperationException("FSCM base URL missing. Configure 'Endpoints:BaseUrl'.");
        return baseUrl;
    }

    private static bool LooksLikeMissingSelectFieldError(string body)
    {
        if (string.IsNullOrWhiteSpace(body)) return false;

        // Common OData messages for missing properties/fields across FO versions
        return body.Contains("Cannot find property", StringComparison.OrdinalIgnoreCase)
            || body.Contains("Could not find a property named", StringComparison.OrdinalIgnoreCase)   // 
            || body.Contains("does not exist", StringComparison.OrdinalIgnoreCase)
            || body.Contains("is not declared", StringComparison.OrdinalIgnoreCase)
            || (body.Contains("$select", StringComparison.OrdinalIgnoreCase) && body.Contains("property", StringComparison.OrdinalIgnoreCase));
    }

    private static string ReplaceSelect(string url, string newSelect)
    {
        if (string.IsNullOrWhiteSpace(url) || string.IsNullOrWhiteSpace(newSelect)) return url;

        // Replace $select=... up to next '&' (or end)
        return System.Text.RegularExpressions.Regex.Replace(
            url,
            @"(\$select=)([^&]+)", //  FIX (CS1009): verbatim string avoids invalid escape sequence
            m => m.Groups[1].Value + newSelect,
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
    }

    private List<FscmJournalLine> ParseODataValueArrayToJournalLines(
        string json,
        IFscmJournalFetchPolicy policy,
        string workOrderLineIdField)
    {
        if (string.IsNullOrWhiteSpace(json))
            return new List<FscmJournalLine>(0);

        using var doc = JsonDocument.Parse(json);

        if (!doc.RootElement.TryGetProperty("value", out var value) || value.ValueKind != JsonValueKind.Array)
            return new List<FscmJournalLine>(0);

        var result = new List<FscmJournalLine>(value.GetArrayLength());

        foreach (var row in value.EnumerateArray())
        {
            // WorkOrderId is REQUIRED to route batched lines back to the correct WO.
            // In  env this is present as RPCWorkOrderGuid.
            var woId =
                TryGetGuidLoose(row, "RPCWorkOrderGuid") ??
                TryGetGuidLoose(row, "WorkOrderGuid") ??
                Guid.Empty;

            if (woId == Guid.Empty)
                continue;

            // WorkOrderLineId is REQUIRED for delta grouping. If missing, skip safely.
            var woLineId =
                TryGetGuid(row, workOrderLineIdField) ??
                TryGetGuid(row, "WorkOrderLineId") ??
                TryGetGuid(row, "WorkOrderLine") ??
                Guid.Empty;

            if (woLineId == Guid.Empty)
                continue;

            // -----------------------------
            // Quantity / Hours (per type)
            // -----------------------------
            var quantity = policy.GetQuantity(row);

            // -----------------------------
            // Unit price (per type)
            // -----------------------------
            decimal? unitPrice = policy.GetUnitPrice(row);

            // -----------------------------
            // Line property (per type)
            // Hour: LineProperty
            // Item: ProjectLinePropertyId
            // Expense: tolerant fallbacks
            // -----------------------------
            var lineProperty =
                TryGetString(row, "LineProperty") ??
                TryGetString(row, "ProjectLinePropertyId") ??
                TryGetString(row, "ProjectLineProperty") ??
                TryGetNumberAsString(row, "ProjectLinePropertyId") ??
                TryGetNumberAsString(row, "ProjectLineProperty");

            // -----------------------------
            // Department + ProductLine
            //
            // RULE (ALL JOURNALS):
            // - Parse DimensionDisplayValue = "<Department>--<ProductLine>"
            // - FSCM will NOT send RPCDimension1/RPCDimension2 anymore.
            // -----------------------------
            var dimKey = policy.JournalType == JournalType.Item
                ? "DefaultDimensionDisplayValue"
                : "DimensionDisplayValue";

            var dimDisplay = TryGetString(row, dimKey);

            var (dept, productLine) = ParseDefaultDimensionDisplayValue(dimDisplay);

            if (dept is null || productLine is null)
            {
                _logger.LogWarning(
                    "DimensionDisplayValue missing/invalid. DimensionField={DimensionField} JournalType={JournalType} WorkOrderId={WorkOrderId} WorkOrderLineId={WorkOrderLineId} Value='{Value}'",
                    dimKey,
                    policy.JournalType,
                    woId,
                    woLineId,
                    dimDisplay);
            }

            // -----------------------------
            // Transaction date
            //  responses use ProjectDate
            // -----------------------------
            var transDate =
                TryGetDate(row, "ProjectDate") ??
                TryGetDate(row, "VoucherDate") ??
                TryGetDate(row, "TransDate") ??
                TryGetDate(row, "PostingDate");

            var subProjectId =
                TryGetString(row, "SubProjectId") ??
                TryGetString(row, "ProjId") ??
                TryGetString(row, "ProjectId");

            var dataArea = TryGetString(row, "DataAreaId");
            var journalNum = TryGetString(row, "JournalNum");

            // ExtendedAmount: compute if FO didn’t supply
            decimal? extAmount =
                TryGetDecimal(row, "LineAmount") ??
                TryGetDecimal(row, "Amount");

            if (!extAmount.HasValue && unitPrice.HasValue)
                extAmount = quantity * unitPrice.Value;

            var snapshot = BuildPayloadSnapshot(row, policy, woLineId, quantity);

            result.Add(new FscmJournalLine(
                JournalType: policy.JournalType,
                WorkOrderId: woId,
                WorkOrderLineId: woLineId,
                SubProjectId: subProjectId,
                Quantity: quantity,
                CalculatedUnitPrice: unitPrice,
                ExtendedAmount: extAmount,
                Department: dept,
                ProductLine: productLine,
                Warehouse: TryGetString(row, "Warehouse") ?? TryGetString(row, "StorageWarehouseId"),
                LineProperty: lineProperty,
                TransactionDate: transDate,
                DataAreaId: dataArea,
                SourceJournalNumber: journalNum,
                PayloadSnapshot: snapshot
            ));
        }

        return result;


        static FscmReversalPayloadSnapshot? BuildPayloadSnapshot(JsonElement row, IFscmJournalFetchPolicy policy, Guid woLineId, decimal quantity)
        {
            // We only create snapshot when relevant fields are present in response.
            // This snapshot is used ONLY for reversal-line payload mapping.
            try
            {
                
                var currency = policy.JournalType == JournalType.Item
                ? TryGetString(row, "ProjectSalesCurrencyId") ?? TryGetString(row, "ProjectSalesCurrencyId")
                : TryGetString(row, "ProjectSalesCurrencyCode") ?? TryGetString(row, "ProjectSalesCurrencyCode"); //TryGetString(row, "ProjectSalesCurrencyId");

                var dim = policy.JournalType == JournalType.Item
                ? TryGetString(row, "DefaultDimensionDisplayValue") ?? TryGetString(row, "DimensionDisplayValue")
                : TryGetString(row, "DimensionDisplayValue") ?? TryGetString(row, "DefaultDimensionDisplayValue");

                var fsaUnitPrice = TryGetDecimal(row, "RPCFSAUnitPrice");
                var itemId = TryGetString(row, "ItemId");
                var projectCategory =
                    TryGetString(row, "ProjectCategory") ??
                    TryGetString(row, "ProjectCategoryId");

                var custProdDesc = TryGetString(row, "RPCFSACustProdDesc");
                var lineProperty = TryGetString(row, "ProjectLinePropertyId");

                var rpcDiscountAmt = TryGetDecimal(row, "RPCFSADiscountAmt");
                var rpcDiscountPrct = TryGetDecimal(row, "RPCFSADiscountPrct");
                var rpcMarkupPrct = TryGetDecimal(row, "RPCFSAMarkupPrct");
                var rpcMarkupAmt = TryGetDecimal(row, "RPCFSAMarkupAmt");
                var rpcOverallDiscAmt = TryGetDecimal(row, "RPCFSAOverallDiscAmt");
                var rpcOverallDiscPrct = TryGetDecimal(row, "RPCFSAOverallDiscPrct");
                var rpcSurchargeAmt = TryGetDecimal(row, "RPCFSASurchargeAmt");
                var rpcSurchargePrct = TryGetDecimal(row, "RPCFSASurchargePrct");

                var projectDate = TryGetDate(row, "ProjectDate");
                var opDate = TryGetDate(row, "RPCOperationDate");

                var projectSalesPrice = TryGetDecimal(row, "ProjectSalesPrice");
                var isPrintable = TryGetBool(row, "RPCFSAIsPrintable");
                var unitId = TryGetString(row, "ProjectUnitID");
                var wh = TryGetString(row, "StorageWarehouseId") ?? TryGetString(row, "Warehouse");
                var site = TryGetString(row, "StorageSiteId");

                var taxabilityType = TryGetString(row, "RPCFSATaxabilityType");

                // If we have none of the mapping fields, skip snapshot.
                if (currency is null
                    && dim is null
                    && !fsaUnitPrice.HasValue
                    && itemId is null
                    && projectCategory is null
                    && custProdDesc is null
                    && lineProperty is null
                    && !projectSalesPrice.HasValue
                    && !projectDate.HasValue
                    && !opDate.HasValue)
                {
                    return null;
                }

                return new FscmReversalPayloadSnapshot(
                    WorkOrderLineId: woLineId,
                    Currency: currency,
                    DimensionDisplayValue: dim,
                    FsaUnitPrice: fsaUnitPrice,
                    ItemId: itemId,
                    ProjectCategory: projectCategory,
                    JournalLineDescription: custProdDesc,
                    LineProperty: lineProperty,
                    Quantity: quantity,
                    RcpCustomerProductReference: null,
                    RpcDiscountAmount: rpcDiscountAmt,
                    RpcDiscountPercent: rpcDiscountPrct,
                    RpcMarkupPercent: rpcMarkupPrct,
                    RpcOverallDiscountAmount: rpcOverallDiscAmt,
                    RpcOverallDiscountPercent: rpcOverallDiscPrct,
                    RpcSurchargeAmount: rpcSurchargeAmt,
                    RpcSurchargePercent: rpcSurchargePrct,
                    RpMarkUpAmount: rpcMarkupAmt,
                    TransactionDate: projectDate,
                    OperationDate: opDate,
                    UnitAmount: projectSalesPrice,
                    UnitCost: fsaUnitPrice,
                    IsPrintable: isPrintable,
                    UnitId: unitId,
                    Warehouse: wh,
                    Site: site,
                    FsaCustomerProductDesc: custProdDesc,
                    FsaTaxabilityType: taxabilityType
                );
            }
            catch
            {
                return null;
            }
        }

        static string? TryGetNumberAsString(JsonElement row, string prop)
        {
            if (!row.TryGetProperty(prop, out var p)) return null;
            return p.ValueKind == JsonValueKind.Number ? p.ToString() : null;
        }

        static (string? Department, string? ProductLine) ParseDefaultDimensionDisplayValue(string? displayValue)
        {
            // FSCM active format uses 8 segments with '-' separator:
            // MainAccount-Department-Product-CountryCode-Project-FixedAsset-Employee-PICode
            // Department = [1], Product = [2]
            if (string.IsNullOrWhiteSpace(displayValue))
                return (null, null);

            var parts = displayValue.Split('-', StringSplitOptions.None); // keep empty entries

            if (parts.Length < 3)
                return (null, null);

            static string? Normalize(string? s)
            {
                if (string.IsNullOrWhiteSpace(s)) return null;
                return s.Trim();
            }

            var dept = Normalize(parts[1]);
            var prod = Normalize(parts[2]);

            return (dept, prod);
        }
    }

    private static IEnumerable<List<Guid>> Chunk(List<Guid> ids, int chunkSize)
    {
        if (chunkSize <= 0) chunkSize = 25;
        for (var i = 0; i < ids.Count; i += chunkSize)
            yield return ids.GetRange(i, Math.Min(chunkSize, ids.Count - i));
    }

    private static Guid? TryGetGuidLoose(JsonElement obj, string propName)
    {
        if (!obj.TryGetProperty(propName, out var p))
            return null;

        if (p.ValueKind == JsonValueKind.String)
        {
            var s = p.GetString();
            if (Guid.TryParse(s, out var g)) return g;
        }

        // Some FSCM OData responses serialize GUID fields as plain text but not always as JSON strings;
        // ToString() is safe for all kinds.
        var t = p.ToString();
        return Guid.TryParse(t, out var g2) ? g2 : null;
    }

    /// <summary>
    /// Executes try get guid.
    /// </summary>
    private static Guid? TryGetGuid(JsonElement obj, string propName)
    {
        if (!obj.TryGetProperty(propName, out var p))
            return null;

        if (p.ValueKind == JsonValueKind.String)
        {
            var s = p.GetString();
            if (Guid.TryParse(s, out var g)) return g;
        }
        return null;
    }

    /// <summary>
    /// Executes try get decimal.
    /// </summary>
    private static decimal? TryGetDecimal(JsonElement obj, string propName)
    {
        if (!obj.TryGetProperty(propName, out var p))
            return null;

        return p.ValueKind switch
        {
            JsonValueKind.Number when p.TryGetDecimal(out var d) => d,
            JsonValueKind.String => TryParseDecimal(p.GetString()),
            _ => null
        };

        static decimal? TryParseDecimal(string? s)
        {
            if (string.IsNullOrWhiteSpace(s)) return null;
            return decimal.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var d) ? d : null;
        }
    }

    /// <summary>
    /// Executes try get date.
    /// </summary>
    private static DateTime? TryGetDate(JsonElement obj, string propName)
    {
        if (!obj.TryGetProperty(propName, out var p))
            return null;

        if (p.ValueKind == JsonValueKind.String)
        {
            var s = p.GetString();
            if (DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dt))
                return dt;
        }

        return null;
    }

    /// <summary>
    /// Executes try get string.
    /// </summary>
    private static string? TryGetString(JsonElement obj, string propName)
    {
        if (!obj.TryGetProperty(propName, out var p))
            return null;

        return p.ValueKind switch
        {
            JsonValueKind.String => p.GetString(),
            JsonValueKind.Number => p.ToString(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            _ => null
        };
    }

    private static bool? TryGetBool(JsonElement obj, string propName)
    {
        if (!obj.TryGetProperty(propName, out var p))
            return null;

        return p.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String => ParseBoolLoose(p.GetString()),
            JsonValueKind.Number => p.TryGetInt32(out var i) ? i != 0 : (bool?)null,
            _ => null
        };

        static bool? ParseBoolLoose(string? s)
        {
            if (string.IsNullOrWhiteSpace(s)) return null;
            if (bool.TryParse(s, out var b)) return b;
            if (int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var i)) return i != 0;
            return null;
        }
    }

    /// <summary>
    /// Executes trim.
    /// </summary>
    private static string Trim(string? s)
    {
        if (string.IsNullOrWhiteSpace(s)) return string.Empty;
        const int max = 4000;
        return s.Length <= max ? s : string.Concat(s.AsSpan(0, max), " ...");
    }
}
