using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Core.Utilities;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Options;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Resilience;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Utilities;

namespace Rpc.AIS.Accrual.Orchestrator.Infrastructure.Clients;

/// <summary>
/// FSCM OData client for CDSReleasedDistinctProducts used to derive ProjCategoryId/RPCProjCategoryId by ItemNumber.
/// </summary>
public sealed class FscmReleasedDistinctProductsHttpClient : IFscmReleasedDistinctProductsClient
{
    private readonly HttpClient _http;
    private readonly FscmOptions _opt;
    private readonly IResilientHttpExecutor _executor;
    private readonly ILogger<FscmReleasedDistinctProductsHttpClient> _logger;

    public FscmReleasedDistinctProductsHttpClient(
        HttpClient http,
        FscmOptions opt,
        IResilientHttpExecutor executor,
        ILogger<FscmReleasedDistinctProductsHttpClient> logger)
    {
        _http = http ?? throw new ArgumentNullException(nameof(http));
        _opt = opt ?? throw new ArgumentNullException(nameof(opt));
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<IReadOnlyDictionary<string, ReleasedDistinctProductCategory>> GetCategoriesByItemNumberAsync(
        RunContext ctx,
        IReadOnlyList<string> itemNumbers,
        CancellationToken ct)
    {
        if (ctx is null) throw new ArgumentNullException(nameof(ctx));
        if (itemNumbers is null) throw new ArgumentNullException(nameof(itemNumbers));

        var clean = itemNumbers
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Select(s => s.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (clean.Count == 0)
            return new Dictionary<string, ReleasedDistinctProductCategory>(StringComparer.OrdinalIgnoreCase);

        var baseUrl = FscmUrlBuilder.ResolveFscmBaseUrl(_opt, null, legacyName: "FscmODataBaseUrl");
        var entity = string.IsNullOrWhiteSpace(_opt.ReleasedDistinctProductsEntitySet) ? "CDSReleasedDistinctProducts" : _opt.ReleasedDistinctProductsEntitySet;
        var url = FscmUrlBuilder.BuildUrl(baseUrl, $"/data/{entity}");

        // Chunk OR filters to avoid URL length blowups.
        var chunkSize = _opt.ReleasedDistinctProductsOrFilterChunkSize <= 0 ? 25 : _opt.ReleasedDistinctProductsOrFilterChunkSize;

        var result = new Dictionary<string, ReleasedDistinctProductCategory>(StringComparer.OrdinalIgnoreCase);

        foreach (var chunk in Chunk(clean, chunkSize))
        {
            var filter = string.Join(" or ", chunk.Select(i => $"ItemNumber eq '{EscapeODataLiteral(i)}'"));
            var query = $"?cross-company=true&$select=ItemNumber,RPCProjCategoryId&$filter={filter}";
            var full = url + query;

            var sw = Stopwatch.StartNew();
            _logger.LogInformation("FSCM CDSReleasedDistinctProducts fetch START. Url={Url} RunId={RunId} CorrelationId={CorrelationId} Items={Count}",
                full, ctx.RunId, ctx.CorrelationId, chunk.Count);

            var resp = await _executor.SendAsync(
                _http,
                () => new HttpRequestMessage(HttpMethod.Get, full),
                ctx,
                operationName: "FSCM_RELEASED_DISTINCT_PRODUCTS",
                ct).ConfigureAwait(false);

            var body = (await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false)) ?? string.Empty;
            sw.Stop();

            _logger.LogInformation("FSCM CDSReleasedDistinctProducts fetch END. Status={Status} ElapsedMs={ElapsedMs} ResponseBytes={Bytes} RunId={RunId} CorrelationId={CorrelationId}",
                (int)resp.StatusCode, sw.ElapsedMilliseconds, Encoding.UTF8.GetByteCount(body), ctx.RunId, ctx.CorrelationId);

            if (resp.StatusCode == HttpStatusCode.Unauthorized || resp.StatusCode == HttpStatusCode.Forbidden)
                throw new HttpRequestException($"FSCM auth failed ({(int)resp.StatusCode}). Body: {LogText.TrimForLog(body)}", null, resp.StatusCode);

            if (!resp.IsSuccessStatusCode)
            {
                _logger.LogWarning("FSCM CDSReleasedDistinctProducts non-success. Status={Status} Body={Body}", (int)resp.StatusCode, LogText.TrimForLog(body));
                continue;
            }

            using var doc = JsonDocument.Parse(body);
            if (!doc.RootElement.TryGetProperty("value", out var arr) || arr.ValueKind != JsonValueKind.Array)
                continue;

            foreach (var row in arr.EnumerateArray())
            {
                if (!row.TryGetProperty("ItemNumber", out var it) || it.ValueKind != JsonValueKind.String)
                    continue;

                var item = it.GetString() ?? string.Empty;
                if (item.Length == 0) continue;

                static string? TryString(JsonElement obj, params string[] names)
                {
                    foreach (var n in names)
                        if (obj.TryGetProperty(n, out var v) && v.ValueKind == JsonValueKind.String)
                            return v.GetString();
                    return null;
                }

                //var item = TryString(row, "ItemNumber") ?? string.Empty;
                if (item.Length == 0) continue;

                var proj = TryString(row, "RPCProjCategoryId", "RpcProjCategoryId", "rpcProjCategoryId");
                var rpc = TryString(row, "RPCProjCategoryId", "RpcProjCategoryId", "rpcProjCategoryId");

                result[item] = new ReleasedDistinctProductCategory(item, proj, rpc);

            }
        }

        return result;
    }

    private static IEnumerable<List<string>> Chunk(List<string> items, int size)
    {
        for (int i = 0; i < items.Count; i += size)
            yield return items.GetRange(i, Math.Min(size, items.Count - i));
    }

    private static string EscapeODataLiteral(string s)
        => (s ?? string.Empty).Replace("'", "''");
}
