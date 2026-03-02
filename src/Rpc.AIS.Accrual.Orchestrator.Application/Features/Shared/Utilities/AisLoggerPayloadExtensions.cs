using System;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;

namespace Rpc.AIS.Accrual.Orchestrator.Core.Utilities;

/// <summary>
/// Production-safe helpers for logging large JSON payloads into App Insights traces.
/// - Logs sha256 + length always
/// - Logs a snippet (and optional chunks) when enabled
/// </summary>
public static class AisLoggerPayloadExtensions
{
    public static async Task LogJsonPayloadAsync(
        this IAisLogger logger,
        string runId,
        string step,
        string message,
        string payloadType,
        string workOrderGuid,
        string? workOrderNumber,
        string json,
        bool logBody,
        int snippetChars,
        int chunkChars,
        CancellationToken ct)
    {
        if (logger is null) throw new ArgumentNullException(nameof(logger));
        if (json is null) json = string.Empty;

        var len = json.Length;
        var sha = Sha256Hex(json);

        // Always log summary (safe, small)
        await logger.InfoAsync(runId, step, message, new
        {
            PayloadType = payloadType,
            WorkOrderGuid = workOrderGuid,
            WorkOrderNumber = workOrderNumber,
            PayloadLength = len,
            PayloadSha256 = sha
        }, ct).ConfigureAwait(false);

        if (!logBody || len == 0) return;

        // Log a short snippet (useful without being huge)
        var snippet = json.Length <= snippetChars ? json : json.Substring(0, snippetChars);
        await logger.InfoAsync(runId, step, $"{message} (snippet)", new
        {
            PayloadType = payloadType,
            WorkOrderGuid = workOrderGuid,
            WorkOrderNumber = workOrderNumber,
            SnippetChars = snippet.Length,
            JsonSnippet = snippet
        }, ct).ConfigureAwait(false);

        // full payload, log in chunks to avoid trace truncation
        if (chunkChars <= 0) return;

        var totalChunks = (int)Math.Ceiling((double)len / chunkChars);
        for (int i = 0; i < totalChunks; i++)
        {
            var start = i * chunkChars;
            var size = Math.Min(chunkChars, len - start);
            var chunk = json.Substring(start, size);

            await logger.InfoAsync(runId, step, $"{message} (chunk)", new
            {
                PayloadType = payloadType,
                WorkOrderGuid = workOrderGuid,
                WorkOrderNumber = workOrderNumber,
                ChunkIndex = i + 1,
                ChunkCount = totalChunks,
                ChunkChars = size,
                JsonChunk = chunk
            }, ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Executes sha 256 hex.
    /// </summary>
    private static string Sha256Hex(string s)
    {
        var bytes = Encoding.UTF8.GetBytes(s);
        var hash = SHA256.HashData(bytes);
        return Convert.ToHexString(hash);
    }
}
