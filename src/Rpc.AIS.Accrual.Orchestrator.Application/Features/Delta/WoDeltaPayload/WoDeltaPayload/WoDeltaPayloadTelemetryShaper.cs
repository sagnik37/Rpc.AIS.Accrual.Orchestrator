// File: .../Core/Services/WoDeltaPayload/WoDeltaPayloadTelemetryShaper.cs
// Extracted from WoDeltaPayloadService.Helpers.cs to improve SRP.

namespace Rpc.AIS.Accrual.Orchestrator.Core.Services;

using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using System;
using System.Security.Cryptography;
using System.Text;

internal sealed class WoDeltaPayloadTelemetryShaper
{
    private readonly IAisLogger _aisLogger;
    private readonly IAisDiagnosticsOptions _diag;

    internal WoDeltaPayloadTelemetryShaper(IAisLogger aisLogger, IAisDiagnosticsOptions diag)
    {
        _aisLogger = aisLogger ?? throw new ArgumentNullException(nameof(aisLogger));
        _diag = diag ?? throw new ArgumentNullException(nameof(diag));
    }

        internal async Task LogPayloadSummaryAsync(
            RunContext context,
            string step,
            string message,
            string payloadType,
            string workOrderGuid,
            string? workOrderNumber,
            string json,
            CancellationToken ct)
        {
            var len = json?.Length ?? 0;
            var sha = Sha256Hex(json ?? string.Empty);

            await _aisLogger.InfoAsync(context.RunId, step, message, new
            {
                context.CorrelationId,
                PayloadType = payloadType,
                WorkOrderGuid = workOrderGuid,
                WorkOrderNumber = workOrderNumber,
                PayloadLength = len,
                PayloadSha256 = sha
            }, ct).ConfigureAwait(false);
        }
        internal async Task LogPayloadBodyAsync(
            RunContext context,
            string step,
            string message,
            string payloadType,
            string workOrderGuid,
            string? workOrderNumber,
            string json,
            CancellationToken ct)
        {
            if (string.IsNullOrEmpty(json))
                return;

            var snippetChars = Math.Clamp(_diag.PayloadSnippetChars, 256, 100_000);
            var chunkChars = Math.Clamp(_diag.PayloadChunkChars, 0, 200_000);

            var snippet = json.Length <= snippetChars ? json : json.Substring(0, snippetChars);
            await _aisLogger.InfoAsync(context.RunId, step, $"{message} (snippet)", new
            {
                context.CorrelationId,
                PayloadType = payloadType,
                WorkOrderGuid = workOrderGuid,
                WorkOrderNumber = workOrderNumber,
                SnippetChars = snippet.Length,
                JsonSnippet = snippet
            }, ct).ConfigureAwait(false);

            if (chunkChars <= 0)
                return;

            var len = json.Length;
            var totalChunks = (int)Math.Ceiling((double)len / chunkChars);

            for (int i = 0; i < totalChunks; i++)
            {
                var start = i * chunkChars;
                var size = Math.Min(chunkChars, len - start);
                var chunk = json.Substring(start, size);

                await _aisLogger.InfoAsync(context.RunId, step, $"{message} (chunk)", new
                {
                    context.CorrelationId,
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
        internal static string Sha256Hex(string s)
        {
            var bytes = Encoding.UTF8.GetBytes(s);
            var hash = SHA256.HashData(bytes);
            return Convert.ToHexString(hash);
        }
}
