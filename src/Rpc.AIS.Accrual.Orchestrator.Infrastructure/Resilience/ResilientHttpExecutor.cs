using System;
using System.Diagnostics;
using System.Net.Http;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Rpc.AIS.Accrual.Orchestrator.Core.Domain;

namespace Rpc.AIS.Accrual.Orchestrator.Infrastructure.Resilience;

/// <summary>
/// Dependency-free resilience executor (retry + basic circuit breaker).
/// </summary>
public sealed class ResilientHttpExecutor : IResilientHttpExecutor
{
    private readonly IHttpFailureClassifier _classifier;
    private readonly HttpResilienceOptions _opt;
    private readonly ILogger<ResilientHttpExecutor> _logger;

    // Simple global circuit breaker state (per-process).
    private int _consecutiveFailures;
    private DateTimeOffset? _openUntil;

    public ResilientHttpExecutor(
        IHttpFailureClassifier classifier,
        IOptions<HttpResilienceOptions> options,
        ILogger<ResilientHttpExecutor> logger)
    {
        _classifier = classifier ?? throw new ArgumentNullException(nameof(classifier));
        _opt = (options?.Value) ?? new HttpResilienceOptions();
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<HttpResponseMessage> SendAsync(
        HttpClient http,
        Func<HttpRequestMessage> requestFactory,
        RunContext ctx,
        string operationName,
        CancellationToken ct)
    {
        if (http is null) throw new ArgumentNullException(nameof(http));
        if (requestFactory is null) throw new ArgumentNullException(nameof(requestFactory));
        if (ctx is null) throw new ArgumentNullException(nameof(ctx));
        if (string.IsNullOrWhiteSpace(operationName)) operationName = "HTTP";

        // Circuit breaker gate
        var openUntil = _openUntil;
        if (openUntil is not null && openUntil.Value > DateTimeOffset.UtcNow)
            throw new HttpRequestException($"Circuit open for operation '{operationName}' until {openUntil:O}.");

        Exception? last = null;

        var maxAttempts = Math.Max(1, _opt.MaxAttempts);

        // Hard rule: never retry HTTP POST (non-idempotent). This prevents duplicate side effects
        // (e.g., duplicate journal creates/posts) when the first attempt succeeded server-side
        // but the response was lost or the client timed out.
        var isPost = false;
        try
        {
            using var probe = requestFactory();
            isPost = probe.Method == HttpMethod.Post;
        }
        catch
        {
            // If we can't probe safely, fall back to configured behavior.
        }

        if (isPost)
        {
            if (maxAttempts != 1)
            {
                _logger.LogInformation(
                    "HTTP retries disabled for POST request. Op={Op} ConfiguredMaxAttempts={ConfiguredMaxAttempts}",
                    operationName, maxAttempts);
            }

            maxAttempts = 1;
        }

        // IMPORTANT:
        // Some operations are non-idempotent (e.g., FSCM journal create/post).
        // Retrying them can create duplicate journals if the first attempt succeeded server-side
        // but the response was lost or the client timed out.
        if (IsNoRetryOperation(operationName))
        {
            if (maxAttempts != 1)
            {
                _logger.LogInformation(
                    "HTTP retries disabled for non-idempotent operation. Op={Op} ConfiguredMaxAttempts={ConfiguredMaxAttempts}",
                    operationName, maxAttempts);
            }

            maxAttempts = 1;
        }

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            using var request = requestFactory();

            try
            {
                var sw = Stopwatch.StartNew();
                var resp = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
                sw.Stop();

                if (_classifier.IsRetryable(resp) && attempt < maxAttempts)
                {
                    string? bodySnippet = null;
                    try
                    {
                        if (resp.Content is not null)
                        {
                            var body = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
                            if (!string.IsNullOrWhiteSpace(body))
                                bodySnippet = body.Length <= 1200 ? body : body.Substring(0, 1200);
                        }
                    }
                    catch
                    {
                        // best effort only
                    }

                    if (!string.IsNullOrWhiteSpace(bodySnippet))
                    {
                        _logger.LogWarning(
                            "HTTP retryable response body snippet. Op={Op} Attempt={Attempt}/{MaxAttempts} Status={Status} Snippet={Snippet} RunId={RunId} CorrelationId={CorrelationId}",
                            operationName,
                            attempt,
                            maxAttempts,
                            (int)resp.StatusCode,
                            bodySnippet,
                            ctx.RunId,
                            ctx.CorrelationId);
                    }

                    await RegisterRetryAsync(ctx, operationName, attempt, maxAttempts, sw.ElapsedMilliseconds, ((int)resp.StatusCode).ToString(), null).ConfigureAwait(false);
                    resp.Dispose();
                    await DelayBeforeRetryAsync(attempt, ct).ConfigureAwait(false);
                    continue;
                }

                RegisterSuccess();
                return resp;
            }
            catch (Exception ex) when (_classifier.IsRetryable(ex) && attempt < maxAttempts)
            {
                last = ex;
                await RegisterRetryAsync(ctx, operationName, attempt, maxAttempts, null, null, ex).ConfigureAwait(false);
                await DelayBeforeRetryAsync(attempt, ct).ConfigureAwait(false);
                continue;
            }
            catch (Exception ex)
            {
                last = ex;
                RegisterFailureFinal(ex);
                throw;
            }
        }

        RegisterFailureFinal(last);
        throw last ?? new HttpRequestException("HTTP call failed without exception.");
    }

    private bool IsNoRetryOperation(string operationName)
    {
        if (string.IsNullOrWhiteSpace(operationName)) return false;

        var list = _opt.NoRetryOperations;
        if (list is null || list.Length == 0) return false;

        foreach (var op in list)
        {
            if (!string.IsNullOrWhiteSpace(op) && string.Equals(op.Trim(), operationName.Trim(), StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private void RegisterSuccess()
    {
        _consecutiveFailures = 0;
        _openUntil = null;
    }

    private void RegisterFailureFinal(Exception? ex)
    {
        var fail = Interlocked.Increment(ref _consecutiveFailures);
        if (fail >= _opt.CircuitBreakerFailureThreshold)
        {
            _openUntil = DateTimeOffset.UtcNow.Add(_opt.CircuitBreakerOpenDuration);
            _logger.LogWarning("Circuit opened after {Failures} consecutive failures. OpenUntil={OpenUntil:o}. LastError={Error}",
                fail, _openUntil, ex?.Message);
        }
    }

    private Task RegisterRetryAsync(RunContext ctx, string op, int attempt, int maxAttempts, long? elapsedMs, string? status, Exception? ex)
    {
        var fail = Interlocked.Increment(ref _consecutiveFailures);

        _logger.LogWarning(
            "HTTP retryable failure. Op={Op} Attempt={Attempt}/{MaxAttempts} Status={Status} ElapsedMs={ElapsedMs} RunId={RunId} CorrelationId={CorrelationId} Failures={Failures} Error={Error}",
            op, attempt, maxAttempts, status ?? "<ex>", elapsedMs, ctx.RunId, ctx.CorrelationId, fail, ex?.Message);

        if (fail >= _opt.CircuitBreakerFailureThreshold)
        {
            _openUntil = DateTimeOffset.UtcNow.Add(_opt.CircuitBreakerOpenDuration);
            _logger.LogWarning("Circuit opened. Op={Op} OpenUntil={OpenUntil:o}", op, _openUntil);
        }

        return Task.CompletedTask;
    }

    private async Task DelayBeforeRetryAsync(int attempt, CancellationToken ct)
    {
        // attempt is 1-based
        var pow = Math.Min(attempt, 8);
        var delayMs = _opt.BaseDelay.TotalMilliseconds * Math.Pow(2, pow - 1);
        delayMs = Math.Min(delayMs, _opt.MaxDelay.TotalMilliseconds);

        if (_opt.UseJitter)
        {
            var jitter = RandomNumberGenerator.GetInt32(-200, 201) / 1000.0; // -0.2..0.2
            delayMs = delayMs * (1.0 + jitter);
        }

        if (delayMs < 0) delayMs = 0;
        await Task.Delay(TimeSpan.FromMilliseconds(delayMs), ct).ConfigureAwait(false);
    }
}
