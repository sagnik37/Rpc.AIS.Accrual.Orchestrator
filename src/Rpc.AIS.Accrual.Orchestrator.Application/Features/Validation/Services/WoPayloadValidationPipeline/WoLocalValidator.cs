using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;

using Microsoft.Extensions.Options;

using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain.Validation;
using Rpc.AIS.Accrual.Orchestrator.Core.Options;
using Rpc.AIS.Accrual.Orchestrator.Core.Services.JournalPolicies;

namespace Rpc.AIS.Accrual.Orchestrator.Core.Services.WoPayloadValidationPipeline;

/// <summary>
/// Performs local (AIS-side) validation and filtering without FSCM calls.
/// </summary>
public sealed class WoLocalValidator : IWoLocalValidator
{
    private readonly PayloadValidationOptions _options;
    private readonly IJournalTypePolicyResolver _journalPolicyResolver;

    // Supports FSCM /Date(1767052800000)/ format (milliseconds)
    private static readonly Regex FscmDateRegex = new(@"\/Date\((\-?\d+)\)\/", RegexOptions.Compiled);

    public WoLocalValidator(
        IOptions<PayloadValidationOptions> options,
        IJournalTypePolicyResolver journalPolicyResolver)
    {
        _options = options?.Value ?? new PayloadValidationOptions();
        _journalPolicyResolver = journalPolicyResolver ?? throw new ArgumentNullException(nameof(journalPolicyResolver));
    }

    public void ValidateLocally(
    FscmEndpointType endpointType,
    JournalType journalType,
    JsonElement woList,
    List<WoPayloadValidationFailure> invalidFailures,
    List<WoPayloadValidationFailure> retryableFailures,
    List<FilteredWorkOrder> validWorkOrders,
    List<FilteredWorkOrder> retryableWorkOrders,
    CancellationToken ct)

    {
        foreach (var wo in woList.EnumerateArray())
        {
            if (ct.IsCancellationRequested) break;

            if (!TryExtractWorkOrderIdentity(wo, journalType, invalidFailures, out var woGuid, out var woNumber))
                continue;

            var policy = _journalPolicyResolver.Resolve(journalType);

            // :
            // SectionKey must NEVER be null/empty because we later emit it as a JSON property name.
            // If a policy returns null (misconfig), fall back to canonical section keys to avoid breaking flows.
            var sectionKey = !string.IsNullOrWhiteSpace(policy.SectionKey)
                ? policy.SectionKey
                : GetDefaultSectionKey(journalType);

            if (string.IsNullOrWhiteSpace(sectionKey))
            {
                invalidFailures.Add(new WoPayloadValidationFailure(
                    woGuid,
                    woNumber,
                    journalType,
                    null,
                    "AIS_INTERNAL_MISSING_SECTIONKEY",
                    $"Resolved journal policy produced an empty SectionKey for journalType '{journalType}'.",
                    ValidationDisposition.Invalid));
                continue;
            }

            if (!TryGetJournalSection(wo, woGuid, woNumber, journalType, sectionKey, invalidFailures, out var section))
                continue;

            if (!TryGetJournalLines(section, woGuid, woNumber, journalType, invalidFailures, out var lines))
                continue;

            ValidateWorkOrderLines(
                wo,
                woGuid,
                woNumber,
                journalType,
                policy,
                sectionKey,
                lines,
                invalidFailures,
                retryableFailures,
                validWorkOrders,
                retryableWorkOrders);
        }
    }

    private static string GetDefaultSectionKey(JournalType journalType) =>
        journalType switch
        {
            JournalType.Item => "WOItemLines",
            JournalType.Expense => "WOExpLines",
            JournalType.Hour => "WOHourLines",
            _ => string.Empty
        };

    private static bool TryExtractWorkOrderIdentity(
        JsonElement wo,
        JournalType journalType,
        List<WoPayloadValidationFailure> invalidFailures,
        out Guid woGuid,
        out string? woNumber)
    {
        woNumber = WoPayloadJsonHelpers.TryGetString(wo, "WorkOrderID") ?? WoPayloadJsonHelpers.TryGetString(wo, "WorkOrderNumber");
        var woGuidMaybe = WoPayloadJsonHelpers.TryGetGuid(wo, "WorkOrderGUID")
                         ?? WoPayloadJsonHelpers.TryGetGuid(wo, "RPCWorkOrderGuid")
                         ?? WoPayloadJsonHelpers.TryGetGuid(wo, "WorkorderGUID")
                         ?? WoPayloadJsonHelpers.TryGetGuid(wo, "Work order GUID");

        if (woGuidMaybe is null)
        {
            invalidFailures.Add(new WoPayloadValidationFailure(
                Guid.Empty,
                woNumber,
                journalType,
                null,
                "AIS_WO_MISSING_GUID",
                "Work order missing WorkOrderGUID.",
                ValidationDisposition.Invalid));

            woGuid = Guid.Empty;
            return false;
        }

        woGuid = woGuidMaybe.Value;
        return true;
    }

    private static bool TryGetJournalSection(
        JsonElement wo,
        Guid woGuid,
        string? woNumber,
        JournalType journalType,
        string sectionKey,
        List<WoPayloadValidationFailure> invalidFailures,
        out JsonElement section)
    {
        section = default;

        if (!wo.TryGetProperty(sectionKey, out section) || section.ValueKind != JsonValueKind.Object)
        {
            invalidFailures.Add(new WoPayloadValidationFailure(
                woGuid,
                woNumber,
                journalType,
                null,
                "AIS_WO_MISSING_SECTION",
                $"Work order missing journal section '{sectionKey}'.",
                ValidationDisposition.Invalid));
            return false;
        }

        return true;
    }

    private static bool TryGetJournalLines(
        JsonElement section,
        Guid woGuid,
        string? woNumber,
        JournalType journalType,
        List<WoPayloadValidationFailure> invalidFailures,
        out JsonElement lines)
    {
        lines = default;
        if (!section.TryGetProperty("JournalLines", out lines) || lines.ValueKind != JsonValueKind.Array)
        {
            invalidFailures.Add(new WoPayloadValidationFailure(
                woGuid,
                woNumber,
                journalType,
                null,
                "AIS_WO_MISSING_LINES",
                "Work order missing JournalLines array.",
                ValidationDisposition.Invalid));
            return false;
        }

        return true;
    }

    private void ValidateWorkOrderLines(
        JsonElement wo,
        Guid woGuid,
        string? woNumber,
        JournalType journalType,
        IJournalTypePolicy policy,
        string sectionKey,
        JsonElement lines,
        List<WoPayloadValidationFailure> invalidFailures,
        List<WoPayloadValidationFailure> retryableFailures,
        List<FilteredWorkOrder> validWorkOrders,
        List<FilteredWorkOrder> retryableWorkOrders)
    {
        var validLines = new List<JsonElement>();
        bool woHasInvalid = false;

        foreach (var ln in lines.EnumerateArray())
        {
            var lineGuid = WoPayloadJsonHelpers.TryGetGuid(ln, "WorkOrderLineGUID")
                        ?? WoPayloadJsonHelpers.TryGetGuid(ln, "WorkOrderLineGuid")
                        ?? WoPayloadJsonHelpers.TryGetGuid(ln, "RPCWorkOrderLineGuid");

            if (lineGuid is null)
            {
                woHasInvalid = true;
                invalidFailures.Add(new WoPayloadValidationFailure(
                    woGuid,
                    woNumber,
                    journalType,
                    null,
                    "AIS_LINE_MISSING_GUID",
                    "Journal line missing WorkOrderLineGUID.",
                    ValidationDisposition.Invalid));
                continue;
            }

            // 1) Journal-type specific required fields (local validations).
            var invalidCountBefore = invalidFailures.Count;
            policy.ValidateLocalLine(woGuid, woNumber, lineGuid.Value, ln, invalidFailures);
            if (invalidFailures.Count > invalidCountBefore)
            {
                woHasInvalid = true;
                continue;
            }

            // 2) REQUIRED: rpc_OperationsDate must be present and parseable.
            if (!TryValidateOperationsDate(woGuid, woNumber, journalType, lineGuid.Value, ln, invalidFailures))
            {
                woHasInvalid = true;
                continue;
            }

            // 3) REQUIRED: TransactionDate must be present and parseable.
            if (!TryValidateTransactionDate(woGuid, woNumber, journalType, lineGuid.Value, ln, invalidFailures))
            {
                woHasInvalid = true;
                continue;
            }

            // 4) REQUIRED: Quantity must be present and numeric.
            if (!TryValidateRequiredQuantity(woGuid, woNumber, journalType, lineGuid.Value, ln, invalidFailures))
            {
                woHasInvalid = true;
                continue;
            }

            // Optional numeric fields (non-blocking if absent, blocking if present but invalid).
            if (!TryValidateOptionalNumericFields(woGuid, woNumber, journalType, lineGuid.Value, ln, invalidFailures))
            {
                woHasInvalid = true;
                continue;
            }

            validLines.Add(ln);
        }

        if (_options.DropWholeWorkOrderOnAnyInvalidLine && woHasInvalid)
        {
            // Drop entire WO from payload.
            return;
        }

        if (validLines.Count > 0)
            validWorkOrders.Add(new FilteredWorkOrder(wo, sectionKey, validLines));
    }

    private static bool TryValidateOperationsDate(
        Guid woGuid,
        string? woNumber,
        JournalType journalType,
        Guid lineGuid,
        JsonElement ln,
        List<WoPayloadValidationFailure> invalidFailures)
    {
        var raw =
    // New canonical key
    WoPayloadJsonHelpers.TryGetString(ln, "OperationDate")

    // Backward compatibility
    ?? WoPayloadJsonHelpers.TryGetString(ln, "RPCWorkingDate")
    ?? WoPayloadJsonHelpers.TryGetString(ln, "rpc_OperationsDate")
    ?? WoPayloadJsonHelpers.TryGetString(ln, "rpc_operationsdate")
    ?? WoPayloadJsonHelpers.TryGetString(ln, "RPCOperationsDate")
    ?? WoPayloadJsonHelpers.TryGetString(ln, "OperationsDate");


        if (string.IsNullOrWhiteSpace(raw))
        {
            invalidFailures.Add(new WoPayloadValidationFailure(
                woGuid,
                woNumber,
                journalType,
                lineGuid,
                "AIS_LINE_MISSING_RPC_OPERATIONSDATE",
                "Journal line missing RPCWorkingDate (Operations/Working Date) derived from Field Service.",
                ValidationDisposition.Invalid));
            return false;
        }

        if (!TryParseFscmOrIsoDate(raw, out _))
        {
            invalidFailures.Add(new WoPayloadValidationFailure(
                woGuid,
                woNumber,
                journalType,
                lineGuid,
                "AIS_LINE_INVALID_RPC_OPERATIONSDATE",
                $"Journal line has invalid rpc_OperationsDate value: '{raw}'.",
                ValidationDisposition.Invalid));
            return false;
        }

        return true;
    }

    private static bool TryValidateTransactionDate(
        Guid woGuid,
        string? woNumber,
        JournalType journalType,
        Guid lineGuid,
        JsonElement ln,
        List<WoPayloadValidationFailure> invalidFailures)
    {
        var raw = WoPayloadJsonHelpers.TryGetString(ln, "TransactionDate")
               ?? WoPayloadJsonHelpers.TryGetString(ln, "transactionDate");

        if (string.IsNullOrWhiteSpace(raw))
        {
            invalidFailures.Add(new WoPayloadValidationFailure(
                woGuid,
                woNumber,
                journalType,
                lineGuid,
                "AIS_LINE_MISSING_TRANSACTIONDATE",
                "Journal line missing TransactionDate.",
                ValidationDisposition.Invalid));
            return false;
        }

        if (!TryParseFscmOrIsoDate(raw, out _))
        {
            invalidFailures.Add(new WoPayloadValidationFailure(
                woGuid,
                woNumber,
                journalType,
                lineGuid,
                "AIS_LINE_INVALID_TRANSACTIONDATE",
                $"Journal line has invalid TransactionDate value: '{raw}'.",
                ValidationDisposition.Invalid));
            return false;
        }

        return true;
    }

    private static bool TryValidateRequiredQuantity(
        Guid woGuid,
        string? woNumber,
        JournalType journalType,
        Guid lineGuid,
        JsonElement ln,
        List<WoPayloadValidationFailure> invalidFailures)
    {
        // Hour lines don't have Quantity; they use Duration.
        var fieldName = journalType == JournalType.Hour ? "Duration" : "Quantity";

        if (!ln.TryGetProperty(fieldName, out _))
        {
            invalidFailures.Add(new WoPayloadValidationFailure(
                woGuid,
                woNumber,
                journalType,
                lineGuid,
                journalType == JournalType.Hour
                    ? "AIS_LINE_MISSING_DURATION"
                    : "AIS_LINE_MISSING_QUANTITY",
                journalType == JournalType.Hour
                    ? "Journal line missing required Duration."
                    : "Journal line missing required Quantity.",
                ValidationDisposition.Invalid));
            return false;
        }

        if (!WoPayloadJsonHelpers.TryGetNumber(ln, fieldName, out var val))
        {
            invalidFailures.Add(new WoPayloadValidationFailure(
                woGuid,
                woNumber,
                journalType,
                lineGuid,
                journalType == JournalType.Hour
                    ? "AIS_LINE_INVALID_DURATION"
                    : "AIS_LINE_INVALID_QUANTITY",
                journalType == JournalType.Hour
                    ? "Journal line has invalid Duration."
                    : "Journal line has invalid Quantity.",
                ValidationDisposition.Invalid));
            return false;
        }

        // Optional: enforce > 0 (recommended to avoid 0 duration/qty lines)
        if (val <= 0m)
        {
            invalidFailures.Add(new WoPayloadValidationFailure(
                woGuid,
                woNumber,
                journalType,
                lineGuid,
                journalType == JournalType.Hour
                    ? "AIS_LINE_NONPOSITIVE_DURATION"
                    : "AIS_LINE_NONPOSITIVE_QUANTITY",
                journalType == JournalType.Hour
                    ? "Journal line Duration must be greater than zero."
                    : "Journal line Quantity must be greater than zero.",
                ValidationDisposition.Invalid));
            return false;
        }

        return true;
    }


    private static bool TryValidateOptionalNumericFields(
        Guid woGuid,
        string? woNumber,
        JournalType journalType,
        Guid lineGuid,
        JsonElement ln,
        List<WoPayloadValidationFailure> invalidFailures)
    {
        if (ln.ValueKind == JsonValueKind.Object && ln.TryGetProperty("Price", out _))
        {
            if (!WoPayloadJsonHelpers.TryGetNumber(ln, "Price", out _))
            {
                invalidFailures.Add(new WoPayloadValidationFailure(
                    woGuid,
                    woNumber,
                    journalType,
                    lineGuid,
                    "AIS_LINE_INVALID_PRICE",
                    "Line has invalid Price.",
                    ValidationDisposition.Invalid));
                return false;
            }
        }

        return true;
    }

    private static bool TryParseFscmOrIsoDate(string raw, out DateTimeOffset utc)
    {
        utc = default;

        if (string.IsNullOrWhiteSpace(raw))
            return false;

        var m = FscmDateRegex.Match(raw);
        if (m.Success && long.TryParse(m.Groups[1].Value, out var ms))
        {
            utc = DateTimeOffset.FromUnixTimeMilliseconds(ms);
            return true;
        }

        return DateTimeOffset.TryParse(
            raw,
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
            out utc);
    }
}
