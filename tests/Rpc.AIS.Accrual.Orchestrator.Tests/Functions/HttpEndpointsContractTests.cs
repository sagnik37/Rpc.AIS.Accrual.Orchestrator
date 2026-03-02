using System;
using System.IO;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using FluentAssertions;

using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain;
using Rpc.AIS.Accrual.Orchestrator.Core.Services;
using Rpc.AIS.Accrual.Orchestrator.Functions.Functions;
using Rpc.AIS.Accrual.Orchestrator.Functions.Services;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Clients.Posting;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Options;
using Rpc.AIS.Accrual.Orchestrator.Tests.TestDoubles;

using Xunit;

namespace Rpc.AIS.Accrual.Orchestrator.Tests.Functions;

public sealed class HttpEndpointsContractTests
{
    [Fact]
    public async Task AdHocSingle_empty_body_returns_400()
    {
        var useCase = BuildAdHocSingle(out _);

        var ctx = new TestFunctionContext();
        var req = NewReq(ctx, body: "");

        var res = await useCase.ExecuteAsync(req, ctx);
        res.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task AdHocSingle_valid_envelope_passes_triggeredBy_to_payload_orchestrator()
    {
        var useCase = BuildAdHocSingle(out var payloadOrch);

        GetFsaDeltaPayloadInputDto? captured = null;
        payloadOrch
            .Setup(x => x.BuildSingleWorkOrderAnyStatusAsync(
                It.IsAny<GetFsaDeltaPayloadInputDto>(),
                It.IsAny<FsOptions>(),
                It.IsAny<CancellationToken>()))
            .Callback<GetFsaDeltaPayloadInputDto, FsOptions, CancellationToken>((dto, _, __) => captured = dto)
            // Force early NotFound response (so we don't execute delta/post/update).
            .ReturnsAsync(new GetFsaDeltaPayloadResultDto(PayloadJson: "", ProductDeltaLinkAfter: null, ServiceDeltaLinkAfter: null, WorkOrderNumbers: Array.Empty<string>()));

        var fctx = new TestFunctionContext();
        var req = NewReq(fctx, body: Envelope(workOrderGuid: "11111111-1111-1111-1111-111111111111"));

        var res = await useCase.ExecuteAsync(req, fctx);
        res.StatusCode.Should().Be(HttpStatusCode.NotFound);

        captured.Should().NotBeNull();
        captured!.TriggeredBy.Should().Be("AdHocSingle");
    }

    [Fact]
    public async Task AdHocAll_schedules_durable_with_triggeredBy_AdHocAll()
    {
        var useCase = new AdHocAllJobsUseCase(
            NullLogger<AdHocAllJobsUseCase>.Instance,
            new NoopAisLogger(),
            new FakeAisDiagnosticsOptions());

        // NOTE:
        // DurableTaskClient is a concrete class without a public parameterless ctor,
        // so Moq cannot proxy it. Use a minimal concrete test double instead.
        var durable = new CapturingDurableTaskClient();

        var fctx = new TestFunctionContext();
        var req = NewReq(fctx, body: "{\"_request\":{}}" );

        var res = await useCase.ExecuteAsync(req, durable, fctx);
        res.StatusCode.Should().Be(HttpStatusCode.Accepted);

        durable.CapturedInput.Should().NotBeNull();
        durable.CapturedInput!.GetType().Name.Should().Contain("RunInputDto");

        var triggeredByProp = durable.CapturedInput.GetType().GetProperty("TriggeredBy");
        triggeredByProp.Should().NotBeNull();
        triggeredByProp!.GetValue(durable.CapturedInput)!.ToString().Should().Be("AdHocAll");

        durable.CapturedInstanceId.Should().NotBeNull();
        durable.CapturedInstanceId!.Should().Contain("-adhoc-all");
    }

    [Fact]
    public async Task CancelJob_empty_payload_requires_company_and_subproject_in_envelope()
    {
        var payloadOrch = new Mock<IFsaDeltaPayloadOrchestrator>(MockBehavior.Strict);
        payloadOrch
            .Setup(x => x.BuildSingleWorkOrderAnyStatusAsync(
                It.IsAny<GetFsaDeltaPayloadInputDto>(),
                It.IsAny<FsOptions>(),
                It.IsAny<CancellationToken>()))
            // Force the empty-payload path
            .ReturnsAsync(new GetFsaDeltaPayloadResultDto(PayloadJson: "", ProductDeltaLinkAfter: null, ServiceDeltaLinkAfter: null, WorkOrderNumbers: Array.Empty<string>()));

        var useCase = new CancelJobUseCase(
            NullLogger<CancelJobUseCase>.Instance,
            new NoopAisLogger(),
            new FakeAisDiagnosticsOptions(),
            payloadOrch.Object,
            new FsOptions(),
            Mock.Of<IPostingClient>(),
            Mock.Of<IWoDeltaPayloadServiceV2>(),
            Mock.Of<IFscmProjectStatusClient>());

        var fctx = new TestFunctionContext();

        // Missing Company/SubProjectId in envelope => should 400
        var req = NewReq(fctx, body: Envelope(workOrderGuid: "11111111-1111-1111-1111-111111111111"));
        var res = await useCase.ExecuteAsync(req, fctx);

        res.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    private static AdHocSingleJobUseCase BuildAdHocSingle(out Mock<IFsaDeltaPayloadOrchestrator> payloadOrch)
    {
        payloadOrch = new Mock<IFsaDeltaPayloadOrchestrator>(MockBehavior.Strict);

        // The rest are not invoked in our early-exit tests.
        var deltaV2 = new Mock<IWoDeltaPayloadServiceV2>(MockBehavior.Loose);
        var posting = new Mock<IPostingClient>(MockBehavior.Loose);

        // Concrete runners need concrete instances, but we keep them unused by early exit.
        var fsaLineFetcher = new Mock<IFsaLineFetcher>(MockBehavior.Loose);
        var fscmInvAttrs = new Mock<IFscmInvoiceAttributesClient>(MockBehavior.Loose);
        var attrMap = new Mock<IFscmGlobalAttributeMappingClient>(MockBehavior.Loose);

        var invoiceSync = new InvoiceAttributeSyncRunner(
            NullLogger<InvoiceAttributeSyncRunner>.Instance,
            fsaLineFetcher.Object,
            fscmInvAttrs.Object,
            attrMap.Object);

        var invoiceUpdate = new InvoiceAttributesUpdateRunner(
            fscmInvAttrs.Object,
            NullLogger<InvoiceAttributesUpdateRunner>.Instance);

        return new AdHocSingleJobUseCase(
            NullLogger<AdHocSingleJobUseCase>.Instance,
            new NoopAisLogger(),
            new FakeAisDiagnosticsOptions(),
            payloadOrch.Object,
            new FsOptions(),
            posting.Object,
            deltaV2.Object,
            invoiceSync,
            invoiceUpdate);
    }

    private static FakeHttpRequestData NewReq(FunctionContext ctx, string body)
    {
        var bytes = Encoding.UTF8.GetBytes(body ?? string.Empty);
        return new FakeHttpRequestData(ctx, new Uri("https://example/api"), new MemoryStream(bytes));
    }

    private static string Envelope(string workOrderGuid)
{
    var obj = new
    {
        _request = new
        {
            WOList = new[]
            {
                new
                {
                    WorkOrderGUID = "{" + workOrderGuid + "}"
                }
            }
        }
    };

    return JsonSerializer.Serialize(obj);
}

}
