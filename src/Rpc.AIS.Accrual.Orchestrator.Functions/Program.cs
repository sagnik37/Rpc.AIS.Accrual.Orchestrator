// File: src/Rpc.AIS.Accrual.Orchestrator.Functions/Program.cs
// .NET 8 isolated worker host bootstrap + DI registrations.

using System;
using System.Configuration;
using System.Linq;
using System.Net.Http;

using Azure.Communication.Email;
using Azure.Identity;
using Microsoft.Extensions.Logging;

using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.ApplicationInsights;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Caching.Memory;
using Rpc.AIS.Accrual.Orchestrator.Core.Abstractions;
using Rpc.AIS.Accrual.Orchestrator.Core.Domain.Delta;
using Rpc.AIS.Accrual.Orchestrator.Core.Options;
using Rpc.AIS.Accrual.Orchestrator.Core.Services;
using Rpc.AIS.Accrual.Orchestrator.Core.Services.FsaDeltaPayload;
using Rpc.AIS.Accrual.Orchestrator.Core.Services.FsaDeltaPayload.Mappers;
using Rpc.AIS.Accrual.Orchestrator.Core.Services.InvoiceAttributes;
using Rpc.AIS.Accrual.Orchestrator.Core.Services.JournalPolicies;
using Rpc.AIS.Accrual.Orchestrator.Core.Services.WoPayloadValidationPipeline;
using Rpc.AIS.Accrual.Orchestrator.Core.Services.WoPayloadValidationRules;
using Rpc.AIS.Accrual.Orchestrator.Core.UseCases.FsaDeltaPayload;
using Rpc.AIS.Accrual.Orchestrator.Functions.Services;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Clients;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Clients.FscmJournalPolicies;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Clients.Posting;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Http;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Logging;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Notifications;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Options;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Resilience;
using Rpc.AIS.Accrual.Orchestrator.Infrastructure.Utilities;

using Polly;

using CoreNotificationOptions = Rpc.AIS.Accrual.Orchestrator.Core.Options.NotificationOptions;
using InfraNotificationOptions = Rpc.AIS.Accrual.Orchestrator.Infrastructure.Options.NotificationOptions;

namespace Rpc.AIS.Accrual.Orchestrator.Functions;

internal static class Program
{
    public static void Main(string[] args)
    {
        var host = new HostBuilder()
            .ConfigureAppConfiguration((ctx, config) =>
            {
                // local.settings.json is loaded by the Functions host. appsettings.json is optional for local/dev convenience.
                config.AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
                      .AddEnvironmentVariables();

                // Add Key Vault on top (Managed Identity / DefaultAzureCredential).
                var built = config.Build();
                var vaultUri = built["KeyVault:VaultUri"] ?? built["KeyVault:Uri"];

                if (!string.IsNullOrWhiteSpace(vaultUri))
                {
                    config.AddAzureKeyVault(new Uri(vaultUri), new DefaultAzureCredential());
                }
                else if (!string.Equals(ctx.HostingEnvironment.EnvironmentName, "Development", StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException("KeyVault:VaultUri is required in non-Development environments.");
                }
            })
            .ConfigureFunctionsWorkerDefaults()
            .ConfigureServices((ctx, services) =>
            {
                var cfg = ctx.Configuration;

                // ----------------------------
                // Observability
                // ----------------------------
                services.AddApplicationInsightsTelemetryWorkerService();
                services.ConfigureFunctionsApplicationInsights();
                services.AddOptions<HttpPolicyOptions>()
                    .Bind(cfg.GetSection(HttpPolicyOptions.SectionName))
                    .ValidateDataAnnotations()
                    .ValidateOnStart();

                // ----------------------------
                // Options binding
                // ----------------------------
                // Core options
                services.AddOptions<ProcessingOptions>()
                    .Bind(cfg.GetSection(ProcessingOptions.SectionName))
                    .ValidateOnStart();

                services.AddOptions<CoreNotificationOptions>()
                    .Bind(cfg.GetSection(CoreNotificationOptions.SectionName))
                    .ValidateOnStart();

                //services.AddOptions<PayloadValidationOptions>()
                //    .Bind(cfg.GetSection(PayloadValidationOptions.SectionName))
                //    .ValidateOnStart();

                //services.AddOptions<FscmCustomValidationOptions>()
                //    .Bind(cfg.GetSection(FscmCustomValidationOptions.SectionName))
                //    .ValidateOnStart();

                // AisDiagnosticsOptions has no SectionName constant in this repo.
                //  local.settings.json uses "AisLogging:*" keys.
                services.AddOptions<AisDiagnosticsOptions>()
                    .Bind(cfg.GetSection("AisLogging"))
                    .ValidateOnStart();



                // Infra options
                services.AddOptions<InfraNotificationOptions>()
                    .Bind(cfg.GetSection(InfraNotificationOptions.SectionName))
                    .ValidateOnStart();

                services.AddOptions<AcsEmailOptions>()
                    .Bind(cfg.GetSection(AcsEmailOptions.SectionName))
                    .ValidateOnStart();

                services.AddOptions<FscmODataStagingOptions>()
                    .Bind(cfg.GetSection(FscmODataStagingOptions.SectionName))
                    .ValidateOnStart();

                services.AddOptions<HttpResilienceOptions>()
                    .Configure<IConfiguration>((opt, c) =>
                    {
                        // Prefer current section name, support legacy.
                        c.GetSection(HttpResilienceOptions.SectionName).Bind(opt);
                        c.GetSection("HttpResilience").Bind(opt);
                    })
                    .ValidateOnStart();

                // FsOptions mapping (supports legacy keys used in local.settings.json).
                // FsOptions mapping (single source of truth: FsaIngestion:* + Dataverse:Auth:*)
                services.AddOptions<FsOptions>()
                    .Configure<IConfiguration>((o, c) =>
                    {
                        // Dataverse API (OData)
                        o.DataverseApiBaseUrl = c["FsaIngestion:DataverseApiBaseUrl"] ?? "";
                        o.WorkOrderFilter = c["FsaIngestion:WorkOrderFilter"];

                        if (int.TryParse(c["FsaIngestion:PageSize"], out var ps)) o.PageSize = ps;
                        if (int.TryParse(c["FsaIngestion:MaxPages"], out var mp)) o.MaxPages = mp;
                        if (int.TryParse(c["FsaIngestion:PreferMaxPageSize"], out var pmp)) o.PreferMaxPageSize = pmp;
                        if (int.TryParse(c["FsaIngestion:OrFilterChunkSize"], out var cs)) o.OrFilterChunkSize = cs;

                        // Auth (flat properties in this repo; NO FsOptions.Auth)
                        o.TenantId = c["Dataverse:Auth:TenantId"] ?? "";
                        o.ClientId = c["Dataverse:Auth:ClientId"] ?? "";
                        o.ClientSecret = c["Dataverse:Auth:ClientSecret"] ?? "";
                    })
                    .Validate(o =>
                        !string.IsNullOrWhiteSpace(o.DataverseApiBaseUrl) &&
                        !string.IsNullOrWhiteSpace(o.TenantId) &&
                        !string.IsNullOrWhiteSpace(o.ClientId) &&
                        !string.IsNullOrWhiteSpace(o.ClientSecret),
                        "Missing required FS config. Required: FsaIngestion:DataverseApiBaseUrl and Dataverse:Auth:*")
                    .ValidateOnStart();


                // FscmOptions mapping (supports legacy Endpoints:* keys used in local.settings.json).
                services.AddOptions<FscmOptions>()
                    .Configure<IConfiguration>((o, c) =>
                    {
                        // Base URL + auth (flat properties in this repo; NO FscmOptions.Auth)
                        o.BaseUrl = c["Endpoints:FscmBaseUrl"] ?? "";

                        o.TenantId = c["Fscm:Auth:TenantId"] ?? "";
                        o.ClientId = c["Fscm:Auth:ClientId"] ?? "";
                        o.ClientSecret = c["Fscm:Auth:ClientSecret"] ?? "";
                        o.DefaultScope = c["Fscm:Auth:DefaultScope"] ?? "";

                        // Subproject endpoint
                        o.SubProjectPath = c["Fscm:SubProjectPath"] ?? "";

                        // Multi-endpoint journal pipeline contract
                        o.JournalValidatePath = c["Fscm:JournalValidatePath"] ?? "";
                        o.JournalCreatePath = c["Fscm:JournalCreatePath"] ?? "";

                        //  bind the real post endpoint
                        // (Ensure FscmOptions has: public string JournalPostPath { get; set; } )
                        o.JournalPostPath = c["Fscm:JournalPostPath"] ?? "";

                        // Optional backward compatibility for any code still using JournalPostCustomPath

                        o.JournalPostCustomPath = o.JournalPostPath;

                        o.UpdateInvoiceAttributesPath = c["Fscm:UpdateInvoiceAttributesPath"] ?? "";

                        // Status update endpoint is used by multiple clients in  codebase
                        o.UpdateProjectStatusPath = c["Fscm:UpdateProjectStatusPath"] ?? "";

                        // OR-filter chunking (optional)
                        if (int.TryParse(c["Fscm:JournalHistoryOrFilterChunkSize"], out var jcs) && jcs > 0)
                            o.JournalHistoryOrFilterChunkSize = Math.Min(jcs, 200);

                        if (int.TryParse(c["Fscm:ReleasedDistinctProductsOrFilterChunkSize"], out var rcs) && rcs > 0)
                            o.ReleasedDistinctProductsOrFilterChunkSize = Math.Min(rcs, 200);
                    })
                    .Validate(o =>
                        !string.IsNullOrWhiteSpace(o.TenantId) &&
                        !string.IsNullOrWhiteSpace(o.ClientId) &&
                        !string.IsNullOrWhiteSpace(o.ClientSecret),
                        "Missing required FSCM auth config under Fscm:Auth:*")
                    .ValidateOnStart();

                // Fail-fast validation for endpoints (multi-endpoint contract).
                services.AddSingleton<IValidateOptions<FscmOptions>, FscmOptionsStartupValidator>();

                // Expose options values directly for constructors that take T (not IOptions<T>).
                services.AddSingleton(sp => sp.GetRequiredService<IOptions<ProcessingOptions>>().Value);

                //  Also expose IAisDiagnosticsOptions abstraction for Core (WoDeltaPayloadService expects IAisDiagnosticsOptions)
                services.AddSingleton(sp => sp.GetRequiredService<IOptions<AisDiagnosticsOptions>>().Value);
                services.AddSingleton<IAisDiagnosticsOptions>(sp => sp.GetRequiredService<AisDiagnosticsOptionsAdapter>());

                services.AddSingleton(sp => sp.GetRequiredService<IOptions<CoreNotificationOptions>>().Value);
                services.AddSingleton(sp => sp.GetRequiredService<IOptions<InfraNotificationOptions>>().Value);
                services.AddSingleton(sp => sp.GetRequiredService<IOptions<FsOptions>>().Value);
                services.AddSingleton(sp => sp.GetRequiredService<IOptions<FscmOptions>>().Value);
                services.AddSingleton(sp => sp.GetRequiredService<IOptions<FscmODataStagingOptions>>().Value);

                //  AcsEmailSender in this repo expects AcsEmailOptions (concrete), not IOptions<AcsEmailOptions>.
                services.AddSingleton(sp => sp.GetRequiredService<IOptions<AcsEmailOptions>>().Value);

                // ----------------------------
                // Core services (Domain/UseCases)
                // ----------------------------
                services.AddSingleton<IRunIdGenerator, RunIdGenerator>();

                services.AddSingleton<JournalReversalPlanner>();
                services.AddSingleton<DeltaCalculationEngine>();
                services.AddScoped<IFsaDeltaPayloadUseCase, FsaDeltaPayloadUseCase>();

                //  Missing dependencies (fixes AggregateException)
                services.AddSingleton<FscmJournalAggregator>(); // WoDeltaPayloadService dependency
                services.AddSingleton<DeltaComparer>();         // FsaDeltaPayloadUseCase dependency

                services.AddSingleton<IWoDeltaPayloadService, WoDeltaPayloadService>();
                services.AddSingleton<IWoDeltaPayloadServiceV2, WoDeltaPayloadService>();

                services.AddSingleton<SubProjectProvisioningService>();

                // Invoice attribute sync
                services.AddSingleton<InvoiceAttributeDeltaBuilder>();
                services.AddSingleton<RuntimeInvoiceAttributeMapper>();

                // FSA snapshot building (required by FsaDeltaPayloadUseCase)
                services.AddSingleton<IFsaProductLineMapper, FsaProductLineMapper>();
                services.AddSingleton<IFsaServiceLineMapper, FsaServiceLineMapper>();
                services.AddSingleton<IFsaSnapshotBuilder, Rpc.AIS.Accrual.Orchestrator.Core.Services.FsaDeltaPayload.Mappers.FsaSnapshotBuilder>();

                // Use cases
                services.AddSingleton<IFsaDeltaPayloadUseCase, FsaDeltaPayloadUseCase>();
                // FSA delta payload enrichment (required by FsaDeltaPayloadUseCase)
                services.AddSingleton<IFsaDeltaPayloadEnricher, FsaDeltaPayloadEnricher>();

                // Outbound payload enrichment pipeline (SRP/OCP): step-per-concern
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Application.Features.Delta.FsaDeltaPayload.Services.EnrichmentPipeline.IFsaDeltaPayloadEnrichmentPipeline,
                    Rpc.AIS.Accrual.Orchestrator.Application.Features.Delta.FsaDeltaPayload.Services.EnrichmentPipeline.DefaultFsaDeltaPayloadEnrichmentPipeline>();

                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Application.Features.Delta.FsaDeltaPayload.Services.EnrichmentPipeline.IFsaDeltaPayloadEnrichmentStep,
                    Rpc.AIS.Accrual.Orchestrator.Application.Features.Delta.FsaDeltaPayload.Services.EnrichmentPipeline.Steps.FsExtrasEnrichmentStep>();
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Application.Features.Delta.FsaDeltaPayload.Services.EnrichmentPipeline.IFsaDeltaPayloadEnrichmentStep,
                    Rpc.AIS.Accrual.Orchestrator.Application.Features.Delta.FsaDeltaPayload.Services.EnrichmentPipeline.Steps.CompanyEnrichmentStep>();
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Application.Features.Delta.FsaDeltaPayload.Services.EnrichmentPipeline.IFsaDeltaPayloadEnrichmentStep,
                    Rpc.AIS.Accrual.Orchestrator.Application.Features.Delta.FsaDeltaPayload.Services.EnrichmentPipeline.Steps.JournalNamesEnrichmentStep>();
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Application.Features.Delta.FsaDeltaPayload.Services.EnrichmentPipeline.IFsaDeltaPayloadEnrichmentStep,
                    Rpc.AIS.Accrual.Orchestrator.Application.Features.Delta.FsaDeltaPayload.Services.EnrichmentPipeline.Steps.SubProjectEnrichmentStep>();
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Application.Features.Delta.FsaDeltaPayload.Services.EnrichmentPipeline.IFsaDeltaPayloadEnrichmentStep,
                    Rpc.AIS.Accrual.Orchestrator.Application.Features.Delta.FsaDeltaPayload.Services.EnrichmentPipeline.Steps.WorkOrderHeaderFieldsEnrichmentStep>();
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Application.Features.Delta.FsaDeltaPayload.Services.EnrichmentPipeline.IFsaDeltaPayloadEnrichmentStep,
                    Rpc.AIS.Accrual.Orchestrator.Application.Features.Delta.FsaDeltaPayload.Services.EnrichmentPipeline.Steps.JournalDescriptionsEnrichmentStep>();

                // Payload validation pipeline
                // Journal type policies (required by WoJournalProjector pruning)
                services.AddSingleton<IJournalTypePolicy, ItemJournalTypePolicy>();
                services.AddSingleton<IJournalTypePolicy, ExpenseJournalTypePolicy>();
                services.AddSingleton<IJournalTypePolicy, HourJournalTypePolicy>();

                services.AddSingleton<IJournalTypePolicyResolver, JournalTypePolicyResolver>();
                services.AddSingleton<IFscmReferenceValidator, FscmReferenceValidator>();
                services.AddSingleton<IWoPayloadValidationEngine, WoPayloadValidationEngine>();
                services.AddSingleton<IWoPayloadRule, WoEnvelopeParseRule>();
                services.AddSingleton<IWoPayloadRule, WoLocalValidationRule>();
                services.AddSingleton<IWoPayloadRule, WoFscmCustomValidationRule>();
                services.AddSingleton<IWoPayloadRule, WoBuildResultRule>();

                //  Missing validation pipeline building blocks
                services.AddSingleton<IWoEnvelopeParser, WoEnvelopeParser>();
                services.AddSingleton<IWoLocalValidator, WoLocalValidator>();
                services.AddSingleton<IWoValidationResultBuilder, WoValidationResultBuilder>();

                // ----------------------------
                // Cross-cutting infra
                // ----------------------------
                services.AddSingleton<IAisLogger, AppInsightsAisLogger>();
                services.AddSingleton<AisDiagnosticsOptionsAdapter>();
                services.AddSingleton<ITelemetry, Telemetry>();

                services.AddSingleton<IResilientHttpExecutor, ResilientHttpExecutor>();
                services.AddSingleton<IHttpFailureClassifier, DefaultHttpFailureClassifier>();

                //  EmailClient required by AcsEmailSender
                services.AddSingleton(sp =>
                {
                    var opt = sp.GetRequiredService<IOptions<AcsEmailOptions>>().Value;
                    return new EmailClient(opt.ConnectionString);
                });

                services.AddSingleton<IEmailSender, AcsEmailSender>();
                services.AddSingleton<IInvalidPayloadNotifier, InvalidPayloadEmailNotifier>();

                // ----------------------------
                // Function-layer services
                // ----------------------------
                services.AddSingleton<IFsaDeltaPayloadOrchestrator, FsaDeltaPayloadOrchestrator>();
                // Job operations HTTP use case (used by endpoint-specific Functions)
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Functions.Functions.IJobOperationsHttpUseCase, Rpc.AIS.Accrual.Orchestrator.Functions.Functions.JobOperationsHttpHandlerCommon>();

                // JobOperations split: shared handler used by endpoint-specific Function classes
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Functions.Functions.JobOperationsHttpHandlerCommon>();

                // Endpoint-specific use cases (ISP/DIP): each Function depends only on the contract it needs.
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Functions.Functions.IAdHocSingleJobUseCase, Rpc.AIS.Accrual.Orchestrator.Functions.Functions.AdHocSingleJobUseCase>();
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Functions.Functions.IAdHocAllJobsUseCase, Rpc.AIS.Accrual.Orchestrator.Functions.Functions.AdHocAllJobsUseCase>();
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Functions.Functions.IPostJobUseCase, Rpc.AIS.Accrual.Orchestrator.Functions.Functions.PostJobUseCase>();
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Functions.Functions.ICancelJobUseCase, Rpc.AIS.Accrual.Orchestrator.Functions.Functions.CancelJobUseCase>();
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Functions.Functions.ICustomerChangeUseCase, Rpc.AIS.Accrual.Orchestrator.Functions.Functions.CustomerChangeUseCase>();

                // Activity handlers (SOLID: one handler per activity)
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Functions.Services.Handlers.ValidateAndPostWoPayloadHandler>();
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Functions.Services.Handlers.PostSingleWorkOrderHandler>();
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Functions.Services.Handlers.UpdateWorkOrderStatusHandler>();
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Functions.Services.Handlers.PostRetryableWoPayloadHandler>();
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Functions.Services.Handlers.SyncInvoiceAttributesHandler>();
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Functions.Services.Handlers.FinalizeAndNotifyWoPayloadHandler>();
                services.AddSingleton<IActivitiesUseCase, ActivitiesUseCase>();

                services.AddSingleton<ICustomerChangeOrchestrator, CustomerChangeOrchestrator>();

                // Job operations lifecycle defaults (NOOP until FSCM endpoints are finalized)
                services.AddSingleton<IFscmProjectLifecycle, NoopFscmProjectLifecycle>();

                services.AddSingleton<InvoiceAttributeSyncRunner>();
                services.AddSingleton<InvoiceAttributesUpdateRunner>();

                // ----------------------------
                // HTTP policies (config-driven, centralized, SOLID)
                // ----------------------------
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Infrastructure.Resilience.HttpPolicies>();

                static IHttpClientBuilder AddDataversePolicies(IHttpClientBuilder builder, string clientName)
                {
                    return builder
                        .ConfigureHttpClient((sp, client) =>
                        {
                            var opt = sp.GetRequiredService<IOptions<HttpPolicyOptions>>().Value.Dataverse;
                            client.Timeout = TimeSpan.FromSeconds(opt.TimeoutSeconds);
                        })
                        .AddPolicyHandler((sp, req) =>
                        {
                            // IMPORTANT: Never retry HTTP POST (non-idempotent). Retrying can create duplicate side effects.
                            if (req?.Method == HttpMethod.Post)
                                return Policy.NoOpAsync<HttpResponseMessage>();

                            var policies = sp.GetRequiredService<
                                Rpc.AIS.Accrual.Orchestrator.Infrastructure.Resilience.HttpPolicies>();

                            var opt = sp.GetRequiredService<IOptions<HttpPolicyOptions>>().Value.Dataverse;

                            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
                            var logger = loggerFactory.CreateLogger($"DataversePolicy:{clientName}");

                            return policies.CreateRetryPolicy(opt, logger, "Dataverse");
                        });
                }

                static IHttpClientBuilder AddFscmPolicies(IHttpClientBuilder builder, string clientName)
                {
                    return builder
                        .ConfigureHttpClient((sp, client) =>
                        {
                            var opt = sp.GetRequiredService<IOptions<HttpPolicyOptions>>().Value.Fscm;
                            client.Timeout = TimeSpan.FromSeconds(opt.TimeoutSeconds);
                        })
                        .AddPolicyHandler((sp, req) =>
                        {
                            // IMPORTANT: Never retry HTTP POST (non-idempotent). Retrying can create duplicate journals/side effects.
                            if (req?.Method == HttpMethod.Post)
                                return Policy.NoOpAsync<HttpResponseMessage>();

                            var policies = sp.GetRequiredService<
                                Rpc.AIS.Accrual.Orchestrator.Infrastructure.Resilience.HttpPolicies>();

                            var opt = sp.GetRequiredService<IOptions<HttpPolicyOptions>>().Value.Fscm;

                            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
                            var logger = loggerFactory.CreateLogger($"FscmPolicy:{clientName}");

                            return policies.CreateRetryPolicy(opt, logger, "FSCM");
                        });
                }

                // ----------------------------
                // HTTP clients + auth handlers
                // ----------------------------
                services.AddSingleton<IDataverseTokenProvider, DataverseBearerTokenProvider>();
                services.AddSingleton<IFscmTokenProvider, FscmBearerTokenProvider>();
                services.AddTransient<DataverseAuthHandler>();
                services.AddTransient<FscmAuthHandler>();

                // Dataverse / FSA
                services.AddSingleton<IFsaODataQueryBuilder, FsaODataQueryBuilder>();
                services.AddSingleton<IODataPagedReader, ODataPagedReader>();
                services.AddSingleton<IFsaRowFlattener, FsaRowFlattener>();

                // Warehouse/Site enrichment used by FsaLineFetcher
                AddDataversePolicies(
                    services.AddHttpClient<IWarehouseSiteEnricher, WarehouseSiteEnricher>((sp, http) =>
                    {
                        var opt = sp.GetRequiredService<IOptions<FsOptions>>().Value;
                        http.BaseAddress = new Uri(opt.DataverseApiBaseUrl!, UriKind.Absolute);
                    }).AddHttpMessageHandler<DataverseAuthHandler>(),
                    clientName: "dataverse-warehouse-site-enricher");

                AddDataversePolicies(
                    services.AddHttpClient<IVirtualLookupNameResolver, VirtualLookupNameResolver>((sp, http) =>
                    {
                        var opt = sp.GetRequiredService<IOptions<FsOptions>>().Value;
                        http.BaseAddress = new Uri(opt.DataverseApiBaseUrl!, UriKind.Absolute);
                    })
                    .AddHttpMessageHandler<DataverseAuthHandler>(),
                    clientName: "dataverse-virtual-lookup-name-resolver");

                AddDataversePolicies(
                    services.AddHttpClient<FsaLineFetcher>((sp, http) =>
                    {
                        var opt = sp.GetRequiredService<IOptions<FsOptions>>().Value;
                        http.BaseAddress = new Uri(opt.DataverseApiBaseUrl!, UriKind.Absolute);
                    }).AddHttpMessageHandler<DataverseAuthHandler>(),
                    clientName: "dataverse-fsa-line-fetcher");

                services.AddSingleton<IFsaLineFetcher>(sp => sp.GetRequiredService<FsaLineFetcher>());

                // FSCM base
                AddFscmPolicies(
                    services.AddHttpClient<PostingHttpClient>((sp, http) =>
                    {
                        var opt = sp.GetRequiredService<IOptions<FscmOptions>>().Value;
                        http.BaseAddress = new Uri(opt.BaseUrl!, UriKind.Absolute);
                    }).AddHttpMessageHandler<FscmAuthHandler>(),
                    clientName: "fscm-posting-http");

                services.AddSingleton<IPostingClient>(sp => sp.GetRequiredService<PostingHttpClient>());

                AddFscmPolicies(
                    services.AddHttpClient<FscmSingleWorkOrderHttpClient>((sp, http) =>
                    {
                        var opt = sp.GetRequiredService<IOptions<FscmOptions>>().Value;
                        http.BaseAddress = new Uri(opt.BaseUrl!, UriKind.Absolute);
                    }).AddHttpMessageHandler<FscmAuthHandler>(),
                    clientName: "fscm-single-workorder");

                services.AddSingleton<ISingleWorkOrderPostingClient>(sp => sp.GetRequiredService<FscmSingleWorkOrderHttpClient>());

                AddFscmPolicies(
                    services.AddHttpClient<FscmWorkOrderStatusUpdateHttpClient>((sp, http) =>
                    {
                        var opt = sp.GetRequiredService<IOptions<FscmOptions>>().Value;
                        http.BaseAddress = new Uri(opt.BaseUrl!, UriKind.Absolute);
                    }).AddHttpMessageHandler<FscmAuthHandler>(),
                    clientName: "fscm-workorder-status-update");

                services.AddSingleton<IWorkOrderStatusUpdateClient>(sp => sp.GetRequiredService<FscmWorkOrderStatusUpdateHttpClient>());

                AddFscmPolicies(
                    services.AddHttpClient<FscmProjectStatusHttpClient>((sp, http) =>
                    {
                        var opt = sp.GetRequiredService<IOptions<FscmOptions>>().Value;
                        http.BaseAddress = new Uri(opt.BaseUrl!, UriKind.Absolute);
                    }).AddHttpMessageHandler<FscmAuthHandler>(),
                    clientName: "fscm-project-status");

                services.AddSingleton<IFscmProjectStatusClient>(sp => sp.GetRequiredService<FscmProjectStatusHttpClient>());

                AddFscmPolicies(
                    services.AddHttpClient<FscmInvoiceAttributesHttpClient>((sp, http) =>
                    {
                        var opt = sp.GetRequiredService<IOptions<FscmOptions>>().Value;
                        http.BaseAddress = new Uri(opt.BaseUrl!, UriKind.Absolute);
                    }).AddHttpMessageHandler<FscmAuthHandler>(),
                    clientName: "fscm-invoice-attributes");

                services.AddSingleton<IFscmInvoiceAttributesClient>(sp => sp.GetRequiredService<FscmInvoiceAttributesHttpClient>());

                services.AddSingleton<PayloadPostingDateAdjuster>();

                services.AddSingleton<IFscmPostRequestFactory, FscmPostRequestFactory>();
                services.AddSingleton<IWoPayloadNormalizer, WoPayloadNormalizer>();
                services.AddSingleton<IWoPayloadShapeGuard, WoPayloadShapeGuard>();
                services.AddSingleton<IWoJournalProjector, WoJournalProjector>();
                services.AddSingleton<IFscmPostingResponseParser, FscmPostingResponseParserAdapter>();
                services.AddSingleton<IPostErrorAggregator, PostErrorAggregator>();
                //services.AddSingleton<IPostResultHandler, DefaultPostResultHandler>();

                services.AddSingleton<IPostingWorkflowFactory, PostingWorkflowFactory>();
                // FSCM Journal Fetch Policies (OCP) + Resolver
                services.AddSingleton<IFscmJournalFetchPolicy, ItemJournalFetchPolicy>();
                services.AddSingleton<IFscmJournalFetchPolicy, ExpenseJournalFetchPolicy>();
                services.AddSingleton<IFscmJournalFetchPolicy, HourJournalFetchPolicy>();
                services.AddSingleton<FscmJournalFetchPolicyResolver>();

                AddFscmPolicies(
                    services.AddHttpClient<FscmJournalFetchHttpClient>((sp, http) =>
                    {
                        var opt = sp.GetRequiredService<IOptions<FscmOptions>>().Value;
                        http.BaseAddress = new Uri(opt.BaseUrl!, UriKind.Absolute);
                    }).AddHttpMessageHandler<FscmAuthHandler>(),
                    clientName: "fscm-journal-fetch");

                services.AddSingleton<IFscmJournalFetchClient>(sp => sp.GetRequiredService<FscmJournalFetchHttpClient>());

                AddFscmPolicies(
                    services.AddHttpClient<FscmAccountingPeriodHttpClient>((sp, http) =>
                    {
                        var opt = sp.GetRequiredService<IOptions<FscmOptions>>().Value;
                        http.BaseAddress = new Uri(opt.BaseUrl!, UriKind.Absolute);
                    }).AddHttpMessageHandler<FscmAuthHandler>(),
                    clientName: "fscm-accounting-period");

                services.AddSingleton<IFscmAccountingPeriodClient>(sp => sp.GetRequiredService<FscmAccountingPeriodHttpClient>());

                // FSCM OData: LegalEntityIntegrationParametersBaseIntParamTables (JournalName ids)
                AddFscmPolicies(
                    services.AddHttpClient<FscmLegalEntityIntegrationParametersHttpClient>((sp, http) =>
                    {
                        var opt = sp.GetRequiredService<IOptions<FscmOptions>>().Value;
                        http.BaseAddress = new Uri(opt.BaseUrl!, UriKind.Absolute);
                    }).AddHttpMessageHandler<FscmAuthHandler>(),
                    clientName: "fscm-legal-entity-integration-params");

                services.AddSingleton<IFscmLegalEntityIntegrationParametersClient>(sp => sp.GetRequiredService<FscmLegalEntityIntegrationParametersHttpClient>());

                AddFscmPolicies(
                    services.AddHttpClient<FscmReleasedDistinctProductsHttpClient>((sp, http) =>
                    {
                        var opt = sp.GetRequiredService<IOptions<FscmOptions>>().Value;
                        http.BaseAddress = new Uri(opt.BaseUrl!, UriKind.Absolute);
                    }).AddHttpMessageHandler<FscmAuthHandler>(),
                    clientName: "fscm-released-distinct-products");

                services.AddMemoryCache();

                services.AddOptions<FscmReleasedDistinctProductsCacheOptions>()
                    .Bind(cfg.GetSection(FscmReleasedDistinctProductsCacheOptions.SectionName))
                    .Validate(o => o.Ttl > TimeSpan.Zero, "Ttl must be > 0")
                    .Validate(o => o.NegativeTtl > TimeSpan.Zero, "NegativeTtl must be > 0")
                    .Validate(o => o.MaxItemCountPerCall > 0, "MaxItemCountPerCall must be > 0")
                    .ValidateOnStart();

                // Ensure inner is registered (prefer typed client if possible)
                services.AddHttpClient<FscmReleasedDistinctProductsHttpClient>();

                // Decorator binding (ONLY binding for the interface)
                services.AddSingleton<IFscmReleasedDistinctProductsClient>(sp =>
                {
                    var inner = sp.GetRequiredService<FscmReleasedDistinctProductsHttpClient>();
                    return new CachedFscmReleasedDistinctProductsClient(
                        inner,
                        sp.GetRequiredService<IMemoryCache>(),
                        sp.GetRequiredService<IOptions<FscmReleasedDistinctProductsCacheOptions>>(),
                        sp.GetRequiredService<ILogger<CachedFscmReleasedDistinctProductsClient>>());
                });


                AddFscmPolicies(
                    services.AddHttpClient<FscmCustomValidationClient>((sp, http) =>
                    {
                        var opt = sp.GetRequiredService<IOptions<FscmOptions>>().Value;
                        http.BaseAddress = new Uri(opt.BaseUrl!, UriKind.Absolute);
                    }).AddHttpMessageHandler<FscmAuthHandler>(),
                    clientName: "fscm-custom-validation");

                services.AddSingleton<IFscmCustomValidationClient>(sp => sp.GetRequiredService<FscmCustomValidationClient>());

                // FSCM WO payload validation client (custom endpoint validation)
                AddFscmPolicies(
                    services.AddHttpClient<Rpc.AIS.Accrual.Orchestrator.Infrastructure.Clients.Posting.FscmWoPayloadValidationClient>((sp, http) =>
                    {
                        var opt = sp.GetRequiredService<IOptions<FscmOptions>>().Value;
                        http.BaseAddress = new Uri(opt.BaseUrl!, UriKind.Absolute);
                    }).AddHttpMessageHandler<FscmAuthHandler>(),
                    clientName: "fscm-wo-payload-validation");

                // Map Core abstraction -> Infrastructure implementation
                services.AddSingleton<Rpc.AIS.Accrual.Orchestrator.Core.Abstractions.IFscmWoPayloadValidationClient>(sp =>
                    sp.GetRequiredService<Rpc.AIS.Accrual.Orchestrator.Infrastructure.Clients.Posting.FscmWoPayloadValidationClient>());

                //  Subproject client required by SubProjectProvisioningService / CustomerChangeOrchestrator
                AddFscmPolicies(
                    services.AddHttpClient<FscmSubProjectHttpClient>((sp, http) =>
                    {
                        var opt = sp.GetRequiredService<IOptions<FscmOptions>>().Value;
                        http.BaseAddress = new Uri(opt.BaseUrl!, UriKind.Absolute);
                    }).AddHttpMessageHandler<FscmAuthHandler>(),
                    clientName: "fscm-subproject");

                services.AddSingleton<IFscmSubProjectClient>(sp => sp.GetRequiredService<FscmSubProjectHttpClient>());

                //  Baseline fetcher required by FsaDeltaPayloadUseCase
                AddFscmPolicies(
                    services.AddHttpClient<FscmBaselineFetcher>((sp, http) =>
                    {
                        var opt = sp.GetRequiredService<IOptions<FscmOptions>>().Value;
                        http.BaseAddress = new Uri(opt.BaseUrl!, UriKind.Absolute);
                    }).AddHttpMessageHandler<FscmAuthHandler>(),
                    clientName: "fscm-baseline-fetcher");

                services.AddSingleton<IFscmBaselineFetcher>(sp => sp.GetRequiredService<FscmBaselineFetcher>());

                AddFscmPolicies(
                    services.AddHttpClient<FscmGlobalAttributeMappingHttpClient>((sp, http) =>
                    {
                        var opt = sp.GetRequiredService<IOptions<FscmOptions>>().Value;
                        http.BaseAddress = new Uri(opt.BaseUrl!, UriKind.Absolute);
                    }).AddHttpMessageHandler<FscmAuthHandler>(),
                    clientName: "fscm-global-attribute-mapping");

                services.AddSingleton<IFscmGlobalAttributeMappingClient>(sp => sp.GetRequiredService<FscmGlobalAttributeMappingHttpClient>());

                services.AddMemoryCache();

                services.AddOptions<FscmReleasedDistinctProductsCacheOptions>()
                    .Bind(cfg.GetSection(FscmReleasedDistinctProductsCacheOptions.SectionName))
                    .Validate(o => o.Ttl > TimeSpan.Zero, "Fscm:ReleasedDistinctProductsCache:Ttl must be > 0")
                    .Validate(o => o.NegativeTtl > TimeSpan.Zero, "Fscm:ReleasedDistinctProductsCache:NegativeTtl must be > 0")
                    .Validate(o => o.MaxItemCountPerCall > 0, "Fscm:ReleasedDistinctProductsCache:MaxItemCountPerCall must be > 0")
                    .ValidateOnStart();
                services.AddSingleton<IJournalDescriptionBuilder, JournalDescriptionBuilder>();
            })
            .Build();

        host.Run();
    }

    /// <summary>
    /// Fail-fast validation so the app refuses to start if endpoints are missing, placeholders exist,
    /// or the journal pipeline endpoints are not distinct (multi-endpoint contract).
    /// </summary>
    private sealed class FscmOptionsStartupValidator : IValidateOptions<FscmOptions>
    {
        public ValidateOptionsResult Validate(string? name, FscmOptions options)
        {
            if (options is null) return ValidateOptionsResult.Fail("FSCM options are null.");

            static bool IsPlaceholder(string? v)
                => string.IsNullOrWhiteSpace(v) || v.Contains("<", StringComparison.OrdinalIgnoreCase);

            // Required endpoints (contract)
            var required = new (string Key, string? Value)[]
            {
                ("Endpoints:FscmBaseUrl", options.BaseUrl),
                ("Fscm:SubProjectPath", options.SubProjectPath),
                ("Fscm:JournalValidatePath", options.JournalValidatePath),
                ("Fscm:JournalCreatePath", options.JournalCreatePath),

                // validate the correct property
                ("Fscm:JournalPostPath", options.JournalPostPath),

                ("Fscm:UpdateInvoiceAttributesPath", options.UpdateInvoiceAttributesPath),
                ("Fscm:UpdateProjectStatusPath", options.UpdateProjectStatusPath),
            };

            var missing = required
                .Where(x => IsPlaceholder(x.Value))
                .Select(x => x.Key)
                .ToArray();

            if (missing.Length > 0)
                return ValidateOptionsResult.Fail("Missing or placeholder FSCM config: " + string.Join(", ", missing));

            // Distinctness check for multi-endpoint journal pipeline
            var journalEndpoints = new[]
            {
                options.JournalValidatePath,
                options.JournalCreatePath,

                // distinctness must use JournalPostPath (not CreatePath / not CustomPath)
                options.JournalPostPath,

                options.UpdateInvoiceAttributesPath,
            }.Where(s => !string.IsNullOrWhiteSpace(s)).ToArray();

            if (journalEndpoints.Length != journalEndpoints.Distinct(StringComparer.OrdinalIgnoreCase).Count())
                return ValidateOptionsResult.Fail("FSCM journal pipeline endpoints must be distinct.");

            return ValidateOptionsResult.Success;
        }
    }
}
