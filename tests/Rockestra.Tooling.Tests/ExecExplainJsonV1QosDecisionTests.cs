using System.Text.Json;
using Rockestra.Core;
using Rockestra.Core.Blueprint;
using Rockestra.Tooling;

namespace Rockestra.Tooling.Tests;

public sealed class ExecExplainJsonV1QosDecisionTests
{
    private static readonly DateTimeOffset FutureDeadline = new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExportJson_WhenFullWithoutReason_ShouldDowngradeAndNotCollectSignals()
    {
        const string flowName = "QosDecision_FullWithoutReason";

        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("test.compute", _ => new ComputeModule());

        registry.Register(
            flowName,
            FlowBlueprint.Define<int, int>(flowName)
                .Step("compute", moduleType: "test.compute")
                .Join<int>(
                    "final",
                    ctx =>
                    {
                        Assert.True(ctx.TryGetNodeOutcome<int>("compute", out var outcome));
                        return new ValueTask<Outcome<int>>(outcome);
                    })
                .Build());

        var provider = new FixedQosProvider(
            tier: QosTier.Emergency,
            reasonCode: "PRESSURE_HIGH",
            signals: new Dictionary<string, string>
            {
                { "b", "2" },
                { "a", "1" },
            });

        var host = new FlowHost(registry, catalog, provider);

        var flowContext = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline);
        flowContext.EnableExecExplain(new ExplainOptions(ExplainLevel.Full));

        var outcome = await host.ExecuteAsync<int, int>(flowName, request: 5, flowContext);
        Assert.True(outcome.IsOk);

        Assert.True(flowContext.TryGetExecExplain(out var explain));

        var json = ExecExplainJsonV1.ExportJson(explain);

        Assert.False(provider.LastRequestCollectSignals);

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        Assert.Equal("standard", root.GetProperty("level").GetString());

        var options = root.GetProperty("explain_options");
        Assert.Equal("full", options.GetProperty("requested_level").GetString());
        Assert.Equal("standard", options.GetProperty("effective_level").GetString());
        Assert.Equal(JsonValueKind.Null, options.GetProperty("reason").ValueKind);
        Assert.Equal("FULL_REASON_REQUIRED", options.GetProperty("downgrade_reason").GetString());
    }

    [Fact]
    public async Task ExportJson_ShouldNotRequestSignals_ByDefault()
    {
        const string flowName = "QosDecision_NoSignals";

        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("test.compute", _ => new ComputeModule());

        registry.Register(
            flowName,
            FlowBlueprint.Define<int, int>(flowName)
                .Step("compute", moduleType: "test.compute")
                .Join<int>(
                    "final",
                    ctx =>
                    {
                        Assert.True(ctx.TryGetNodeOutcome<int>("compute", out var outcome));
                        return new ValueTask<Outcome<int>>(outcome);
                    })
                .Build());

        var provider = new FixedQosProvider(
            tier: QosTier.Emergency,
            reasonCode: "PRESSURE_HIGH",
            signals: new Dictionary<string, string>
            {
                { "b", "2" },
                { "a", "1" },
            });

        var host = new FlowHost(registry, catalog, provider);

        var flowContext = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline);
        flowContext.EnableExecExplain(new ExplainOptions(ExplainLevel.Standard));

        var outcome = await host.ExecuteAsync<int, int>(flowName, request: 5, flowContext);
        Assert.True(outcome.IsOk);

        Assert.True(flowContext.TryGetExecExplain(out var explain));

        var json = ExecExplainJsonV1.ExportJson(explain);

        Assert.False(provider.LastRequestCollectSignals);

        using var document = JsonDocument.Parse(json);
        var qos = document.RootElement.GetProperty("qos");

        Assert.Equal("emergency", qos.GetProperty("selected_tier").GetString());
        Assert.Equal("PRESSURE_HIGH", qos.GetProperty("reason_code").GetString());
        Assert.Equal(JsonValueKind.Null, qos.GetProperty("signals").ValueKind);
    }

    [Fact]
    public async Task ExportJson_ShouldIncludeSortedSignals_WhenEnabled()
    {
        const string flowName = "QosDecision_Signals";

        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("test.compute", _ => new ComputeModule());

        registry.Register(
            flowName,
            FlowBlueprint.Define<int, int>(flowName)
                .Step("compute", moduleType: "test.compute")
                .Join<int>(
                    "final",
                    ctx =>
                    {
                        Assert.True(ctx.TryGetNodeOutcome<int>("compute", out var outcome));
                        return new ValueTask<Outcome<int>>(outcome);
                    })
                .Build());

        var provider = new FixedQosProvider(
            tier: QosTier.Emergency,
            reasonCode: "PRESSURE_HIGH",
            signals: new Dictionary<string, string>
            {
                { "b", "2" },
                { "a", "1" },
            });

        var host = new FlowHost(registry, catalog, provider);

        var flowContext = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline);
        flowContext.EnableExecExplain(new ExplainOptions(ExplainLevel.Full, reason: "qos_signals"));

        var outcome = await host.ExecuteAsync<int, int>(flowName, request: 5, flowContext);
        Assert.True(outcome.IsOk);

        Assert.True(flowContext.TryGetExecExplain(out var explain));

        var json = ExecExplainJsonV1.ExportJson(explain);

        Assert.True(provider.LastRequestCollectSignals);

        using var document = JsonDocument.Parse(json);
        var qos = document.RootElement.GetProperty("qos");

        Assert.Equal("emergency", qos.GetProperty("selected_tier").GetString());
        Assert.Equal("PRESSURE_HIGH", qos.GetProperty("reason_code").GetString());

        var signals = qos.GetProperty("signals");
        Assert.Equal(JsonValueKind.Object, signals.ValueKind);

        var propIndex = 0;

        foreach (var property in signals.EnumerateObject())
        {
            if (propIndex == 0)
            {
                Assert.Equal("a", property.Name);
                Assert.Equal("1", property.Value.GetString());
            }
            else if (propIndex == 1)
            {
                Assert.Equal("b", property.Name);
                Assert.Equal("2", property.Value.GetString());
            }
            else
            {
                throw new InvalidOperationException("Unexpected signals property.");
            }

            propIndex++;
        }

        Assert.Equal(2, propIndex);
    }

    [Fact]
    public async Task ExportJson_ShouldTruncateReasonCode()
    {
        const string flowName = "QosDecision_ReasonCodeTruncation";

        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("test.compute", _ => new ComputeModule());

        registry.Register(
            flowName,
            FlowBlueprint.Define<int, int>(flowName)
                .Step("compute", moduleType: "test.compute")
                .Join<int>(
                    "final",
                    ctx =>
                    {
                        Assert.True(ctx.TryGetNodeOutcome<int>("compute", out var outcome));
                        return new ValueTask<Outcome<int>>(outcome);
                    })
                .Build());

        var reasonCode = new string('A', 128);

        var provider = new FixedQosProvider(
            tier: QosTier.Emergency,
            reasonCode: reasonCode,
            signals: null);

        var host = new FlowHost(registry, catalog, provider);

        var flowContext = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline);
        flowContext.EnableExecExplain(new ExplainOptions(ExplainLevel.Minimal));

        var outcome = await host.ExecuteAsync<int, int>(flowName, request: 5, flowContext);
        Assert.True(outcome.IsOk);

        Assert.True(flowContext.TryGetExecExplain(out var explain));

        var json = ExecExplainJsonV1.ExportJson(explain);

        using var document = JsonDocument.Parse(json);
        var qos = document.RootElement.GetProperty("qos");

        var exported = qos.GetProperty("reason_code").GetString();
        Assert.NotNull(exported);
        Assert.Equal(new string('A', 64), exported);
    }

    [Fact]
    public async Task ExportJson_WhenProviderThrows_ShouldFallbackToFullAndSetReasonCode()
    {
        const string flowName = "QosDecision_ProviderThrows";

        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("test.compute", _ => new ComputeModule());

        registry.Register(
            flowName,
            FlowBlueprint.Define<int, int>(flowName)
                .Step("compute", moduleType: "test.compute")
                .Join<int>(
                    "final",
                    ctx =>
                    {
                        Assert.True(ctx.TryGetNodeOutcome<int>("compute", out var outcome));
                        return new ValueTask<Outcome<int>>(outcome);
                    })
                .Build());

        var host = new FlowHost(registry, catalog, new ThrowingQosProvider());

        var flowContext = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline);
        flowContext.EnableExecExplain(new ExplainOptions(ExplainLevel.Full, reason: "qos_provider_error"));

        var outcome = await host.ExecuteAsync<int, int>(flowName, request: 5, flowContext);
        Assert.True(outcome.IsOk);

        Assert.True(flowContext.TryGetExecExplain(out var explain));

        var json = ExecExplainJsonV1.ExportJson(explain);

        using var document = JsonDocument.Parse(json);
        var qos = document.RootElement.GetProperty("qos");

        Assert.Equal("full", qos.GetProperty("selected_tier").GetString());
        Assert.Equal("PROVIDER_ERROR", qos.GetProperty("reason_code").GetString());
        Assert.Equal(JsonValueKind.Null, qos.GetProperty("signals").ValueKind);
    }

    private sealed class ComputeModule : IModule<int, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(context.Args + 1));
        }
    }

    private sealed class FixedQosProvider : IQosProvider
    {
        private readonly QosTier _tier;
        private readonly string? _reasonCode;
        private readonly IReadOnlyDictionary<string, string>? _signals;

        public bool LastRequestCollectSignals { get; private set; }

        public FixedQosProvider(QosTier tier, string? reasonCode, IReadOnlyDictionary<string, string>? signals)
        {
            _tier = tier;
            _reasonCode = reasonCode;
            _signals = signals;
        }

        public QosDecision Select(string flowName, FlowContext context, QosSelectContext selectContext)
        {
            _ = flowName;
            _ = context;

            LastRequestCollectSignals = selectContext.CollectSignals;

            return selectContext.CollectSignals
                ? new QosDecision(_tier, _reasonCode, _signals)
                : new QosDecision(_tier, _reasonCode, signals: null);
        }
    }

    private sealed class ThrowingQosProvider : IQosProvider
    {
        public QosDecision Select(string flowName, FlowContext context, QosSelectContext selectContext)
        {
            _ = flowName;
            _ = context;
            _ = selectContext;
            throw new InvalidOperationException("boom");
        }
    }

    private sealed class EmptyServiceProvider : IServiceProvider
    {
        public static readonly EmptyServiceProvider Instance = new();

        private EmptyServiceProvider()
        {
        }

        public object? GetService(Type serviceType)
        {
            _ = serviceType;
            return null;
        }
    }
}

