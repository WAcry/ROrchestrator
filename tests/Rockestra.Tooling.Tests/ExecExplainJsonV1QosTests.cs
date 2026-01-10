using System.Text.Json;
using Rockestra.Core;
using Rockestra.Core.Blueprint;
using Rockestra.Tooling;

namespace Rockestra.Tooling.Tests;

public sealed class ExecExplainJsonV1QosTests
{
    private static readonly DateTimeOffset FutureDeadline = new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExportJson_ShouldIncludeSelectedTier()
    {
        const string flowName = "QosJsonFlow";

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
                        if (!ctx.TryGetNodeOutcome<int>("compute", out var outcome))
                        {
                            throw new InvalidOperationException("Outcome for node 'compute' has not been recorded.");
                        }

                        return new ValueTask<Outcome<int>>(outcome);
                    })
                .Build());

        var host = new FlowHost(registry, catalog, new FixedQosTierProvider(QosTier.Emergency));

        var flowContext = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline);
        flowContext.EnableExecExplain(new ExplainOptions(ExplainLevel.Standard));

        var outcome = await host.ExecuteAsync<int, int>(flowName, request: 5, flowContext);
        Assert.True(outcome.IsOk);

        Assert.True(flowContext.TryGetExecExplain(out var explain));

        var json = ExecExplainJsonV1.ExportJson(explain);

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        Assert.True(root.TryGetProperty("qos", out var qos));
        Assert.True(qos.TryGetProperty("selected_tier", out var selectedTier));
        Assert.Equal("emergency", selectedTier.GetString());

        Assert.True(qos.TryGetProperty("reason_code", out var reasonCode));
        Assert.Equal(JsonValueKind.Null, reasonCode.ValueKind);

        Assert.True(qos.TryGetProperty("signals", out var signals));
        Assert.Equal(JsonValueKind.Null, signals.ValueKind);
    }

    private sealed class ComputeModule : IModule<int, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(context.Args + 1));
        }
    }

    private sealed class FixedQosTierProvider : IQosTierProvider
    {
        private readonly QosTier _tier;

        public FixedQosTierProvider(QosTier tier)
        {
            _tier = tier;
        }

        public QosTier SelectTier(string flowName, FlowContext context)
        {
            _ = flowName;
            _ = context;
            return _tier;
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

