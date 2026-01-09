using ROrchestrator.Core;
using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Core.Tests;

public sealed class QosTierTests
{
    private static readonly DateTimeOffset FutureDeadline = new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_ShouldRecordSelectedTier_InExecExplain()
    {
        const string flowName = "qos_flow";

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        var registry = new FlowRegistry();
        registry.Register<int, int>(flowName, CreateTestFlowBlueprint(flowName));

        var services = new DummyServiceProvider();

        var fullContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        fullContext.EnableExecExplain(ExplainLevel.Standard);

        var hostDefault = new FlowHost(registry, catalog);

        var fullOutcome = await hostDefault.ExecuteAsync<int, int>(flowName, request: 1, fullContext);
        Assert.True(fullOutcome.IsOk);

        Assert.True(fullContext.TryGetExecExplain(out var fullExplain));
        Assert.Equal(QosTier.Full, fullExplain.QosSelectedTier);

        var conserveContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        conserveContext.EnableExecExplain(ExplainLevel.Standard);

        var qosProvider = new FixedQosTierProvider(QosTier.Conserve);
        var hostConserve = new FlowHost(registry, catalog, qosProvider);

        var conserveOutcome = await hostConserve.ExecuteAsync<int, int>(flowName, request: 1, conserveContext);
        Assert.True(conserveOutcome.IsOk);

        Assert.True(conserveContext.TryGetExecExplain(out var conserveExplain));
        Assert.Equal(QosTier.Conserve, conserveExplain.QosSelectedTier);
    }

    private static FlowBlueprint<int, int> CreateTestFlowBlueprint(string flowName)
    {
        return FlowBlueprint.Define<int, int>(flowName)
            .Step("step_a", "m.add_one")
            .Join<int>(
                "final",
                ctx =>
                {
                    Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var stepOutcome));
                    Assert.True(stepOutcome.IsOk);
                    return new ValueTask<Outcome<int>>(Outcome<int>.Ok(stepOutcome.Value));
                })
            .Build();
    }

    private sealed class AddOneModule : IModule<int, int>
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

    private sealed class DummyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType)
        {
            _ = serviceType;
            return null;
        }
    }
}

