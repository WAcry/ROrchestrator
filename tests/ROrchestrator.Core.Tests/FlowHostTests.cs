using ROrchestrator.Core;
using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Core.Tests;

public sealed class FlowHostTests
{
    private static readonly DateTimeOffset FutureDeadline = new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_ShouldExecuteFlowAndReturnOutcome()
    {
        var services = new DummyServiceProvider();
        var context = new FlowContext(services, CancellationToken.None, FutureDeadline);

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        var registry = new FlowRegistry();
        registry.Register<int, int>(
            "test_flow",
            FlowBlueprint.Define<int, int>("TestFlow")
                .Step("step_a", "m.add_one")
                .Join<int>(
                    "final",
                    ctx =>
                    {
                        Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var stepOutcome));
                        Assert.True(stepOutcome.IsOk);
                        return new ValueTask<Outcome<int>>(Outcome<int>.Ok(stepOutcome.Value + 10));
                    })
                .Build());

        var host = new FlowHost(registry, catalog);

        var result = await host.ExecuteAsync<int, int>("test_flow", request: 5, context);

        Assert.True(result.IsOk);
        Assert.Equal(16, result.Value);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldThrow_WhenFlowNameIsUnknown()
    {
        var services = new DummyServiceProvider();
        var context = new FlowContext(services, CancellationToken.None, FutureDeadline);

        var host = new FlowHost(new FlowRegistry(), new ModuleCatalog());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => host.ExecuteAsync<int, int>("unknown_flow", request: 1, context).AsTask());

        Assert.Contains("unknown_flow", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldReuseCachedPlanTemplate_ForSameFlowName()
    {
        var services = new DummyServiceProvider();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        var registry = new FlowRegistry();
        registry.Register<int, int>(
            "test_flow",
            FlowBlueprint.Define<int, int>("TestFlow")
                .Step("step_a", "m.add_one")
                .Join<int>(
                    "final",
                    ctx =>
                    {
                        Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var stepOutcome));
                        Assert.True(stepOutcome.IsOk);
                        return new ValueTask<Outcome<int>>(Outcome<int>.Ok(stepOutcome.Value));
                    })
                .Build());

        var host = new FlowHost(registry, catalog);

        Assert.Equal(0, host.CachedPlanTemplateCount);

        var contextA = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var resultA = await host.ExecuteAsync<int, int>("test_flow", request: 1, contextA);
        Assert.True(resultA.IsOk);
        Assert.Equal(2, resultA.Value);
        Assert.Equal(1, host.CachedPlanTemplateCount);

        var contextB = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var resultB = await host.ExecuteAsync<int, int>("test_flow", request: 2, contextB);
        Assert.True(resultB.IsOk);
        Assert.Equal(3, resultB.Value);
        Assert.Equal(1, host.CachedPlanTemplateCount);
    }

    private sealed class DummyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType)
        {
            return null;
        }
    }

    private sealed class AddOneModule : IModule<int, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(context.Args + 1));
        }
    }
}

