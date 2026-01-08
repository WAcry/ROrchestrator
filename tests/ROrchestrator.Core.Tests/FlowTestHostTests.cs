using ROrchestrator.Core;
using ROrchestrator.Core.Blueprint;
using ROrchestrator.Core.Testing;

namespace ROrchestrator.Core.Tests;

public sealed class FlowTestHostTests
{
    private static readonly DateTimeOffset FutureDeadline = new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task RunAsync_WhenOverrideMatches_ShouldNotCallRealModule()
    {
        var counter = new CallCounter();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.count", _ => new CountingModule(counter, delta: 1));

        var registry = new FlowRegistry();
        registry.Register<int, int>(
            "test_flow",
            FlowBlueprint.Define<int, int>("TestFlow.OverrideSkipsReal")
                .Step("step_a", "m.count")
                .Join<int>(
                    "final",
                    ctx =>
                    {
                        Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var stepOutcome));
                        return new ValueTask<Outcome<int>>(stepOutcome);
                    })
                .Build());

        var host = FlowTestHost.Create(registry, catalog)
            .WithOverrides(overrides => overrides.OverrideOutcome("step_a", Outcome<int>.Ok(123)))
            .WithDeadline(FutureDeadline)
            .Build();

        var result = await host.RunAsync<int, int>("test_flow", req: 0);

        Assert.True(result.Outcome.IsOk);
        Assert.Equal(123, result.Outcome.Value);
        Assert.Equal(0, counter.Count);
    }

    [Fact]
    public async Task RunAsync_WhenNoOverrideConfigured_ShouldCallRealModule()
    {
        var counter = new CallCounter();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.count", _ => new CountingModule(counter, delta: 2));

        var registry = new FlowRegistry();
        registry.Register<int, int>(
            "test_flow",
            FlowBlueprint.Define<int, int>("TestFlow.NoOverrideUsesReal")
                .Step("step_a", "m.count")
                .Join<int>(
                    "final",
                    ctx =>
                    {
                        Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var stepOutcome));
                        return new ValueTask<Outcome<int>>(stepOutcome);
                    })
                .Build());

        var host = FlowTestHost.Create(registry, catalog)
            .WithDeadline(FutureDeadline)
            .Build();

        var result = await host.RunAsync<int, int>("test_flow", req: 10);

        Assert.True(result.Outcome.IsOk);
        Assert.Equal(12, result.Outcome.Value);
        Assert.Equal(1, counter.Count);
    }

    [Fact]
    public async Task RunAsync_ShouldEnableExecExplain_AndRecordInvocations_WithOverrideAndRealInOrder()
    {
        var counterA = new CallCounter();
        var counterB = new CallCounter();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.a", _ => new CountingModule(counterA, delta: 1));
        catalog.Register<int, int>("m.b", _ => new CountingModule(counterB, delta: 10));

        var registry = new FlowRegistry();
        registry.Register<int, int>(
            "test_flow",
            FlowBlueprint.Define<int, int>("TestFlow.ExplainAndInvocations")
                .Step("step_a", "m.a")
                .Step("step_b", "m.b")
                .Join<int>(
                    "final",
                    ctx =>
                    {
                        Assert.True(ctx.TryGetNodeOutcome<int>("step_b", out var stepB));
                        Assert.True(stepB.IsOk);
                        return new ValueTask<Outcome<int>>(Outcome<int>.Ok(stepB.Value));
                    })
                .Build());

        var host = FlowTestHost.Create(registry, catalog)
            .WithOverrides(overrides => overrides.OverrideOutcome("step_a", Outcome<int>.Error("OVERRIDDEN")))
            .WithDeadline(FutureDeadline)
            .Build();

        var result = await host.RunAsync<int, int>("test_flow", req: 1);

        Assert.True(result.Outcome.IsOk);
        Assert.Equal(11, result.Outcome.Value);

        Assert.Equal(0, counterA.Count);
        Assert.Equal(1, counterB.Count);

        Assert.Equal(3, result.Explain.Nodes.Count);
        Assert.Equal("step_a", result.Explain.Nodes[0].Name);
        Assert.Equal(OutcomeKind.Error, result.Explain.Nodes[0].OutcomeKind);
        Assert.Equal("OVERRIDDEN", result.Explain.Nodes[0].OutcomeCode);

        Assert.Equal("step_b", result.Explain.Nodes[1].Name);
        Assert.Equal(OutcomeKind.Ok, result.Explain.Nodes[1].OutcomeKind);
        Assert.Equal("OK", result.Explain.Nodes[1].OutcomeCode);

        Assert.Equal("final", result.Explain.Nodes[2].Name);
        Assert.Equal(OutcomeKind.Ok, result.Explain.Nodes[2].OutcomeKind);
        Assert.Equal("OK", result.Explain.Nodes[2].OutcomeCode);

        Assert.Equal(2, result.Invocations.Count);
        Assert.Equal("step_a", result.Invocations[0].ModuleId);
        Assert.Equal(ModuleInvocationSource.Override, result.Invocations[0].Source);
        Assert.Equal("step_b", result.Invocations[1].ModuleId);
        Assert.Equal(ModuleInvocationSource.Real, result.Invocations[1].Source);
    }

    private sealed class CallCounter
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public void Increment()
        {
            Interlocked.Increment(ref _count);
        }
    }

    private sealed class CountingModule : IModule<int, int>
    {
        private readonly CallCounter _counter;
        private readonly int _delta;

        public CountingModule(CallCounter counter, int delta)
        {
            _counter = counter;
            _delta = delta;
        }

        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            _counter.Increment();
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(context.Args + _delta));
        }
    }
}

