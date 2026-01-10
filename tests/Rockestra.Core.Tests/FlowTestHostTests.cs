using Rockestra.Core;
using Rockestra.Core.Blueprint;
using Rockestra.Core.Testing;

namespace Rockestra.Core.Tests;

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

    [Fact]
    public async Task RunAsync_ShouldSupportConfigSnapshotAndRequestOptions_ForPatchDrivenStageFanout()
    {
        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => new CountingModule(new CallCounter(), delta: 1));
        catalog.Register<EmptyArgs, int>("m.ok", _ => new OkModule());

        var registry = new FlowRegistry();
        registry.Register<int, int>(
            "test_flow",
            FlowBlueprint.Define<int, int>("TestFlow.PatchFanout")
                .Stage(
                    "s1",
                    stage =>
                        stage
                            .Step("step_a", "m.add_one")
                            .Join<int>(
                                "final",
                                ctx =>
                                {
                                    Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var stepOutcome));
                                    return new ValueTask<Outcome<int>>(stepOutcome);
                                }))
                .Build());

        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"test_flow\":{\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[" +
            "{\"id\":\"m_base\",\"use\":\"m.ok\",\"with\":{},\"priority\":0}," +
            "{\"id\":\"m_user\",\"use\":\"m.ok\",\"with\":{},\"priority\":10,\"gate\":{\"rollout\":{\"percent\":100,\"salt\":\"s\"}}}" +
            "]}},\"experiments\":[{\"layer\":\"l1\",\"variant\":\"A\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[" +
            "{\"id\":\"m_exp\",\"use\":\"m.ok\",\"with\":{},\"priority\":20}" +
            "]}}}}]}}}";

        var snapshot = new ConfigSnapshot(configVersion: 123, patchJson);

        var host = FlowTestHost.Create(registry, catalog)
            .WithDeadline(FutureDeadline)
            .Build();

        var resultVariantA = await host.RunAsync<int, int>(
            "test_flow",
            req: 1,
            requestOptions: new FlowRequestOptions(
                variants: new Dictionary<string, string> { { "l1", "A" } },
                userId: "u1"),
            configSnapshot: snapshot);

        Assert.True(resultVariantA.Outcome.IsOk);
        Assert.Equal(2, resultVariantA.Invocations.Count);
        Assert.Equal("m_exp", resultVariantA.Invocations[0].ModuleId);
        Assert.Equal("step_a", resultVariantA.Invocations[1].ModuleId);
        Assert.True(resultVariantA.Explain.Variants.TryGetValue("l1", out var l1VariantA));
        Assert.Equal("A", l1VariantA);
        Assert.Equal(2, resultVariantA.Explain.OverlaysApplied.Count);
        Assert.Equal("experiment", resultVariantA.Explain.OverlaysApplied[1].Layer);

        var resultVariantB = await host.RunAsync<int, int>(
            "test_flow",
            req: 1,
            requestOptions: new FlowRequestOptions(
                variants: new Dictionary<string, string> { { "l1", "B" } },
                userId: "u1"),
            configSnapshot: snapshot);

        Assert.True(resultVariantB.Outcome.IsOk);
        Assert.Equal(2, resultVariantB.Invocations.Count);
        Assert.Equal("m_user", resultVariantB.Invocations[0].ModuleId);
        Assert.Equal("step_a", resultVariantB.Invocations[1].ModuleId);
        Assert.True(resultVariantB.Explain.Variants.TryGetValue("l1", out var l1VariantB));
        Assert.Equal("B", l1VariantB);
        Assert.Single(resultVariantB.Explain.OverlaysApplied);
        Assert.Equal("base", resultVariantB.Explain.OverlaysApplied[0].Layer);

        var resultMissingUser = await host.RunAsync<int, int>(
            "test_flow",
            req: 1,
            requestOptions: new FlowRequestOptions(
                variants: new Dictionary<string, string> { { "l1", "B" } },
                userId: null),
            configSnapshot: snapshot);

        Assert.True(resultMissingUser.Outcome.IsOk);
        Assert.Equal(2, resultMissingUser.Invocations.Count);
        Assert.Equal("m_base", resultMissingUser.Invocations[0].ModuleId);
        Assert.Equal("step_a", resultMissingUser.Invocations[1].ModuleId);
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

    private sealed class EmptyArgs
    {
    }

    private sealed class OkModule : IModule<EmptyArgs, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<EmptyArgs> context)
        {
            _ = context;
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(1));
        }
    }
}

