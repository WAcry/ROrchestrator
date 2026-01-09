using ROrchestrator.Core.Blueprint;
using ROrchestrator.Core.Testing;

namespace ROrchestrator.Core.Tests;

public sealed class ExecutionEngineStageFanoutTests
{
    private static readonly DateTimeOffset FutureDeadline = new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_Template_ShouldSkipDisabledGateFalseAndFanoutTrim_AndRecordExecExplain()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"FanoutFlow\":{\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[" +
            "{\"id\":\"m_disabled\",\"use\":\"test.ok\",\"with\":{},\"enabled\":false}," +
            "{\"id\":\"m_gate_false\",\"use\":\"test.ok\",\"with\":{},\"gate\":{\"experiment\":{\"layer\":\"l1\",\"in\":[\"B\"]}}}," +
            "{\"id\":\"m_high\",\"use\":\"test.ok\",\"with\":{},\"priority\":10}," +
            "{\"id\":\"m_low\",\"use\":\"test.ok\",\"with\":{},\"priority\":0}" +
            "]}}}}}";

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(
            services,
            CancellationToken.None,
            FutureDeadline,
            requestOptions: new FlowRequestOptions(
                variants: new Dictionary<string, string>
                {
                    { "l1", "A" },
                }));

        flowContext.EnableExecExplain();

        var invocationCollector = new FlowTestInvocationCollector();
        flowContext.ConfigureForTesting(overrideProvider: null, invocationCollector);

        _ = await flowContext.GetConfigSnapshotAsync(new StaticConfigProvider(configVersion: 1, patchJson));

        var catalog = new ModuleCatalog();
        catalog.Register<OkArgs, int>("test.ok", _ => new OkModule());

        var blueprint = FlowBlueprint
            .Define<int, int>("FanoutFlow")
            .Stage(
                "s1",
                stage =>
                    stage.Join<int>(
                        "final",
                        _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(template, request: 0, flowContext);

        Assert.True(result.IsOk);

        Assert.True(flowContext.TryGetExecExplain(out var explain));

        Assert.Equal(4, explain.StageModules.Count);

        AssertStageModuleOutcome(explain, "m_disabled", OutcomeKind.Skipped, "DISABLED");
        AssertStageModuleOutcome(explain, "m_gate_false", OutcomeKind.Skipped, "GATE_FALSE");
        AssertStageModuleOutcome(explain, "m_high", OutcomeKind.Ok, "OK");
        AssertStageModuleOutcome(explain, "m_low", OutcomeKind.Skipped, "FANOUT_TRIM");

        Assert.Single(invocationCollector.ToArray());
        Assert.Equal("m_high", invocationCollector.ToArray()[0].ModuleId);
    }

    [Fact]
    public async Task ExecuteAsync_Template_ShouldExecuteFanoutModulesConcurrently()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"FanoutFlow\":{\"stages\":{\"s1\":{\"fanoutMax\":2,\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.barrier\",\"with\":{}}," +
            "{\"id\":\"m2\",\"use\":\"test.barrier\",\"with\":{}}" +
            "]}}}}}";

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        flowContext.EnableExecExplain();

        _ = await flowContext.GetConfigSnapshotAsync(new StaticConfigProvider(configVersion: 1, patchJson));

        var state = new BarrierState(expectedStartCount: 2);

        var catalog = new ModuleCatalog();
        catalog.Register<EmptyArgs, int>("test.barrier", _ => new BarrierModule(state));

        var blueprint = FlowBlueprint
            .Define<int, int>("FanoutFlow")
            .Stage(
                "s1",
                stage =>
                    stage.Join<int>(
                        "final",
                        _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var executionTask = engine.ExecuteAsync(template, request: 0, flowContext).AsTask();

        var startedOrTimeout = await Task.WhenAny(state.AllStarted.Task, Task.Delay(millisecondsDelay: 1000));
        state.Release.TrySetResult();
        _ = await executionTask;

        Assert.Same(state.AllStarted.Task, startedOrTimeout);
    }

    [Fact]
    public async Task ExecuteAsync_Template_WhenCanceledDuringFanout_ShouldReturnCanceled_AndRecordModuleOutcome()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"FanoutFlow\":{\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[" +
            "{\"id\":\"m_cancel\",\"use\":\"test.wait_cancel\",\"with\":{}}" +
            "]}}}}}";

        using var cts = new CancellationTokenSource();

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, cts.Token, FutureDeadline);
        flowContext.EnableExecExplain();

        _ = await flowContext.GetConfigSnapshotAsync(new StaticConfigProvider(configVersion: 1, patchJson));

        var state = new CancelState();

        var catalog = new ModuleCatalog();
        catalog.Register<EmptyArgs, int>("test.wait_cancel", _ => new WaitForCancelModule(state));

        var blueprint = FlowBlueprint
            .Define<int, int>("FanoutFlow")
            .Stage(
                "s1",
                stage =>
                    stage.Join<int>(
                        "final",
                        _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var executionTask = engine.ExecuteAsync(template, request: 0, flowContext).AsTask();

        var startedOrTimeout = await Task.WhenAny(state.Started.Task, Task.Delay(millisecondsDelay: 1000));
        Assert.Same(state.Started.Task, startedOrTimeout);

        cts.Cancel();

        var result = await executionTask;

        Assert.Equal(OutcomeKind.Canceled, result.Kind);
        Assert.Equal(ExecutionEngine.UpstreamCanceledCode, result.Code);

        Assert.True(flowContext.TryGetExecExplain(out var explain));
        AssertStageModuleOutcome(explain, "m_cancel", OutcomeKind.Canceled, ExecutionEngine.UpstreamCanceledCode);
    }

    private static void AssertStageModuleOutcome(ExecExplain explain, string moduleId, OutcomeKind kind, string code)
    {
        var modules = explain.StageModules;

        for (var i = 0; i < modules.Count; i++)
        {
            if (!string.Equals(modules[i].ModuleId, moduleId, StringComparison.Ordinal))
            {
                continue;
            }

            Assert.Equal(kind, modules[i].OutcomeKind);
            Assert.Equal(code, modules[i].OutcomeCode);
            return;
        }

        Assert.Fail($"Stage module '{moduleId}' was not recorded.");
    }

    private sealed class DummyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType)
        {
            return null;
        }
    }

    private sealed class StaticConfigProvider : IConfigProvider
    {
        private readonly ConfigSnapshot _snapshot;

        public StaticConfigProvider(ulong configVersion, string patchJson)
        {
            _snapshot = new ConfigSnapshot(configVersion, patchJson);
        }

        public ValueTask<ConfigSnapshot> GetSnapshotAsync(FlowContext context)
        {
            return new ValueTask<ConfigSnapshot>(_snapshot);
        }
    }

    private sealed class EmptyArgs
    {
    }

    private sealed class OkArgs
    {
    }

    private sealed class OkModule : IModule<OkArgs, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<OkArgs> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(1));
        }
    }

    private sealed class BarrierState
    {
        private readonly int _expectedStartCount;
        private int _startedCount;

        public TaskCompletionSource AllStarted { get; }

        public TaskCompletionSource Release { get; }

        public BarrierState(int expectedStartCount)
        {
            _expectedStartCount = expectedStartCount;
            AllStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public void RecordStart()
        {
            if (Interlocked.Increment(ref _startedCount) == _expectedStartCount)
            {
                AllStarted.TrySetResult();
            }
        }
    }

    private sealed class BarrierModule : IModule<EmptyArgs, int>
    {
        private readonly BarrierState _state;

        public BarrierModule(BarrierState state)
        {
            _state = state;
        }

        public async ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<EmptyArgs> context)
        {
            _state.RecordStart();
            await _state.Release.Task.ConfigureAwait(false);
            return Outcome<int>.Ok(0);
        }
    }

    private sealed class CancelState
    {
        public TaskCompletionSource Started { get; }

        public CancelState()
        {
            Started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }

    private sealed class WaitForCancelModule : IModule<EmptyArgs, int>
    {
        private readonly CancelState _state;

        public WaitForCancelModule(CancelState state)
        {
            _state = state;
        }

        public async ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<EmptyArgs> context)
        {
            _state.Started.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, context.CancellationToken).ConfigureAwait(false);
            return Outcome<int>.Ok(0);
        }
    }
}
