using Rockestra.Core.Blueprint;
using Rockestra.Core.Testing;

namespace Rockestra.Core.Tests;

public sealed class ExecutionEngineStageFanoutTests
{
    private static readonly DateTimeOffset FutureDeadline = new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_Template_ShouldSkipDisabledGateFalseAndFanoutTrim_AndRecordExecExplain()
    {
        var patchJson = """
            {
              "schemaVersion": "v1",
              "flows": {
                "FanoutFlow": {
                  "stages": {
                    "s1": {
                      "fanoutMax": 1,
                      "modules": [
                        { "id": "m_disabled", "use": "test.ok", "with": {}, "enabled": false },
                        { "id": "m_gate_false", "use": "test.ok", "with": {}, "gate": { "experiment": { "layer": "l1", "in": [ "B" ] } } },
                        { "id": "m_high", "use": "test.ok", "with": {}, "priority": 10 },
                        { "id": "m_low", "use": "test.ok", "with": {}, "priority": 0 }
                      ]
                    }
                  }
                }
              }
            }
            """;

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

        flowContext.EnableExecExplain(new ExplainOptions(ExplainLevel.Standard));

        var invocationCollector = new FlowTestInvocationCollector();
        flowContext.ConfigureForTesting(overrideProvider: null, invocationCollector);

        _ = await flowContext.GetConfigSnapshotAsync(new StaticConfigProvider(configVersion: 1, patchJson));

        var catalog = new ModuleCatalog();
        catalog.Register<OkArgs, int>("test.ok", _ => new OkModule());

        var blueprint = FlowBlueprint
            .Define<int, int>("FanoutFlow")
            .Stage(
                "s1",
                contract => contract.AllowDynamicModules(),
                stage =>
                    stage.Join<int>(
                        "final",
                        _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(template, request: 0, flowContext);

        Assert.True(result.IsOk);

        Assert.True(flowContext.TryGetStageFanoutSnapshot("s1", out var snapshot));
        Assert.Single(snapshot.EnabledModuleIds);
        Assert.Equal("m_high", snapshot.EnabledModuleIds[0]);

        Assert.Equal(3, snapshot.SkippedModules.Count);
        AssertStageModuleSkip(snapshot, "m_disabled", ExecutionEngine.DisabledCode);
        AssertStageModuleSkip(snapshot, "m_gate_false", ExecutionEngine.GateFalseCode);
        AssertStageModuleSkip(snapshot, "m_low", ExecutionEngine.FanoutTrimCode);

        Assert.True(flowContext.TryGetExecExplain(out var explain));
        Assert.Equal(ExplainLevel.Standard, explain.Level);

        Assert.Equal(4, explain.StageModules.Count);

        AssertStageModuleOutcome(explain, "m_disabled", OutcomeKind.Skipped, "DISABLED");
        AssertStageModuleOutcome(explain, "m_gate_false", OutcomeKind.Skipped, "GATE_FALSE");
        AssertStageModuleOutcome(explain, "m_high", OutcomeKind.Ok, "OK");
        AssertStageModuleOutcome(explain, "m_low", OutcomeKind.Skipped, "FANOUT_TRIM");

        AssertStageModuleTimingAndGate(explain, "m_disabled", expectedGateDecisionCode: string.Empty, expectExecuted: false);
        AssertStageModuleTimingAndGate(explain, "m_gate_false", expectedGateDecisionCode: ExecutionEngine.GateFalseCode, expectExecuted: false);
        AssertStageModuleTimingAndGate(explain, "m_high", expectedGateDecisionCode: string.Empty, expectExecuted: true);
        AssertStageModuleTimingAndGate(explain, "m_low", expectedGateDecisionCode: string.Empty, expectExecuted: false);

        Assert.Single(invocationCollector.ToArray());
        Assert.Equal("m_high", invocationCollector.ToArray()[0].ModuleId);
    }

    [Fact]
    public async Task ExecuteAsync_Template_WhenStageContractForbidsModuleType_ShouldSkipWithoutExecuting_AndRecordExplain()
    {
        var patchJson = """
            {
              "schemaVersion": "v1",
              "flows": {
                "FanoutFlow": {
                  "stages": {
                    "s1": {
                      "fanoutMax": 1,
                      "modules": [
                        { "id": "m1", "use": "test.ok", "with": {} }
                      ]
                    }
                  }
                }
              }
            }
            """;

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        flowContext.EnableExecExplain(new ExplainOptions(ExplainLevel.Standard));

        var invocationCollector = new FlowTestInvocationCollector();
        flowContext.ConfigureForTesting(overrideProvider: null, invocationCollector);

        _ = await flowContext.GetConfigSnapshotAsync(new StaticConfigProvider(configVersion: 1, patchJson));

        var catalog = new ModuleCatalog();
        catalog.Register<OkArgs, int>("test.ok", _ => new OkModule());
        catalog.Register<OkArgs, int>("test.allowed", _ => new OkModule());

        var blueprint = FlowBlueprint
            .Define<int, int>("FanoutFlow")
            .Stage(
                "s1",
                contract => contract.AllowDynamicModules().AllowModuleTypes("test.allowed"),
                stage =>
                    stage.Join<int>(
                        "final",
                        _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(template, request: 0, flowContext);

        Assert.True(result.IsOk);

        Assert.True(flowContext.TryGetStageFanoutSnapshot("s1", out var snapshot));
        Assert.Empty(snapshot.EnabledModuleIds);
        Assert.Single(snapshot.SkippedModules);
        AssertStageModuleSkip(snapshot, "m1", ExecutionEngine.StageContractModuleTypeForbiddenCode);

        Assert.True(flowContext.TryGetExecExplain(out var explain));
        Assert.Equal(ExplainLevel.Standard, explain.Level);
        Assert.Single(explain.StageModules);
        AssertStageModuleOutcome(explain, "m1", OutcomeKind.Skipped, ExecutionEngine.StageContractModuleTypeForbiddenCode);

        Assert.Empty(invocationCollector.ToArray());
    }

    [Fact]
    public async Task ExecuteAsync_Template_WhenStageContractHardLimitIsExceeded_ShouldCullAndRecordExplain()
    {
        var patchJson = """
            {
              "schemaVersion": "v1",
              "flows": {
                "FanoutFlow": {
                  "stages": {
                    "s1": {
                      "fanoutMax": 2,
                      "modules": [
                        { "id": "m_high", "use": "test.ok", "with": {}, "priority": 10 },
                        { "id": "m_low", "use": "test.ok", "with": {}, "priority": 0 }
                      ]
                    }
                  }
                }
              }
            }
            """;

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        flowContext.EnableExecExplain(new ExplainOptions(ExplainLevel.Standard));

        var invocationCollector = new FlowTestInvocationCollector();
        flowContext.ConfigureForTesting(overrideProvider: null, invocationCollector);

        _ = await flowContext.GetConfigSnapshotAsync(new StaticConfigProvider(configVersion: 1, patchJson));

        var catalog = new ModuleCatalog();
        catalog.Register<OkArgs, int>("test.ok", _ => new OkModule());

        var blueprint = FlowBlueprint
            .Define<int, int>("FanoutFlow")
            .Stage(
                "s1",
                contract => contract.AllowDynamicModules().MaxModules(warn: 0, hard: 1),
                stage =>
                    stage.Join<int>(
                        "final",
                        _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(template, request: 0, flowContext);

        Assert.True(result.IsOk);

        Assert.True(flowContext.TryGetStageFanoutSnapshot("s1", out var snapshot));
        Assert.Single(snapshot.EnabledModuleIds);
        Assert.Equal("m_high", snapshot.EnabledModuleIds[0]);

        Assert.Single(snapshot.SkippedModules);
        AssertStageModuleSkip(snapshot, "m_low", ExecutionEngine.StageContractMaxModulesHardExceededCode);

        Assert.True(flowContext.TryGetExecExplain(out var explain));
        Assert.Equal(ExplainLevel.Standard, explain.Level);
        Assert.Equal(2, explain.StageModules.Count);

        AssertStageModuleOutcome(explain, "m_high", OutcomeKind.Ok, "OK");
        AssertStageModuleOutcome(explain, "m_low", OutcomeKind.Skipped, ExecutionEngine.StageContractMaxModulesHardExceededCode);

        Assert.Single(invocationCollector.ToArray());
        Assert.Equal("m_high", invocationCollector.ToArray()[0].ModuleId);
    }

    [Fact]
    public async Task ExecuteAsync_Template_WhenStageContractFanoutMaxIsExceeded_ShouldClampAndTrim()
    {
        var patchJson = """
            {
              "schemaVersion": "v1",
              "flows": {
                "FanoutFlow": {
                  "stages": {
                    "s1": {
                      "fanoutMax": 3,
                      "modules": [
                        { "id": "m_high", "use": "test.ok", "with": {}, "priority": 10 },
                        { "id": "m_low", "use": "test.ok", "with": {}, "priority": 0 }
                      ]
                    }
                  }
                }
              }
            }
            """;

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        flowContext.EnableExecExplain(new ExplainOptions(ExplainLevel.Standard));

        var invocationCollector = new FlowTestInvocationCollector();
        flowContext.ConfigureForTesting(overrideProvider: null, invocationCollector);

        _ = await flowContext.GetConfigSnapshotAsync(new StaticConfigProvider(configVersion: 1, patchJson));

        var catalog = new ModuleCatalog();
        catalog.Register<OkArgs, int>("test.ok", _ => new OkModule());

        var blueprint = FlowBlueprint
            .Define<int, int>("FanoutFlow")
            .Stage(
                "s1",
                contract => contract.AllowDynamicModules().FanoutMaxRange(min: 0, max: 1),
                stage =>
                    stage.Join<int>(
                        "final",
                        _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(template, request: 0, flowContext);

        Assert.True(result.IsOk);

        Assert.True(flowContext.TryGetStageFanoutSnapshot("s1", out var snapshot));
        Assert.Single(snapshot.EnabledModuleIds);
        Assert.Equal("m_high", snapshot.EnabledModuleIds[0]);

        Assert.Single(snapshot.SkippedModules);
        AssertStageModuleSkip(snapshot, "m_low", ExecutionEngine.FanoutTrimCode);

        Assert.True(flowContext.TryGetExecExplain(out var explain));
        Assert.Equal(ExplainLevel.Standard, explain.Level);
        Assert.Equal(2, explain.StageModules.Count);
        AssertStageModuleOutcome(explain, "m_high", OutcomeKind.Ok, "OK");
        AssertStageModuleOutcome(explain, "m_low", OutcomeKind.Skipped, ExecutionEngine.FanoutTrimCode);

        Assert.Single(invocationCollector.ToArray());
        Assert.Equal("m_high", invocationCollector.ToArray()[0].ModuleId);
    }

    [Fact]
    public async Task ExecuteAsync_Template_ShouldExecuteFanoutModulesConcurrently()
    {
        var patchJson = """
            {
              "schemaVersion": "v1",
              "flows": {
                "FanoutFlow": {
                  "stages": {
                    "s1": {
                      "fanoutMax": 2,
                      "modules": [
                        { "id": "m1", "use": "test.barrier", "with": {} },
                        { "id": "m2", "use": "test.barrier", "with": {} }
                      ]
                    }
                  }
                }
              }
            }
            """;

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
                contract => contract.AllowDynamicModules(),
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
        var patchJson = """
            {
              "schemaVersion": "v1",
              "flows": {
                "FanoutFlow": {
                  "stages": {
                    "s1": {
                      "fanoutMax": 1,
                      "modules": [
                        { "id": "m_cancel", "use": "test.wait_cancel", "with": {} }
                      ]
                    }
                  }
                }
              }
            }
            """;

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
                contract => contract.AllowDynamicModules(),
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

    private static void AssertStageModuleTimingAndGate(
        ExecExplain explain,
        string moduleId,
        string expectedGateDecisionCode,
        bool expectExecuted)
    {
        var modules = explain.StageModules;

        for (var i = 0; i < modules.Count; i++)
        {
            if (!string.Equals(modules[i].ModuleId, moduleId, StringComparison.Ordinal))
            {
                continue;
            }

            Assert.Equal(expectedGateDecisionCode, modules[i].GateDecisionCode);

            if (expectExecuted)
            {
                Assert.True(modules[i].StartTimestamp > 0);
                Assert.True(modules[i].EndTimestamp >= modules[i].StartTimestamp);
            }
            else
            {
                Assert.Equal(0, modules[i].StartTimestamp);
                Assert.Equal(0, modules[i].EndTimestamp);
            }

            Assert.Equal(modules[i].EndTimestamp - modules[i].StartTimestamp, modules[i].DurationStopwatchTicks);
            return;
        }

        Assert.Fail($"Stage module '{moduleId}' was not recorded.");
    }

    private static void AssertStageModuleSkip(StageFanoutSnapshot snapshot, string moduleId, string code)
    {
        var skipped = snapshot.SkippedModules;

        for (var i = 0; i < skipped.Count; i++)
        {
            if (!string.Equals(skipped[i].ModuleId, moduleId, StringComparison.Ordinal))
            {
                continue;
            }

            Assert.Equal(code, skipped[i].ReasonCode);
            return;
        }

        Assert.Fail($"Stage module '{moduleId}' was not skipped.");
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
            _snapshot = new ConfigSnapshot(
                configVersion,
                patchJson,
                new ConfigSnapshotMeta(source: "static", timestampUtc: DateTimeOffset.UtcNow));
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

