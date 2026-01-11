using Rockestra.Core.Blueprint;
using System.Text.Json;

namespace Rockestra.Core.Tests;

public sealed class ExecutionEngineShadowTests
{
    private static readonly DateTimeOffset FutureDeadline = new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_Template_ShouldExecuteShadowModule_WithoutAffectingPrimaryResult_AndRecordExecExplain()
    {
        var patchJson = """
            {
              "schemaVersion": "v1",
              "flows": {
                "ShadowFlow": {
                  "stages": {
                    "s1": {
                      "fanoutMax": 1,
                      "modules": [
                        { "id": "m_primary", "use": "test.value", "with": {} },
                        { "id": "m_shadow", "use": "test.value", "with": {}, "shadow": { "sample": 1 } }
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

        _ = await flowContext.GetConfigSnapshotAsync(new StaticConfigProvider(configVersion: 1, patchJson));

        var catalog = new ModuleCatalog();
        catalog.Register<JsonElement, int>("test.value", _ => new IdBasedValueModule());

        var blueprint = FlowBlueprint
            .Define<int, int>("ShadowFlow")
            .Stage(
                "s1",
                contract => contract.AllowDynamicModules(),
                stage =>
                    stage.Join<int>(
                        "final",
                        ctx =>
                        {
                            var sum = 0;

                            if (ctx.TryGetNodeOutcome<int>("m_primary", out var primary) && primary.IsOk)
                            {
                                sum += primary.Value;
                            }

                            if (ctx.TryGetNodeOutcome<int>("m_shadow", out var shadow) && shadow.IsOk)
                            {
                                sum += shadow.Value;
                            }

                            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(sum));
                        }))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(template, request: 0, flowContext);

        Assert.True(result.IsOk);
        Assert.Equal(1, result.Value);

        Assert.False(flowContext.TryGetNodeOutcome<int>("m_shadow", out _));

        Assert.True(flowContext.TryGetStageFanoutSnapshot("s1", out var snapshot));
        Assert.Single(snapshot.EnabledModuleIds);
        Assert.Equal("m_primary", snapshot.EnabledModuleIds[0]);

        Assert.True(flowContext.TryGetExecExplain(out var explain));

        AssertStageModule(explain, "m_primary", expectedIsShadow: false, expectedShadowSampleBps: 0);
        AssertStageModule(explain, "m_shadow", expectedIsShadow: true, expectedShadowSampleBps: 10000);
    }

    [Fact]
    public async Task ExecuteAsync_Template_WhenStageContractDisallowsShadow_ShouldSkipShadowModuleWithoutExecuting()
    {
        var patchJson = """
            {
              "schemaVersion": "v1",
              "flows": {
                "ShadowFlow": {
                  "stages": {
                    "s1": {
                      "fanoutMax": 1,
                      "modules": [
                        { "id": "m_shadow", "use": "test.counter", "with": {}, "shadow": { "sample": 1 } }
                      ]
                    }
                  }
                }
              }
            }
            """;

        var state = new CounterState();

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        flowContext.EnableExecExplain(new ExplainOptions(ExplainLevel.Standard));

        _ = await flowContext.GetConfigSnapshotAsync(new StaticConfigProvider(configVersion: 1, patchJson));

        var catalog = new ModuleCatalog();
        catalog.Register<JsonElement, int>("test.counter", _ => new CounterModule(state));

        var blueprint = FlowBlueprint
            .Define<int, int>("ShadowFlow")
            .Stage(
                "s1",
                contract => contract.AllowDynamicModules().DisallowShadowModules(),
                stage =>
                    stage.Join<int>(
                        "final",
                        _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(template, request: 0, flowContext);

        Assert.True(result.IsOk);
        Assert.Equal(0, Volatile.Read(ref state.InvocationCount));

        Assert.True(flowContext.TryGetExecExplain(out var explain));
        AssertStageModuleSkip(explain, "m_shadow", ExecutionEngine.StageContractShadowModulesForbiddenCode, expectedShadowSampleBps: 10000);
    }

    [Fact]
    public async Task ExecuteAsync_Template_WhenStageContractShadowHardLimitIsExceeded_ShouldTrimByPriority()
    {
        var patchJson = """
            {
              "schemaVersion": "v1",
              "flows": {
                "ShadowFlow": {
                  "stages": {
                    "s1": {
                      "fanoutMax": 1,
                      "modules": [
                        { "id": "m_high", "use": "test.counter", "with": {}, "priority": 10, "shadow": { "sample": 1 } },
                        { "id": "m_low", "use": "test.counter", "with": {}, "priority": 0, "shadow": { "sample": 1 } }
                      ]
                    }
                  }
                }
              }
            }
            """;

        var state = new CounterState();

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        flowContext.EnableExecExplain(new ExplainOptions(ExplainLevel.Standard));

        _ = await flowContext.GetConfigSnapshotAsync(new StaticConfigProvider(configVersion: 1, patchJson));

        var catalog = new ModuleCatalog();
        catalog.Register<JsonElement, int>("test.counter", _ => new CounterModule(state));

        var blueprint = FlowBlueprint
            .Define<int, int>("ShadowFlow")
            .Stage(
                "s1",
                contract => contract.AllowDynamicModules().MaxShadowModules(1),
                stage =>
                    stage.Join<int>(
                        "final",
                        _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(template, request: 0, flowContext);

        Assert.True(result.IsOk);
        Assert.Equal(1, Volatile.Read(ref state.InvocationCount));

        Assert.True(flowContext.TryGetExecExplain(out var explain));
        AssertStageModuleOk(explain, "m_high", expectedIsShadow: true, expectedShadowSampleBps: 10000);
        AssertStageModuleSkip(explain, "m_low", ExecutionEngine.StageContractMaxShadowModulesHardExceededCode, expectedShadowSampleBps: 10000);
    }

    [Fact]
    public async Task ExecuteAsync_Template_WhenStageContractMaxShadowSampleBpsIsExceeded_ShouldClampAndSkipWhenNotSampled()
    {
        var patchJson = """
            {
              "schemaVersion": "v1",
              "flows": {
                "ShadowFlow": {
                  "stages": {
                    "s1": {
                      "fanoutMax": 1,
                      "modules": [
                        { "id": "m_shadow", "use": "test.counter", "with": {}, "shadow": { "sample": 1 } }
                      ]
                    }
                  }
                }
              }
            }
            """;

        var state = new CounterState();

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        flowContext.EnableExecExplain(new ExplainOptions(ExplainLevel.Standard));

        _ = await flowContext.GetConfigSnapshotAsync(new StaticConfigProvider(configVersion: 1, patchJson));

        var catalog = new ModuleCatalog();
        catalog.Register<JsonElement, int>("test.counter", _ => new CounterModule(state));

        var blueprint = FlowBlueprint
            .Define<int, int>("ShadowFlow")
            .Stage(
                "s1",
                contract => contract.AllowDynamicModules().MaxShadowSampleBps(0),
                stage =>
                    stage.Join<int>(
                        "final",
                        _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(template, request: 0, flowContext);

        Assert.True(result.IsOk);
        Assert.Equal(0, Volatile.Read(ref state.InvocationCount));

        Assert.True(flowContext.TryGetExecExplain(out var explain));
        AssertStageModuleSkip(explain, "m_shadow", ExecutionEngine.ShadowNotSampledCode, expectedShadowSampleBps: 0);
    }

    private static void AssertStageModule(ExecExplain explain, string moduleId, bool expectedIsShadow, ushort expectedShadowSampleBps)
    {
        var modules = explain.StageModules;

        for (var i = 0; i < modules.Count; i++)
        {
            if (!string.Equals(modules[i].ModuleId, moduleId, StringComparison.Ordinal))
            {
                continue;
            }

            Assert.True(modules[i].OutcomeKind == OutcomeKind.Ok);
            Assert.Equal("OK", modules[i].OutcomeCode);
            Assert.Equal(expectedIsShadow, modules[i].IsShadow);
            Assert.Equal(expectedShadowSampleBps, modules[i].ShadowSampleBps);
            Assert.True(modules[i].StartTimestamp > 0);
            Assert.True(modules[i].EndTimestamp >= modules[i].StartTimestamp);
            return;
        }

        Assert.Fail($"Stage module '{moduleId}' was not recorded.");
    }

    private static void AssertStageModuleOk(ExecExplain explain, string moduleId, bool expectedIsShadow, ushort expectedShadowSampleBps)
    {
        var modules = explain.StageModules;

        for (var i = 0; i < modules.Count; i++)
        {
            if (!string.Equals(modules[i].ModuleId, moduleId, StringComparison.Ordinal))
            {
                continue;
            }

            Assert.True(modules[i].OutcomeKind == OutcomeKind.Ok);
            Assert.Equal("OK", modules[i].OutcomeCode);
            Assert.Equal(expectedIsShadow, modules[i].IsShadow);
            Assert.Equal(expectedShadowSampleBps, modules[i].ShadowSampleBps);
            return;
        }

        Assert.Fail($"Stage module '{moduleId}' was not recorded.");
    }

    private static void AssertStageModuleSkip(ExecExplain explain, string moduleId, string expectedCode, ushort expectedShadowSampleBps)
    {
        var modules = explain.StageModules;

        for (var i = 0; i < modules.Count; i++)
        {
            if (!string.Equals(modules[i].ModuleId, moduleId, StringComparison.Ordinal))
            {
                continue;
            }

            Assert.True(modules[i].OutcomeKind == OutcomeKind.Skipped);
            Assert.Equal(expectedCode, modules[i].OutcomeCode);
            Assert.True(modules[i].IsShadow);
            Assert.Equal(expectedShadowSampleBps, modules[i].ShadowSampleBps);
            Assert.Equal(0, modules[i].StartTimestamp);
            Assert.Equal(0, modules[i].EndTimestamp);
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

    private sealed class IdBasedValueModule : IModule<JsonElement, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<JsonElement> context)
        {
            var value = string.Equals(context.ModuleId, "m_primary", StringComparison.Ordinal) ? 1 : 100;
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(value));
        }
    }

    private sealed class CounterState
    {
        public int InvocationCount;
    }

    private sealed class CounterModule : IModule<JsonElement, int>
    {
        private readonly CounterState _state;

        public CounterModule(CounterState state)
        {
            _state = state;
        }

        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<JsonElement> context)
        {
            _ = context;
            Interlocked.Increment(ref _state.InvocationCount);
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(0));
        }
    }
}


