using Rockestra.Core.Blueprint;
using Rockestra.Core.Selectors;
using Rockestra.Core.Testing;
using System.Text.Json;

namespace Rockestra.Core.Tests;

public sealed class ExecutionEngineSelectorGateTests
{
    private static readonly DateTimeOffset FutureDeadline = new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_Template_ShouldEvaluateSelectorGate_AndRecordExecExplain()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"FanoutFlow.SelectorGate\":{\"stages\":{\"s1\":{\"fanoutMax\":10,\"modules\":[" +
            "{\"id\":\"m_allow\",\"use\":\"test.ok\",\"with\":{},\"gate\":{\"selector\":\"allow\"}}," +
            "{\"id\":\"m_deny\",\"use\":\"test.ok\",\"with\":{},\"gate\":{\"selector\":\"deny\"}}," +
            "{\"id\":\"m_no_gate\",\"use\":\"test.ok\",\"with\":{}}" +
            "]}}}}}";

        var selectors = new SelectorRegistry();
        selectors.Register("allow", _ => true);
        selectors.Register("deny", _ => false);

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        flowContext.EnableExecExplain(ExplainLevel.Standard);

        var invocationCollector = new FlowTestInvocationCollector();
        flowContext.ConfigureForTesting(overrideProvider: null, invocationCollector);

        _ = await flowContext.GetConfigSnapshotAsync(new StaticConfigProvider(configVersion: 1, patchJson));

        var catalog = new ModuleCatalog();
        catalog.Register<JsonElement, int>("test.ok", _ => new OkJsonElementModule());

        var blueprint = FlowBlueprint
            .Define<int, int>("FanoutFlow.SelectorGate")
            .Stage(
                "s1",
                stage =>
                    stage.Join<int>(
                        "final",
                        _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog, selectors);

        var result = await engine.ExecuteAsync(template, request: 0, flowContext);

        Assert.True(result.IsOk);

        Assert.True(flowContext.TryGetStageFanoutSnapshot("s1", out var snapshot));
        Assert.Equal(2, snapshot.EnabledModuleIds.Count);
        Assert.Equal("m_allow", snapshot.EnabledModuleIds[0]);
        Assert.Equal("m_no_gate", snapshot.EnabledModuleIds[1]);

        Assert.Single(snapshot.SkippedModules);
        Assert.Equal("m_deny", snapshot.SkippedModules[0].ModuleId);
        Assert.Equal(ExecutionEngine.GateFalseCode, snapshot.SkippedModules[0].ReasonCode);

        Assert.True(flowContext.TryGetExecExplain(out var explain));
        Assert.Equal(ExplainLevel.Standard, explain.Level);

        AssertStageModule(explain, "m_allow", OutcomeKind.Ok, "OK", gateDecisionCode: "GATE_TRUE", gateSelectorName: "allow", isShadow: false, shadowSampleBps: 0);
        AssertStageModule(explain, "m_deny", OutcomeKind.Skipped, "GATE_FALSE", gateDecisionCode: "GATE_FALSE", gateSelectorName: "deny", isShadow: false, shadowSampleBps: 0);
        AssertStageModule(explain, "m_no_gate", OutcomeKind.Ok, "OK", gateDecisionCode: string.Empty, gateSelectorName: string.Empty, isShadow: false, shadowSampleBps: 0);

        Assert.Equal(2, invocationCollector.ToArray().Length);
    }

    private static void AssertStageModule(
        ExecExplain explain,
        string moduleId,
        OutcomeKind expectedOutcomeKind,
        string expectedOutcomeCode,
        string gateDecisionCode,
        string gateSelectorName,
        bool isShadow,
        ushort shadowSampleBps)
    {
        var modules = explain.StageModules;

        for (var i = 0; i < modules.Count; i++)
        {
            if (!string.Equals(modules[i].ModuleId, moduleId, StringComparison.Ordinal))
            {
                continue;
            }

            Assert.Equal(expectedOutcomeKind, modules[i].OutcomeKind);
            Assert.Equal(expectedOutcomeCode, modules[i].OutcomeCode);
            Assert.Equal(gateDecisionCode, modules[i].GateDecisionCode);
            Assert.Equal(gateSelectorName, modules[i].GateSelectorName);
            Assert.Equal(isShadow, modules[i].IsShadow);
            Assert.Equal(shadowSampleBps, modules[i].ShadowSampleBps);

            if (expectedOutcomeKind == OutcomeKind.Ok)
            {
                Assert.True(modules[i].StartTimestamp > 0);
                Assert.True(modules[i].EndTimestamp >= modules[i].StartTimestamp);
            }
            else
            {
                Assert.Equal(0, modules[i].StartTimestamp);
                Assert.Equal(0, modules[i].EndTimestamp);
            }

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

    private sealed class OkJsonElementModule : IModule<JsonElement, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<JsonElement> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(1));
        }
    }
}


