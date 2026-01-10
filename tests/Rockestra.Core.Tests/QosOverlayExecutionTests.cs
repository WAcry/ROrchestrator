using System.Text.Json;
using Rockestra.Core.Blueprint;

namespace Rockestra.Core.Tests;

public sealed class QosOverlayExecutionTests
{
    private static readonly DateTimeOffset FutureDeadline = new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_ShouldApplyTierOverlay_ToStageFanoutPlan_AndRecordExecExplain()
    {
        const string flowName = "QosOverlayFlow";

        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"QosOverlayFlow\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":2,\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.ok\",\"with\":{}}," +
            "{\"id\":\"m2\",\"use\":\"test.ok\",\"with\":{}}" +
            "]}},\"qos\":{\"tiers\":{\"emergency\":{\"patch\":{\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[{\"id\":\"m2\",\"enabled\":false}]}}}}}}" +
            "}}}";

        var catalog = new ModuleCatalog();
        catalog.Register<JsonElement, int>("test.ok", _ => new OkModule());

        var registry = new FlowRegistry();
        registry.Register(
            flowName,
            FlowBlueprint.Define<int, int>(flowName)
                .Stage(
                    "s1",
                    stage =>
                        stage.Join<int>(
                            "final",
                            _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
                .Build());

        var services = new DummyServiceProvider();

        var configProvider = new StaticConfigProvider(configVersion: 1, patchJson);

        var hostFull = new FlowHost(registry, catalog, configProvider);

        var fullContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        fullContext.EnableExecExplain(ExplainLevel.Standard);

        var fullOutcome = await hostFull.ExecuteAsync<int, int>(flowName, request: 0, fullContext);
        Assert.True(fullOutcome.IsOk);

        Assert.True(fullContext.TryGetStageFanoutSnapshot("s1", out var fullSnapshot));
        Assert.Equal(2, fullSnapshot.EnabledModuleIds.Count);
        Assert.Equal("m1", fullSnapshot.EnabledModuleIds[0]);
        Assert.Equal("m2", fullSnapshot.EnabledModuleIds[1]);

        Assert.True(fullContext.TryGetExecExplain(out var fullExplain));
        Assert.Equal(QosTier.Full, fullExplain.QosSelectedTier);
        Assert.Single(fullExplain.OverlaysApplied);
        Assert.Equal("base", fullExplain.OverlaysApplied[0].Layer);

        var hostEmergency = new FlowHost(registry, catalog, configProvider, new FixedQosTierProvider(QosTier.Emergency));

        var emergencyContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        emergencyContext.EnableExecExplain(ExplainLevel.Standard);

        var emergencyOutcome = await hostEmergency.ExecuteAsync<int, int>(flowName, request: 0, emergencyContext);
        Assert.True(emergencyOutcome.IsOk);

        Assert.True(emergencyContext.TryGetStageFanoutSnapshot("s1", out var emergencySnapshot));
        Assert.Single(emergencySnapshot.EnabledModuleIds);
        Assert.Equal("m1", emergencySnapshot.EnabledModuleIds[0]);

        Assert.True(emergencyContext.TryGetExecExplain(out var emergencyExplain));
        Assert.Equal(QosTier.Emergency, emergencyExplain.QosSelectedTier);
        Assert.Equal(2, emergencyExplain.OverlaysApplied.Count);
        Assert.Equal("base", emergencyExplain.OverlaysApplied[0].Layer);
        Assert.Equal("qos", emergencyExplain.OverlaysApplied[1].Layer);

        AssertStageModuleSkipped(emergencyExplain, "m2", ExecutionEngine.DisabledCode);
    }

    private static void AssertStageModuleSkipped(ExecExplain explain, string moduleId, string expectedCode)
    {
        var modules = explain.StageModules;

        for (var i = 0; i < modules.Count; i++)
        {
            if (!string.Equals(modules[i].ModuleId, moduleId, StringComparison.Ordinal))
            {
                continue;
            }

            Assert.Equal(OutcomeKind.Skipped, modules[i].OutcomeKind);
            Assert.Equal(expectedCode, modules[i].OutcomeCode);
            return;
        }

        Assert.Fail($"Stage module '{moduleId}' was not recorded.");
    }

    private sealed class DummyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType)
        {
            _ = serviceType;
            return null;
        }
    }

    private sealed class OkModule : IModule<JsonElement, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<JsonElement> context)
        {
            _ = context;
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(0));
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
            _ = context;
            return new ValueTask<ConfigSnapshot>(_snapshot);
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
}


