using Rockestra.Core.Blueprint;
using System.Text.Json;

namespace Rockestra.Core.Tests;

public sealed class ExecutionEngineShadowTests
{
    private static readonly DateTimeOffset FutureDeadline = new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_Template_ShouldExecuteShadowModule_WithoutAffectingPrimaryResult_AndRecordExecExplain()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"ShadowFlow\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[" +
            "{\"id\":\"m_primary\",\"use\":\"test.value\",\"with\":{}}," +
            "{\"id\":\"m_shadow\",\"use\":\"test.value\",\"with\":{},\"shadow\":{\"sample\":1}}" +
            "]}}}}}";

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        flowContext.EnableExecExplain(ExplainLevel.Standard);

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

    private sealed class IdBasedValueModule : IModule<JsonElement, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<JsonElement> context)
        {
            var value = string.Equals(context.ModuleId, "m_primary", StringComparison.Ordinal) ? 1 : 100;
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(value));
        }
    }
}


