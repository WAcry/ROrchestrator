using System.Text;
using System.Text.Json;
using ROrchestrator.Core;
using ROrchestrator.Core.Blueprint;
using ROrchestrator.Tooling;

namespace ROrchestrator.Tooling.Tests;

public sealed class ExecExplainJsonV2Tests
{
    private static readonly DateTimeOffset FutureDeadline = new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExportJson_ShouldMatchGoldenFile_AfterTimingNormalization()
    {
        const string flowName = "ExecExplainFlow";

        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();

        catalog.Register<EmptyArgs, int>("test.ok", _ => new OkModule());
        catalog.Register<int, int>("test.compute", _ => new ComputeModule());

        var blueprint = FlowBlueprint
            .Define<int, int>(flowName)
            .Stage(
                "s1",
                stage =>
                    stage
                        .Step("compute", moduleType: "test.compute")
                        .Join<int>(
                            "final",
                            ctx =>
                            {
                                if (!ctx.TryGetNodeOutcome<int>("compute", out var outcome))
                                {
                                    throw new InvalidOperationException("Outcome for node 'compute' has not been recorded.");
                                }

                                return new ValueTask<Outcome<int>>(outcome);
                            }))
            .Build();

        registry.Register(flowName, blueprint);

        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"ExecExplainFlow\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":3,\"modules\":[" +
            "{\"id\":\"m_disabled\",\"use\":\"test.ok\",\"with\":{}}," +
            "{\"id\":\"m_gate_false\",\"use\":\"test.ok\",\"with\":{},\"gate\":{\"experiment\":{\"layer\":\"l1\",\"in\":[\"B\"]}}}," +
            "{\"id\":\"m_high\",\"use\":\"test.ok\",\"with\":{},\"limitKey\":\"depA\",\"priority\":10}," +
            "{\"id\":\"m_low\",\"use\":\"test.ok\",\"with\":{},\"priority\":0}," +
            "{\"id\":\"m_shadow\",\"use\":\"test.ok\",\"with\":{},\"shadow\":{\"sample\":1}}" +
            "]}},\"experiments\":[{\"layer\":\"l1\",\"variant\":\"A\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[" +
            "{\"id\":\"m_exp\",\"use\":\"test.ok\",\"with\":{},\"priority\":5}" +
            "]}}}}]," +
            "\"emergency\":{\"reason\":\"r\",\"operator\":\"op\",\"ttl_minutes\":30,\"patch\":{\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[{\"id\":\"m_disabled\",\"enabled\":false}]}}}}" +
            "}}}";

        var host = new FlowHost(registry, catalog, new StaticConfigProvider(configVersion: 42, patchJson));

        var requestOptions = new FlowRequestOptions(
            variants: new Dictionary<string, string>
            {
                { "l2", "ignored" },
                { "l1", "A" },
            },
            userId: "u1");

        var flowContext = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline, requestOptions);
        flowContext.EnableExecExplain(ExplainLevel.Standard);

        var outcome = await host.ExecuteAsync<int, int>(flowName, request: 5, flowContext);
        Assert.True(outcome.IsOk);

        Assert.True(flowContext.TryGetExecExplain(out var explain));

        var json = ExecExplainJsonV2.ExportJson(explain);
        var normalized = JsonTimingNormalizer.NormalizeDurationFields(json);
        var expected = ReadGoldenFile("exec_explain_json_v2.json");

        Assert.Equal(expected, normalized);
    }

    [Fact]
    public async Task ExportJson_ShouldIncludeTiming_ForFlowNodesAndStageModules()
    {
        const string flowName = "ExecExplainTimingFlow";

        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();

        catalog.Register<EmptyArgs, int>("test.ok", _ => new OkModule());
        catalog.Register<int, int>("test.compute", _ => new ComputeModule());

        var blueprint = FlowBlueprint
            .Define<int, int>(flowName)
            .Stage(
                "s1",
                stage =>
                    stage
                        .Step("compute", moduleType: "test.compute")
                        .Join<int>(
                            "final",
                            ctx =>
                            {
                                if (!ctx.TryGetNodeOutcome<int>("compute", out var outcome))
                                {
                                    throw new InvalidOperationException("Outcome for node 'compute' has not been recorded.");
                                }

                                return new ValueTask<Outcome<int>>(outcome);
                            }))
            .Build();

        registry.Register(flowName, blueprint);

        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"" + flowName + "\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.ok\",\"with\":{}}" +
            "]}}}}}";

        var host = new FlowHost(registry, catalog, new StaticConfigProvider(configVersion: 1, patchJson));

        var flowContext = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline);
        flowContext.EnableExecExplain(ExplainLevel.Standard);

        var outcome = await host.ExecuteAsync<int, int>(flowName, request: 5, flowContext);
        Assert.True(outcome.IsOk);

        Assert.True(flowContext.TryGetExecExplain(out var explain));

        var json = ExecExplainJsonV2.ExportJson(explain);

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        var timing = root.GetProperty("timing");
        Assert.True(timing.GetProperty("duration_ticks").GetInt64() >= 0);
        Assert.True(timing.GetProperty("duration_ms").GetDouble() >= 0);

        var nodes = root.GetProperty("nodes");
        Assert.True(nodes.GetArrayLength() > 0);

        foreach (var node in nodes.EnumerateArray())
        {
            var nodeTiming = node.GetProperty("timing");
            Assert.True(nodeTiming.GetProperty("duration_ticks").GetInt64() >= 0);
            Assert.True(nodeTiming.GetProperty("duration_ms").GetDouble() >= 0);
        }

        var stageModules = root.GetProperty("stage_modules");
        Assert.True(stageModules.GetArrayLength() > 0);

        foreach (var stageModule in stageModules.EnumerateArray())
        {
            var stageTiming = stageModule.GetProperty("timing");
            Assert.True(stageTiming.GetProperty("duration_ticks").GetInt64() >= 0);
            Assert.True(stageTiming.GetProperty("duration_ms").GetDouble() >= 0);
        }
    }

    private static string ReadGoldenFile(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Golden", fileName);
        return File.ReadAllText(path, Encoding.UTF8).TrimEnd('\r', '\n');
    }

    private sealed class EmptyArgs
    {
    }

    private sealed class OkModule : IModule<EmptyArgs, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<EmptyArgs> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(1));
        }
    }

    private sealed class ComputeModule : IModule<int, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(context.Args + 1));
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

    private sealed class EmptyServiceProvider : IServiceProvider
    {
        public static readonly EmptyServiceProvider Instance = new();

        private EmptyServiceProvider()
        {
        }

        public object? GetService(Type serviceType)
        {
            _ = serviceType;
            return null;
        }
    }
}
