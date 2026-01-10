using Rockestra.Core;
using Rockestra.Core.Blueprint;
using Rockestra.Tooling;

namespace Rockestra.Runner;

public static class RunnerApp
{
    private const string FlowName = "RunnerFlow";
    private static readonly DateTimeOffset FutureDeadline = new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    public static async Task<int> RunAsync(TextWriter writer, CancellationToken cancellationToken)
    {
        if (writer is null)
        {
            throw new ArgumentNullException(nameof(writer));
        }

        var requestOptions = new FlowRequestOptions(
            variants: new Dictionary<string, string>
            {
                { "l1", "B" },
            });

        var patchJson = BuildPatchJson();

        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();

        catalog.Register<EmptyArgs, int>("test.ok", _ => new OkModule());
        catalog.Register<int, int>("test.add_params", _ => new AddParamsModule());

        var blueprint = FlowBlueprint
            .Define<int, int>(FlowName)
            .Stage(
                "s1",
                stage =>
                    stage
                        .Step("compute", moduleType: "test.add_params")
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

        registry.Register<int, int, RunnerParams, RunnerParamsPatch>(
            FlowName,
            blueprint,
            defaultParams: new RunnerParams { Addend = 1 });

        var validate = ToolingJsonV1.ValidatePatchJson(patchJson, registry, catalog);
        writer.Write("validate:");
        writer.WriteLine(validate.Json);

        var explainFlow = ToolingJsonV1.ExplainFlowJson(FlowName, registry, catalog, includeMermaid: true);
        writer.Write("explain_flow:");
        writer.WriteLine(explainFlow.Json);

        var explainPatch = ToolingJsonV1.ExplainPatchJson(FlowName, patchJson, requestOptions, includeMermaid: true);
        writer.Write("explain_patch:");
        writer.WriteLine(explainPatch.Json);

        var diffPatch = ToolingJsonV1.DiffPatchJson(BuildOldPatchJson(), patchJson);
        writer.Write("diff_patch:");
        writer.WriteLine(diffPatch.Json);

        var configProvider = new StaticConfigProvider(configVersion: 1, patchJson);
        var host = new FlowHost(registry, catalog, configProvider);

        var services = new EmptyServiceProvider();
        var flowContext = new FlowContext(services, cancellationToken, FutureDeadline, requestOptions);
        flowContext.EnableExecExplain(ExplainLevel.Standard);

        var outcomeResult = await host.ExecuteAsync<int, int>(FlowName, request: 5, flowContext).ConfigureAwait(false);

        if (!outcomeResult.IsOk)
        {
            writer.WriteLine("execution_failed");
            writer.WriteLine(outcomeResult.Kind.ToString());
            writer.WriteLine(outcomeResult.Code);
            return 1;
        }

        if (!flowContext.TryGetExecExplain(out var execExplain))
        {
            writer.WriteLine("exec_explain_missing");
            return 1;
        }

        var execExplainJson = ExecExplainJsonV1.ExportJson(execExplain);
        writer.Write("exec_explain:");
        writer.WriteLine(execExplainJson);

        return 0;
    }

    public static Task<int> RunAsync(TextWriter writer)
    {
        return RunAsync(writer, CancellationToken.None);
    }

    private static string BuildPatchJson()
    {
        return
            "{\"schemaVersion\":\"v1\",\"flows\":{\"RunnerFlow\":{" +
            "\"params\":{\"Addend\":10}," +
            "\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[" +
            "{\"id\":\"m_disabled\",\"use\":\"test.ok\",\"with\":{},\"enabled\":false}," +
            "{\"id\":\"m_gate_false\",\"use\":\"test.ok\",\"with\":{},\"gate\":{\"experiment\":{\"layer\":\"l1\",\"in\":[\"A\"]}}}," +
            "{\"id\":\"m_high\",\"use\":\"test.ok\",\"with\":{},\"priority\":10}," +
            "{\"id\":\"m_low\",\"use\":\"test.ok\",\"with\":{},\"priority\":0}" +
            "]}}," +
            "\"experiments\":[{\"layer\":\"l1\",\"variant\":\"B\",\"patch\":{" +
            "\"params\":{\"Addend\":20}," +
            "\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m_exp\",\"use\":\"test.ok\",\"with\":{},\"priority\":5}]}}" +
            "}}]," +
            "\"emergency\":{\"reason\":\"r\",\"operator\":\"op\",\"ttl_minutes\":30,\"patch\":{" +
            "\"params\":{\"Addend\":30}," +
            "\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m_low\",\"enabled\":false}]}}" +
            "}}" +
            "}}}";
    }

    private static string BuildOldPatchJson()
    {
        return
            "{\"schemaVersion\":\"v1\",\"flows\":{\"RunnerFlow\":{" +
            "\"params\":{\"Addend\":10}," +
            "\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[" +
            "{\"id\":\"m_high\",\"use\":\"test.ok\",\"with\":{},\"priority\":10}" +
            "]}}" +
            "}}}";
    }

    private sealed class RunnerParams
    {
        public int Addend { get; init; }
    }

    private sealed class RunnerParamsPatch
    {
        public int? Addend { get; init; }
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

    private sealed class AddParamsModule : IModule<int, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            var p = context.FlowContext.Params<RunnerParams>();
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(context.Args + p.Addend));
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

    private sealed class EmptyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType)
        {
            return null;
        }
    }
}

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        _ = args;
        return await RunnerApp.RunAsync(Console.Out).ConfigureAwait(false);
    }
}

