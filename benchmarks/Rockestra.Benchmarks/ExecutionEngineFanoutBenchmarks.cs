using System.Globalization;
using System.Reflection;
using System.Text;
using BenchmarkDotNet.Attributes;
using Rockestra.Core;
using Rockestra.Core.Blueprint;

namespace Rockestra.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(launchCount: 1, warmupCount: 1, iterationCount: 10)]
public class ExecutionEngineFanoutBenchmarks
{
    private readonly DateTimeOffset _deadline = new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private ExecutionEngine _engine = null!;
    private PlanTemplate<int, int> _template = null!;
    private FlowContext _context = null!;

    [GlobalSetup]
    public void Setup()
    {
        var patchJson = BuildPatchJson(moduleCount: 16, fanoutMax: 8);

        var services = new DummyServiceProvider();
        _context = new FlowContext(services, CancellationToken.None, _deadline);
        WarmUpConfigSnapshot(_context, new StaticConfigProvider(configVersion: 1, patchJson));

        var catalog = new ModuleCatalog();
        catalog.Register<EmptyArgs, int>("bench.noop", _ => NoopModule.Instance);

        var blueprint = FlowBlueprint.Define<int, int>("Bench.Flow.ExecutionEngine.Fanout")
            .Stage(
                "s1",
                stage =>
                    stage.Join<int>(
                        "final",
                        _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
            .Build();

        _template = PlanCompiler.Compile(blueprint, catalog);
        _engine = new ExecutionEngine(catalog);
    }

    [Benchmark]
    public async Task<int> Execute()
    {
        var outcome = await _engine.ExecuteAsync(_template, request: 0, _context).ConfigureAwait(false);
        return outcome.IsOk ? outcome.Value : -1;
    }

    private static string BuildPatchJson(int moduleCount, int fanoutMax)
    {
        var estimated = 128 + (moduleCount * 64);

        var builder = new StringBuilder(capacity: estimated);
        builder.Append("{\"schemaVersion\":\"v1\",\"flows\":{\"Bench.Flow.ExecutionEngine.Fanout\":{\"stages\":{\"s1\":{\"fanoutMax\":");
        builder.Append(fanoutMax.ToString(CultureInfo.InvariantCulture));
        builder.Append(",\"modules\":[");

        for (var i = 0; i < moduleCount; i++)
        {
            if (i != 0)
            {
                builder.Append(',');
            }

            builder.Append("{\"id\":\"m");
            builder.Append(i.ToString(CultureInfo.InvariantCulture));
            builder.Append("\",\"use\":\"bench.noop\",\"with\":{}}");
        }

        builder.Append("]}}}}}");
        return builder.ToString();
    }

    private static void WarmUpConfigSnapshot(FlowContext context, IConfigProvider provider)
    {
        var method = typeof(FlowContext).GetMethod("GetConfigSnapshotAsync", BindingFlags.Instance | BindingFlags.NonPublic);
        if (method is null)
        {
            throw new InvalidOperationException("FlowContext.GetConfigSnapshotAsync was not found.");
        }

        var task = (ValueTask<ConfigSnapshot>)method.Invoke(context, new object?[] { provider })!;
        _ = task.GetAwaiter().GetResult();
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

    private readonly struct EmptyArgs
    {
    }

    private sealed class NoopModule : IModule<EmptyArgs, int>
    {
        public static readonly NoopModule Instance = new();

        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<EmptyArgs> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(1));
        }
    }
}

