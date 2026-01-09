using BenchmarkDotNet.Attributes;
using ROrchestrator.Core;
using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(launchCount: 1, warmupCount: 1, iterationCount: 10)]
public class PlanCompilerBenchmarks
{
    private static readonly ModuleCatalog Catalog = CreateCatalog();
    private static readonly FlowBlueprint<int, int> Blueprint = CreateBlueprint();

    [Benchmark]
    public PlanTemplate<int, int> Compile()
    {
        return PlanCompiler.Compile(Blueprint, Catalog);
    }

    private static ModuleCatalog CreateCatalog()
    {
        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("bench.add_one", _ => NoopAddOneModule.Instance);
        return catalog;
    }

    private static FlowBlueprint<int, int> CreateBlueprint()
    {
        return FlowBlueprint.Define<int, int>("Bench.Flow.PlanCompiler")
            .Stage("s1", stage => stage.Step("step_a", "bench.add_one"))
            .Stage("s2", stage => stage.Step("step_b", "bench.add_one"))
            .Stage(
                "s3",
                stage =>
                    stage.Join<int>(
                        "final",
                        ctx =>
                        {
                            if (!ctx.TryGetNodeOutcome<int>("step_a", out var a) || !a.IsOk)
                            {
                                return new ValueTask<Outcome<int>>(Outcome<int>.Error("MISSING_A"));
                            }

                            if (!ctx.TryGetNodeOutcome<int>("step_b", out var b) || !b.IsOk)
                            {
                                return new ValueTask<Outcome<int>>(Outcome<int>.Error("MISSING_B"));
                            }

                            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(a.Value + b.Value));
                        }))
            .Build();
    }

    private sealed class NoopAddOneModule : IModule<int, int>
    {
        public static readonly NoopAddOneModule Instance = new();

        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(context.Args + 1));
        }
    }
}
