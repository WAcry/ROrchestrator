using BenchmarkDotNet.Attributes;
using ROrchestrator.Core;

namespace ROrchestrator.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(launchCount: 1, warmupCount: 1, iterationCount: 10)]
public class PatchEvaluatorBenchmarks
{
    private const ulong ConfigVersion = 1;

    private const string PatchJson = """
    {
      "schemaVersion": "v1",
      "flows": {
        "Bench.Flow.PatchEvaluator": {
          "stages": {
            "s1": {
              "fanoutMax": 4,
              "modules": [
                { "id": "m1", "use": "bench.noop", "with": {} },
                { "id": "m2", "use": "bench.noop", "with": {} },
                { "id": "m3", "use": "bench.noop", "with": {} },
                { "id": "m4", "use": "bench.noop", "with": {} }
              ]
            }
          },
          "experiments": [
            {
              "layer": "l1",
              "variant": "A",
              "patch": {
                "stages": {
                  "s1": {
                    "fanoutMax": 2,
                    "modules": [
                      { "id": "m5", "use": "bench.noop", "with": {} }
                    ]
                  }
                }
              }
            }
          ],
          "emergency": {
            "reason": "r",
            "operator": "op",
            "ttl_minutes": 30,
            "patch": {
              "stages": {
                "s1": {
                  "modules": [
                    { "id": "m3", "enabled": false }
                  ]
                }
              }
            }
          }
        }
      }
    }
    """;

    private static readonly FlowRequestOptions RequestOptions = new(
        variants: new Dictionary<string, string>
        {
            { "l1", "A" },
        },
        userId: "u1",
        requestAttributes: new Dictionary<string, string>
        {
            { "region", "US" },
        });

    [GlobalSetup]
    public void Setup()
    {
        using var _ = PatchEvaluatorV1.Evaluate("Bench.Flow.PatchEvaluator", PatchJson, RequestOptions, configVersion: ConfigVersion);
    }

    [Benchmark(Baseline = true)]
    public int Evaluate_NoCache()
    {
        using var evaluation = PatchEvaluatorV1.Evaluate("Bench.Flow.PatchEvaluator", PatchJson, RequestOptions, configVersion: 0);
        return evaluation.Stages.Count;
    }

    [Benchmark]
    public int Evaluate_Cached()
    {
        using var evaluation = PatchEvaluatorV1.Evaluate("Bench.Flow.PatchEvaluator", PatchJson, RequestOptions, configVersion: ConfigVersion);
        return evaluation.Stages.Count;
    }
}
