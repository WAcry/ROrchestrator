using BenchmarkDotNet.Attributes;
using ROrchestrator.Core;
using ROrchestrator.Core.Selectors;
using ROrchestrator.Tooling;

namespace ROrchestrator.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(launchCount: 1, warmupCount: 1, iterationCount: 10)]
public class ToolingJsonBenchmarks
{
    private const string FlowName = "Bench.Flow.ToolingJson";

    private const string PatchJson = """
    {
      "schemaVersion": "v1",
      "flows": {
        "Bench.Flow.ToolingJson": {
          "stages": {
            "s1": {
              "fanoutMax": 2,
              "modules": [
                { "id": "m1", "use": "bench.noop", "with": {}, "priority": 0, "gate": { "selector": "is_allowed" } },
                { "id": "m2", "use": "bench.noop", "with": {}, "priority": 10, "gate": { "request": { "field": "region", "in": ["US"] } } }
              ]
            }
          }
        }
      }
    }
    """;

    private static readonly SelectorRegistry Selectors = CreateSelectors();

    private static readonly FlowRequestOptions RequestOptions = new(
        variants: null,
        userId: "u1",
        requestAttributes: new Dictionary<string, string>
        {
            { "region", "US" },
        });

    [Benchmark]
    public string ExplainPatchJson()
    {
        return ToolingJsonV1.ExplainPatchJson(
            flowName: FlowName,
            patchJson: PatchJson,
            requestOptions: RequestOptions,
            selectorRegistry: Selectors).Json;
    }

    private static SelectorRegistry CreateSelectors()
    {
        var selectors = new SelectorRegistry();
        selectors.Register("is_allowed", _ => true);
        return selectors;
    }
}

