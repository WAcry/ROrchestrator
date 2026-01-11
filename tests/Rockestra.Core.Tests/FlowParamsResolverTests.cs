using System.Text.Json;

namespace Rockestra.Core.Tests;

public sealed class FlowParamsResolverTests
{
    [Fact]
    public void TryComputeParamsHash_ShouldBeStable_AcrossDifferentJsonKeyOrders()
    {
        const string defaultJsonA = """{"b":2,"a":1,"nested":{"y":20,"x":10}}""";
        const string defaultJsonB = """{"nested":{"x":10,"y":20},"a":1,"b":2}""";

        const string flowPatchJsonA = """
            {
              "params": { "a": 5, "nested": { "x": 11 } },
              "experiments": [
                { "layer": "l1", "variant": "A", "patch": { "params": { "nested": { "y": 22 }, "exp": true } } }
              ],
              "qos": { "tiers": { "emergency": { "patch": { "params": { "qos": true } } } } },
              "emergency": { "patch": { "params": { "a": 7, "em": "on" } } }
            }
            """;

        const string flowPatchJsonB = """
            {
              "emergency": { "patch": { "params": { "em": "on", "a": 7 } } },
              "qos": { "tiers": { "emergency": { "patch": { "params": { "qos": true } } } } },
              "experiments": [
                { "variant": "A", "patch": { "params": { "exp": true, "nested": { "y": 22 } } }, "layer": "l1" }
              ],
              "params": { "nested": { "x": 11 }, "a": 5 }
            }
            """;

        var variants = new Dictionary<string, string>(capacity: 1)
        {
            { "l1", "A" },
        };

        using var defaultDocA = JsonDocument.Parse(defaultJsonA);
        using var defaultDocB = JsonDocument.Parse(defaultJsonB);
        using var flowPatchDocA = JsonDocument.Parse(flowPatchJsonA);
        using var flowPatchDocB = JsonDocument.Parse(flowPatchJsonB);

        Assert.True(
            FlowParamsResolver.TryComputeParamsHash(
                defaultDocA.RootElement,
                flowPatchDocA.RootElement,
                variants,
                qosTier: QosTier.Emergency,
                out var hashA));

        Assert.True(
            FlowParamsResolver.TryComputeParamsHash(
                defaultDocB.RootElement,
                flowPatchDocB.RootElement,
                variants,
                qosTier: QosTier.Emergency,
                out var hashB));

        Assert.Equal(hashA, hashB);
    }

    [Fact]
    public void TryComputeExplainFull_ShouldTrackSources_AndHonorResetSemantics()
    {
        const string defaultJson = """
            {
              "a": 1,
              "b": 1,
              "nested": { "x": 1, "y": 1, "keep": 1 },
              "reset": { "from_default": true, "will_be_removed": "yes" },
              "default_only": 123
            }
            """;

        const string flowPatchJson = """
            {
              "params": { "a": 2, "nested": { "x": 2 }, "reset": { "from_base": "x" } },
              "experiments": [
                { "layer": "l1", "variant": "A", "patch": { "params": { "b": 3, "nested": { "y": 3 }, "reset": 5 } } }
              ],
              "qos": {
                "tiers": {
                  "emergency": { "patch": { "params": { "b": 4, "reset": { "from_qos": "q" }, "qos_only": true } } }
                }
              },
              "emergency": { "patch": { "params": { "b": 5 } } }
            }
            """;

        var variants = new Dictionary<string, string>(capacity: 1)
        {
            { "l1", "A" },
        };

        using var defaultDoc = JsonDocument.Parse(defaultJson);
        using var flowPatchDoc = JsonDocument.Parse(flowPatchJson);

        Assert.True(
            FlowParamsResolver.TryComputeExplainFull(
                defaultDoc.RootElement,
                flowPatchDoc.RootElement,
                variants,
                qosTier: QosTier.Emergency,
                out var effectiveJsonUtf8,
                out var sources,
                out var hash));

        _ = hash;

        using var effectiveDoc = JsonDocument.Parse(effectiveJsonUtf8);
        var root = effectiveDoc.RootElement;

        Assert.Equal(2, root.GetProperty("a").GetInt32());
        Assert.Equal(5, root.GetProperty("b").GetInt32());
        Assert.Equal(123, root.GetProperty("default_only").GetInt32());
        Assert.True(root.TryGetProperty("qos_only", out var qosOnly) && qosOnly.GetBoolean());

        var nested = root.GetProperty("nested");
        Assert.Equal(2, nested.GetProperty("x").GetInt32());
        Assert.Equal(3, nested.GetProperty("y").GetInt32());
        Assert.Equal(1, nested.GetProperty("keep").GetInt32());

        var reset = root.GetProperty("reset");
        Assert.True(reset.TryGetProperty("from_qos", out var fromQos));
        Assert.Equal("q", fromQos.GetString());

        Assert.False(reset.TryGetProperty("from_default", out _));
        Assert.False(reset.TryGetProperty("will_be_removed", out _));
        Assert.False(reset.TryGetProperty("from_base", out _));

        Assert.Equal("base", FindLayer(sources, "a"));
        Assert.Equal("emergency", FindLayer(sources, "b"));
        Assert.Equal("default", FindLayer(sources, "default_only"));
        Assert.Equal("qos", FindLayer(sources, "qos_only"));
        Assert.Equal("base", FindLayer(sources, "nested.x"));
        Assert.Equal("experiment", FindLayer(sources, "nested.y"));
        Assert.Equal("default", FindLayer(sources, "nested.keep"));
        Assert.Equal("qos", FindLayer(sources, "reset.from_qos"));
    }

    private static string FindLayer(IReadOnlyList<ParamsSourceEntry> sources, string path)
    {
        for (var i = 0; i < sources.Count; i++)
        {
            var entry = sources[i];
            if (string.Equals(entry.Path, path, StringComparison.Ordinal))
            {
                return entry.Layer;
            }
        }

        throw new InvalidOperationException($"Missing source entry for path '{path}'.");
    }
}
