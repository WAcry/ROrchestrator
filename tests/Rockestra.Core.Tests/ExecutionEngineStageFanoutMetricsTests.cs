using System.Diagnostics.Metrics;
using System.Text.Json;
using Rockestra.Core.Blueprint;

namespace Rockestra.Core.Tests;

public sealed class ExecutionEngineStageFanoutMetricsTests
{
    private const string StageFanoutModuleLatencyInstrumentName = "rockestra.stage.fanout.module.latency.ms";
    private const string StageFanoutModuleOutcomeInstrumentName = "rockestra.stage.fanout.module.outcomes";

    private static readonly DateTimeOffset FutureDeadline = new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_Template_ShouldEmitStageFanoutModuleMetrics()
    {
        var patchJson = """
            {
              "schemaVersion": "v1",
              "flows": {
                "MetricsTestFlow.Fanout": {
                  "stages": {
                    "s1": {
                      "fanoutMax": 2,
                      "modules": [
                        { "id": "m1", "use": "test.ok", "with": {} },
                        { "id": "m2", "use": "test.ok", "with": {} }
                      ]
                    }
                  }
                }
              }
            }
            """;

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        _ = await flowContext.GetConfigSnapshotAsync(new StaticConfigProvider(configVersion: 123, patchJson));

        var catalog = new ModuleCatalog();
        catalog.Register<JsonElement, int>("test.ok", _ => new OkJsonElementModule());

        var blueprint = FlowBlueprint.Define<int, int>("MetricsTestFlow.Fanout")
            .Stage(
                "s1",
                contract => contract.AllowDynamicModules(),
                stage =>
                    stage.Join<int>(
                        "final",
                        _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var samples = new List<MetricSample>();
        using var listener = CreateListener(samples, expectedFlowName: template.Name);
        listener.Start();

        var result = await engine.ExecuteAsync(template, request: 0, flowContext);
        Assert.True(result.IsOk);

        Assert.Equal(2, CountSamples(samples, StageFanoutModuleOutcomeInstrumentName));
        Assert.Equal(2, CountSamples(samples, StageFanoutModuleLatencyInstrumentName));

        AssertAllSamplesHaveTag(samples, StageFanoutModuleOutcomeInstrumentName, "flow.name", template.Name);
        AssertAllSamplesHaveTag(samples, StageFanoutModuleOutcomeInstrumentName, "stage.name", "s1");
        AssertAllSamplesHaveTag(samples, StageFanoutModuleOutcomeInstrumentName, "module.type", "test.ok");
        AssertAllSamplesHaveTag(samples, StageFanoutModuleOutcomeInstrumentName, "outcome.kind", "ok");
    }

    [Fact]
    public async Task ExecuteAsync_Template_ShouldEmitStageFanoutModuleMetrics_WithExecutionPath()
    {
        var patchJson = """
            {
              "schemaVersion": "v1",
              "flows": {
                "MetricsTestFlow.FanoutShadow": {
                  "stages": {
                    "s1": {
                      "fanoutMax": 1,
                      "modules": [
                        { "id": "m_primary", "use": "test.ok", "with": {} },
                        { "id": "m_shadow", "use": "test.ok", "with": {}, "shadow": { "sample": 1 } }
                      ]
                    }
                  }
                }
              }
            }
            """;

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        _ = await flowContext.GetConfigSnapshotAsync(new StaticConfigProvider(configVersion: 123, patchJson));

        var catalog = new ModuleCatalog();
        catalog.Register<JsonElement, int>("test.ok", _ => new OkJsonElementModule());

        var blueprint = FlowBlueprint.Define<int, int>("MetricsTestFlow.FanoutShadow")
            .Stage(
                "s1",
                contract => contract.AllowDynamicModules(),
                stage =>
                    stage.Join<int>(
                        "final",
                        _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var samples = new List<MetricSample>();
        using var listener = CreateListener(samples, expectedFlowName: template.Name);
        listener.Start();

        var result = await engine.ExecuteAsync(template, request: 0, flowContext);
        Assert.True(result.IsOk);

        Assert.Equal(2, CountSamples(samples, StageFanoutModuleOutcomeInstrumentName));

        Assert.Contains(samples, sample => sample.InstrumentName == StageFanoutModuleOutcomeInstrumentName && HasTag(sample.Tags, "execution.path", "primary"));
        Assert.Contains(samples, sample => sample.InstrumentName == StageFanoutModuleOutcomeInstrumentName && HasTag(sample.Tags, "execution.path", "shadow"));
    }

    private static MeterListener CreateListener(List<MetricSample> samples, string expectedFlowName)
    {
        bool ShouldCaptureInstrumentName(string instrumentName)
        {
            return instrumentName == StageFanoutModuleLatencyInstrumentName || instrumentName == StageFanoutModuleOutcomeInstrumentName;
        }

        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Meter.Name != Observability.FlowActivitySource.ActivitySourceName)
                {
                    return;
                }

                if (!ShouldCaptureInstrumentName(instrument.Name))
                {
                    return;
                }

                meterListener.EnableMeasurementEvents(instrument);
            },
        };

        listener.SetMeasurementEventCallback<double>(
            (instrument, measurement, tags, _) =>
            {
                if (!ShouldCaptureInstrumentName(instrument.Name))
                {
                    return;
                }

                if (!TryGetTagString(tags, Observability.FlowActivitySource.TagFlowName, out var flowName) || flowName != expectedFlowName)
                {
                    return;
                }

                samples.Add(new MetricSample(instrument.Name, measurement, CopyTags(tags)));
            });

        listener.SetMeasurementEventCallback<long>(
            (instrument, measurement, tags, _) =>
            {
                if (!ShouldCaptureInstrumentName(instrument.Name))
                {
                    return;
                }

                if (!TryGetTagString(tags, Observability.FlowActivitySource.TagFlowName, out var flowName) || flowName != expectedFlowName)
                {
                    return;
                }

                samples.Add(new MetricSample(instrument.Name, measurement, CopyTags(tags)));
            });

        return listener;
    }

    private static int CountSamples(List<MetricSample> samples, string instrumentName)
    {
        var count = 0;

        for (var i = 0; i < samples.Count; i++)
        {
            if (samples[i].InstrumentName == instrumentName)
            {
                count++;
            }
        }

        return count;
    }

    private static void AssertAllSamplesHaveTag(List<MetricSample> samples, string instrumentName, string tagKey, string expectedValue)
    {
        AssertAllSamplesHaveTag(samples, instrumentName, tagKey, expectedValue, allowMissing: false);
    }

    private static void AssertAllSamplesHaveTag(
        List<MetricSample> samples,
        string instrumentName,
        string tagKey,
        string expectedValue,
        bool allowMissing)
    {
        for (var i = 0; i < samples.Count; i++)
        {
            var sample = samples[i];
            if (sample.InstrumentName != instrumentName)
            {
                continue;
            }

            if (!TryGetTagString(sample.Tags, tagKey, out var value))
            {
                Assert.True(allowMissing, $"Expected tag '{tagKey}' to be present.");
                continue;
            }

            Assert.Equal(expectedValue, value);
        }
    }

    private static bool HasTag(KeyValuePair<string, object?>[] tags, string key, string expectedValue)
    {
        return TryGetTagString(tags, key, out var value) && value == expectedValue;
    }

    private static KeyValuePair<string, object?>[] CopyTags(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        if (tags.Length == 0)
        {
            return Array.Empty<KeyValuePair<string, object?>>();
        }

        var copy = new KeyValuePair<string, object?>[tags.Length];
        for (var i = 0; i < tags.Length; i++)
        {
            copy[i] = new KeyValuePair<string, object?>(tags[i].Key, tags[i].Value);
        }

        return copy;
    }

    private static bool TryGetTagString(ReadOnlySpan<KeyValuePair<string, object?>> tags, string key, out string? value)
    {
        for (var i = 0; i < tags.Length; i++)
        {
            var tag = tags[i];
            if (tag.Key == key)
            {
                value = tag.Value as string;
                return value is not null;
            }
        }

        value = null;
        return false;
    }

    private static bool TryGetTagString(KeyValuePair<string, object?>[] tags, string key, out string? value)
    {
        for (var i = 0; i < tags.Length; i++)
        {
            var tag = tags[i];
            if (tag.Key == key)
            {
                value = tag.Value as string;
                return value is not null;
            }
        }

        value = null;
        return false;
    }

    private readonly struct MetricSample
    {
        public string InstrumentName { get; }

        public double DoubleValue { get; }

        public long LongValue { get; }

        public KeyValuePair<string, object?>[] Tags { get; }

        public bool IsDouble { get; }

        public MetricSample(string instrumentName, double measurement, KeyValuePair<string, object?>[] tags)
        {
            InstrumentName = instrumentName;
            DoubleValue = measurement;
            LongValue = default;
            Tags = tags;
            IsDouble = true;
        }

        public MetricSample(string instrumentName, long measurement, KeyValuePair<string, object?>[] tags)
        {
            InstrumentName = instrumentName;
            DoubleValue = default;
            LongValue = measurement;
            Tags = tags;
            IsDouble = false;
        }
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
            _snapshot = new ConfigSnapshot(
                configVersion,
                patchJson,
                new ConfigSnapshotMeta(source: "static", timestampUtc: DateTimeOffset.UtcNow));
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
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(0));
        }
    }
}

