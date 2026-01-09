using System.Diagnostics.Metrics;
using ROrchestrator.Core;
using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Core.Tests;

public sealed class FlowMetricsQosAndLkgTests
{
    private const string QosTierSelectedInstrumentName = "rorchestrator.qos.tier.selected";
    private const string LkgFallbacksInstrumentName = "rorchestrator.config.lkg.fallbacks";

    private static readonly DateTimeOffset FutureDeadline = new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_ShouldEmitQosTierSelectedMetric_WithExpectedTags()
    {
        const string flowName = "qos_metric_flow";

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.ok", _ => new OkModule());

        var registry = new FlowRegistry();
        registry.Register<int, int>(flowName, CreateBlueprint(flowName));

        var services = new DummyServiceProvider();
        var context = new FlowContext(services, CancellationToken.None, FutureDeadline);

        var samples = new List<MetricSample>();
        using var listener = CreateListener(
            samples,
            instrumentName: QosTierSelectedInstrumentName,
            expectedFlowName: flowName);
        listener.Start();

        var host = new FlowHost(registry, catalog, new FixedQosTierProvider(QosTier.Conserve));
        var outcome = await host.ExecuteAsync<int, int>(flowName, request: 0, context);
        Assert.True(outcome.IsOk);

        Assert.Contains(
            samples,
            sample =>
                sample.InstrumentName == QosTierSelectedInstrumentName
                && sample.Measurement == 1
                && HasTag(sample.Tags, "flow_name", flowName)
                && HasTag(sample.Tags, "qos_tier", "conserve"));
    }

    [Fact]
    public async Task ExecuteAsync_ShouldEmitLkgFallbackMetric_WithExpectedTags()
    {
        const string flowName = "lkg_metric_flow";

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.ok", _ => new OkModule());

        var registry = new FlowRegistry();
        registry.Register<int, int>(flowName, CreateBlueprint(flowName));

        var validPatch = "{\"schemaVersion\":\"v1\",\"flows\":{}}";
        var invalidPatch = "{\"flows\":{}}";

        var configProvider = new SequenceConfigProvider(
            new ConfigSnapshot(configVersion: 1, validPatch),
            new ConfigSnapshot(configVersion: 2, invalidPatch));

        var host = new FlowHost(registry, catalog, configProvider);

        var services = new DummyServiceProvider();

        var contextA = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var outcomeA = await host.ExecuteAsync<int, int>(flowName, request: 0, contextA);
        Assert.True(outcomeA.IsOk);

        var samples = new List<MetricSample>();
        using var listener = CreateListener(
            samples,
            instrumentName: LkgFallbacksInstrumentName,
            expectedFlowName: flowName);
        listener.Start();

        var contextB = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var outcomeB = await host.ExecuteAsync<int, int>(flowName, request: 0, contextB);
        Assert.True(outcomeB.IsOk);

        Assert.True(contextB.TryGetConfigVersion(out var configVersion));
        Assert.Equal((ulong)1, configVersion);

        Assert.Contains(
            samples,
            sample =>
                sample.InstrumentName == LkgFallbacksInstrumentName
                && sample.Measurement == 1
                && HasTag(sample.Tags, "flow_name", flowName));
    }

    private static FlowBlueprint<int, int> CreateBlueprint(string flowName)
    {
        return FlowBlueprint.Define<int, int>(flowName)
            .Step("step_a", "m.ok")
            .Join<int>(
                "final",
                ctx =>
                {
                    Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var stepOutcome));
                    Assert.True(stepOutcome.IsOk);
                    return new ValueTask<Outcome<int>>(Outcome<int>.Ok(stepOutcome.Value));
                })
            .Build();
    }

    private static MeterListener CreateListener(List<MetricSample> samples, string instrumentName, string expectedFlowName)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Meter.Name != Observability.FlowActivitySource.ActivitySourceName)
                {
                    return;
                }

                if (instrument.Name != instrumentName)
                {
                    return;
                }

                meterListener.EnableMeasurementEvents(instrument);
            },
        };

        listener.SetMeasurementEventCallback<long>(
            (instrument, measurement, tags, _) =>
            {
                if (instrument.Name != instrumentName)
                {
                    return;
                }

                if (!HasTag(tags, "flow_name", expectedFlowName))
                {
                    return;
                }

                samples.Add(new MetricSample(instrument.Name, measurement, CopyTags(tags)));
            });

        return listener;
    }

    private static bool HasTag(ReadOnlySpan<KeyValuePair<string, object?>> tags, string key, string expectedValue)
    {
        return TryGetTagString(tags, key, out var value) && value == expectedValue;
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
            copy[i] = tags[i];
        }

        return copy;
    }

    private static bool TryGetTagString(ReadOnlySpan<KeyValuePair<string, object?>> tags, string key, out string? value)
    {
        for (var i = 0; i < tags.Length; i++)
        {
            if (tags[i].Key != key)
            {
                continue;
            }

            value = tags[i].Value?.ToString();
            return value is not null;
        }

        value = null;
        return false;
    }

    private sealed record MetricSample(string InstrumentName, long Measurement, KeyValuePair<string, object?>[] Tags);

    private sealed class DummyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType)
        {
            _ = serviceType;
            return null;
        }
    }

    private sealed class OkModule : IModule<int, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            _ = context;
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(0));
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

    private sealed class SequenceConfigProvider : IConfigProvider
    {
        private readonly ConfigSnapshot[] _snapshots;
        private int _nextIndex;

        public SequenceConfigProvider(params ConfigSnapshot[] snapshots)
        {
            _snapshots = snapshots;
        }

        public ValueTask<ConfigSnapshot> GetSnapshotAsync(FlowContext context)
        {
            _ = context;

            var index = Interlocked.Increment(ref _nextIndex) - 1;
            if ((uint)index < (uint)_snapshots.Length)
            {
                return new ValueTask<ConfigSnapshot>(_snapshots[index]);
            }

            return new ValueTask<ConfigSnapshot>(_snapshots[_snapshots.Length - 1]);
        }
    }
}

