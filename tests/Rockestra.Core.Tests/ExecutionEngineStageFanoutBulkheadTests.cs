using System.Diagnostics.Metrics;
using System.Text.Json;
using Rockestra.Core.Blueprint;

namespace Rockestra.Core.Tests;

public sealed class ExecutionEngineStageFanoutBulkheadTests
{
    private const string StageFanoutSkipReasonInstrumentName = "rockestra.stage.fanout.module.skipped.reasons";

    private static readonly DateTimeOffset FutureDeadline = new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_Template_ShouldSkipPrimaryAndShadow_WhenBulkheadQuotaIsUnavailable()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"limits\":{\"moduleConcurrency\":{\"maxInFlight\":{\"depA\":1}}},\"flows\":{\"BulkheadFlow\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[" +
            "{\"id\":\"m_primary\",\"use\":\"test.primary\",\"with\":{},\"limitKey\":\"depA\"}," +
            "{\"id\":\"m_shadow\",\"use\":\"test.shadow\",\"with\":{},\"limitKey\":\"depA\",\"shadow\":{\"sample\":1}}" +
            "]}}}}}";

        var services = new DummyServiceProvider();

        var state = new PrimaryHoldState();

        var catalog = new ModuleCatalog();
        catalog.Register<JsonElement, int>("test.primary", _ => new PrimaryHoldModule(state));
        catalog.Register<JsonElement, int>("test.shadow", _ => new OkModule());

        var blueprint = FlowBlueprint.Define<int, int>("BulkheadFlow")
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

        var flowContextA = new FlowContext(
            services,
            CancellationToken.None,
            FutureDeadline,
            requestOptions: new FlowRequestOptions(userId: "A"));
        flowContextA.EnableExecExplain(new ExplainOptions(ExplainLevel.Standard));
        _ = await flowContextA.GetConfigSnapshotAsync(new StaticConfigProvider(configVersion: 1, patchJson));

        var taskA = engine.ExecuteAsync(template, request: 0, flowContextA).AsTask();

        var startedOrTimeout = await Task.WhenAny(state.PrimaryStarted.Task, Task.Delay(millisecondsDelay: 1000));
        Assert.Same(state.PrimaryStarted.Task, startedOrTimeout);

        var samples = new List<MetricSample>();
        var listener = CreateListener(samples, expectedFlowName: template.Name);
        listener.Start();

        var flowContextB = new FlowContext(
            services,
            CancellationToken.None,
            FutureDeadline,
            requestOptions: new FlowRequestOptions(userId: "B"));
        flowContextB.EnableExecExplain(new ExplainOptions(ExplainLevel.Standard));
        _ = await flowContextB.GetConfigSnapshotAsync(new StaticConfigProvider(configVersion: 1, patchJson));

        var resultB = await engine.ExecuteAsync(template, request: 0, flowContextB);
        Assert.True(resultB.IsOk);

        Assert.True(flowContextB.TryGetStageFanoutSnapshot("s1", out var snapshotB));
        Assert.Empty(snapshotB.EnabledModuleIds);
        Assert.Single(snapshotB.SkippedModules);
        Assert.Equal("m_primary", snapshotB.SkippedModules[0].ModuleId);
        Assert.Equal(ExecutionEngine.BulkheadRejectedCode, snapshotB.SkippedModules[0].ReasonCode);

        Assert.True(flowContextB.TryGetExecExplain(out var explainB));
        AssertStageModuleOutcome(explainB, "m_primary", OutcomeKind.Skipped, ExecutionEngine.BulkheadRejectedCode, expectedIsShadow: false);
        AssertStageModuleOutcome(explainB, "m_shadow", OutcomeKind.Skipped, ExecutionEngine.BulkheadRejectedCode, expectedIsShadow: true);

        Assert.Contains(
            samples,
            sample =>
                sample.InstrumentName == StageFanoutSkipReasonInstrumentName
                && HasTag(sample.Tags, Observability.FlowActivitySource.TagExecutionPath, Observability.FlowActivitySource.ExecutionPathPrimary)
                && HasTag(sample.Tags, Observability.FlowActivitySource.TagSkipCode, ExecutionEngine.BulkheadRejectedCode));
        Assert.Contains(
            samples,
            sample =>
                sample.InstrumentName == StageFanoutSkipReasonInstrumentName
                && HasTag(sample.Tags, Observability.FlowActivitySource.TagExecutionPath, Observability.FlowActivitySource.ExecutionPathShadow)
                && HasTag(sample.Tags, Observability.FlowActivitySource.TagSkipCode, ExecutionEngine.BulkheadRejectedCode));

        listener.Dispose();

        state.ReleasePrimary.TrySetResult();
        var resultA = await taskA;
        Assert.True(resultA.IsOk);
    }

    private static void AssertStageModuleOutcome(
        ExecExplain explain,
        string moduleId,
        OutcomeKind expectedKind,
        string expectedCode,
        bool expectedIsShadow)
    {
        var modules = explain.StageModules;

        for (var i = 0; i < modules.Count; i++)
        {
            if (!string.Equals(modules[i].ModuleId, moduleId, StringComparison.Ordinal))
            {
                continue;
            }

            Assert.Equal(expectedKind, modules[i].OutcomeKind);
            Assert.Equal(expectedCode, modules[i].OutcomeCode);
            Assert.Equal(expectedIsShadow, modules[i].IsShadow);
            Assert.Equal(0, modules[i].StartTimestamp);
            Assert.Equal(0, modules[i].EndTimestamp);
            return;
        }

        Assert.Fail($"Stage module '{moduleId}' was not recorded.");
    }

    private static MeterListener CreateListener(List<MetricSample> samples, string expectedFlowName)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Meter.Name != Observability.FlowActivitySource.ActivitySourceName)
                {
                    return;
                }

                if (instrument.Name != StageFanoutSkipReasonInstrumentName)
                {
                    return;
                }

                meterListener.EnableMeasurementEvents(instrument);
            },
        };

        listener.SetMeasurementEventCallback<long>(
            (instrument, measurement, tags, _) =>
            {
                _ = measurement;

                if (instrument.Name != StageFanoutSkipReasonInstrumentName)
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
            _ = context;
            return new ValueTask<ConfigSnapshot>(_snapshot);
        }
    }

    private sealed class PrimaryHoldState
    {
        public TaskCompletionSource PrimaryStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource ReleasePrimary { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class PrimaryHoldModule : IModule<JsonElement, int>
    {
        private readonly PrimaryHoldState _state;

        public PrimaryHoldModule(PrimaryHoldState state)
        {
            _state = state;
        }

        public async ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<JsonElement> context)
        {
            if (string.Equals(context.FlowContext.UserId, "A", StringComparison.Ordinal))
            {
                _state.PrimaryStarted.TrySetResult();
                await _state.ReleasePrimary.Task.ConfigureAwait(false);
            }

            return Outcome<int>.Ok(0);
        }
    }

    private sealed class OkModule : IModule<JsonElement, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<JsonElement> context)
        {
            _ = context;
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(0));
        }
    }
}

