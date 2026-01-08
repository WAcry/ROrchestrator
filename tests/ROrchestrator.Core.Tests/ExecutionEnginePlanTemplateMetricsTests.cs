using System.Diagnostics.Metrics;
using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Core.Tests;

public sealed class ExecutionEnginePlanTemplateMetricsTests
{
    private const string FlowLatencyInstrumentName = "rorchestrator.flow.latency.ms";
    private const string FlowOutcomeInstrumentName = "rorchestrator.flow.outcomes";
    private const string StepLatencyInstrumentName = "rorchestrator.step.latency.ms";
    private const string StepOutcomeInstrumentName = "rorchestrator.step.outcomes";
    private const string StepSkipReasonInstrumentName = "rorchestrator.step.skipped.reasons";
    private const string JoinLatencyInstrumentName = "rorchestrator.join.latency.ms";
    private const string JoinOutcomeInstrumentName = "rorchestrator.join.outcomes";

    private const string SkipCodeTagKey = Observability.FlowActivitySource.TagSkipCode;

    private static readonly DateTimeOffset FutureDeadline = new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset PastDeadline = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_Template_ShouldEmitMetrics_OnlyWhenMeterListenerIsStarted()
    {
        var services = new DummyServiceProvider();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        var blueprint = FlowBlueprint.Define<int, int>("MetricsTestFlow.Success")
            .Step("step_a", "m.add_one")
            .Join<int>(
                "final",
                ctx =>
                {
                    Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var stepOutcome));
                    Assert.True(stepOutcome.IsOk);
                    return new ValueTask<Outcome<int>>(Outcome<int>.Ok(stepOutcome.Value + 10));
                })
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var samples = new List<MetricSample>();
        using var listener = CreateListener(samples, expectedFlowName: template.Name);

        var firstContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var firstResult = await engine.ExecuteAsync(template, request: 5, firstContext);
        Assert.True(firstResult.IsOk);
        Assert.Equal(16, firstResult.Value);

        Assert.Empty(samples);

        listener.Start();

        var secondContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var secondResult = await engine.ExecuteAsync(template, request: 5, secondContext);
        Assert.True(secondResult.IsOk);
        Assert.Equal(16, secondResult.Value);

        AssertHasSingleSample(samples, FlowLatencyInstrumentName, out var flowLatency);
        AssertHasSingleSample(samples, FlowOutcomeInstrumentName, out var flowOutcome);
        AssertHasSingleSample(samples, StepLatencyInstrumentName, out var stepLatency);
        AssertHasSingleSample(samples, StepOutcomeInstrumentName, out var stepOutcome);
        AssertHasSingleSample(samples, JoinLatencyInstrumentName, out var joinLatency);
        AssertHasSingleSample(samples, JoinOutcomeInstrumentName, out var joinOutcome);

        AssertFlowMetricTags(flowLatency.Tags, template.Name, expectedOutcomeKind: "ok");
        AssertFlowMetricTags(flowOutcome.Tags, template.Name, expectedOutcomeKind: "ok");

        AssertStepMetricTags(stepLatency.Tags, template.Name, expectedModuleType: "m.add_one", expectedOutcomeKind: "ok");
        AssertStepMetricTags(stepOutcome.Tags, template.Name, expectedModuleType: "m.add_one", expectedOutcomeKind: "ok");

        AssertJoinMetricTags(joinLatency.Tags, template.Name, expectedOutcomeKind: "ok");
        AssertJoinMetricTags(joinOutcome.Tags, template.Name, expectedOutcomeKind: "ok");
    }

    [Fact]
    public async Task ExecuteAsync_Template_ShouldRecordFlowOutcome_Timeout_WhenDeadlineExceededBeforeStart()
    {
        var services = new DummyServiceProvider();

        var blueprint = FlowBlueprint.Define<int, int>("MetricsTestFlow.Timeout.BeforeStart")
            .Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(1)))
            .Build();

        var template = PlanCompiler.Compile(blueprint, new ModuleCatalog());
        var engine = new ExecutionEngine(new ModuleCatalog());

        var samples = new List<MetricSample>();
        using var listener = CreateListener(samples, expectedFlowName: template.Name);
        listener.Start();

        var context = new FlowContext(services, CancellationToken.None, PastDeadline);
        var result = await engine.ExecuteAsync(template, request: 1, context);

        Assert.True(result.IsTimeout);

        AssertHasSingleSample(samples, FlowLatencyInstrumentName, out var flowLatency);
        AssertHasSingleSample(samples, FlowOutcomeInstrumentName, out var flowOutcome);
        AssertNoSample(samples, StepLatencyInstrumentName);
        AssertNoSample(samples, StepOutcomeInstrumentName);
        AssertNoSample(samples, StepSkipReasonInstrumentName);
        AssertNoSample(samples, JoinLatencyInstrumentName);
        AssertNoSample(samples, JoinOutcomeInstrumentName);

        AssertFlowMetricTags(flowLatency.Tags, template.Name, expectedOutcomeKind: "timeout");
        AssertFlowMetricTags(flowOutcome.Tags, template.Name, expectedOutcomeKind: "timeout");
    }

    [Fact]
    public async Task ExecuteAsync_Template_ShouldRecordFlowOutcome_Canceled_WhenCancellationIsRequestedBeforeStart()
    {
        var services = new DummyServiceProvider();

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var blueprint = FlowBlueprint.Define<int, int>("MetricsTestFlow.Canceled.BeforeStart")
            .Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(1)))
            .Build();

        var template = PlanCompiler.Compile(blueprint, new ModuleCatalog());
        var engine = new ExecutionEngine(new ModuleCatalog());

        var samples = new List<MetricSample>();
        using var listener = CreateListener(samples, expectedFlowName: template.Name);
        listener.Start();

        var context = new FlowContext(services, cts.Token, FutureDeadline);
        var result = await engine.ExecuteAsync(template, request: 1, context);

        Assert.True(result.IsCanceled);

        AssertHasSingleSample(samples, FlowLatencyInstrumentName, out var flowLatency);
        AssertHasSingleSample(samples, FlowOutcomeInstrumentName, out var flowOutcome);
        AssertNoSample(samples, StepLatencyInstrumentName);
        AssertNoSample(samples, StepOutcomeInstrumentName);
        AssertNoSample(samples, StepSkipReasonInstrumentName);
        AssertNoSample(samples, JoinLatencyInstrumentName);
        AssertNoSample(samples, JoinOutcomeInstrumentName);

        AssertFlowMetricTags(flowLatency.Tags, template.Name, expectedOutcomeKind: "canceled");
        AssertFlowMetricTags(flowOutcome.Tags, template.Name, expectedOutcomeKind: "canceled");
    }

    [Fact]
    public async Task ExecuteAsync_Template_ShouldRecordStepOutcome_Error_WhenModuleThrows()
    {
        var services = new DummyServiceProvider();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.boom", _ => new ThrowingModule());

        var blueprint = FlowBlueprint.Define<int, int>("MetricsTestFlow.Step.Error")
            .Step("step_a", "m.boom")
            .Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(1)))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var samples = new List<MetricSample>();
        using var listener = CreateListener(samples, expectedFlowName: template.Name);
        listener.Start();

        var context = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var result = await engine.ExecuteAsync(template, request: 1, context);
        Assert.True(result.IsOk);

        AssertHasSingleSample(samples, StepLatencyInstrumentName, out var stepLatency);
        AssertHasSingleSample(samples, StepOutcomeInstrumentName, out var stepOutcome);
        AssertNoSample(samples, StepSkipReasonInstrumentName);

        AssertStepMetricTags(stepLatency.Tags, template.Name, expectedModuleType: "m.boom", expectedOutcomeKind: "error");
        AssertStepMetricTags(stepOutcome.Tags, template.Name, expectedModuleType: "m.boom", expectedOutcomeKind: "error");
    }

    [Fact]
    public async Task ExecuteAsync_Template_ShouldRecordStepOutcome_Canceled_WhenModuleThrowsOperationCanceledException()
    {
        var services = new DummyServiceProvider();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.cancel", _ => new CancelingModule());

        var blueprint = FlowBlueprint.Define<int, int>("MetricsTestFlow.Step.Canceled")
            .Step("step_a", "m.cancel")
            .Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(1)))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var samples = new List<MetricSample>();
        using var listener = CreateListener(samples, expectedFlowName: template.Name);
        listener.Start();

        var context = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var result = await engine.ExecuteAsync(template, request: 1, context);
        Assert.True(result.IsOk);

        AssertHasSingleSample(samples, StepLatencyInstrumentName, out var stepLatency);
        AssertHasSingleSample(samples, StepOutcomeInstrumentName, out var stepOutcome);
        AssertNoSample(samples, StepSkipReasonInstrumentName);

        AssertStepMetricTags(stepLatency.Tags, template.Name, expectedModuleType: "m.cancel", expectedOutcomeKind: "canceled");
        AssertStepMetricTags(stepOutcome.Tags, template.Name, expectedModuleType: "m.cancel", expectedOutcomeKind: "canceled");
    }

    [Fact]
    public async Task ExecuteAsync_Template_ShouldRecordJoinOutcome_Canceled_WhenJoinThrowsOperationCanceledException()
    {
        var services = new DummyServiceProvider();

        var blueprint = FlowBlueprint.Define<int, int>("MetricsTestFlow.Join.Canceled")
            .Join<int>("final", _ => throw new OperationCanceledException())
            .Build();

        var template = PlanCompiler.Compile(blueprint, new ModuleCatalog());
        var engine = new ExecutionEngine(new ModuleCatalog());

        var samples = new List<MetricSample>();
        using var listener = CreateListener(samples, expectedFlowName: template.Name);
        listener.Start();

        var context = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var result = await engine.ExecuteAsync(template, request: 1, context);
        Assert.True(result.IsCanceled);

        AssertHasSingleSample(samples, FlowOutcomeInstrumentName, out var flowOutcome);
        AssertHasSingleSample(samples, JoinOutcomeInstrumentName, out var joinOutcome);

        AssertFlowMetricTags(flowOutcome.Tags, template.Name, expectedOutcomeKind: "canceled");
        AssertJoinMetricTags(joinOutcome.Tags, template.Name, expectedOutcomeKind: "canceled");
    }

    [Fact]
    public async Task ExecuteAsync_Template_ShouldRecordJoinOutcome_Error_WhenJoinThrows()
    {
        var services = new DummyServiceProvider();

        var blueprint = FlowBlueprint.Define<int, int>("MetricsTestFlow.Join.Error")
            .Join<int>("final", _ => throw new InvalidOperationException("boom"))
            .Build();

        var template = PlanCompiler.Compile(blueprint, new ModuleCatalog());
        var engine = new ExecutionEngine(new ModuleCatalog());

        var samples = new List<MetricSample>();
        using var listener = CreateListener(samples, expectedFlowName: template.Name);
        listener.Start();

        var context = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var result = await engine.ExecuteAsync(template, request: 1, context);
        Assert.True(result.IsError);

        AssertHasSingleSample(samples, FlowOutcomeInstrumentName, out var flowOutcome);
        AssertHasSingleSample(samples, JoinOutcomeInstrumentName, out var joinOutcome);

        AssertFlowMetricTags(flowOutcome.Tags, template.Name, expectedOutcomeKind: "error");
        AssertJoinMetricTags(joinOutcome.Tags, template.Name, expectedOutcomeKind: "error");
    }

    [Fact]
    public async Task ExecuteAsync_Template_ShouldRecordStepOutcome_Skipped_AndEmitSkipReasonMetric()
    {
        var services = new DummyServiceProvider();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.skip", _ => new SkippingModule(code: "GATE_FALSE"));

        var blueprint = FlowBlueprint.Define<int, int>("MetricsTestFlow.Step.Skipped")
            .Step("step_a", "m.skip")
            .Join<int>(
                "final",
                ctx =>
                {
                    Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var stepOutcome));
                    Assert.True(stepOutcome.IsSkipped);
                    return new ValueTask<Outcome<int>>(Outcome<int>.Ok(1));
                })
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var samples = new List<MetricSample>();
        using var listener = CreateListener(samples, expectedFlowName: template.Name);

        var firstContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var firstResult = await engine.ExecuteAsync(template, request: 1, firstContext);
        Assert.True(firstResult.IsOk);
        Assert.Empty(samples);

        listener.Start();

        var secondContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var secondResult = await engine.ExecuteAsync(template, request: 1, secondContext);
        Assert.True(secondResult.IsOk);

        AssertHasSingleSample(samples, StepOutcomeInstrumentName, out var stepOutcome);
        AssertStepMetricTags(stepOutcome.Tags, template.Name, expectedModuleType: "m.skip", expectedOutcomeKind: "skipped");

        AssertHasSingleSample(samples, StepSkipReasonInstrumentName, out var skipReason);
        AssertSkipReasonMetricTags(skipReason.Tags, template.Name, expectedCode: "GATE_FALSE");
        Assert.Equal(1, skipReason.LongValue);
    }

    [Fact]
    public async Task ExecuteAsync_Template_ShouldRecordStepOutcome_Fallback_WhenModuleReturnsFallback()
    {
        var services = new DummyServiceProvider();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.fallback", _ => new FallbackModule(value: 42, code: "CACHE_HIT"));

        var blueprint = FlowBlueprint.Define<int, int>("MetricsTestFlow.Step.Fallback")
            .Step("step_a", "m.fallback")
            .Join<int>(
                "final",
                ctx =>
                {
                    Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var stepOutcome));
                    Assert.True(stepOutcome.IsFallback);
                    Assert.Equal(42, stepOutcome.Value);
                    return new ValueTask<Outcome<int>>(Outcome<int>.Ok(stepOutcome.Value));
                })
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var samples = new List<MetricSample>();
        using var listener = CreateListener(samples, expectedFlowName: template.Name);
        listener.Start();

        var context = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var result = await engine.ExecuteAsync(template, request: 1, context);
        Assert.True(result.IsOk);
        Assert.Equal(42, result.Value);

        AssertHasSingleSample(samples, StepOutcomeInstrumentName, out var stepOutcomeMetric);
        AssertStepMetricTags(stepOutcomeMetric.Tags, template.Name, expectedModuleType: "m.fallback", expectedOutcomeKind: "fallback");
        AssertNoSample(samples, StepSkipReasonInstrumentName);
    }

    [Fact]
    public async Task ExecuteAsync_Template_ShouldRecordFlowAndJoinOutcome_Fallback_WhenJoinReturnsFallback()
    {
        var services = new DummyServiceProvider();

        var blueprint = FlowBlueprint.Define<int, int>("MetricsTestFlow.Join.Fallback")
            .Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Fallback(42, code: "DEGRADED")))
            .Build();

        var template = PlanCompiler.Compile(blueprint, new ModuleCatalog());
        var engine = new ExecutionEngine(new ModuleCatalog());

        var samples = new List<MetricSample>();
        using var listener = CreateListener(samples, expectedFlowName: template.Name);
        listener.Start();

        var context = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var result = await engine.ExecuteAsync(template, request: 1, context);
        Assert.True(result.IsFallback);
        Assert.Equal(42, result.Value);
        Assert.Equal("DEGRADED", result.Code);

        AssertHasSingleSample(samples, FlowOutcomeInstrumentName, out var flowOutcome);
        AssertFlowMetricTags(flowOutcome.Tags, template.Name, expectedOutcomeKind: "fallback");

        AssertHasSingleSample(samples, JoinOutcomeInstrumentName, out var joinOutcome);
        AssertJoinMetricTags(joinOutcome.Tags, template.Name, expectedOutcomeKind: "fallback");

        AssertNoSample(samples, StepOutcomeInstrumentName);
        AssertNoSample(samples, StepSkipReasonInstrumentName);
    }

    [Fact]
    public async Task ExecuteAsync_Template_ShouldEmitSkipReasonMetric_WhenOnlySkipReasonInstrumentIsEnabled()
    {
        var services = new DummyServiceProvider();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.skip", _ => new SkippingModule(code: "GATE_12345"));

        var blueprint = FlowBlueprint.Define<int, int>("MetricsTestFlow.Step.Skipped.OnlySkipReason")
            .Step("step_a", "m.skip")
            .Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(1)))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var samples = new List<MetricSample>();
        using var listener = CreateListener(samples, expectedFlowName: template.Name, instrumentNameFilter: StepSkipReasonInstrumentName);
        listener.Start();

        var context = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var result = await engine.ExecuteAsync(template, request: 1, context);
        Assert.True(result.IsOk);

        AssertHasSingleSample(samples, StepSkipReasonInstrumentName, out var skipReason);
        AssertSkipReasonMetricTags(skipReason.Tags, template.Name, expectedCode: "GATE_12345");
        Assert.Equal(1, skipReason.LongValue);

        AssertNoSample(samples, FlowLatencyInstrumentName);
        AssertNoSample(samples, FlowOutcomeInstrumentName);
        AssertNoSample(samples, StepLatencyInstrumentName);
        AssertNoSample(samples, StepOutcomeInstrumentName);
        AssertNoSample(samples, JoinLatencyInstrumentName);
        AssertNoSample(samples, JoinOutcomeInstrumentName);
    }

    [Fact]
    public async Task ExecuteAsync_Template_ShouldApplyDimensionProtection_ToSkipReasonCode()
    {
        var services = new DummyServiceProvider();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.skip", _ => new SkippingModule(code: "user:1234567890123456789012345678901234567890"));

        var blueprint = FlowBlueprint.Define<int, int>("MetricsTestFlow.Step.Skipped.HighCard")
            .Step("step_a", "m.skip")
            .Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(1)))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var samples = new List<MetricSample>();
        using var listener = CreateListener(samples, expectedFlowName: template.Name);
        listener.Start();

        var context = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var result = await engine.ExecuteAsync(template, request: 1, context);
        Assert.True(result.IsOk);

        AssertHasSingleSample(samples, StepSkipReasonInstrumentName, out var skipReason);
        AssertSkipReasonMetricTags(skipReason.Tags, template.Name, expectedCode: "OTHER");
    }

    [Fact]
    public async Task ExecuteAsync_Template_ShouldApplyDimensionProtection_ToSkipReasonCode_WhenDigitRunIsTooLong()
    {
        var services = new DummyServiceProvider();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.skip", _ => new SkippingModule(code: "USER_123456"));

        var blueprint = FlowBlueprint.Define<int, int>("MetricsTestFlow.Step.Skipped.HighCard.DigitRun")
            .Step("step_a", "m.skip")
            .Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(1)))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var samples = new List<MetricSample>();
        using var listener = CreateListener(samples, expectedFlowName: template.Name);
        listener.Start();

        var context = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var result = await engine.ExecuteAsync(template, request: 1, context);
        Assert.True(result.IsOk);

        AssertHasSingleSample(samples, StepSkipReasonInstrumentName, out var skipReason);
        AssertSkipReasonMetricTags(skipReason.Tags, template.Name, expectedCode: "OTHER");
    }

    [Fact]
    public async Task ExecuteAsync_Template_ShouldApplyDimensionProtection_ToSkipReasonCode_WhenCodeIsTooLong()
    {
        var services = new DummyServiceProvider();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.skip", _ => new SkippingModule(code: new string('A', 65)));

        var blueprint = FlowBlueprint.Define<int, int>("MetricsTestFlow.Step.Skipped.HighCard.Length")
            .Step("step_a", "m.skip")
            .Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(1)))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var samples = new List<MetricSample>();
        using var listener = CreateListener(samples, expectedFlowName: template.Name);
        listener.Start();

        var context = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var result = await engine.ExecuteAsync(template, request: 1, context);
        Assert.True(result.IsOk);

        AssertHasSingleSample(samples, StepSkipReasonInstrumentName, out var skipReason);
        AssertSkipReasonMetricTags(skipReason.Tags, template.Name, expectedCode: "OTHER");
    }

    private static MeterListener CreateListener(List<MetricSample> samples, string expectedFlowName)
    {
        return CreateListener(samples, expectedFlowName, instrumentNameFilter: null);
    }

    private static MeterListener CreateListener(List<MetricSample> samples, string expectedFlowName, string? instrumentNameFilter)
    {
        bool ShouldCaptureInstrumentName(string instrumentName)
        {
            if (instrumentNameFilter is not null)
            {
                return instrumentName == instrumentNameFilter;
            }

            return IsSupportedInstrumentName(instrumentName);
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

    private static bool IsSupportedInstrumentName(string instrumentName)
    {
        return instrumentName is FlowLatencyInstrumentName
            or FlowOutcomeInstrumentName
            or StepLatencyInstrumentName
            or StepOutcomeInstrumentName
            or StepSkipReasonInstrumentName
            or JoinLatencyInstrumentName
            or JoinOutcomeInstrumentName;
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

    private static void AssertHasSingleSample(List<MetricSample> samples, string instrumentName, out MetricSample sample)
    {
        sample = default;

        var found = false;
        for (var i = 0; i < samples.Count; i++)
        {
            var current = samples[i];
            if (current.InstrumentName != instrumentName)
            {
                continue;
            }

            if (found)
            {
                throw new InvalidOperationException($"Expected only one measurement for instrument '{instrumentName}'.");
            }

            sample = current;
            found = true;
        }

        Assert.True(found, $"Expected to capture at least one measurement for instrument '{instrumentName}'.");
    }

    private static void AssertNoSample(List<MetricSample> samples, string instrumentName)
    {
        for (var i = 0; i < samples.Count; i++)
        {
            if (samples[i].InstrumentName == instrumentName)
            {
                throw new InvalidOperationException($"Expected no measurements for instrument '{instrumentName}'.");
            }
        }
    }

    private static void AssertFlowMetricTags(KeyValuePair<string, object?>[] tags, string expectedFlowName, string expectedOutcomeKind)
    {
        AssertTagsAreLowCardinality(tags);

        AssertTag(tags, Observability.FlowActivitySource.TagFlowName, expectedFlowName);
        AssertTag(tags, Observability.FlowActivitySource.TagOutcomeKind, expectedOutcomeKind);
        Assert.False(HasTag(tags, Observability.FlowActivitySource.TagModuleType));
    }

    private static void AssertStepMetricTags(
        KeyValuePair<string, object?>[] tags,
        string expectedFlowName,
        string expectedModuleType,
        string expectedOutcomeKind)
    {
        AssertTagsAreLowCardinality(tags);

        AssertTag(tags, Observability.FlowActivitySource.TagFlowName, expectedFlowName);
        AssertTag(tags, Observability.FlowActivitySource.TagModuleType, expectedModuleType);
        AssertTag(tags, Observability.FlowActivitySource.TagOutcomeKind, expectedOutcomeKind);
    }

    private static void AssertJoinMetricTags(KeyValuePair<string, object?>[] tags, string expectedFlowName, string expectedOutcomeKind)
    {
        AssertTagsAreLowCardinality(tags);

        AssertTag(tags, Observability.FlowActivitySource.TagFlowName, expectedFlowName);
        AssertTag(tags, Observability.FlowActivitySource.TagOutcomeKind, expectedOutcomeKind);
        Assert.False(HasTag(tags, Observability.FlowActivitySource.TagModuleType));
    }

    private static void AssertTagsAreLowCardinality(KeyValuePair<string, object?>[] tags)
    {
        for (var i = 0; i < tags.Length; i++)
        {
            var key = tags[i].Key;

            if (key == Observability.FlowActivitySource.TagFlowName)
            {
                continue;
            }

            if (key == Observability.FlowActivitySource.TagModuleType)
            {
                continue;
            }

            if (key == Observability.FlowActivitySource.TagOutcomeKind)
            {
                continue;
            }

            throw new InvalidOperationException($"Unexpected tag key '{key}'.");
        }
    }

    private static void AssertSkipReasonMetricTags(KeyValuePair<string, object?>[] tags, string expectedFlowName, string expectedCode)
    {
        for (var i = 0; i < tags.Length; i++)
        {
            var key = tags[i].Key;

            if (key == Observability.FlowActivitySource.TagFlowName)
            {
                continue;
            }

            if (key == SkipCodeTagKey)
            {
                continue;
            }

            throw new InvalidOperationException($"Unexpected tag key '{key}'.");
        }

        AssertTag(tags, Observability.FlowActivitySource.TagFlowName, expectedFlowName);
        AssertTag(tags, SkipCodeTagKey, expectedCode);
    }

    private static void AssertTag(KeyValuePair<string, object?>[] tags, string key, string expected)
    {
        Assert.True(TryGetTagString(tags, key, out var value), $"Expected tag '{key}' to be present.");
        Assert.Equal(expected, value);
    }

    private static bool HasTag(KeyValuePair<string, object?>[] tags, string key)
    {
        for (var i = 0; i < tags.Length; i++)
        {
            if (tags[i].Key == key)
            {
                return true;
            }
        }

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

    private sealed class AddOneModule : IModule<int, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(context.Args + 1));
        }
    }

    private sealed class ThrowingModule : IModule<int, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            throw new InvalidOperationException("boom");
        }
    }

    private sealed class CancelingModule : IModule<int, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            throw new OperationCanceledException();
        }
    }

    private sealed class SkippingModule : IModule<int, int>
    {
        private readonly string _code;

        public SkippingModule(string code)
        {
            _code = code;
        }

        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Skipped(_code));
        }
    }

    private sealed class FallbackModule : IModule<int, int>
    {
        private readonly int _value;
        private readonly string _code;

        public FallbackModule(int value, string code)
        {
            _value = value;
            _code = code;
        }

        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Fallback(_value, _code));
        }
    }
}
