using System.Diagnostics;
using System.Text.Json;
using Rockestra.Core.Blueprint;

namespace Rockestra.Core.Tests;

public sealed class ExecutionEngineTracingTests
{
    private static readonly DateTimeOffset FutureDeadline = new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_Template_WithListener_ShouldCreateFlowAndNodeActivities_WithExpectedTags()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var configProvider = new StaticConfigProvider(configVersion: 123, patchJson: string.Empty);
        _ = await flowContext.GetConfigSnapshotAsync(configProvider);
        var expectedConfigVersion = "123";

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        var blueprint = FlowBlueprint.Define<int, int>("TracingTestFlow.Success")
            .Stage("stage_a", stage => stage.Step("step_a", "m.add_one"))
            .Step("step_b", "m.add_one")
            .Stage(
                "stage_b",
                stage =>
                    stage.Join<int>(
                        "final",
                        ctx =>
                        {
                            Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var stepA));
                            Assert.True(stepA.IsOk);

                            Assert.True(ctx.TryGetNodeOutcome<int>("step_b", out var stepB));
                            Assert.True(stepB.IsOk);

                            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(stepA.Value + stepB.Value));
                        }))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var activities = new List<Activity>();
        var expectedPlanHash = template.PlanHash.ToString("X16");

        using var listener = CreateListener(activities, template.Name, expectedPlanHash);

        var result = await engine.ExecuteAsync(template, request: 5, flowContext);
        Assert.True(result.IsOk);
        Assert.Equal(12, result.Value);

        Assert.Equal(template.Nodes.Count + 1, activities.Count);

        Assert.True(TryGetSingleActivity(activities, activityName: Observability.FlowActivitySource.FlowActivityName, out var flowActivity));
        AssertTag(flowActivity, "flow.name", template.Name);
        AssertTag(flowActivity, "plan.hash", expectedPlanHash);
        AssertTag(flowActivity, "config.version", expectedConfigVersion);

        var nodeActivities = new Dictionary<string, Activity>(capacity: template.Nodes.Count);
        for (var i = 0; i < activities.Count; i++)
        {
            var activity = activities[i];

            if (activity == flowActivity)
            {
                continue;
            }

            var nodeName = GetTagString(activity, "node.name");
            Assert.False(string.IsNullOrEmpty(nodeName));
            nodeActivities.Add(nodeName!, activity);
        }

        Assert.Equal(template.Nodes.Count, nodeActivities.Count);

        for (var i = 0; i < template.Nodes.Count; i++)
        {
            var node = template.Nodes[i];
            Assert.True(nodeActivities.TryGetValue(node.Name, out var nodeActivity));

            Assert.Equal(flowActivity.SpanId, nodeActivity.ParentSpanId);

            AssertTag(nodeActivity, "flow.name", template.Name);
            AssertTag(nodeActivity, "plan.hash", expectedPlanHash);
            AssertTag(nodeActivity, "config.version", expectedConfigVersion);
            AssertTag(nodeActivity, "node.name", node.Name);

            if (node.Kind == BlueprintNodeKind.Step)
            {
                Assert.Equal(Observability.FlowActivitySource.StepActivityName, nodeActivity.DisplayName);
                AssertTag(nodeActivity, "node.kind", "step");
                AssertTag(nodeActivity, "module.type", node.ModuleType);
            }
            else if (node.Kind == BlueprintNodeKind.Join)
            {
                Assert.Equal(Observability.FlowActivitySource.JoinActivityName, nodeActivity.DisplayName);
                AssertTag(nodeActivity, "node.kind", "join");
                Assert.Null(GetTagObject(nodeActivity, "module.type"));
            }
            else
            {
                throw new InvalidOperationException($"Unsupported node kind: '{node.Kind}'.");
            }

            if (string.IsNullOrEmpty(node.StageName))
            {
                Assert.Null(GetTagObject(nodeActivity, "stage.name"));
            }
            else
            {
                AssertTag(nodeActivity, "stage.name", node.StageName);
            }

            AssertTag(nodeActivity, "outcome.kind", "ok");
            AssertTag(nodeActivity, "outcome.code", "OK");
        }
    }

    [Fact]
    public async Task ExecuteAsync_Template_WithListener_ShouldCreateStageFanoutModuleActivities_WithExpectedTags()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"TracingTestFlow.Fanout\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":2,\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.ok\",\"with\":{}}," +
            "{\"id\":\"m2\",\"use\":\"test.ok\",\"with\":{}}" +
            "]}}}}}";

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var configProvider = new StaticConfigProvider(configVersion: 123, patchJson: patchJson);
        _ = await flowContext.GetConfigSnapshotAsync(configProvider);

        var catalog = new ModuleCatalog();
        catalog.Register<JsonElement, int>("test.ok", _ => new OkJsonElementModule());

        var blueprint = FlowBlueprint.Define<int, int>("TracingTestFlow.Fanout")
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

        var activities = new List<Activity>();

        using var listener = CreateListenerForFlowName(activities, expectedFlowName: template.Name);

        var result = await engine.ExecuteAsync(template, request: 0, flowContext);
        Assert.True(result.IsOk);

        Assert.True(TryGetSingleActivity(activities, activityName: Observability.FlowActivitySource.FlowActivityName, out var flowActivity));

        var stageFanoutActivities = new List<Activity>(capacity: 2);
        for (var i = 0; i < activities.Count; i++)
        {
            var activity = activities[i];
            if (activity.DisplayName == Observability.FlowActivitySource.StageFanoutModuleActivityName)
            {
                stageFanoutActivities.Add(activity);
            }
        }

        Assert.Equal(2, stageFanoutActivities.Count);

        for (var i = 0; i < stageFanoutActivities.Count; i++)
        {
            var activity = stageFanoutActivities[i];

            Assert.Equal(flowActivity.SpanId, activity.ParentSpanId);
            AssertTag(activity, "flow.name", template.Name);
            AssertTag(activity, "stage.name", "s1");
            AssertTag(activity, "module.type", "test.ok");
            AssertTag(activity, "outcome.kind", "ok");
            AssertTag(activity, "outcome.code", "OK");

            var moduleId = GetTagString(activity, "module.id");
            Assert.True(moduleId == "m1" || moduleId == "m2");
        }
    }

    [Fact]
    public async Task ExecuteAsync_Template_WithListener_ShouldTagShadowStageFanoutModuleActivities()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"TracingTestFlow.FanoutShadow\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[" +
            "{\"id\":\"m_primary\",\"use\":\"test.ok\",\"with\":{}}," +
            "{\"id\":\"m_shadow\",\"use\":\"test.ok\",\"with\":{},\"shadow\":{\"sample\":1}}" +
            "]}}}}}";

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var configProvider = new StaticConfigProvider(configVersion: 123, patchJson: patchJson);
        _ = await flowContext.GetConfigSnapshotAsync(configProvider);

        var catalog = new ModuleCatalog();
        catalog.Register<JsonElement, int>("test.ok", _ => new OkJsonElementModule());

        var blueprint = FlowBlueprint.Define<int, int>("TracingTestFlow.FanoutShadow")
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

        var activities = new List<Activity>();

        using var listener = CreateListenerForFlowName(activities, expectedFlowName: template.Name);

        var result = await engine.ExecuteAsync(template, request: 0, flowContext);
        Assert.True(result.IsOk);

        Assert.True(TryGetSingleActivity(activities, activityName: Observability.FlowActivitySource.FlowActivityName, out var flowActivity));

        var stageFanoutActivities = new List<Activity>(capacity: 2);
        for (var i = 0; i < activities.Count; i++)
        {
            var activity = activities[i];
            if (activity.DisplayName == Observability.FlowActivitySource.StageFanoutModuleActivityName)
            {
                stageFanoutActivities.Add(activity);
            }
        }

        Assert.Equal(2, stageFanoutActivities.Count);

        for (var i = 0; i < stageFanoutActivities.Count; i++)
        {
            var activity = stageFanoutActivities[i];
            var moduleId = GetTagString(activity, "module.id");

            Assert.Equal(flowActivity.SpanId, activity.ParentSpanId);
            AssertTag(activity, "flow.name", template.Name);
            AssertTag(activity, "stage.name", "s1");
            AssertTag(activity, "module.type", "test.ok");
            AssertTag(activity, "outcome.kind", "ok");
            AssertTag(activity, "outcome.code", "OK");

            if (moduleId == "m_primary")
            {
                AssertTag(activity, "execution.path", "primary");
                Assert.Null(GetTagObject(activity, "shadow.sample_rate_bps"));
            }
            else if (moduleId == "m_shadow")
            {
                AssertTag(activity, "execution.path", "shadow");
                AssertTag(activity, "shadow.sample_rate_bps", 10000L);
            }
            else
            {
                Assert.Fail("Unexpected module.id tag value.");
            }
        }
    }

    [Fact]
    public async Task ExecuteAsync_Template_WithListener_ShouldSetOutcomeTags_ForErrorOutcome()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.boom", _ => new ThrowingModule());

        var blueprint = FlowBlueprint.Define<int, string>("TracingTestFlow.Error")
            .Step("step_a", "m.boom")
            .Join<string>(
                "final",
                ctx =>
                {
                    Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var outcome));
                    return new ValueTask<Outcome<string>>(
                        Outcome<string>.Ok(outcome.IsError ? "error:" + outcome.Code : "ok"));
                })
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var activities = new List<Activity>();
        var expectedPlanHash = template.PlanHash.ToString("X16");
        using var listener = CreateListener(activities, template.Name, expectedPlanHash);

        var result = await engine.ExecuteAsync(template, request: 1, flowContext);
        Assert.True(result.IsOk);

        Assert.True(TryGetNodeActivityByName(activities, "step_a", out var stepActivity));
        AssertTag(stepActivity, "outcome.kind", "error");
        AssertTag(stepActivity, "outcome.code", ExecutionEngine.UnhandledExceptionCode);
    }

    [Fact]
    public async Task ExecuteAsync_Template_WithListener_ShouldSetOutcomeTags_ForCanceledOutcome()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);

        var blueprint = FlowBlueprint.Define<int, int>("TracingTestFlow.Canceled")
            .Join<int>("final", (Func<FlowContext, ValueTask<Outcome<int>>>)(_ => throw new OperationCanceledException()))
            .Build();

        var template = PlanCompiler.Compile(blueprint, new ModuleCatalog());
        var engine = new ExecutionEngine(new ModuleCatalog());

        var activities = new List<Activity>();
        var expectedPlanHash = template.PlanHash.ToString("X16");
        using var listener = CreateListener(activities, template.Name, expectedPlanHash);

        var result = await engine.ExecuteAsync(template, request: 1, flowContext);
        Assert.True(result.IsCanceled);

        Assert.True(TryGetNodeActivityByName(activities, "final", out var finalActivity));
        AssertTag(finalActivity, "outcome.kind", "canceled");
        AssertTag(finalActivity, "outcome.code", ExecutionEngine.UpstreamCanceledCode);
    }

    [Fact]
    public async Task ExecuteAsync_Template_WithListener_ShouldSetOutcomeTags_ForTimeoutOutcome()
    {
        var services = new DummyServiceProvider();
        var deadline = DateTimeOffset.UtcNow.AddMilliseconds(200);
        var flowContext = new FlowContext(services, CancellationToken.None, deadline);

        var blueprint = FlowBlueprint.Define<int, int>("TracingTestFlow.Timeout")
            .Join<int>(
                "final",
                async _ =>
                {
                    await Task.Delay(350);
                    throw new OperationCanceledException();
                })
            .Build();

        var template = PlanCompiler.Compile(blueprint, new ModuleCatalog());
        var engine = new ExecutionEngine(new ModuleCatalog());

        var activities = new List<Activity>();
        var expectedPlanHash = template.PlanHash.ToString("X16");
        using var listener = CreateListener(activities, template.Name, expectedPlanHash);

        var result = await engine.ExecuteAsync(template, request: 1, flowContext);
        Assert.True(result.IsTimeout);

        Assert.True(TryGetNodeActivityByName(activities, "final", out var finalActivity));
        AssertTag(finalActivity, "outcome.kind", "timeout");
        AssertTag(finalActivity, "outcome.code", ExecutionEngine.DeadlineExceededCode);
    }

    [Fact]
    public async Task ExecuteAsync_Template_WithListener_ShouldSetOutcomeTags_ForSkippedOutcome()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.skip", _ => new SkippingModule(code: "GATE_FALSE"));

        var blueprint = FlowBlueprint.Define<int, int>("TracingTestFlow.Skipped")
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

        var activities = new List<Activity>();
        var expectedPlanHash = template.PlanHash.ToString("X16");
        using var listener = CreateListener(activities, template.Name, expectedPlanHash);

        var result = await engine.ExecuteAsync(template, request: 1, flowContext);
        Assert.True(result.IsOk);

        Assert.True(TryGetNodeActivityByName(activities, "step_a", out var stepActivity));
        AssertTag(stepActivity, "outcome.kind", "skipped");
        AssertTag(stepActivity, "outcome.code", "GATE_FALSE");

        Assert.True(TryGetNodeActivityByName(activities, "final", out var finalActivity));
        AssertTag(finalActivity, "outcome.kind", "ok");
        AssertTag(finalActivity, "outcome.code", "OK");
    }

    [Fact]
    public async Task ExecuteAsync_Template_WithListener_ShouldSetOutcomeTags_ForFallbackOutcome()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.fallback", _ => new FallbackModule(value: 42, code: "CACHE_HIT"));

        var blueprint = FlowBlueprint.Define<int, int>("TracingTestFlow.Fallback")
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

        var activities = new List<Activity>();
        var expectedPlanHash = template.PlanHash.ToString("X16");
        using var listener = CreateListener(activities, template.Name, expectedPlanHash);

        var result = await engine.ExecuteAsync(template, request: 1, flowContext);
        Assert.True(result.IsOk);

        Assert.True(TryGetNodeActivityByName(activities, "step_a", out var stepActivity));
        AssertTag(stepActivity, "outcome.kind", "fallback");
        AssertTag(stepActivity, "outcome.code", "CACHE_HIT");

        Assert.True(TryGetNodeActivityByName(activities, "final", out var finalActivity));
        AssertTag(finalActivity, "outcome.kind", "ok");
        AssertTag(finalActivity, "outcome.code", "OK");
    }

    [Fact]
    public async Task ExecuteAsync_Template_NoListener_ShouldNotCreateActivities()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);

        var module = new ActivityCapturingModule();
        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.capture", _ => module);

        var blueprint = FlowBlueprint.Define<int, int>("TracingTestFlow.NoListener")
            .Step("step_a", "m.capture")
            .Join<int>(
                "final",
                ctx =>
                {
                    Assert.Null(Activity.Current);
                    Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var stepA));
                    return new ValueTask<Outcome<int>>(stepA);
                })
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        Assert.False(Observability.FlowActivitySource.Instance.HasListeners());

        var result = await engine.ExecuteAsync(template, request: 1, flowContext);
        Assert.True(result.IsOk);

        Assert.Null(module.ObservedActivity);
    }

    private static ActivityListener CreateListener(List<Activity> stopped, string expectedFlowName, string expectedPlanHash)
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == Observability.FlowActivitySource.ActivitySourceName,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                var flowName = GetTagString(activity, "flow.name");
                if (flowName != expectedFlowName)
                {
                    return;
                }

                var planHash = GetTagString(activity, "plan.hash");
                if (planHash != expectedPlanHash)
                {
                    return;
                }

                stopped.Add(activity);
            },
        };

        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    private static ActivityListener CreateListenerForFlowName(List<Activity> stopped, string expectedFlowName)
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == Observability.FlowActivitySource.ActivitySourceName,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                var flowName = GetTagString(activity, "flow.name");
                if (flowName != expectedFlowName)
                {
                    return;
                }

                stopped.Add(activity);
            },
        };

        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    private static bool TryGetSingleActivity(List<Activity> activities, string activityName, out Activity activity)
    {
        activity = null!;

        Activity? candidate = null;
        for (var i = 0; i < activities.Count; i++)
        {
            var current = activities[i];
            if (current.DisplayName != activityName)
            {
                continue;
            }

            if (candidate is not null)
            {
                return false;
            }

            candidate = current;
        }

        if (candidate is null)
        {
            return false;
        }

        activity = candidate;
        return true;
    }

    private static bool TryGetNodeActivityByName(List<Activity> activities, string nodeName, out Activity activity)
    {
        activity = null!;

        for (var i = 0; i < activities.Count; i++)
        {
            var current = activities[i];
            if (GetTagString(current, "node.name") == nodeName)
            {
                activity = current;
                return true;
            }
        }

        return false;
    }

    private static object? GetTagObject(Activity activity, string key)
    {
        foreach (var tag in activity.TagObjects)
        {
            if (tag.Key == key)
            {
                return tag.Value;
            }
        }

        return null;
    }

    private static string? GetTagString(Activity activity, string key)
    {
        return GetTagObject(activity, key) as string;
    }

    private static void AssertTag(Activity activity, string key, object? expected)
    {
        var actual = GetTagObject(activity, key);
        Assert.Equal(expected, actual);
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

    private sealed class ActivityCapturingModule : IModule<int, int>
    {
        public Activity? ObservedActivity { get; private set; }

        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            ObservedActivity = Activity.Current;
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(context.Args));
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

