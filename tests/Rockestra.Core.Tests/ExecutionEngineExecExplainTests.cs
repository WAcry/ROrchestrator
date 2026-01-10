using Rockestra.Core;
using Rockestra.Core.Blueprint;
using System.Text.Json;

namespace Rockestra.Core.Tests;

public sealed class ExecutionEngineExecExplainTests
{
    private static readonly DateTimeOffset FutureDeadline = new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_Template_WithExecExplainEnabled_ShouldRecordFlowAndNodes()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        flowContext.EnableExecExplain();

        var configProvider = new StaticConfigProvider(configVersion: 123, patchJson: string.Empty);
        _ = await flowContext.GetConfigSnapshotAsync(configProvider);

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        var blueprint = FlowBlueprint.Define<int, int>("ExplainTestFlow.Success")
            .Stage("stage_a", stage => stage.Step("step_a", "m.add_one"))
            .Stage(
                "stage_b",
                stage =>
                    stage.Join<int>(
                        "final",
                        ctx =>
                        {
                            Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var stepOutcome));
                            Assert.True(stepOutcome.IsOk);
                            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(stepOutcome.Value + 10));
                        }))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(template, request: 5, flowContext);

        Assert.True(result.IsOk);
        Assert.Equal(16, result.Value);

        Assert.True(flowContext.TryGetExecExplain(out var explain));

        Assert.Equal(template.Name, explain.FlowName);
        Assert.Equal(template.PlanHash, explain.PlanHash);

        Assert.True(explain.TryGetConfigVersion(out var configVersion));
        Assert.Equal(123UL, configVersion);

        Assert.Equal(template.Nodes.Count, explain.Nodes.Count);

        var step = explain.Nodes[0];
        Assert.Equal(BlueprintNodeKind.Step, step.Kind);
        Assert.Equal("step_a", step.Name);
        Assert.Equal("stage_a", step.StageName);
        Assert.Equal("m.add_one", step.ModuleType);
        Assert.Equal(OutcomeKind.Ok, step.OutcomeKind);
        Assert.Equal("OK", step.OutcomeCode);
        Assert.True(step.EndTimestamp >= step.StartTimestamp);
        Assert.Equal(step.EndTimestamp - step.StartTimestamp, step.DurationStopwatchTicks);

        var join = explain.Nodes[1];
        Assert.Equal(BlueprintNodeKind.Join, join.Kind);
        Assert.Equal("final", join.Name);
        Assert.Equal("stage_b", join.StageName);
        Assert.Null(join.ModuleType);
        Assert.Equal(OutcomeKind.Ok, join.OutcomeKind);
        Assert.Equal("OK", join.OutcomeCode);
        Assert.True(join.EndTimestamp >= join.StartTimestamp);
        Assert.Equal(join.EndTimestamp - join.StartTimestamp, join.DurationStopwatchTicks);
    }

    [Fact]
    public async Task ExecuteAsync_Template_WithExecExplainEnabled_ShouldRecordVariantsAndOverlaysApplied()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"ExplainTestFlow.Overlays\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[{\"id\":\"m1\",\"use\":\"test.ok\",\"with\":{}}]}}," +
            "\"experiments\":[{\"layer\":\"l1\",\"variant\":\"A\",\"patch\":{}}]," +
            "\"emergency\":{\"reason\":\"r\",\"operator\":\"op\",\"ttl_minutes\":30,\"patch\":{}}" +
            "}}}";

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(
            services,
            CancellationToken.None,
            FutureDeadline,
            requestOptions: new FlowRequestOptions(
                variants: new Dictionary<string, string>
                {
                    { "l1", "A" },
                }));

        flowContext.EnableExecExplain(ExplainLevel.Standard);

        var configProvider = new StaticConfigProvider(configVersion: 123, patchJson);
        _ = await flowContext.GetConfigSnapshotAsync(configProvider);

        var catalog = new ModuleCatalog();
        catalog.Register<JsonElement, int>("test.ok", _ => new OkJsonElementModule());

        var blueprint = FlowBlueprint.Define<int, int>("ExplainTestFlow.Overlays")
            .Stage(
                "s1",
                stage =>
                    stage.Join<int>(
                        "final",
                        _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(template, request: 0, flowContext);
        Assert.True(result.IsOk);

        Assert.True(flowContext.TryGetExecExplain(out var explain));
        Assert.Equal(ExplainLevel.Standard, explain.Level);

        Assert.Equal(3, explain.OverlaysApplied.Count);
        Assert.Equal("base", explain.OverlaysApplied[0].Layer);

        Assert.Equal("experiment", explain.OverlaysApplied[1].Layer);
        Assert.Equal("l1", explain.OverlaysApplied[1].ExperimentLayer);
        Assert.Equal("A", explain.OverlaysApplied[1].ExperimentVariant);

        Assert.Equal("emergency", explain.OverlaysApplied[2].Layer);

        _ = Assert.Single(explain.Variants);
        Assert.True(explain.Variants.TryGetValue("l1", out var variant));
        Assert.Equal("A", variant);
    }

    [Fact]
    public async Task ExecuteAsync_Template_WithExecExplainEnabledMinimal_ShouldNotRecordVariantsOrOverlaysApplied()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"ExplainTestFlow.Overlays\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[{\"id\":\"m1\",\"use\":\"test.ok\",\"with\":{}}]}}," +
            "\"experiments\":[{\"layer\":\"l1\",\"variant\":\"A\",\"patch\":{}}]," +
            "\"emergency\":{\"reason\":\"r\",\"operator\":\"op\",\"ttl_minutes\":30,\"patch\":{}}" +
            "}}}";

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(
            services,
            CancellationToken.None,
            FutureDeadline,
            requestOptions: new FlowRequestOptions(
                variants: new Dictionary<string, string>
                {
                    { "l1", "A" },
                }));

        flowContext.EnableExecExplain(ExplainLevel.Minimal);

        var configProvider = new StaticConfigProvider(configVersion: 123, patchJson);
        _ = await flowContext.GetConfigSnapshotAsync(configProvider);

        var catalog = new ModuleCatalog();
        catalog.Register<JsonElement, int>("test.ok", _ => new OkJsonElementModule());

        var blueprint = FlowBlueprint.Define<int, int>("ExplainTestFlow.Overlays")
            .Stage(
                "s1",
                stage =>
                    stage.Join<int>(
                        "final",
                        _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(template, request: 0, flowContext);
        Assert.True(result.IsOk);

        Assert.True(flowContext.TryGetExecExplain(out var explain));
        Assert.Equal(ExplainLevel.Minimal, explain.Level);
        Assert.Empty(explain.OverlaysApplied);
        Assert.Empty(explain.Variants);
    }

    [Fact]
    public async Task ExecuteAsync_Template_WithExecExplainEnabled_ShouldNotMutatePreviousExplain()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);
        flowContext.EnableExecExplain();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        var blueprint1 = FlowBlueprint.Define<int, int>("ExplainTestFlow.First")
            .Stage("stage_first_a", stage => stage.Step("step_first", "m.add_one"))
            .Stage(
                "stage_first_b",
                stage =>
                    stage.Join<int>(
                        "final_first",
                        ctx =>
                        {
                            Assert.True(ctx.TryGetNodeOutcome<int>("step_first", out var stepOutcome));
                            Assert.True(stepOutcome.IsOk);
                            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(stepOutcome.Value));
                        }))
            .Build();

        var blueprint2 = FlowBlueprint.Define<int, int>("ExplainTestFlow.Second")
            .Stage("stage_second_a", stage => stage.Step("step_second", "m.add_one"))
            .Stage(
                "stage_second_b",
                stage =>
                    stage.Join<int>(
                        "final_second",
                        ctx =>
                        {
                            Assert.True(ctx.TryGetNodeOutcome<int>("step_second", out var stepOutcome));
                            Assert.True(stepOutcome.IsOk);
                            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(stepOutcome.Value));
                        }))
            .Build();

        var blueprint3 = FlowBlueprint.Define<int, int>("ExplainTestFlow.Third")
            .Stage("stage_third_a", stage => stage.Step("step_third", "m.add_one"))
            .Stage(
                "stage_third_b",
                stage =>
                    stage.Join<int>(
                        "final_third",
                        ctx =>
                        {
                            Assert.True(ctx.TryGetNodeOutcome<int>("step_third", out var stepOutcome));
                            Assert.True(stepOutcome.IsOk);
                            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(stepOutcome.Value));
                        }))
            .Build();

        var template1 = PlanCompiler.Compile(blueprint1, catalog);
        var template2 = PlanCompiler.Compile(blueprint2, catalog);
        var template3 = PlanCompiler.Compile(blueprint3, catalog);
        var engine = new ExecutionEngine(catalog);

        var result1 = await engine.ExecuteAsync(template1, request: 5, flowContext);
        Assert.True(result1.IsOk);
        Assert.True(flowContext.TryGetExecExplain(out var explain1));

        var result2 = await engine.ExecuteAsync(template2, request: 5, flowContext);
        Assert.True(result2.IsOk);
        Assert.True(flowContext.TryGetExecExplain(out var explain2));

        var result3 = await engine.ExecuteAsync(template3, request: 5, flowContext);
        Assert.True(result3.IsOk);
        Assert.True(flowContext.TryGetExecExplain(out var explain3));

        Assert.Equal(template1.Name, explain1.FlowName);
        Assert.Equal("step_first", explain1.Nodes[0].Name);
        Assert.Equal("final_first", explain1.Nodes[1].Name);
        Assert.Equal("stage_first_a", explain1.Nodes[0].StageName);
        Assert.Equal("stage_first_b", explain1.Nodes[1].StageName);

        Assert.Equal(template2.Name, explain2.FlowName);
        Assert.Equal("step_second", explain2.Nodes[0].Name);
        Assert.Equal("final_second", explain2.Nodes[1].Name);
        Assert.Equal("stage_second_a", explain2.Nodes[0].StageName);
        Assert.Equal("stage_second_b", explain2.Nodes[1].StageName);

        Assert.Equal(template3.Name, explain3.FlowName);
        Assert.Equal("step_third", explain3.Nodes[0].Name);
        Assert.Equal("final_third", explain3.Nodes[1].Name);
        Assert.Equal("stage_third_a", explain3.Nodes[0].StageName);
        Assert.Equal("stage_third_b", explain3.Nodes[1].StageName);
    }

    [Fact]
    public async Task ExecuteAsync_Template_WithExecExplainEnabled_WhenCanceled_ShouldPrefillUnexecutedNodes()
    {
        var services = new DummyServiceProvider();
        using var cts = new CancellationTokenSource();
        var flowContext = new FlowContext(services, cts.Token, FutureDeadline);
        flowContext.EnableExecExplain();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.cancel", _ => new CancelModule(cts));
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        var blueprint = FlowBlueprint.Define<int, int>("ExplainTestFlow.Canceled")
            .Stage("stage_a", stage => stage.Step("step_cancel", "m.cancel"))
            .Stage("stage_b", stage => stage.Step("step_unused", "m.add_one"))
            .Stage(
                "stage_c",
                stage =>
                    stage.Join<int>(
                        "final",
                        _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(template, request: 5, flowContext);

        Assert.Equal(OutcomeKind.Canceled, result.Kind);
        Assert.True(flowContext.TryGetExecExplain(out var explain));
        Assert.Equal(template.Nodes.Count, explain.Nodes.Count);

        Assert.Equal("step_cancel", explain.Nodes[0].Name);
        Assert.Equal(OutcomeKind.Ok, explain.Nodes[0].OutcomeKind);

        Assert.Equal("step_unused", explain.Nodes[1].Name);
        Assert.Equal(BlueprintNodeKind.Step, explain.Nodes[1].Kind);
        Assert.Equal(OutcomeKind.Unspecified, explain.Nodes[1].OutcomeKind);
        Assert.Equal(string.Empty, explain.Nodes[1].OutcomeCode);
        Assert.Equal(0, explain.Nodes[1].StartTimestamp);
        Assert.Equal(0, explain.Nodes[1].EndTimestamp);

        Assert.Equal("final", explain.Nodes[2].Name);
        Assert.Equal(BlueprintNodeKind.Join, explain.Nodes[2].Kind);
        Assert.Equal(OutcomeKind.Unspecified, explain.Nodes[2].OutcomeKind);
        Assert.Equal(string.Empty, explain.Nodes[2].OutcomeCode);
        Assert.Equal(0, explain.Nodes[2].StartTimestamp);
        Assert.Equal(0, explain.Nodes[2].EndTimestamp);
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

    private sealed class CancelModule : IModule<int, int>
    {
        private readonly CancellationTokenSource _cts;

        public CancelModule(CancellationTokenSource cts)
        {
            _cts = cts;
        }

        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            _cts.Cancel();
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(context.Args));
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

