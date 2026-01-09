namespace ROrchestrator.Core.Tests;

public sealed class PatchEvaluatorV1Tests
{
    [Fact]
    public void Evaluate_WhenVariantMatches_ShouldApplyExperimentPatch()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":2,\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}]}}," +
            "\"experiments\":[{\"layer\":\"l1\",\"variant\":\"B\",\"patch\":{\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[{\"id\":\"m2\",\"use\":\"test.module\",\"with\":{}}]}}}}]" +
            "}}}";

        var requestOptions = new FlowRequestOptions(
            variants: new Dictionary<string, string>
            {
                { "l1", "B" },
            });

        using var evaluation = PatchEvaluatorV1.Evaluate("HomeFeed", patchJson, requestOptions);

        Assert.Equal("HomeFeed", evaluation.FlowName);
        Assert.Equal(2, evaluation.OverlaysApplied.Count);
        Assert.Equal("base", evaluation.OverlaysApplied[0].Layer);
        Assert.Equal("experiment", evaluation.OverlaysApplied[1].Layer);
        Assert.Equal("l1", evaluation.OverlaysApplied[1].ExperimentLayer);
        Assert.Equal("B", evaluation.OverlaysApplied[1].ExperimentVariant);

        Assert.Single(evaluation.Stages);
        var stage = evaluation.Stages[0];
        Assert.Equal("s1", stage.StageName);
        Assert.True(stage.HasFanoutMax);
        Assert.Equal(1, stage.FanoutMax);

        Assert.Equal(2, stage.Modules.Count);
        Assert.Equal("m1", stage.Modules[0].ModuleId);
        Assert.Equal("m2", stage.Modules[1].ModuleId);
    }

    [Fact]
    public void Evaluate_WhenVariantDoesNotMatch_ShouldNotApplyExperimentPatch()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":2,\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}]}}," +
            "\"experiments\":[{\"layer\":\"l1\",\"variant\":\"B\",\"patch\":{\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[{\"id\":\"m2\",\"use\":\"test.module\",\"with\":{}}]}}}}]" +
            "}}}";

        var requestOptions = new FlowRequestOptions(
            variants: new Dictionary<string, string>
            {
                { "l1", "A" },
            });

        using var evaluation = PatchEvaluatorV1.Evaluate("HomeFeed", patchJson, requestOptions);

        Assert.Equal("HomeFeed", evaluation.FlowName);
        Assert.Single(evaluation.OverlaysApplied);
        Assert.Equal("base", evaluation.OverlaysApplied[0].Layer);

        Assert.Single(evaluation.Stages);
        var stage = evaluation.Stages[0];
        Assert.Equal("s1", stage.StageName);
        Assert.True(stage.HasFanoutMax);
        Assert.Equal(2, stage.FanoutMax);

        Assert.Single(stage.Modules);
        Assert.Equal("m1", stage.Modules[0].ModuleId);
    }

    [Fact]
    public void Evaluate_ShouldApplyEmergencyPatch_LastAndOverrideEnabled()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":4,\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"enabled\":true}," +
            "{\"id\":\"m2\",\"use\":\"test.module\",\"with\":{},\"enabled\":true}" +
            "]}},\"experiments\":[{\"layer\":\"l1\",\"variant\":\"A\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"enabled\":true}" +
            "]}}}}]," +
            "\"emergency\":{\"reason\":\"r\",\"operator\":\"op\",\"ttl_minutes\":30,\"patch\":{\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[{\"id\":\"m2\",\"enabled\":false}]}}}}" +
            "}}}";

        var requestOptions = new FlowRequestOptions(
            variants: new Dictionary<string, string>
            {
                { "l1", "A" },
            });

        using var evaluation = PatchEvaluatorV1.Evaluate("HomeFeed", patchJson, requestOptions);

        Assert.Equal(3, evaluation.OverlaysApplied.Count);
        Assert.Equal("base", evaluation.OverlaysApplied[0].Layer);
        Assert.Equal("experiment", evaluation.OverlaysApplied[1].Layer);
        Assert.Equal("emergency", evaluation.OverlaysApplied[2].Layer);

        var stage = evaluation.Stages[0];
        Assert.True(stage.HasFanoutMax);
        Assert.Equal(1, stage.FanoutMax);

        Assert.Equal(2, stage.Modules.Count);
        Assert.Equal("m1", stage.Modules[0].ModuleId);
        Assert.True(stage.Modules[0].Enabled);

        Assert.Equal("m2", stage.Modules[1].ModuleId);
        Assert.False(stage.Modules[1].Enabled);
        Assert.True(stage.Modules[1].DisabledByEmergency);
    }
}

