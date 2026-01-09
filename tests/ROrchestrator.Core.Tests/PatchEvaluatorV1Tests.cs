using System.Text.Json;

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

    [Fact]
    public void Evaluate_ShouldSeparateShadowModules()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":2,\"modules\":[" +
            "{\"id\":\"m_primary\",\"use\":\"test.module\",\"with\":{}}," +
            "{\"id\":\"m_shadow\",\"use\":\"test.module\",\"with\":{},\"shadow\":{\"sample\":0.5}}" +
            "]}}" +
            "}}}";

        using var evaluation = PatchEvaluatorV1.Evaluate("HomeFeed", patchJson, requestOptions: new FlowRequestOptions());

        Assert.Single(evaluation.Stages);
        var stage = evaluation.Stages[0];

        Assert.Single(stage.Modules);
        Assert.Equal("m_primary", stage.Modules[0].ModuleId);

        Assert.Single(stage.ShadowModules);
        Assert.Equal("m_shadow", stage.ShadowModules[0].ModuleId);
        Assert.True(stage.ShadowModules[0].IsShadow);
        Assert.Equal((ushort)5000, stage.ShadowModules[0].ShadowSampleBps);
    }

    [Fact]
    public async Task Evaluate_ShouldReuseParsedPatchDocument_ByConfigVersion_Concurrently()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":2,\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}," +
            "{\"id\":\"m2\",\"use\":\"test.module\",\"with\":{}}" +
            "]}}" +
            "}}}";

        const ulong configVersion = 0x1234_5678_9ABC_DEF0;
        const int taskCount = 32;

        using var warmup = PatchEvaluatorV1.Evaluate("HomeFeed", patchJson, requestOptions: default, configVersion: configVersion);
        var cachedDocument = GetEvaluationDocument(warmup);

        using var startGate = new ManualResetEventSlim(initialState: false);

        var tasks = new Task[taskCount];

        for (var i = 0; i < taskCount; i++)
        {
            tasks[i] = Task.Run(
                () =>
                {
                    startGate.Wait();

                    using var evaluation = PatchEvaluatorV1.Evaluate(
                        "HomeFeed",
                        patchJson,
                        requestOptions: default,
                        configVersion: configVersion);

                    Assert.Single(evaluation.Stages);
                    Assert.Same(cachedDocument, GetEvaluationDocument(evaluation));
                });
        }

        startGate.Set();
        await Task.WhenAll(tasks);
    }

    private static JsonDocument GetEvaluationDocument(PatchEvaluatorV1.FlowPatchEvaluationV1 evaluation)
    {
        var documentField = typeof(PatchEvaluatorV1.FlowPatchEvaluationV1).GetField(
            "_document",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);

        var document = documentField?.GetValue(evaluation) as JsonDocument;
        Assert.NotNull(document);
        return document!;
    }
}
