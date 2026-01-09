using ROrchestrator.Core;
using ROrchestrator.Core.Blueprint;
using ROrchestrator.Tooling;

namespace ROrchestrator.Tooling.Tests;

public sealed class ToolingJsonV1Tests
{
    [Fact]
    public void ValidatePatchJson_ShouldProduceStableJson()
    {
        var registry = CreateRegistry();
        var catalog = CreateCatalog();

        var result = ToolingJsonV1.ValidatePatchJson(
            patchJson: "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\"}]}}}}}",
            registry,
            catalog);

        Assert.Equal(2, result.ExitCode);

        Assert.Equal(
            "{\"kind\":\"validate\",\"is_valid\":false,\"findings\":[{\"severity\":\"error\",\"code\":\"CFG_MODULE_ARGS_MISSING\",\"path\":\"$.flows.HomeFeed.stages.s1.modules[0].with\",\"message\":\"modules[].with is required.\"}]}",
            result.Json);
    }

    [Fact]
    public void ExplainFlowJson_ShouldProduceStableJson()
    {
        var registry = CreateRegistry();
        var catalog = CreateCatalog();

        var result = ToolingJsonV1.ExplainFlowJson("HomeFeed", registry, catalog);

        Assert.Equal(0, result.ExitCode);

        Assert.Equal(
            "{\"kind\":\"explain\",\"flow_name\":\"HomeFeed\",\"plan_template_hash\":\"EB1FEB02AEE93B82\",\"nodes\":[{\"kind\":\"step\",\"name\":\"n1\",\"stage_name\":\"s1\",\"module_type\":\"test.module\",\"output_type\":\"System.Int32\"},{\"kind\":\"join\",\"name\":\"join\",\"stage_name\":\"s1\",\"module_type\":null,\"output_type\":\"System.Int32\"}]}",
            result.Json);
    }

    [Fact]
    public void ExplainFlowJson_ShouldIncludeMermaid_WhenRequested()
    {
        var registry = CreateRegistry();
        var catalog = CreateCatalog();

        var result = ToolingJsonV1.ExplainFlowJson("HomeFeed", registry, catalog, includeMermaid: true);

        Assert.Equal(0, result.ExitCode);

        Assert.Equal(
            "{\"kind\":\"explain\",\"flow_name\":\"HomeFeed\",\"plan_template_hash\":\"EB1FEB02AEE93B82\",\"nodes\":[{\"kind\":\"step\",\"name\":\"n1\",\"stage_name\":\"s1\",\"module_type\":\"test.module\",\"output_type\":\"System.Int32\"},{\"kind\":\"join\",\"name\":\"join\",\"stage_name\":\"s1\",\"module_type\":null,\"output_type\":\"System.Int32\"}],\"mermaid\":\"flowchart TD\\n  n0[\\\"n1\\\\nstep\\\\n(test.module)\\\\nSystem.Int32\\\"] --> n1[\\\"join\\\\njoin\\\\nSystem.Int32\\\"]\\n\"}",
            result.Json);
    }

    [Fact]
    public void ExplainFlowJson_ShouldFormatNestedGenericTypes()
    {
        var registry = new FlowRegistry();
        registry.Register(
            "NestedFlow",
            CreateBlueprintWithStage<NestedArgs, Outer<int>.Inner<string>>(
                "NestedFlow",
                stageName: "s1",
                okValue: new Outer<int>.Inner<string>()));

        var catalog = new ModuleCatalog();
        catalog.Register<NestedArgs, Outer<int>.Inner<string>>("test.module", _ => new NestedModule());

        var result = ToolingJsonV1.ExplainFlowJson("NestedFlow", registry, catalog);

        Assert.Equal(0, result.ExitCode);
        Assert.Contains("Outer+Inner<System.Int32,System.String>", result.Json, StringComparison.Ordinal);
    }

    [Fact]
    public void DiffPatchJson_ShouldProduceStableJson()
    {
        var result = ToolingJsonV1.DiffPatchJson(
            oldPatchJson: "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}]}}}}}",
            newPatchJson: "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}},{\"id\":\"m2\",\"use\":\"test.module\",\"with\":{}}]}}}}}");

        Assert.Equal(0, result.ExitCode);

        Assert.Equal(
            "{\"kind\":\"diff\",\"module_diffs\":[{\"kind\":\"added\",\"flow_name\":\"HomeFeed\",\"stage_name\":\"s1\",\"module_id\":\"m2\",\"path\":\"$.flows.HomeFeed.stages.s1.modules[1]\",\"experiment_layer\":null,\"experiment_variant\":null}],\"param_diffs\":[],\"fanout_max_diffs\":[],\"emergency_diffs\":[]}",
            result.Json);
    }

    [Fact]
    public void DiffPatchJson_ShouldProduceStableErrorJson_WhenOldPatchJsonIsInvalid()
    {
        var result = ToolingJsonV1.DiffPatchJson(
            oldPatchJson: "not json",
            newPatchJson: "{\"schemaVersion\":\"v1\",\"flows\":{}}");

        Assert.Equal(2, result.ExitCode);

        Assert.Equal(
            "{\"kind\":\"diff\",\"error\":{\"code\":\"DIFF_INPUT_INVALID\",\"message\":\"oldPatchJson is not a valid JSON document.\"}}",
            result.Json);
    }

    [Fact]
    public void ExplainPatchJson_ShouldProduceStableJson()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":3,\"modules\":[" +
            "{\"id\":\"m_disabled\",\"use\":\"test.module\",\"with\":{}}," +
            "{\"id\":\"m_gate_false\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"experiment\":{\"layer\":\"l1\",\"in\":[\"B\"]}}}," +
            "{\"id\":\"m_high\",\"use\":\"test.module\",\"with\":{},\"priority\":10}," +
            "{\"id\":\"m_low\",\"use\":\"test.module\",\"with\":{},\"priority\":0}" +
            "]}},\"experiments\":[{\"layer\":\"l1\",\"variant\":\"A\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[" +
            "{\"id\":\"m_exp\",\"use\":\"test.module\",\"with\":{},\"priority\":5}" +
            "]}}}}]," +
            "\"emergency\":{\"reason\":\"r\",\"operator\":\"op\",\"ttl_minutes\":30,\"patch\":{\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[{\"id\":\"m_disabled\",\"enabled\":false}]}}}}" +
            "}}}";

        var requestOptions = new FlowRequestOptions(
            variants: new Dictionary<string, string>
            {
                { "l1", "A" },
            });

        var result = ToolingJsonV1.ExplainPatchJson(
            flowName: "HomeFeed",
            patchJson: patchJson,
            requestOptions);

        Assert.Equal(0, result.ExitCode);

        Assert.Equal(
            "{\"kind\":\"explain_patch\",\"flow_name\":\"HomeFeed\",\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null},{\"layer\":\"experiment\",\"experiment_layer\":\"l1\",\"experiment_variant\":\"A\"},{\"layer\":\"emergency\",\"experiment_layer\":null,\"experiment_variant\":null}],\"stages\":[{\"stage_name\":\"s1\",\"fanout_max\":1,\"modules\":[{\"module_id\":\"m_disabled\",\"module_type\":\"test.module\",\"enabled\":false,\"disabled_by_emergency\":true,\"priority\":0,\"decision_kind\":\"skip\",\"decision_code\":\"DISABLED\"},{\"module_id\":\"m_gate_false\",\"module_type\":\"test.module\",\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"decision_kind\":\"skip\",\"decision_code\":\"GATE_FALSE\"},{\"module_id\":\"m_high\",\"module_type\":\"test.module\",\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":10,\"decision_kind\":\"execute\",\"decision_code\":\"SELECTED\"},{\"module_id\":\"m_low\",\"module_type\":\"test.module\",\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"decision_kind\":\"skip\",\"decision_code\":\"FANOUT_TRIM\"},{\"module_id\":\"m_exp\",\"module_type\":\"test.module\",\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":5,\"decision_kind\":\"skip\",\"decision_code\":\"FANOUT_TRIM\"}]}]}",
            result.Json);
    }

    [Fact]
    public void ExplainPatchJson_ShouldIncludeMermaid_WhenRequested()
    {
        var patchJson = "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}]}}}}}";

        var result = ToolingJsonV1.ExplainPatchJson(
            flowName: "HomeFeed",
            patchJson: patchJson,
            requestOptions: default,
            includeMermaid: true);

        Assert.Equal(0, result.ExitCode);

        Assert.Equal(
            "{\"kind\":\"explain_patch\",\"flow_name\":\"HomeFeed\",\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null}],\"stages\":[{\"stage_name\":\"s1\",\"fanout_max\":1,\"modules\":[{\"module_id\":\"m1\",\"module_type\":\"test.module\",\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"decision_kind\":\"execute\",\"decision_code\":\"SELECTED\"}]}],\"mermaid\":\"flowchart TD\\n  s0[\\\"s1\\\\nfanout_max=1\\\"]\\n  s0 --> m0[\\\"m1\\\\nexecute\\\\nSELECTED\\\"]\\n\"}",
            result.Json);
    }

    private static FlowRegistry CreateRegistry()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<TestArgs, int>("HomeFeed", stageName: "s1", okValue: 0));
        return registry;
    }

    private static ModuleCatalog CreateCatalog()
    {
        var catalog = new ModuleCatalog();
        catalog.Register<TestArgs, int>("test.module", _ => new DummyModule());
        return catalog;
    }

    private static FlowBlueprint<TReq, TResp> CreateBlueprintWithStage<TReq, TResp>(string name, string stageName, TResp okValue)
    {
        return FlowBlueprint
            .Define<TReq, TResp>(name)
            .Stage(
                stageName,
                stage =>
                {
                    stage
                        .Step("n1", moduleType: "test.module")
                        .Join(
                            "join",
                            join: _ => new ValueTask<Outcome<TResp>>(Outcome<TResp>.Ok(okValue)));
                })
            .Build();
    }

    private sealed class TestArgs
    {
    }

    private sealed class DummyModule : IModule<TestArgs, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<TestArgs> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(0));
        }
    }

    private sealed class NestedArgs
    {
    }

    private sealed class Outer<T>
    {
        public sealed class Inner<U>
        {
        }
    }

    private sealed class NestedModule : IModule<NestedArgs, Outer<int>.Inner<string>>
    {
        public ValueTask<Outcome<Outer<int>.Inner<string>>> ExecuteAsync(ModuleContext<NestedArgs> context)
        {
            return new ValueTask<Outcome<Outer<int>.Inner<string>>>(
                Outcome<Outer<int>.Inner<string>>.Ok(new Outer<int>.Inner<string>()));
        }
    }
}
