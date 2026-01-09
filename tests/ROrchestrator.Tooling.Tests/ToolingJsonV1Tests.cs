using ROrchestrator.Core;
using ROrchestrator.Core.Blueprint;
using ROrchestrator.Core.Selectors;
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
            "{\"kind\":\"validate\",\"tooling_json_version\":\"v1\",\"is_valid\":false,\"findings\":[{\"severity\":\"error\",\"code\":\"CFG_MODULE_ARGS_MISSING\",\"path\":\"$.flows.HomeFeed.stages.s1.modules[0].with\",\"message\":\"modules[].with is required.\"}]}",
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
            "{\"kind\":\"explain\",\"tooling_json_version\":\"v1\",\"flow_name\":\"HomeFeed\",\"plan_template_hash\":\"EB1FEB02AEE93B82\",\"nodes\":[{\"kind\":\"step\",\"name\":\"n1\",\"stage_name\":\"s1\",\"module_type\":\"test.module\",\"output_type\":\"System.Int32\"},{\"kind\":\"join\",\"name\":\"join\",\"stage_name\":\"s1\",\"module_type\":null,\"output_type\":\"System.Int32\"}]}",
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
            "{\"kind\":\"explain\",\"tooling_json_version\":\"v1\",\"flow_name\":\"HomeFeed\",\"plan_template_hash\":\"EB1FEB02AEE93B82\",\"nodes\":[{\"kind\":\"step\",\"name\":\"n1\",\"stage_name\":\"s1\",\"module_type\":\"test.module\",\"output_type\":\"System.Int32\"},{\"kind\":\"join\",\"name\":\"join\",\"stage_name\":\"s1\",\"module_type\":null,\"output_type\":\"System.Int32\"}],\"mermaid\":\"flowchart TD\\n  n0[\\\"n1\\\\nstep\\\\n(test.module)\\\\nSystem.Int32\\\"] --> n1[\\\"join\\\\njoin\\\\nSystem.Int32\\\"]\\n\"}",
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
            "{\"kind\":\"diff\",\"tooling_json_version\":\"v1\",\"module_diffs\":[{\"kind\":\"added\",\"flow_name\":\"HomeFeed\",\"stage_name\":\"s1\",\"module_id\":\"m2\",\"path\":\"$.flows.HomeFeed.stages.s1.modules[1]\",\"experiment_layer\":null,\"experiment_variant\":null}],\"param_diffs\":[],\"fanout_max_diffs\":[],\"emergency_diffs\":[],\"risk_report\":{\"level\":\"medium\",\"fanout_increase_count\":0,\"module_added_count\":1,\"module_removed_count\":0,\"shadow_change_count\":0,\"param_change_count\":0,\"emergency_change_count\":0}}",
            result.Json);
    }

    [Fact]
    public void PreviewMatrixJson_ShouldProduceStableJson()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"priority\":0}," +
            "{\"id\":\"m2\",\"use\":\"test.module\",\"with\":{},\"priority\":10}" +
            "]}},\"experiments\":[{\"layer\":\"l1\",\"variant\":\"B\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[" +
            "{\"id\":\"m3\",\"use\":\"test.module\",\"with\":{},\"priority\":20}" +
            "]}}}}]" +
            "}}}";

        var variantsMatrix = new Dictionary<string, string>[]
        {
            new() { { "l1", "B" } },
            new() { { "l1", "A" } },
        };

        var result = ToolingJsonV1.PreviewMatrixJson(flowName: "HomeFeed", patchJson: patchJson, variantsMatrix: variantsMatrix);

        Assert.Equal(0, result.ExitCode);
        Assert.Equal(
            "{\"kind\":\"preview_matrix\",\"tooling_json_version\":\"v1\",\"flow_name\":\"HomeFeed\",\"qos\":{\"selected_tier\":\"full\"},\"previews\":[{\"variants\":{\"l1\":\"A\"},\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null}],\"stages\":[{\"stage_name\":\"s1\",\"fanout_max\":1,\"selected_module_ids\":[\"m2\"],\"selected_shadow_module_ids\":[]}]},{\"variants\":{\"l1\":\"B\"},\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null},{\"layer\":\"experiment\",\"experiment_layer\":\"l1\",\"experiment_variant\":\"B\"}],\"stages\":[{\"stage_name\":\"s1\",\"fanout_max\":1,\"selected_module_ids\":[\"m3\"],\"selected_shadow_module_ids\":[]}]}]}",
            result.Json);
    }

    [Fact]
    public void DiffPatchJson_ShouldReportShadowDiffs_AndProduceStableJson()
    {
        var result = ToolingJsonV1.DiffPatchJson(
            oldPatchJson: "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[" +
                         "{\"id\":\"m_shadow_added\",\"use\":\"test.module\",\"with\":{}}," +
                         "{\"id\":\"m_shadow_removed\",\"use\":\"test.module\",\"with\":{},\"shadow\":{\"sample\":0.5}}," +
                         "{\"id\":\"m_shadow_sample\",\"use\":\"test.module\",\"with\":{},\"shadow\":{\"sample\":0.1}}" +
                         "]}}}}}",
            newPatchJson: "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[" +
                         "{\"id\":\"m_shadow_added\",\"use\":\"test.module\",\"with\":{},\"shadow\":{\"sample\":0.3}}," +
                         "{\"id\":\"m_shadow_removed\",\"use\":\"test.module\",\"with\":{}}," +
                         "{\"id\":\"m_shadow_sample\",\"use\":\"test.module\",\"with\":{},\"shadow\":{\"sample\":0.2}}" +
                         "]}}}}}");

        Assert.Equal(0, result.ExitCode);

        Assert.Equal(
            "{\"kind\":\"diff\",\"tooling_json_version\":\"v1\",\"module_diffs\":[{\"kind\":\"shadow_added\",\"flow_name\":\"HomeFeed\",\"stage_name\":\"s1\",\"module_id\":\"m_shadow_added\",\"path\":\"$.flows.HomeFeed.stages.s1.modules[0].shadow\",\"experiment_layer\":null,\"experiment_variant\":null},{\"kind\":\"shadow_removed\",\"flow_name\":\"HomeFeed\",\"stage_name\":\"s1\",\"module_id\":\"m_shadow_removed\",\"path\":\"$.flows.HomeFeed.stages.s1.modules[1].shadow\",\"experiment_layer\":null,\"experiment_variant\":null},{\"kind\":\"shadow_sample_changed\",\"flow_name\":\"HomeFeed\",\"stage_name\":\"s1\",\"module_id\":\"m_shadow_sample\",\"path\":\"$.flows.HomeFeed.stages.s1.modules[2].shadow.sample\",\"experiment_layer\":null,\"experiment_variant\":null}],\"param_diffs\":[],\"fanout_max_diffs\":[],\"emergency_diffs\":[],\"risk_report\":{\"level\":\"medium\",\"fanout_increase_count\":0,\"module_added_count\":0,\"module_removed_count\":0,\"shadow_change_count\":3,\"param_change_count\":0,\"emergency_change_count\":0}}",
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
            "{\"kind\":\"diff\",\"tooling_json_version\":\"v1\",\"error\":{\"code\":\"DIFF_INPUT_INVALID\",\"message\":\"oldPatchJson is not a valid JSON document.\"}}",
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
            "{\"id\":\"m_low\",\"use\":\"test.module\",\"with\":{},\"priority\":0}," +
            "{\"id\":\"m_shadow\",\"use\":\"test.module\",\"with\":{},\"shadow\":{\"sample\":1}}" +
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
            "{\"kind\":\"explain_patch\",\"tooling_json_version\":\"v1\",\"flow_name\":\"HomeFeed\",\"qos\":{\"selected_tier\":\"full\"},\"variants\":{\"l1\":\"A\"},\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null},{\"layer\":\"experiment\",\"experiment_layer\":\"l1\",\"experiment_variant\":\"A\"},{\"layer\":\"emergency\",\"experiment_layer\":null,\"experiment_variant\":null}],\"stages\":[{\"stage_name\":\"s1\",\"fanout_max\":1,\"modules\":[{\"module_id\":\"m_disabled\",\"module_type\":\"test.module\",\"enabled\":false,\"disabled_by_emergency\":true,\"priority\":0,\"gate_decision_code\":null,\"gate_selector_name\":null,\"decision_kind\":\"skip\",\"decision_code\":\"DISABLED\"},{\"module_id\":\"m_gate_false\",\"module_type\":\"test.module\",\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"gate_decision_code\":\"GATE_FALSE\",\"gate_selector_name\":null,\"decision_kind\":\"skip\",\"decision_code\":\"GATE_FALSE\"},{\"module_id\":\"m_high\",\"module_type\":\"test.module\",\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":10,\"gate_decision_code\":null,\"gate_selector_name\":null,\"decision_kind\":\"execute\",\"decision_code\":\"SELECTED\"},{\"module_id\":\"m_low\",\"module_type\":\"test.module\",\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"gate_decision_code\":null,\"gate_selector_name\":null,\"decision_kind\":\"skip\",\"decision_code\":\"FANOUT_TRIM\"},{\"module_id\":\"m_exp\",\"module_type\":\"test.module\",\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":5,\"gate_decision_code\":null,\"gate_selector_name\":null,\"decision_kind\":\"skip\",\"decision_code\":\"FANOUT_TRIM\"}],\"shadow_modules\":[{\"module_id\":\"m_shadow\",\"module_type\":\"test.module\",\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"shadow_sample_rate_bps\":10000,\"gate_decision_code\":null,\"gate_selector_name\":null,\"decision_kind\":\"execute\",\"decision_code\":\"SELECTED\"}]}]}",
            result.Json);
    }

    [Fact]
    public void ExplainPatchJson_ShouldIncludeMermaid_WhenRequested()
    {
        var patchJson = "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}},{\"id\":\"m_shadow\",\"use\":\"test.module\",\"with\":{},\"shadow\":{\"sample\":1}}]}}}}}";

        var result = ToolingJsonV1.ExplainPatchJson(
            flowName: "HomeFeed",
            patchJson: patchJson,
            requestOptions: default,
            includeMermaid: true);

        Assert.Equal(0, result.ExitCode);

        Assert.Equal(
            "{\"kind\":\"explain_patch\",\"tooling_json_version\":\"v1\",\"flow_name\":\"HomeFeed\",\"qos\":{\"selected_tier\":\"full\"},\"variants\":null,\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null}],\"stages\":[{\"stage_name\":\"s1\",\"fanout_max\":1,\"modules\":[{\"module_id\":\"m1\",\"module_type\":\"test.module\",\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"gate_decision_code\":null,\"gate_selector_name\":null,\"decision_kind\":\"execute\",\"decision_code\":\"SELECTED\"}],\"shadow_modules\":[{\"module_id\":\"m_shadow\",\"module_type\":\"test.module\",\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"shadow_sample_rate_bps\":10000,\"gate_decision_code\":null,\"gate_selector_name\":null,\"decision_kind\":\"execute\",\"decision_code\":\"SELECTED\"}]}],\"mermaid\":\"flowchart TD\\n  s0[\\\"s1\\\\nfanout_max=1\\\"]\\n  s0 --> m0[\\\"m1\\\\nexecute\\\\nSELECTED\\\"]\\n  s0 -.-> m1[\\\"m_shadow\\\\nshadow\\\\nsample_bps=10000\\\\nexecute\\\\nSELECTED\\\"]\\n\"}",
            result.Json);
    }

    [Fact]
    public void ExplainPatchJson_ShouldEvaluateSelectorGate_WhenSelectorRegistryIsProvided()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":2,\"modules\":[" +
            "{\"id\":\"m_sel_true\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"selector\":\"always_true\"}}," +
            "{\"id\":\"m_sel_false\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"selector\":\"always_false\"}}" +
            "]}}}}}";

        var selectors = new SelectorRegistry();
        selectors.Register("always_true", _ => true);
        selectors.Register("always_false", _ => false);

        var result = ToolingJsonV1.ExplainPatchJson(
            flowName: "HomeFeed",
            patchJson: patchJson,
            requestOptions: default,
            includeMermaid: false,
            selectorRegistry: selectors);

        Assert.Equal(0, result.ExitCode);

        Assert.Equal(
            "{\"kind\":\"explain_patch\",\"tooling_json_version\":\"v1\",\"flow_name\":\"HomeFeed\",\"qos\":{\"selected_tier\":\"full\"},\"variants\":null,\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null}],\"stages\":[{\"stage_name\":\"s1\",\"fanout_max\":2,\"modules\":[{\"module_id\":\"m_sel_true\",\"module_type\":\"test.module\",\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"gate_decision_code\":\"GATE_TRUE\",\"gate_selector_name\":\"always_true\",\"decision_kind\":\"execute\",\"decision_code\":\"SELECTED\"},{\"module_id\":\"m_sel_false\",\"module_type\":\"test.module\",\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"gate_decision_code\":\"GATE_FALSE\",\"gate_selector_name\":\"always_false\",\"decision_kind\":\"skip\",\"decision_code\":\"GATE_FALSE\"}],\"shadow_modules\":[]}]}",
            result.Json);
    }

    [Fact]
    public void ExplainPatchJson_ShouldSortVariantsByKey()
    {
        var patchJson = "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}]}}}}}";

        var requestOptions = new FlowRequestOptions(
            variants: new Dictionary<string, string>
            {
                { "l2", "A" },
                { "l1", "B" },
            });

        var result = ToolingJsonV1.ExplainPatchJson(
            flowName: "HomeFeed",
            patchJson: patchJson,
            requestOptions: requestOptions,
            includeMermaid: false);

        Assert.Equal(0, result.ExitCode);

        Assert.Equal(
            "{\"kind\":\"explain_patch\",\"tooling_json_version\":\"v1\",\"flow_name\":\"HomeFeed\",\"qos\":{\"selected_tier\":\"full\"},\"variants\":{\"l1\":\"B\",\"l2\":\"A\"},\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null}],\"stages\":[{\"stage_name\":\"s1\",\"fanout_max\":1,\"modules\":[{\"module_id\":\"m1\",\"module_type\":\"test.module\",\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"gate_decision_code\":null,\"gate_selector_name\":null,\"decision_kind\":\"execute\",\"decision_code\":\"SELECTED\"}],\"shadow_modules\":[]}]}",
            result.Json);
    }

    [Fact]
    public void ExplainPatchJson_ShouldApplyQosOverlay_WhenTierIsEmergency()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":2,\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}," +
            "{\"id\":\"m2\",\"use\":\"test.module\",\"with\":{}}" +
            "]}},\"qos\":{\"tiers\":{\"emergency\":{\"patch\":{\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[{\"id\":\"m2\",\"enabled\":false}]}}}}}}" +
            "}}}";

        var full = ToolingJsonV1.ExplainPatchJson(flowName: "HomeFeed", patchJson: patchJson, qosTier: QosTier.Full);
        var emergency = ToolingJsonV1.ExplainPatchJson(flowName: "HomeFeed", patchJson: patchJson, qosTier: QosTier.Emergency);

        Assert.Equal(0, full.ExitCode);
        Assert.Contains("\"qos\":{\"selected_tier\":\"full\"}", full.Json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"layer\":\"qos\"", full.Json, StringComparison.Ordinal);
        Assert.Contains("\"fanout_max\":2", full.Json, StringComparison.Ordinal);

        Assert.Equal(0, emergency.ExitCode);
        Assert.Contains("\"qos\":{\"selected_tier\":\"emergency\"}", emergency.Json, StringComparison.Ordinal);
        Assert.Contains("\"layer\":\"qos\"", emergency.Json, StringComparison.Ordinal);
        Assert.Contains("\"fanout_max\":1", emergency.Json, StringComparison.Ordinal);
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
