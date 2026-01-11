using Rockestra.Core;
using Rockestra.Core.Blueprint;
using Rockestra.Core.Selectors;
using Rockestra.Tooling;

namespace Rockestra.Tooling.Tests;

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
            "{\"kind\":\"explain\",\"tooling_json_version\":\"v1\",\"flow_name\":\"HomeFeed\",\"plan_template_hash\":\"01BF10E88E40BACA\",\"stages\":[{\"stage_name\":\"s1\",\"contract\":{\"allows_dynamic_modules\":true,\"allows_shadow_modules\":true,\"allowed_module_types\":null,\"max_modules_warn\":0,\"max_modules_hard\":0,\"max_shadow_modules_hard\":0,\"max_shadow_sample_bps\":10000,\"min_fanout_max\":0,\"max_fanout_max\":8}}],\"nodes\":[{\"kind\":\"step\",\"name\":\"n1\",\"stage_name\":\"s1\",\"module_type\":\"test.module\",\"output_type\":\"System.Int32\"},{\"kind\":\"join\",\"name\":\"join\",\"stage_name\":\"s1\",\"module_type\":null,\"output_type\":\"System.Int32\"}]}",
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
            "{\"kind\":\"explain\",\"tooling_json_version\":\"v1\",\"flow_name\":\"HomeFeed\",\"plan_template_hash\":\"01BF10E88E40BACA\",\"stages\":[{\"stage_name\":\"s1\",\"contract\":{\"allows_dynamic_modules\":true,\"allows_shadow_modules\":true,\"allowed_module_types\":null,\"max_modules_warn\":0,\"max_modules_hard\":0,\"max_shadow_modules_hard\":0,\"max_shadow_sample_bps\":10000,\"min_fanout_max\":0,\"max_fanout_max\":8}}],\"nodes\":[{\"kind\":\"step\",\"name\":\"n1\",\"stage_name\":\"s1\",\"module_type\":\"test.module\",\"output_type\":\"System.Int32\"},{\"kind\":\"join\",\"name\":\"join\",\"stage_name\":\"s1\",\"module_type\":null,\"output_type\":\"System.Int32\"}],\"mermaid\":\"flowchart TD\\n  n0[\\\"n1\\\\nstep\\\\n(test.module)\\\\nSystem.Int32\\\"] --> n1[\\\"join\\\\njoin\\\\nSystem.Int32\\\"]\\n\"}",
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
            "{\"kind\":\"diff\",\"tooling_json_version\":\"v1\",\"module_diffs\":[{\"kind\":\"added\",\"flow_name\":\"HomeFeed\",\"stage_name\":\"s1\",\"module_id\":\"m2\",\"path\":\"$.flows.HomeFeed.stages.s1.modules[1]\",\"experiment_layer\":null,\"experiment_variant\":null}],\"qos_tier_diffs\":[],\"limit_diffs\":[],\"param_diffs\":[],\"fanout_max_diffs\":[],\"emergency_diffs\":[],\"risk_report\":{\"level\":\"medium\",\"fanout_increase_count\":0,\"module_added_count\":1,\"module_removed_count\":0,\"shadow_change_count\":0,\"limit_key_change_count\":0,\"limit_change_count\":0,\"qos_change_count\":0,\"param_change_count\":0,\"emergency_change_count\":0}}",
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
            "{\"kind\":\"preview_matrix\",\"tooling_json_version\":\"v1\",\"flow_name\":\"HomeFeed\",\"qos\":{\"selected_tier\":\"full\",\"reason_code\":null,\"signals\":null},\"previews\":[{\"variants\":{\"l1\":\"A\"},\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null}],\"params\":{\"effective\":null,\"sources\":null},\"stages\":[{\"stage_name\":\"s1\",\"fanout_max\":1,\"selected_modules\":[{\"module_id\":\"m2\",\"limit_key\":\"test.module\",\"max_in_flight\":null}],\"selected_shadow_modules\":[]}]},{\"variants\":{\"l1\":\"B\"},\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null},{\"layer\":\"experiment\",\"experiment_layer\":\"l1\",\"experiment_variant\":\"B\"}],\"params\":{\"effective\":null,\"sources\":null},\"stages\":[{\"stage_name\":\"s1\",\"fanout_max\":1,\"selected_modules\":[{\"module_id\":\"m3\",\"limit_key\":\"test.module\",\"max_in_flight\":null}],\"selected_shadow_modules\":[]}]}]}",
            result.Json);
    }

    [Fact]
    public void PreviewMatrixJson_ShouldExposeBulkheadConfiguration_AndProduceStableJson()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"limits\":{\"moduleConcurrency\":{\"maxInFlight\":{\"depA\":3,\"test.module\":5}}},\"flows\":{\"HomeFeed\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":2,\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"limitKey\":\"depA\"}," +
            "{\"id\":\"m2\",\"use\":\"test.module\",\"with\":{}}," +
            "{\"id\":\"m_shadow\",\"use\":\"test.module\",\"with\":{},\"limitKey\":\"depA\",\"shadow\":{\"sample\":1}}" +
            "]}}}}}";

        var variantsMatrix = new Dictionary<string, string>[]
        {
            new(),
        };

        var result = ToolingJsonV1.PreviewMatrixJson(flowName: "HomeFeed", patchJson: patchJson, variantsMatrix: variantsMatrix);

        Assert.Equal(0, result.ExitCode);
        Assert.Equal(
            "{\"kind\":\"preview_matrix\",\"tooling_json_version\":\"v1\",\"flow_name\":\"HomeFeed\",\"qos\":{\"selected_tier\":\"full\",\"reason_code\":null,\"signals\":null},\"previews\":[{\"variants\":{},\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null}],\"params\":{\"effective\":null,\"sources\":null},\"stages\":[{\"stage_name\":\"s1\",\"fanout_max\":2,\"selected_modules\":[{\"module_id\":\"m1\",\"limit_key\":\"depA\",\"max_in_flight\":3},{\"module_id\":\"m2\",\"limit_key\":\"test.module\",\"max_in_flight\":5}],\"selected_shadow_modules\":[{\"module_id\":\"m_shadow\",\"limit_key\":\"depA\",\"max_in_flight\":3}]}]}]}",
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
            "{\"kind\":\"diff\",\"tooling_json_version\":\"v1\",\"module_diffs\":[{\"kind\":\"shadow_added\",\"flow_name\":\"HomeFeed\",\"stage_name\":\"s1\",\"module_id\":\"m_shadow_added\",\"path\":\"$.flows.HomeFeed.stages.s1.modules[0].shadow\",\"experiment_layer\":null,\"experiment_variant\":null},{\"kind\":\"shadow_removed\",\"flow_name\":\"HomeFeed\",\"stage_name\":\"s1\",\"module_id\":\"m_shadow_removed\",\"path\":\"$.flows.HomeFeed.stages.s1.modules[1].shadow\",\"experiment_layer\":null,\"experiment_variant\":null},{\"kind\":\"shadow_sample_changed\",\"flow_name\":\"HomeFeed\",\"stage_name\":\"s1\",\"module_id\":\"m_shadow_sample\",\"path\":\"$.flows.HomeFeed.stages.s1.modules[2].shadow.sample\",\"experiment_layer\":null,\"experiment_variant\":null}],\"qos_tier_diffs\":[],\"limit_diffs\":[],\"param_diffs\":[],\"fanout_max_diffs\":[],\"emergency_diffs\":[],\"risk_report\":{\"level\":\"medium\",\"fanout_increase_count\":0,\"module_added_count\":0,\"module_removed_count\":0,\"shadow_change_count\":3,\"limit_key_change_count\":0,\"limit_change_count\":0,\"qos_change_count\":0,\"param_change_count\":0,\"emergency_change_count\":0}}",
            result.Json);
    }

    [Fact]
    public void DiffPatchJson_ShouldReportQosLimitsAndLimitKeyDiffs_AndProduceStableJson()
    {
        var oldPatchJson =
            "{\"schemaVersion\":\"v1\",\"limits\":{\"moduleConcurrency\":{\"maxInFlight\":{\"depA\":10}}},\"flows\":{\"HomeFeed\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":3,\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"limitKey\":\"depA\"}," +
            "{\"id\":\"m_shadow\",\"use\":\"test.module\",\"with\":{},\"limitKey\":\"depA\",\"shadow\":{\"sample\":0.5}}" +
            "]}},\"experiments\":[{\"layer\":\"l1\",\"variant\":\"B\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[" +
            "{\"id\":\"m_exp\",\"use\":\"test.module\",\"with\":{},\"limitKey\":\"depA\"}" +
            "]}}}}],\"qos\":{\"tiers\":{\"emergency\":{\"patch\":{\"stages\":{\"s1\":{" +
            "\"fanoutMax\":2,\"modules\":[{\"id\":\"m1\",\"enabled\":false},{\"id\":\"m_shadow\",\"shadow\":{\"sample\":0.2}}]" +
            "}}}},\"conserve\":{\"patch\":{}}}}}}}";

        var newPatchJson =
            "{\"schemaVersion\":\"v1\",\"limits\":{\"moduleConcurrency\":{\"maxInFlight\":{\"depA\":5,\"depB\":1}}},\"flows\":{\"HomeFeed\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":3,\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"limitKey\":\"depB\"}," +
            "{\"id\":\"m_shadow\",\"use\":\"test.module\",\"with\":{},\"limitKey\":\"depA\",\"shadow\":{\"sample\":0.5}}" +
            "]}},\"experiments\":[{\"layer\":\"l1\",\"variant\":\"B\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[" +
            "{\"id\":\"m_exp\",\"use\":\"test.module\",\"with\":{},\"limitKey\":\"depB\"}" +
            "]}}}}],\"qos\":{\"tiers\":{\"emergency\":{\"patch\":{\"stages\":{\"s1\":{" +
            "\"fanoutMax\":1,\"modules\":[{\"id\":\"m1\",\"enabled\":true},{\"id\":\"m_shadow\",\"shadow\":{\"sample\":0.1}}]" +
            "}}}},\"fallback\":{\"patch\":{}}}}}}}";

        var result = ToolingJsonV1.DiffPatchJson(oldPatchJson: oldPatchJson, newPatchJson: newPatchJson);

        Assert.Equal(0, result.ExitCode);

        Assert.Equal(
            "{\"kind\":\"diff\",\"tooling_json_version\":\"v1\"," +
            "\"module_diffs\":[" +
            "{\"kind\":\"limit_key_changed\",\"flow_name\":\"HomeFeed\",\"stage_name\":\"s1\",\"module_id\":\"m1\",\"path\":\"$.flows.HomeFeed.stages.s1.modules[0].limitKey\",\"experiment_layer\":null,\"experiment_variant\":null}," +
            "{\"kind\":\"limit_key_changed\",\"flow_name\":\"HomeFeed\",\"stage_name\":\"s1\",\"module_id\":\"m_exp\",\"path\":\"$.flows.HomeFeed.experiments[0].patch.stages.s1.modules[0].limitKey\",\"experiment_layer\":\"l1\",\"experiment_variant\":\"B\"}" +
            "]," +
            "\"qos_tier_diffs\":[" +
            "{\"kind\":\"removed\",\"flow_name\":\"HomeFeed\",\"qos_tier\":\"conserve\",\"path\":\"$.flows.HomeFeed.qos.tiers.conserve.patch\"}," +
            "{\"kind\":\"changed\",\"flow_name\":\"HomeFeed\",\"qos_tier\":\"emergency\",\"path\":\"$.flows.HomeFeed.qos.tiers.emergency.patch.stages.s1.fanoutMax\"}," +
            "{\"kind\":\"changed\",\"flow_name\":\"HomeFeed\",\"qos_tier\":\"emergency\",\"path\":\"$.flows.HomeFeed.qos.tiers.emergency.patch.stages.s1.modules[0].enabled\"}," +
            "{\"kind\":\"changed\",\"flow_name\":\"HomeFeed\",\"qos_tier\":\"emergency\",\"path\":\"$.flows.HomeFeed.qos.tiers.emergency.patch.stages.s1.modules[1].shadow.sample\"}," +
            "{\"kind\":\"added\",\"flow_name\":\"HomeFeed\",\"qos_tier\":\"fallback\",\"path\":\"$.flows.HomeFeed.qos.tiers.fallback.patch\"}" +
            "]," +
            "\"limit_diffs\":[" +
            "{\"kind\":\"changed\",\"limit_key\":\"depA\",\"path\":\"$.limits.moduleConcurrency.maxInFlight.depA\",\"old_max_in_flight\":10,\"new_max_in_flight\":5}," +
            "{\"kind\":\"added\",\"limit_key\":\"depB\",\"path\":\"$.limits.moduleConcurrency.maxInFlight.depB\",\"old_max_in_flight\":null,\"new_max_in_flight\":1}" +
            "]," +
            "\"param_diffs\":[],\"fanout_max_diffs\":[],\"emergency_diffs\":[]," +
            "\"risk_report\":{\"level\":\"high\",\"fanout_increase_count\":0,\"module_added_count\":0,\"module_removed_count\":0,\"shadow_change_count\":0,\"limit_key_change_count\":2,\"limit_change_count\":2,\"qos_change_count\":5,\"param_change_count\":0,\"emergency_change_count\":0}}",
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
            "{\"kind\":\"explain_patch\",\"tooling_json_version\":\"v1\",\"flow_name\":\"HomeFeed\",\"qos\":{\"selected_tier\":\"full\",\"reason_code\":null,\"signals\":null},\"variants\":{\"l1\":\"A\"},\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null},{\"layer\":\"experiment\",\"experiment_layer\":\"l1\",\"experiment_variant\":\"A\"},{\"layer\":\"emergency\",\"experiment_layer\":null,\"experiment_variant\":null}],\"params\":{\"effective\":null,\"sources\":null},\"stages\":[{\"stage_name\":\"s1\",\"fanout_max\":1,\"modules\":[{\"module_id\":\"m_disabled\",\"module_type\":\"test.module\",\"limit_key\":\"test.module\",\"max_in_flight\":null,\"enabled\":false,\"disabled_by_emergency\":true,\"priority\":0,\"gate_decision_code\":null,\"gate_reason_code\":null,\"gate_selector_name\":null,\"decision_kind\":\"skip\",\"decision_code\":\"DISABLED\"},{\"module_id\":\"m_gate_false\",\"module_type\":\"test.module\",\"limit_key\":\"test.module\",\"max_in_flight\":null,\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"gate_decision_code\":\"GATE_FALSE\",\"gate_reason_code\":\"VARIANT_MISMATCH\",\"gate_selector_name\":null,\"decision_kind\":\"skip\",\"decision_code\":\"GATE_FALSE\"},{\"module_id\":\"m_high\",\"module_type\":\"test.module\",\"limit_key\":\"test.module\",\"max_in_flight\":null,\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":10,\"gate_decision_code\":null,\"gate_reason_code\":null,\"gate_selector_name\":null,\"decision_kind\":\"execute\",\"decision_code\":\"SELECTED\"},{\"module_id\":\"m_low\",\"module_type\":\"test.module\",\"limit_key\":\"test.module\",\"max_in_flight\":null,\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"gate_decision_code\":null,\"gate_reason_code\":null,\"gate_selector_name\":null,\"decision_kind\":\"skip\",\"decision_code\":\"FANOUT_TRIM\"},{\"module_id\":\"m_exp\",\"module_type\":\"test.module\",\"limit_key\":\"test.module\",\"max_in_flight\":null,\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":5,\"gate_decision_code\":null,\"gate_reason_code\":null,\"gate_selector_name\":null,\"decision_kind\":\"skip\",\"decision_code\":\"FANOUT_TRIM\"}],\"shadow_modules\":[{\"module_id\":\"m_shadow\",\"module_type\":\"test.module\",\"limit_key\":\"test.module\",\"max_in_flight\":null,\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"shadow_sample_rate_bps\":10000,\"gate_decision_code\":null,\"gate_reason_code\":null,\"gate_selector_name\":null,\"decision_kind\":\"execute\",\"decision_code\":\"SELECTED\"}]}]}",
            result.Json);
    }

    [Fact]
    public void ExplainPatchJson_ShouldExposeBulkheadConfiguration_AndProduceStableJson()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"limits\":{\"moduleConcurrency\":{\"maxInFlight\":{\"depA\":3,\"test.module\":5}}},\"flows\":{\"HomeFeed\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":2,\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"limitKey\":\"depA\"}," +
            "{\"id\":\"m2\",\"use\":\"test.module\",\"with\":{}}," +
            "{\"id\":\"m_shadow\",\"use\":\"test.module\",\"with\":{},\"limitKey\":\"depA\",\"shadow\":{\"sample\":1}}" +
            "]}}}}}";

        var result = ToolingJsonV1.ExplainPatchJson(flowName: "HomeFeed", patchJson: patchJson);

        Assert.Equal(0, result.ExitCode);
        Assert.Equal(
            "{\"kind\":\"explain_patch\",\"tooling_json_version\":\"v1\",\"flow_name\":\"HomeFeed\",\"qos\":{\"selected_tier\":\"full\",\"reason_code\":null,\"signals\":null},\"variants\":null,\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null}],\"params\":{\"effective\":null,\"sources\":null},\"stages\":[{\"stage_name\":\"s1\",\"fanout_max\":2,\"modules\":[{\"module_id\":\"m1\",\"module_type\":\"test.module\",\"limit_key\":\"depA\",\"max_in_flight\":3,\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"gate_decision_code\":null,\"gate_reason_code\":null,\"gate_selector_name\":null,\"decision_kind\":\"execute\",\"decision_code\":\"SELECTED\"},{\"module_id\":\"m2\",\"module_type\":\"test.module\",\"limit_key\":\"test.module\",\"max_in_flight\":5,\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"gate_decision_code\":null,\"gate_reason_code\":null,\"gate_selector_name\":null,\"decision_kind\":\"execute\",\"decision_code\":\"SELECTED\"}],\"shadow_modules\":[{\"module_id\":\"m_shadow\",\"module_type\":\"test.module\",\"limit_key\":\"depA\",\"max_in_flight\":3,\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"shadow_sample_rate_bps\":10000,\"gate_decision_code\":null,\"gate_reason_code\":null,\"gate_selector_name\":null,\"decision_kind\":\"execute\",\"decision_code\":\"SELECTED\"}]}]}",
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
            "{\"kind\":\"explain_patch\",\"tooling_json_version\":\"v1\",\"flow_name\":\"HomeFeed\",\"qos\":{\"selected_tier\":\"full\",\"reason_code\":null,\"signals\":null},\"variants\":null,\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null}],\"params\":{\"effective\":null,\"sources\":null},\"stages\":[{\"stage_name\":\"s1\",\"fanout_max\":1,\"modules\":[{\"module_id\":\"m1\",\"module_type\":\"test.module\",\"limit_key\":\"test.module\",\"max_in_flight\":null,\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"gate_decision_code\":null,\"gate_reason_code\":null,\"gate_selector_name\":null,\"decision_kind\":\"execute\",\"decision_code\":\"SELECTED\"}],\"shadow_modules\":[{\"module_id\":\"m_shadow\",\"module_type\":\"test.module\",\"limit_key\":\"test.module\",\"max_in_flight\":null,\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"shadow_sample_rate_bps\":10000,\"gate_decision_code\":null,\"gate_reason_code\":null,\"gate_selector_name\":null,\"decision_kind\":\"execute\",\"decision_code\":\"SELECTED\"}]}],\"mermaid\":\"flowchart TD\\n  s0[\\\"s1\\\\nfanout_max=1\\\"]\\n  s0 --> m0[\\\"m1\\\\nexecute\\\\nSELECTED\\\"]\\n  s0 -.-> m1[\\\"m_shadow\\\\nshadow\\\\nsample_bps=10000\\\\nexecute\\\\nSELECTED\\\"]\\n\"}",
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
            "{\"kind\":\"explain_patch\",\"tooling_json_version\":\"v1\",\"flow_name\":\"HomeFeed\",\"qos\":{\"selected_tier\":\"full\",\"reason_code\":null,\"signals\":null},\"variants\":null,\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null}],\"params\":{\"effective\":null,\"sources\":null},\"stages\":[{\"stage_name\":\"s1\",\"fanout_max\":2,\"modules\":[{\"module_id\":\"m_sel_true\",\"module_type\":\"test.module\",\"limit_key\":\"test.module\",\"max_in_flight\":null,\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"gate_decision_code\":\"GATE_TRUE\",\"gate_reason_code\":\"SELECTOR_TRUE\",\"gate_selector_name\":\"always_true\",\"decision_kind\":\"execute\",\"decision_code\":\"SELECTED\"},{\"module_id\":\"m_sel_false\",\"module_type\":\"test.module\",\"limit_key\":\"test.module\",\"max_in_flight\":null,\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"gate_decision_code\":\"GATE_FALSE\",\"gate_reason_code\":\"SELECTOR_FALSE\",\"gate_selector_name\":\"always_false\",\"decision_kind\":\"skip\",\"decision_code\":\"GATE_FALSE\"}],\"shadow_modules\":[]}]}",
            result.Json);
    }

    [Fact]
    public void ExplainPatchJson_ShouldEvaluateCompositeSelectorGate_WhenSelectorRegistryIsProvided()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":2,\"modules\":[" +
            "{\"id\":\"m_comp_true\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"any\":[{\"selector\":\"always_false\"},{\"selector\":\"always_true\"}]}}," +
            "{\"id\":\"m_comp_false\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"all\":[{\"selector\":\"always_true\"},{\"selector\":\"always_false\"}]}}" +
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
            "{\"kind\":\"explain_patch\",\"tooling_json_version\":\"v1\",\"flow_name\":\"HomeFeed\",\"qos\":{\"selected_tier\":\"full\",\"reason_code\":null,\"signals\":null},\"variants\":null,\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null}],\"params\":{\"effective\":null,\"sources\":null},\"stages\":[{\"stage_name\":\"s1\",\"fanout_max\":2,\"modules\":[{\"module_id\":\"m_comp_true\",\"module_type\":\"test.module\",\"limit_key\":\"test.module\",\"max_in_flight\":null,\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"gate_decision_code\":\"GATE_TRUE\",\"gate_reason_code\":\"SELECTOR_TRUE\",\"gate_selector_name\":null,\"decision_kind\":\"execute\",\"decision_code\":\"SELECTED\"},{\"module_id\":\"m_comp_false\",\"module_type\":\"test.module\",\"limit_key\":\"test.module\",\"max_in_flight\":null,\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"gate_decision_code\":\"GATE_FALSE\",\"gate_reason_code\":\"SELECTOR_FALSE\",\"gate_selector_name\":null,\"decision_kind\":\"skip\",\"decision_code\":\"GATE_FALSE\"}],\"shadow_modules\":[]}]}",
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
            "{\"kind\":\"explain_patch\",\"tooling_json_version\":\"v1\",\"flow_name\":\"HomeFeed\",\"qos\":{\"selected_tier\":\"full\",\"reason_code\":null,\"signals\":null},\"variants\":{\"l1\":\"B\",\"l2\":\"A\"},\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null}],\"params\":{\"effective\":null,\"sources\":null},\"stages\":[{\"stage_name\":\"s1\",\"fanout_max\":1,\"modules\":[{\"module_id\":\"m1\",\"module_type\":\"test.module\",\"limit_key\":\"test.module\",\"max_in_flight\":null,\"enabled\":true,\"disabled_by_emergency\":false,\"priority\":0,\"gate_decision_code\":null,\"gate_reason_code\":null,\"gate_selector_name\":null,\"decision_kind\":\"execute\",\"decision_code\":\"SELECTED\"}],\"shadow_modules\":[]}]}",
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
        Assert.Contains("\"qos\":{\"selected_tier\":\"full\",\"reason_code\":null,\"signals\":null}", full.Json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"layer\":\"qos\"", full.Json, StringComparison.Ordinal);
        Assert.Contains("\"fanout_max\":2", full.Json, StringComparison.Ordinal);

        Assert.Equal(0, emergency.ExitCode);
        Assert.Contains("\"qos\":{\"selected_tier\":\"emergency\",\"reason_code\":null,\"signals\":null}", emergency.Json, StringComparison.Ordinal);
        Assert.Contains("\"layer\":\"qos\"", emergency.Json, StringComparison.Ordinal);
        Assert.Contains("\"fanout_max\":1", emergency.Json, StringComparison.Ordinal);
    }

    [Fact]
    public void ExplainPatchJson_ShouldIncludeEffectiveParamsAndSources_AndApplyOverlayPrecedence()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{" +
            "\"params\":{\"b\":2,\"a\":1,\"nested\":{\"x\":1,\"y\":2}}," +
            "\"experiments\":[{\"layer\":\"l1\",\"variant\":\"A\",\"patch\":{\"params\":{\"a\":10,\"nested\":{\"y\":20},\"exp\":true}}}]," +
            "\"qos\":{\"tiers\":{\"emergency\":{\"patch\":{\"params\":{\"a\":30,\"nested\":5,\"qos\":true}}}}}," +
            "\"emergency\":{\"reason\":\"r\",\"operator\":\"op\",\"ttl_minutes\":30,\"patch\":{\"stages\":{},\"params\":{\"a\":40,\"em\":\"on\"}}}" +
            "}}}";

        var requestOptions = new FlowRequestOptions(
            variants: new Dictionary<string, string>
            {
                { "l1", "A" },
            });

        var result = ToolingJsonV1.ExplainPatchJson(
            flowName: "HomeFeed",
            patchJson: patchJson,
            requestOptions: requestOptions,
            selectorRegistry: SelectorRegistry.Empty,
            qosTier: QosTier.Emergency);

        Assert.Equal(0, result.ExitCode);

        Assert.Equal(
            "{\"kind\":\"explain_patch\",\"tooling_json_version\":\"v1\",\"flow_name\":\"HomeFeed\",\"qos\":{\"selected_tier\":\"emergency\",\"reason_code\":null,\"signals\":null},\"variants\":{\"l1\":\"A\"},\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null},{\"layer\":\"experiment\",\"experiment_layer\":\"l1\",\"experiment_variant\":\"A\"},{\"layer\":\"qos\",\"experiment_layer\":null,\"experiment_variant\":null},{\"layer\":\"emergency\",\"experiment_layer\":null,\"experiment_variant\":null}],\"params\":{\"effective\":{\"a\":\"[REDACTED]\",\"b\":\"[REDACTED]\",\"em\":\"[REDACTED]\",\"exp\":\"[REDACTED]\",\"nested\":\"[REDACTED]\",\"qos\":\"[REDACTED]\"},\"sources\":[{\"path\":\"a\",\"layer\":\"emergency\",\"experiment_layer\":null,\"experiment_variant\":null,\"qos_tier\":null},{\"path\":\"b\",\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null,\"qos_tier\":null},{\"path\":\"em\",\"layer\":\"emergency\",\"experiment_layer\":null,\"experiment_variant\":null,\"qos_tier\":null},{\"path\":\"exp\",\"layer\":\"experiment\",\"experiment_layer\":\"l1\",\"experiment_variant\":\"A\",\"qos_tier\":null},{\"path\":\"nested\",\"layer\":\"qos\",\"experiment_layer\":null,\"experiment_variant\":null,\"qos_tier\":\"emergency\"},{\"path\":\"qos\",\"layer\":\"qos\",\"experiment_layer\":null,\"experiment_variant\":null,\"qos_tier\":\"emergency\"}]},\"stages\":[]}",
            result.Json);
    }

    [Fact]
    public void ExplainPatchJson_ShouldRedactEffectiveParams_ByDefault()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{" +
            "\"params\":{\"token\":\"t1\",\"password\":\"p1\",\"nested\":{\"api_key\":\"k1\",\"value\":\"ok\"}}" +
            "}}}";

        var result = ToolingJsonV1.ExplainPatchJson(flowName: "HomeFeed", patchJson: patchJson);

        Assert.Equal(0, result.ExitCode);

        Assert.Equal(
            "{\"kind\":\"explain_patch\",\"tooling_json_version\":\"v1\",\"flow_name\":\"HomeFeed\",\"qos\":{\"selected_tier\":\"full\",\"reason_code\":null,\"signals\":null},\"variants\":null,\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null}],\"params\":{\"effective\":{\"nested\":{\"api_key\":\"[REDACTED]\",\"value\":\"[REDACTED]\"},\"password\":\"[REDACTED]\",\"token\":\"[REDACTED]\"},\"sources\":[{\"path\":\"nested.api_key\",\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null,\"qos_tier\":null},{\"path\":\"nested.value\",\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null,\"qos_tier\":null},{\"path\":\"password\",\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null,\"qos_tier\":null},{\"path\":\"token\",\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null,\"qos_tier\":null}]},\"stages\":[]}",
            result.Json);
    }

    [Fact]
    public void PreviewMatrixJson_ShouldIncludeEffectiveParamsAndSources_PerPreview()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{" +
            "\"params\":{\"b\":2,\"a\":1}," +
            "\"experiments\":[{\"layer\":\"l1\",\"variant\":\"A\",\"patch\":{\"params\":{\"a\":10,\"c\":true}}}]" +
            "}}}";

        var variantsMatrix = new Dictionary<string, string>[]
        {
            new() { { "l1", "B" } },
            new() { { "l1", "A" } },
        };

        var result = ToolingJsonV1.PreviewMatrixJson(
            flowName: "HomeFeed",
            patchJson: patchJson,
            variantsMatrix: variantsMatrix,
            selectorRegistry: SelectorRegistry.Empty,
            qosTier: QosTier.Full,
            requestOptions: default);

        Assert.Equal(0, result.ExitCode);

        Assert.Equal(
            "{\"kind\":\"preview_matrix\",\"tooling_json_version\":\"v1\",\"flow_name\":\"HomeFeed\",\"qos\":{\"selected_tier\":\"full\",\"reason_code\":null,\"signals\":null},\"previews\":[{\"variants\":{\"l1\":\"A\"},\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null},{\"layer\":\"experiment\",\"experiment_layer\":\"l1\",\"experiment_variant\":\"A\"}],\"params\":{\"effective\":{\"a\":\"[REDACTED]\",\"b\":\"[REDACTED]\",\"c\":\"[REDACTED]\"},\"sources\":[{\"path\":\"a\",\"layer\":\"experiment\",\"experiment_layer\":\"l1\",\"experiment_variant\":\"A\",\"qos_tier\":null},{\"path\":\"b\",\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null,\"qos_tier\":null},{\"path\":\"c\",\"layer\":\"experiment\",\"experiment_layer\":\"l1\",\"experiment_variant\":\"A\",\"qos_tier\":null}]},\"stages\":[]},{\"variants\":{\"l1\":\"B\"},\"overlays_applied\":[{\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null}],\"params\":{\"effective\":{\"a\":\"[REDACTED]\",\"b\":\"[REDACTED]\"},\"sources\":[{\"path\":\"a\",\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null,\"qos_tier\":null},{\"path\":\"b\",\"layer\":\"base\",\"experiment_layer\":null,\"experiment_variant\":null,\"qos_tier\":null}]},\"stages\":[]}]}",
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
                contract => contract.AllowDynamicModules(),
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

