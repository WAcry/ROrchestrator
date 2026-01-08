using System.Text.Json;
using System.Text.Json.Serialization;
using ROrchestrator.Core;
using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Core.Tests;

public sealed class ConfigValidatorTests
{
    [Fact]
    public void ValidatePatchJson_ShouldReportParseError_WhenJsonIsInvalid()
    {
        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson("{");

        var finding = GetSingleFinding(report, "CFG_PARSE_ERROR");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportSchemaVersionUnsupported_WhenSchemaVersionIsMissing()
    {
        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson("{\"flows\":{}}");

        var finding = GetSingleFinding(report, "CFG_SCHEMA_VERSION_UNSUPPORTED");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.schemaVersion", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportUnknownField_WhenTopLevelFieldIsUnknown()
    {
        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson("{\"schemaVersion\":\"v1\",\"flows\":{},\"unknown\":123}");

        var finding = GetSingleFinding(report, "CFG_UNKNOWN_FIELD");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.unknown", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportFlowsNotObject_WhenFlowsIsNotObject()
    {
        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson("{\"schemaVersion\":\"v1\",\"flows\":123}");

        var finding = GetSingleFinding(report, "CFG_FLOWS_NOT_OBJECT");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportFlowNotRegistered_WhenFlowIsNotInRegistry()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprint<int, int>("TestFlow", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson("{\"schemaVersion\":\"v1\",\"flows\":{\"NotAFlow\":{}}}");

        var finding = GetSingleFinding(report, "CFG_FLOW_NOT_REGISTERED");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.NotAFlow", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportFlowPatchNotObject_WhenFlowPatchIsNotObject()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprint<int, int>("TestFlow", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson("{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":123}}");

        var finding = GetSingleFinding(report, "CFG_FLOW_PATCH_NOT_OBJECT");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportStageNotInBlueprint_WhenStageKeyIsNotInBlueprint()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s2\":{}}}}}");

        Assert.Single(report.Findings);

        var finding = GetSingleFinding(report, "CFG_STAGE_NOT_IN_BLUEPRINT");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s2", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportStagesNotObject_WhenStagesIsNotObject()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson("{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":123}}}");

        var finding = GetSingleFinding(report, "CFG_STAGES_NOT_OBJECT");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportStagePatchNotObject_WhenStagePatchIsNotObject()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson("{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":123}}}}");

        var finding = GetSingleFinding(report, "CFG_STAGE_PATCH_NOT_OBJECT");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportUnknownField_WhenStagePatchContainsUnknownField()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson("{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"unknown\":123}}}}}");

        var finding = GetSingleFinding(report, "CFG_UNKNOWN_FIELD");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.unknown", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportStageNotInBlueprint_WhenFlowIsRegisteredWithParams()
    {
        var registry = new FlowRegistry();
        var blueprint = CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0);
        registry.Register<int, int, TestParams, TestPatch>("HomeFeed", blueprint, new TestParams());

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s2\":{}}}}}");

        Assert.Single(report.Findings);

        var finding = GetSingleFinding(report, "CFG_STAGE_NOT_IN_BLUEPRINT");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s2", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldBeValid_WhenStagesIsAbsent()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson("{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{}}}");

        Assert.True(report.IsValid);
    }

    [Fact]
    public void ValidatePatchJson_ShouldBeValid_WhenStageKeysExistInBlueprint()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{}}}}}");

        Assert.True(report.IsValid);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportExperimentMappingInvalid_WhenExperimentsIsNotArray()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprint<int, int>("TestFlow", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":123}}}");

        var finding = GetSingleFinding(report, "CFG_EXPERIMENT_MAPPING_INVALID");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportExperimentMappingInvalid_WhenExperimentEntryIsNotObject()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprint<int, int>("TestFlow", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[123]}}}");

        var finding = GetSingleFinding(report, "CFG_EXPERIMENT_MAPPING_INVALID");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0]", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportExperimentMappingInvalid_WhenExperimentLayerIsMissing()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprint<int, int>("TestFlow", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"variant\":\"A\",\"patch\":{}}]}}}");

        var finding = GetSingleFinding(report, "CFG_EXPERIMENT_MAPPING_INVALID");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0].layer", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportExperimentMappingInvalid_WhenExperimentVariantIsEmpty()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprint<int, int>("TestFlow", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"recall_layer\",\"variant\":\"\",\"patch\":{}}]}}}");

        var finding = GetSingleFinding(report, "CFG_EXPERIMENT_MAPPING_INVALID");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0].variant", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportExperimentMappingDuplicate_WhenLayerAndVariantDuplicateWithinFlow()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprint<int, int>("TestFlow", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"recall_layer\",\"variant\":\"B\",\"patch\":{}},{\"layer\":\"recall_layer\",\"variant\":\"B\",\"patch\":{}}]}}}");

        var pathsFound = 0;
        var findings = report.Findings;

        for (var i = 0; i < findings.Count; i++)
        {
            var finding = findings[i];
            if (!string.Equals("CFG_EXPERIMENT_MAPPING_DUPLICATE", finding.Code, StringComparison.Ordinal))
            {
                continue;
            }

            if (string.Equals("$.flows.HomeFeed.experiments[0]", finding.Path, StringComparison.Ordinal)
                || string.Equals("$.flows.HomeFeed.experiments[1]", finding.Path, StringComparison.Ordinal))
            {
                pathsFound++;
            }
        }

        Assert.Equal(2, pathsFound);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportExperimentPatchInvalid_WhenExperimentPatchIsNull()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprint<int, int>("TestFlow", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"recall_layer\",\"variant\":\"B\",\"patch\":null}]}}}");

        var finding = GetSingleFinding(report, "CFG_EXPERIMENT_PATCH_INVALID");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportExperimentPatchInvalid_WhenExperimentPatchContainsInvalidParamsPatch()
    {
        var registry = new FlowRegistry();
        var blueprint = CreateBlueprint<int, int>("TestFlow", okValue: 0);
        registry.Register<int, int, TestParams, ParamsPatchWithMaxCandidates>("HomeFeed", blueprint, new TestParams());

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"recall_layer\",\"variant\":\"B\",\"patch\":{\"params\":{\"MaxCandidate\":10}}}]}}}");

        var finding = GetSingleFinding(report, "CFG_EXPERIMENT_PATCH_INVALID");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch.params.MaxCandidate", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldBeValid_WhenParamsPatchIsValid()
    {
        var registry = new FlowRegistry();
        var blueprint = CreateBlueprint<int, int>("TestFlow", okValue: 0);
        registry.Register<int, int, TestParams, ParamsPatchWithMaxCandidate>("HomeFeed", blueprint, new TestParams());

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"params\":{\"MaxCandidate\":10}}}}");

        Assert.True(report.IsValid);
    }

    [Fact]
    public void ValidatePatchJson_ShouldBeValid_WhenParamsPatchPropertyIsJsonIgnoredOnWrite()
    {
        var registry = new FlowRegistry();
        var blueprint = CreateBlueprint<int, int>("TestFlow", okValue: 0);
        registry.Register<int, int, TestParams, ParamsPatchWithJsonIgnoreWhenWritingNull>("HomeFeed", blueprint, new TestParams());

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"params\":{\"MaxCandidate\":10}}}}");

        Assert.True(report.IsValid);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportParamsBindFailed_WhenParamsPatchHasTypeMismatch()
    {
        var registry = new FlowRegistry();
        var blueprint = CreateBlueprint<int, int>("TestFlow", okValue: 0);
        registry.Register<int, int, TestParams, ParamsPatchWithMaxCandidate>("HomeFeed", blueprint, new TestParams());

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"params\":{\"MaxCandidate\":\"oops\"}}}}");

        var finding = GetSingleFinding(report, "CFG_PARAMS_BIND_FAILED");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.StartsWith("$.flows.HomeFeed.params", finding.Path, StringComparison.Ordinal);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportParamsUnknownField_WhenParamsPatchContainsUnknownField()
    {
        var registry = new FlowRegistry();
        var blueprint = CreateBlueprint<int, int>("TestFlow", okValue: 0);
        registry.Register<int, int, TestParams, ParamsPatchWithMaxCandidates>("HomeFeed", blueprint, new TestParams());

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"params\":{\"MaxCandidate\":10}}}}");

        var finding = GetSingleFinding(report, "CFG_PARAMS_UNKNOWN_FIELD");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.params.MaxCandidate", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportParamsBindFailed_WhenFlowIsRegisteredWithoutPatchType_ButParamsPresent()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprint<int, int>("TestFlow", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"params\":{\"MaxCandidate\":10}}}}");

        var finding = GetSingleFinding(report, "CFG_PARAMS_BIND_FAILED");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.params", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportParamsBindFailed_WhenParamsPatchConverterThrows()
    {
        var registry = new FlowRegistry();
        var blueprint = CreateBlueprint<int, int>("TestFlow", okValue: 0);
        registry.Register<int, int, TestParams, ParamsPatchWithThrowingConverter>("HomeFeed", blueprint, new TestParams());

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"params\":{\"MaxCandidate\":10}}}}");

        var finding = GetSingleFinding(report, "CFG_PARAMS_BIND_FAILED");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.params", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldBeValid_WhenModulesPatchIsValid()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}]}}}}}");

        Assert.True(report.IsValid);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportModuleArgsMissing_WhenModuleWithIsMissing()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\"}]}}}}}");

        Assert.False(report.IsValid);
        Assert.Single(report.Findings);

        var finding = GetSingleFinding(report, "CFG_MODULE_ARGS_MISSING");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].with", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportModuleArgsMissing_WhenModuleWithIsNull()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":null}]}}}}}");

        Assert.False(report.IsValid);
        Assert.Single(report.Findings);

        var finding = GetSingleFinding(report, "CFG_MODULE_ARGS_MISSING");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].with", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportModuleArgsBindFailed_WhenModuleWithCannotBind()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgsWithMaxCandidate, int>("test.module", _ => new TestModuleWithMaxCandidateArgs());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{\"MaxCandidate\":\"oops\"}}]}}}}}");

        Assert.False(report.IsValid);
        Assert.Single(report.Findings);

        var finding = GetSingleFinding(report, "CFG_MODULE_ARGS_BIND_FAILED");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].with.MaxCandidate", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportModuleArgsUnknownField_WhenModuleWithContainsUnknownField()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgsWithMaxCandidate, int>("test.module", _ => new TestModuleWithMaxCandidateArgs());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{\"MaxCandidate\":10,\"Unknown\":123}}]}}}}}");

        Assert.False(report.IsValid);
        Assert.Single(report.Findings);

        var finding = GetSingleFinding(report, "CFG_MODULE_ARGS_UNKNOWN_FIELD");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].with.Unknown", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldBeValid_WhenModuleArgsTypeIsJsonElement()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<JsonElement, int>("test.module", _ => new TestJsonElementModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{\"Unknown\":123}}]}}}}}");

        Assert.True(report.IsValid);
        AssertNoFinding(report, "CFG_MODULE_ARGS_UNKNOWN_FIELD");
    }

    [Fact]
    public void ValidatePatchJson_ShouldBeValid_WhenModuleArgsTypeIsDictionary()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<IDictionary<string, object>, int>("test.module", _ => new TestDictionaryModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{\"Unknown\":123}}]}}}}}");

        Assert.True(report.IsValid);
        AssertNoFinding(report, "CFG_MODULE_ARGS_UNKNOWN_FIELD");
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportModuleIdInvalidFormat_WhenModuleIdIsInvalid()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"M1\",\"use\":\"test.module\",\"with\":{}}]}}}}}");

        Assert.True(report.IsValid);
        Assert.Single(report.Findings);

        var finding = GetSingleFinding(report, "CFG_MODULE_ID_INVALID_FORMAT");
        Assert.Equal(ValidationSeverity.Warn, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].id", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportModulesNotArray_WhenModulesIsNotArray()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":{}}}}}}");

        var finding = GetSingleFinding(report, "CFG_MODULES_NOT_ARRAY");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportModuleIdMissing_WhenModuleIdIsMissing()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"use\":\"test.module\",\"with\":{}}]}}}}}");

        var finding = GetSingleFinding(report, "CFG_MODULE_ID_MISSING");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].id", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportModuleIdDuplicate_WhenModuleIdsDuplicateWithinStage()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}},{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}]}}}}}");

        var pathsFound = 0;
        var findings = report.Findings;

        for (var i = 0; i < findings.Count; i++)
        {
            var finding = findings[i];
            if (!string.Equals("CFG_MODULE_ID_DUPLICATE", finding.Code, StringComparison.Ordinal))
            {
                continue;
            }

            if (string.Equals("$.flows.HomeFeed.stages.s1.modules[0].id", finding.Path, StringComparison.Ordinal)
                || string.Equals("$.flows.HomeFeed.stages.s1.modules[1].id", finding.Path, StringComparison.Ordinal))
            {
                pathsFound++;
            }
        }

        Assert.Equal(2, pathsFound);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportModuleIdDuplicate_WhenModuleIdsDuplicateAcrossStages()
    {
        var registry = new FlowRegistry();
        registry.Register(
            "HomeFeed",
            FlowBlueprint.Define<int, int>("TestFlow")
                .Stage(
                    "s1",
                    stage =>
                    {
                        stage.Join<int>(
                            name: "j1",
                            join: _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0)));
                    })
                .Stage(
                    "s2",
                    stage =>
                    {
                        stage.Join<int>(
                            name: "j2",
                            join: _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0)));
                    })
                .Build());

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}]},\"s2\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}]}}}}}");

        var pathsFound = 0;
        var findings = report.Findings;

        for (var i = 0; i < findings.Count; i++)
        {
            var finding = findings[i];
            if (!string.Equals("CFG_MODULE_ID_DUPLICATE", finding.Code, StringComparison.Ordinal))
            {
                continue;
            }

            if (string.Equals("$.flows.HomeFeed.stages.s1.modules[0].id", finding.Path, StringComparison.Ordinal)
                || string.Equals("$.flows.HomeFeed.stages.s2.modules[0].id", finding.Path, StringComparison.Ordinal))
            {
                pathsFound++;
            }
        }

        Assert.Equal(2, pathsFound);
    }

    [Fact]
    public void ValidatePatchJson_ShouldNotReportModuleIdDuplicate_WhenModuleIdsAreDistinctAcrossStages()
    {
        var registry = new FlowRegistry();
        registry.Register(
            "HomeFeed",
            FlowBlueprint.Define<int, int>("TestFlow")
                .Stage(
                    "s1",
                    stage =>
                    {
                        stage.Join<int>(
                            name: "j1",
                            join: _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0)));
                    })
                .Stage(
                    "s2",
                    stage =>
                    {
                        stage.Join<int>(
                            name: "j2",
                            join: _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0)));
                    })
                .Build());

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}]},\"s2\":{\"modules\":[{\"id\":\"m2\",\"use\":\"test.module\",\"with\":{}}]}}}}}");

        AssertNoFinding(report, "CFG_MODULE_ID_DUPLICATE");
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportModuleTypeMissing_WhenModuleUseIsMissing()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"with\":{}}]}}}}}");

        var finding = GetSingleFinding(report, "CFG_MODULE_TYPE_MISSING");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].use", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportModuleTypeNotRegistered_WhenModuleUseIsNotInCatalog()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"not.registered\",\"with\":{}}]}}}}}");

        var finding = GetSingleFinding(report, "CFG_MODULE_TYPE_NOT_REGISTERED");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].use", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportUnknownField_WhenModulePatchContainsUnknownField()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"unknown\":123}]}}}}}");

        var finding = GetSingleFinding(report, "CFG_UNKNOWN_FIELD");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].unknown", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldBeValid_WhenModuleGateIsValid()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"experiment\":{\"layer\":\"recall_layer\",\"in\":[\"B\"]}}}]}}}}}");

        Assert.True(report.IsValid);
    }

    [Fact]
    public void ValidatePatchJson_ShouldBeValid_WhenExperimentPatchModuleGateIsValid()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"not\":{\"experiment\":{\"layer\":\"recall_layer\",\"in\":[\"B\"]}}}}]}}}}]}}}");

        Assert.True(report.IsValid);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportGateUnknownType_WhenGateTypeIsUnknown()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"magicExpr\":\"x > 1\"}}]}}}}}");

        var finding = GetSingleFinding(report, "CFG_GATE_UNKNOWN_TYPE");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].gate", finding.Path);
        Assert.Equal("gate type is unknown or unsupported.", finding.Message);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportGateUnknownType_WhenExperimentPatchGateTypeIsUnknown()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"magicExpr\":\"x > 1\"}}]}}}}]}}}");

        Assert.Single(report.Findings);

        var finding = GetSingleFinding(report, "CFG_GATE_UNKNOWN_TYPE");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch.stages.s1.modules[0].gate", finding.Path);
        Assert.Equal("gate type is unknown or unsupported.", finding.Message);

        AssertNoFinding(report, "CFG_EXPERIMENT_PATCH_INVALID");
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportGateEmptyComposite_WhenAllIsEmpty()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"all\":[]}}]}}}}}");

        var finding = GetSingleFinding(report, "CFG_GATE_EMPTY_COMPOSITE");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].gate.all", finding.Path);
        Assert.Equal("gate composite must be a non-empty array.", finding.Message);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportGateExperimentInvalid_WhenLayerIsEmpty()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"experiment\":{\"layer\":\"\",\"in\":[\"B\"]}}}]}}}}}");

        var finding = GetSingleFinding(report, "CFG_GATE_EXPERIMENT_INVALID");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].gate.experiment.layer", finding.Path);
        Assert.Equal("gate.experiment.layer is required and must be a non-empty string.", finding.Message);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportGateExperimentInvalid_WhenInIsEmpty()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"experiment\":{\"layer\":\"recall_layer\",\"in\":[]}}}]}}}}}");

        var finding = GetSingleFinding(report, "CFG_GATE_EXPERIMENT_INVALID");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].gate.experiment.in", finding.Path);
        Assert.Equal("gate.experiment.in is required and must be a non-empty array of strings.", finding.Message);
    }

    private static ValidationFinding GetSingleFinding(ValidationReport report, string code)
    {
        if (report is null)
        {
            throw new ArgumentNullException(nameof(report));
        }

        if (string.IsNullOrEmpty(code))
        {
            throw new ArgumentException("Code must be non-empty.", nameof(code));
        }

        ValidationFinding found = default;
        var foundCount = 0;

        var findings = report.Findings;

        for (var i = 0; i < findings.Count; i++)
        {
            var candidate = findings[i];
            if (string.Equals(code, candidate.Code, StringComparison.Ordinal))
            {
                found = candidate;
                foundCount++;
            }
        }

        Assert.Equal(1, foundCount);
        return found;
    }

    private static void AssertNoFinding(ValidationReport report, string code)
    {
        if (report is null)
        {
            throw new ArgumentNullException(nameof(report));
        }

        if (string.IsNullOrEmpty(code))
        {
            throw new ArgumentException("Code must be non-empty.", nameof(code));
        }

        var findings = report.Findings;

        for (var i = 0; i < findings.Count; i++)
        {
            Assert.NotEqual(code, findings[i].Code);
        }
    }

    private static FlowBlueprint<TReq, TResp> CreateBlueprint<TReq, TResp>(string name, TResp okValue)
    {
        return FlowBlueprint.Define<TReq, TResp>(name)
            .Join<TResp>(
                name: "j1",
                join: _ => new ValueTask<Outcome<TResp>>(Outcome<TResp>.Ok(okValue)))
            .Build();
    }

    private static FlowBlueprint<TReq, TResp> CreateBlueprintWithStage<TReq, TResp>(string name, string stageName, TResp okValue)
    {
        return FlowBlueprint.Define<TReq, TResp>(name)
            .Stage(
                stageName,
                stage =>
                {
                    stage.Join<TResp>(
                        name: "j1",
                        join: _ => new ValueTask<Outcome<TResp>>(Outcome<TResp>.Ok(okValue)));
                })
            .Build();
    }

    private sealed class TestParams
    {
    }

    private sealed class TestPatch
    {
    }

    private sealed class ParamsPatchWithMaxCandidate
    {
        public int MaxCandidate { get; set; }
    }

    private sealed class ParamsPatchWithMaxCandidates
    {
        public int MaxCandidates { get; set; }
    }

    private sealed class ParamsPatchWithJsonIgnoreWhenWritingNull
    {
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? MaxCandidate { get; set; }
    }

    private sealed class ParamsPatchWithThrowingConverter
    {
        [JsonConverter(typeof(ThrowingInt32Converter))]
        public int MaxCandidate { get; set; }
    }

    private sealed class ThrowingInt32Converter : JsonConverter<int>
    {
        public override int Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            throw new FormatException("Invalid value.");
        }

        public override void Write(Utf8JsonWriter writer, int value, JsonSerializerOptions options)
        {
            writer.WriteNumberValue(value);
        }
    }

    private sealed class ModuleArgs
    {
    }

    private sealed class ModuleArgsWithMaxCandidate
    {
        public int MaxCandidate { get; set; }
    }

    private sealed class TestModule : IModule<ModuleArgs, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<ModuleArgs> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(0));
        }
    }

    private sealed class TestModuleWithMaxCandidateArgs : IModule<ModuleArgsWithMaxCandidate, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<ModuleArgsWithMaxCandidate> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(0));
        }
    }

    private sealed class TestJsonElementModule : IModule<JsonElement, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<JsonElement> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(0));
        }
    }

    private sealed class TestDictionaryModule : IModule<IDictionary<string, object>, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<IDictionary<string, object>> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(0));
        }
    }
}
