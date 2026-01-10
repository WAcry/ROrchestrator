using System.Text.Json;
using System.Text.Json.Serialization;
using Rockestra.Core;
using Rockestra.Core.Blueprint;
using Rockestra.Core.Selectors;

namespace Rockestra.Core.Tests;

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
    public void ValidatePatchJson_ShouldBeValid_WhenFanoutMaxIsValid()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"fanoutMax\":8}}}}}");

        Assert.True(report.IsValid);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportFanoutTrimLikely_WhenEnabledModulesCountExceedsFanoutMax()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"fanoutMax\":2,\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}," +
            "{\"id\":\"m2\",\"use\":\"test.module\",\"with\":{}}," +
            "{\"id\":\"m3\",\"use\":\"test.module\",\"with\":{}}," +
            "{\"id\":\"m4\",\"use\":\"test.module\",\"with\":{}}," +
            "{\"id\":\"m5\",\"use\":\"test.module\",\"with\":{}}]}}}}}");

        Assert.True(report.IsValid);
        Assert.Single(report.Findings);

        var finding = GetSingleFinding(report, "CFG_FANOUT_TRIM_LIKELY");
        Assert.Equal(ValidationSeverity.Warn, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldNotReportFanoutTrimLikely_WhenEnabledModulesCountDoesNotExceedFanoutMax()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"fanoutMax\":2,\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}," +
            "{\"id\":\"m2\",\"use\":\"test.module\",\"with\":{}}," +
            "{\"id\":\"m3\",\"use\":\"test.module\",\"with\":{},\"enabled\":false}]}}}}}");

        Assert.True(report.IsValid);
        AssertNoFinding(report, "CFG_FANOUT_TRIM_LIKELY");
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportFanoutTrimLikely_WhenEnabledModulesCountExceedsFanoutMaxInExperimentPatch()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"fanoutMax\":2,\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}," +
            "{\"id\":\"m2\",\"use\":\"test.module\",\"with\":{}}," +
            "{\"id\":\"m3\",\"use\":\"test.module\",\"with\":{}}]}}}}]}}}");

        Assert.True(report.IsValid);
        Assert.Single(report.Findings);

        var finding = GetSingleFinding(report, "CFG_FANOUT_TRIM_LIKELY");
        Assert.Equal(ValidationSeverity.Warn, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch.stages.s1", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportFanoutTrimLikely_WhenEmergencyFanoutMaxIsLowerThanEnabledModulesCount()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{" +
            "\"stages\":{\"s1\":{\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}," +
            "{\"id\":\"m2\",\"use\":\"test.module\",\"with\":{}}," +
            "{\"id\":\"m3\",\"use\":\"test.module\",\"with\":{}}]}},\"emergency\":{\"reason\":\"r\",\"operator\":\"op\",\"ttl_minutes\":30,\"patch\":{\"stages\":{\"s1\":{\"fanoutMax\":1}}}}}}}");

        Assert.True(report.IsValid);
        Assert.Single(report.Findings);

        var finding = GetSingleFinding(report, "CFG_FANOUT_TRIM_LIKELY");
        Assert.Equal(ValidationSeverity.Warn, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.emergency.patch.stages.s1", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldNotReportFanoutTrimLikely_WhenEmergencyDisablesModulesToFitFanoutMax()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{" +
            "\"stages\":{\"s1\":{\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}," +
            "{\"id\":\"m2\",\"use\":\"test.module\",\"with\":{}}," +
            "{\"id\":\"m3\",\"use\":\"test.module\",\"with\":{}}]}},\"emergency\":{\"reason\":\"r\",\"operator\":\"op\",\"ttl_minutes\":30,\"patch\":{\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[{\"id\":\"m2\",\"enabled\":false},{\"id\":\"m3\",\"enabled\":false}]}}}}}}}");

        Assert.True(report.IsValid);
        AssertNoFinding(report, "CFG_FANOUT_TRIM_LIKELY");
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportFanoutMaxInvalid_WhenFanoutMaxIsNotInteger()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"fanoutMax\":\"8\"}}}}}");

        var finding = GetSingleFinding(report, "CFG_FANOUT_MAX_INVALID");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.fanoutMax", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportFanoutMaxInvalid_WhenFanoutMaxIsNegative()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"fanoutMax\":-1}}}}}");

        var finding = GetSingleFinding(report, "CFG_FANOUT_MAX_INVALID");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.fanoutMax", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportFanoutMaxExceeded_WhenFanoutMaxExceedsMaxAllowed()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"fanoutMax\":9}}}}}");

        var finding = GetSingleFinding(report, "CFG_FANOUT_MAX_EXCEEDED");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.fanoutMax", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldBeValid_WhenExperimentPatchFanoutMaxIsValid()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"fanoutMax\":1}}}}]}}}");

        Assert.True(report.IsValid);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportFanoutMaxExceeded_WhenExperimentPatchFanoutMaxExceedsMaxAllowed()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"fanoutMax\":9}}}}]}}}");

        var finding = GetSingleFinding(report, "CFG_FANOUT_MAX_EXCEEDED");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch.stages.s1.fanoutMax", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
        AssertNoFinding(report, "CFG_EXPERIMENT_PATCH_INVALID");
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportFanoutMaxInvalid_WhenExperimentPatchFanoutMaxIsNegative()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"fanoutMax\":-1}}}}]}}}");

        var finding = GetSingleFinding(report, "CFG_FANOUT_MAX_INVALID");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch.stages.s1.fanoutMax", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
        AssertNoFinding(report, "CFG_EXPERIMENT_PATCH_INVALID");
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
    public void ValidatePatchJson_ShouldReportExperimentOverrideForbidden_WhenExperimentPatchContainsNestedExperiments()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprint<int, int>("TestFlow", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"experiments\":[{\"layer\":\"l2\",\"variant\":\"v2\",\"patch\":{}}]}}]}}}");

        var finding = GetSingleFinding(report, "CFG_EXPERIMENT_OVERRIDE_FORBIDDEN");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch.experiments", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportExperimentOverrideForbidden_WhenExperimentPatchContainsEmergency()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprint<int, int>("TestFlow", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"emergency\":{}}}]}}}");

        var finding = GetSingleFinding(report, "CFG_EXPERIMENT_OVERRIDE_FORBIDDEN");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch.emergency", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportEmergencyAuditMissing_WhenEmergencyMissingRequiredAuditFields()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprint<int, int>("TestFlow", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"emergency\":{\"operator\":\"op\",\"ttl_minutes\":30,\"patch\":{\"stages\":{}}}}}}");

        var finding = GetSingleFinding(report, "CFG_EMERGENCY_AUDIT_MISSING");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.emergency", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportEmergencyOverrideForbidden_WhenEmergencyPatchOverridesExperiments()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprint<int, int>("TestFlow", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"emergency\":{\"reason\":\"r\",\"operator\":\"op\",\"ttl_minutes\":30,\"patch\":{\"experiments\":[]}}}}}");

        var finding = GetSingleFinding(report, "CFG_EMERGENCY_OVERRIDE_FORBIDDEN");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.emergency.patch.experiments", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportQosFanoutMaxIncreaseForbidden_WhenQosIncreasesFanoutMax()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}]}}," +
            "\"qos\":{\"tiers\":{\"emergency\":{\"patch\":{\"stages\":{\"s1\":{\"fanoutMax\":2}}}}}}" +
            "}}}");

        var finding = GetSingleFinding(report, "CFG_QOS_FANOUT_MAX_INCREASE_FORBIDDEN");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.qos.tiers.emergency.patch.stages.s1.fanoutMax", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportQosModuleEnableForbidden_WhenQosEnablesModule()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{" +
            "\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"enabled\":false}]}}," +
            "\"qos\":{\"tiers\":{\"emergency\":{\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"enabled\":true}]}}}}}}" +
            "}}}");

        var finding = GetSingleFinding(report, "CFG_QOS_MODULE_ENABLE_FORBIDDEN");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.qos.tiers.emergency.patch.stages.s1.modules[0].enabled", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportQosShadowSampleIncreaseForbidden_WhenQosIncreasesShadowSample()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{" +
            "\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m_shadow\",\"use\":\"test.module\",\"with\":{},\"shadow\":{\"sample\":0.1}}]}}," +
            "\"qos\":{\"tiers\":{\"emergency\":{\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m_shadow\",\"shadow\":{\"sample\":0.2}}]}}}}}}" +
            "}}}");

        var finding = GetSingleFinding(report, "CFG_QOS_SHADOW_SAMPLE_INCREASE_FORBIDDEN");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.qos.tiers.emergency.patch.stages.s1.modules[0].shadow.sample", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportParamsUnknownField_WhenExperimentPatchContainsUnknownParamsField()
    {
        var registry = new FlowRegistry();
        var blueprint = CreateBlueprint<int, int>("TestFlow", okValue: 0);
        registry.Register<int, int, TestParams, ParamsPatchWithMaxCandidates>("HomeFeed", blueprint, new TestParams());

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"recall_layer\",\"variant\":\"B\",\"patch\":{\"params\":{\"MaxCandidate\":10}}}]}}}");

        var finding = GetSingleFinding(report, "CFG_PARAMS_UNKNOWN_FIELD");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch.params.MaxCandidate", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportLayerParamLeak_WhenExperimentLayerModifiesUnownedParamsPath()
    {
        var contract = new ExperimentLayerOwnershipContractBuilder()
            .AddLayer("l1", ownedParamPathPrefixes: new[] { "Other" }, ownedModuleIds: Array.Empty<string>())
            .Build();

        var registry = new FlowRegistry();
        var blueprint = CreateBlueprint<int, int>("TestFlow", okValue: 0);
        registry.Register<int, int, TestParams, ParamsPatchWithMaxCandidate>("HomeFeed", blueprint, new TestParams(), contract);

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"params\":{\"MaxCandidate\":1}}}]}}}");

        var finding = GetSingleFinding(report, "CFG_LAYER_PARAM_LEAK");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch.params.MaxCandidate", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportLayerParamLeak_WhenExperimentLayerModifiesUnownedModuleId()
    {
        var contract = new ExperimentLayerOwnershipContractBuilder()
            .AddLayer("l1", ownedParamPathPrefixes: Array.Empty<string>(), ownedModuleIds: new[] { "m1" })
            .Build();

        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0), contract);

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m2\",\"use\":\"test.module\",\"with\":{}}]}}}}]}}}");

        var finding = GetSingleFinding(report, "CFG_LAYER_PARAM_LEAK");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch.stages.s1.modules[0].id", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ExperimentLayerOwnershipContractBuilder_ShouldNotMutatePreviousContract_WhenBuilderIsReused()
    {
        var builder = new ExperimentLayerOwnershipContractBuilder()
            .AddLayer("l1", ownedParamPathPrefixes: new[] { "MaxCandidate" }, ownedModuleIds: Array.Empty<string>());

        var contract = builder.Build();

        var registry = new FlowRegistry();
        var blueprint = CreateBlueprint<int, int>("TestFlow", okValue: 0);
        registry.Register<int, int, TestParams, ParamsPatchWithMaxCandidate>("HomeFeed", blueprint, new TestParams(), contract);

        builder.AddLayer("l2", ownedParamPathPrefixes: new[] { "MaxCandidate" }, ownedModuleIds: Array.Empty<string>());
        _ = builder.Build();

        var validator = new ConfigValidator(registry, new ModuleCatalog());

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l2\",\"variant\":\"v1\",\"patch\":{\"params\":{\"MaxCandidate\":1}}}]}}}");

        var finding = GetSingleFinding(report, "CFG_LAYER_PARAM_LEAK");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportLayerConflict_WhenDifferentLayersOverrideSameParamsPath()
    {
        var registry = new FlowRegistry();
        var blueprint = CreateBlueprint<int, int>("TestFlow", okValue: 0);
        registry.Register<int, int, TestParams, ParamsPatchWithMaxCandidate>("HomeFeed", blueprint, new TestParams());

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[" +
            "{\"layer\":\"l1\",\"variant\":\"A\",\"patch\":{\"params\":{\"MaxCandidate\":1}}}," +
            "{\"layer\":\"l2\",\"variant\":\"B\",\"patch\":{\"params\":{\"MaxCandidate\":2}}}" +
            "]}}}");

        Assert.False(report.IsValid);

        var findings = report.Findings;
        var foundIndex0 = false;
        var foundIndex1 = false;
        var conflictCount = 0;

        for (var i = 0; i < findings.Count; i++)
        {
            var finding = findings[i];
            if (!string.Equals("CFG_LAYER_CONFLICT", finding.Code, StringComparison.Ordinal))
            {
                continue;
            }

            conflictCount++;

            if (string.Equals("$.flows.HomeFeed.experiments[0].patch.params.MaxCandidate", finding.Path, StringComparison.Ordinal))
            {
                foundIndex0 = true;
                continue;
            }

            if (string.Equals("$.flows.HomeFeed.experiments[1].patch.params.MaxCandidate", finding.Path, StringComparison.Ordinal))
            {
                foundIndex1 = true;
            }
        }

        Assert.Equal(2, conflictCount);
        Assert.True(foundIndex0);
        Assert.True(foundIndex1);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportLayerConflict_WhenDifferentLayersOverrideSameFanoutMax()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[" +
            "{\"layer\":\"l1\",\"variant\":\"A\",\"patch\":{\"stages\":{\"s1\":{\"fanoutMax\":1}}}}," +
            "{\"layer\":\"l2\",\"variant\":\"B\",\"patch\":{\"stages\":{\"s1\":{\"fanoutMax\":2}}}}" +
            "]}}}");

        Assert.False(report.IsValid);

        var findings = report.Findings;
        var foundIndex0 = false;
        var foundIndex1 = false;
        var conflictCount = 0;

        for (var i = 0; i < findings.Count; i++)
        {
            var finding = findings[i];
            if (!string.Equals("CFG_LAYER_CONFLICT", finding.Code, StringComparison.Ordinal))
            {
                continue;
            }

            conflictCount++;

            if (string.Equals("$.flows.HomeFeed.experiments[0].patch.stages.s1.fanoutMax", finding.Path, StringComparison.Ordinal))
            {
                foundIndex0 = true;
                continue;
            }

            if (string.Equals("$.flows.HomeFeed.experiments[1].patch.stages.s1.fanoutMax", finding.Path, StringComparison.Ordinal))
            {
                foundIndex1 = true;
            }
        }

        Assert.Equal(2, conflictCount);
        Assert.True(foundIndex0);
        Assert.True(foundIndex1);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportLayerConflict_WhenDifferentLayersOverrideSameModuleId()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[" +
            "{\"layer\":\"l1\",\"variant\":\"A\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}]}}}}," +
            "{\"layer\":\"l2\",\"variant\":\"B\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}]}}}}" +
            "]}}}");

        Assert.False(report.IsValid);

        var findings = report.Findings;
        var foundIndex0 = false;
        var foundIndex1 = false;
        var conflictCount = 0;

        for (var i = 0; i < findings.Count; i++)
        {
            var finding = findings[i];
            if (!string.Equals("CFG_LAYER_CONFLICT", finding.Code, StringComparison.Ordinal))
            {
                continue;
            }

            conflictCount++;

            if (string.Equals("$.flows.HomeFeed.experiments[0].patch.stages.s1.modules[0].id", finding.Path, StringComparison.Ordinal))
            {
                foundIndex0 = true;
                continue;
            }

            if (string.Equals("$.flows.HomeFeed.experiments[1].patch.stages.s1.modules[0].id", finding.Path, StringComparison.Ordinal))
            {
                foundIndex1 = true;
            }
        }

        Assert.Equal(2, conflictCount);
        Assert.True(foundIndex0);
        Assert.True(foundIndex1);
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
    public void ValidatePatchJson_ShouldBeValid_WhenModuleEnabledAndPriorityAreValid()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"enabled\":false,\"priority\":10}]}}}}}");

        Assert.True(report.IsValid);
        Assert.Empty(report.Findings);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportModuleEnabledInvalid_WhenEnabledIsNotBoolean()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"enabled\":\"false\"}]}}}}}");

        var finding = GetSingleFinding(report, "CFG_MODULE_ENABLED_INVALID");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].enabled", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldBeValid_WhenExperimentPatchModuleEnabledAndPriorityAreValid()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"enabled\":true,\"priority\":-5}]}}}}]}}}");

        Assert.True(report.IsValid);
        Assert.Empty(report.Findings);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportPriorityInvalid_WhenModulePriorityIsOutOfRange()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"priority\":1001}]}}}}}");

        Assert.True(report.IsValid);

        var finding = GetSingleFinding(report, "CFG_PRIORITY_INVALID");
        Assert.Equal(ValidationSeverity.Warn, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].priority", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportPriorityInvalid_WhenExperimentPatchModulePriorityIsOutOfRange()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"priority\":1001}]}}}}]}}}");

        Assert.True(report.IsValid);

        var finding = GetSingleFinding(report, "CFG_PRIORITY_INVALID");
        Assert.Equal(ValidationSeverity.Warn, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch.stages.s1.modules[0].priority", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportGateRedundant_WhenEnabledIsFalseAndGateIsPresent()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"enabled\":false,\"gate\":{\"experiment\":{\"layer\":\"recall_layer\",\"in\":[\"B\"]}}}]}}}}}");

        Assert.True(report.IsValid);

        var finding = GetSingleFinding(report, "CFG_GATE_REDUNDANT");
        Assert.Equal(ValidationSeverity.Info, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].gate", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportGateRedundant_WhenEnabledIsFalseAndGateIsPresentInExperimentPatch()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"enabled\":false,\"gate\":{\"experiment\":{\"layer\":\"recall_layer\",\"in\":[\"B\"]}}}]}}}}]}}}");

        Assert.True(report.IsValid);

        var finding = GetSingleFinding(report, "CFG_GATE_REDUNDANT");
        Assert.Equal(ValidationSeverity.Info, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch.stages.s1.modules[0].gate", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
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
    public void ValidatePatchJson_ShouldReportModuleArgsInvalid_WhenArgsValidatorReturnsFalse()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgsWithMaxCandidate, int>(
            "test.module",
            _ => new TestModuleWithMaxCandidateArgs(),
            argsValidator: new MaxCandidateGreaterThanZeroValidator());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{\"MaxCandidate\":0}}]}}}}}");

        Assert.False(report.IsValid);
        Assert.Single(report.Findings);

        var finding = GetSingleFinding(report, "CFG_MODULE_ARGS_INVALID");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].with.MaxCandidate", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportModuleArgsInvalid_WhenArgsValidatorReturnsFalseInExperimentPatch()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgsWithMaxCandidate, int>(
            "test.module",
            _ => new TestModuleWithMaxCandidateArgs(),
            argsValidator: new MaxCandidateGreaterThanZeroValidator());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{\"MaxCandidate\":0}}]}}}}]}}}");

        Assert.False(report.IsValid);
        Assert.Single(report.Findings);

        var finding = GetSingleFinding(report, "CFG_MODULE_ARGS_INVALID");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch.stages.s1.modules[0].with.MaxCandidate", finding.Path);
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
    public void ValidatePatchJson_ShouldReportModuleIdConflict_WhenModuleIdMatchesBlueprintNodeName()
    {
        var registry = new FlowRegistry();
        registry.Register(
            "HomeFeed",
            FlowBlueprint
                .Define<int, int>("TestFlow")
                .Stage(
                    "s1",
                    contract => contract.AllowDynamicModules(),
                    stage =>
                    {
                        stage
                            .Step("m1", moduleType: "test.module")
                            .Join<int>(
                                name: "j1",
                                join: _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0)));
                    })
                .Build());

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}]}}}}}");

        var finding = GetSingleFinding(report, "CFG_MODULE_ID_CONFLICTS_WITH_BLUEPRINT_NODE_NAME");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
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
                    contract => contract.AllowDynamicModules(),
                    stage =>
                    {
                        stage.Join<int>(
                            name: "j1",
                            join: _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0)));
                    })
                .Stage(
                    "s2",
                    contract => contract.AllowDynamicModules(),
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
                    contract => contract.AllowDynamicModules(),
                    stage =>
                    {
                        stage.Join<int>(
                            name: "j1",
                            join: _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0)));
                    })
                .Stage(
                    "s2",
                    contract => contract.AllowDynamicModules(),
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
    public void ValidatePatchJson_ShouldBeValid_WhenModuleGateRolloutIsValid()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"rollout\":{\"percent\":5,\"salt\":\"m1\"}}}]}}}}}");

        Assert.True(report.IsValid);
        Assert.Empty(report.Findings);
    }

    [Fact]
    public void ValidatePatchJson_ShouldBeValid_WhenExperimentPatchModuleGateRolloutIsValid()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"rollout\":{\"percent\":5,\"salt\":\"m1\"}}}]}}}}]}}}");

        Assert.True(report.IsValid);
        Assert.Empty(report.Findings);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportUnknownField_WhenGateRolloutHasUnknownField()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"rollout\":{\"percent\":5,\"salt\":\"m1\",\"oops\":1}}}]}}}}}");

        var finding = GetSingleFinding(report, "CFG_UNKNOWN_FIELD");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].gate.rollout.oops", finding.Path);
        Assert.Equal("Unknown field: oops", finding.Message);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportGateRolloutInvalid_WhenPercentIsOutOfRange()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"rollout\":{\"percent\":120,\"salt\":\"m1\"}}}]}}}}}");

        var finding = GetSingleFinding(report, "CFG_GATE_ROLLOUT_INVALID");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].gate.rollout.percent", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportGateRolloutInvalid_WhenPercentIsOutOfRangeInExperimentPatch()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"rollout\":{\"percent\":120,\"salt\":\"m1\"}}}]}}}}]}}}");

        var finding = GetSingleFinding(report, "CFG_GATE_ROLLOUT_INVALID");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch.stages.s1.modules[0].gate.rollout.percent", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportGateRolloutInvalid_WhenSaltIsMissing()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"rollout\":{\"percent\":5}}}]}}}}}");

        var finding = GetSingleFinding(report, "CFG_GATE_ROLLOUT_INVALID");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].gate.rollout.salt", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportGateRolloutInvalid_WhenSaltIsMissingInExperimentPatch()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"rollout\":{\"percent\":5}}}]}}}}]}}}");

        var finding = GetSingleFinding(report, "CFG_GATE_ROLLOUT_INVALID");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch.stages.s1.modules[0].gate.rollout.salt", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldBeValid_WhenModuleGateRequestIsValid()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"request\":{\"field\":\"region\",\"in\":[\"US\"]}}}]}}}}}");

        Assert.True(report.IsValid);
        Assert.Empty(report.Findings);
    }

    [Fact]
    public void ValidatePatchJson_ShouldBeValid_WhenExperimentPatchModuleGateRequestIsValid()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"request\":{\"field\":\"region\",\"in\":[\"US\"]}}}]}}}}]}}}");

        Assert.True(report.IsValid);
        Assert.Empty(report.Findings);
    }

    [Fact]
    public void ValidatePatchJson_ShouldBeValid_WhenModuleGateSelectorIsRegistered()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var selectors = new SelectorRegistry();
        selectors.Register("is_new_user", _ => true);

        var validator = new ConfigValidator(registry, catalog, selectors);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"selector\":\"is_new_user\"}}]}}}}}");

        Assert.True(report.IsValid);
        Assert.Empty(report.Findings);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportSelectorNotRegistered_WhenModuleGateSelectorIsNotRegistered()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var selectors = new SelectorRegistry();
        var validator = new ConfigValidator(registry, catalog, selectors);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"selector\":\"is_new_user\"}}]}}}}}");

        var finding = GetSingleFinding(report, "CFG_SELECTOR_NOT_REGISTERED");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].gate.selector", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldBeValid_WhenModuleShadowSampleIsValid()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m_shadow\",\"use\":\"test.module\",\"with\":{},\"shadow\":{\"sample\":0.02}}]}}}}}");

        Assert.True(report.IsValid);
        Assert.Empty(report.Findings);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportShadowSampleInvalid_WhenModuleShadowSampleIsOutOfRange()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m_shadow\",\"use\":\"test.module\",\"with\":{},\"shadow\":{\"sample\":1.1}}]}}}}}");

        var finding = GetSingleFinding(report, "CFG_SHADOW_SAMPLE_INVALID");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].shadow.sample", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportGateRequestFieldNotAllowed_WhenFieldIsNotAllowed()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"request\":{\"field\":\"raw_query\",\"in\":[\"x\"]}}}]}}}}}");

        var finding = GetSingleFinding(report, "CFG_GATE_REQUEST_FIELD_NOT_ALLOWED");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].gate.request.field", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportGateRequestFieldNotAllowed_WhenFieldIsNotAllowedInExperimentPatch()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var catalog = new ModuleCatalog();
        catalog.Register<ModuleArgs, int>("test.module", _ => new TestModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{},\"gate\":{\"request\":{\"field\":\"raw_query\",\"in\":[\"x\"]}}}]}}}}]}}}");

        var finding = GetSingleFinding(report, "CFG_GATE_REQUEST_FIELD_NOT_ALLOWED");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch.stages.s1.modules[0].gate.request.field", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
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
                contract => contract.AllowDynamicModules(),
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

    private sealed class MaxCandidateGreaterThanZeroValidator : IModuleArgsValidator<ModuleArgsWithMaxCandidate>
    {
        public bool TryValidate(ModuleArgsWithMaxCandidate args, out string? path, out string message)
        {
            if (args.MaxCandidate > 0)
            {
                path = null;
                message = string.Empty;
                return true;
            }

            path = nameof(ModuleArgsWithMaxCandidate.MaxCandidate);
            message = "MaxCandidate must be greater than 0.";
            return false;
        }
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

