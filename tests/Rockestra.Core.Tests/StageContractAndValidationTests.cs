using System.Text.Json;
using Rockestra.Core.Blueprint;

namespace Rockestra.Core.Tests;

public sealed class StageContractAndValidationTests
{
    [Fact]
    public void ValidatePatchJson_DefaultStageContract_ShouldDisallowDynamicModules()
    {
        var registry = new FlowRegistry();
        registry.Register(
            "HomeFeed",
            FlowBlueprint.Define<int, int>("TestFlow")
                .Stage(
                    "s1",
                    stage =>
                        stage.Join<int>(
                            "final",
                            _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
                .Build());

        var catalog = new ModuleCatalog();
        catalog.Register<EmptyArgs, int>("test.ok", _ => new OkModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.ok\",\"with\":{}}]}}}}}");

        var finding = GetSingleFinding(report, "CFG_STAGE_DYNAMIC_MODULES_FORBIDDEN");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldRejectModuleTypeNotInStageAllowlist()
    {
        var registry = new FlowRegistry();
        registry.Register(
            "HomeFeed",
            FlowBlueprint.Define<int, int>("TestFlow")
                .Stage(
                    "s1",
                    contract => contract.AllowDynamicModules().AllowModuleTypes("test.allowed"),
                    stage =>
                        stage.Join<int>(
                            "final",
                            _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
                .Build());

        var catalog = new ModuleCatalog();
        catalog.Register<EmptyArgs, int>("test.ok", _ => new OkModule());
        catalog.Register<EmptyArgs, int>("test.allowed", _ => new OkModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.ok\",\"with\":{}}]}}}}}");

        var finding = GetSingleFinding(report, "CFG_STAGE_MODULE_TYPE_FORBIDDEN");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].use", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldWarn_WhenEnabledModulesExceedStageWarnLimit()
    {
        var registry = new FlowRegistry();
        registry.Register(
            "HomeFeed",
            FlowBlueprint.Define<int, int>("TestFlow")
                .Stage(
                    "s1",
                    contract => contract.AllowDynamicModules().MaxModules(warn: 1, hard: 2),
                    stage =>
                        stage.Join<int>(
                            "final",
                            _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
                .Build());

        var catalog = new ModuleCatalog();
        catalog.Register<EmptyArgs, int>("test.ok", _ => new OkModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.ok\",\"with\":{}}," +
            "{\"id\":\"m2\",\"use\":\"test.ok\",\"with\":{}}" +
            "]}}}}}");

        Assert.True(report.IsValid);

        var finding = GetSingleFinding(report, "CFG_STAGE_MODULE_COUNT_WARN_EXCEEDED");
        Assert.Equal(ValidationSeverity.Warn, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldError_WhenEnabledModulesExceedStageHardLimit()
    {
        var registry = new FlowRegistry();
        registry.Register(
            "HomeFeed",
            FlowBlueprint.Define<int, int>("TestFlow")
                .Stage(
                    "s1",
                    contract => contract.AllowDynamicModules().MaxModules(warn: 1, hard: 1),
                    stage =>
                        stage.Join<int>(
                            "final",
                            _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
                .Build());

        var catalog = new ModuleCatalog();
        catalog.Register<EmptyArgs, int>("test.ok", _ => new OkModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.ok\",\"with\":{}}," +
            "{\"id\":\"m2\",\"use\":\"test.ok\",\"with\":{}}" +
            "]}}}}}");

        var finding = GetSingleFinding(report, "CFG_STAGE_MODULE_COUNT_HARD_EXCEEDED");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldRejectShadow_WhenStageContractDisallowsShadow()
    {
        var registry = new FlowRegistry();
        registry.Register(
            "HomeFeed",
            FlowBlueprint.Define<int, int>("TestFlow")
                .Stage(
                    "s1",
                    contract => contract.AllowDynamicModules().DisallowShadowModules(),
                    stage =>
                        stage.Join<int>(
                            "final",
                            _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
                .Build());

        var catalog = new ModuleCatalog();
        catalog.Register<EmptyArgs, int>("test.ok", _ => new OkModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.ok\",\"with\":{},\"shadow\":{\"sample\":1}}" +
            "]}}}}}");

        var finding = GetSingleFinding(report, "CFG_STAGE_SHADOW_FORBIDDEN");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].shadow", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldError_WhenShadowModulesExceedStageHardLimit()
    {
        var registry = new FlowRegistry();
        registry.Register(
            "HomeFeed",
            FlowBlueprint.Define<int, int>("TestFlow")
                .Stage(
                    "s1",
                    contract => contract.AllowDynamicModules().MaxShadowModules(1),
                    stage =>
                        stage.Join<int>(
                            "final",
                            _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
                .Build());

        var catalog = new ModuleCatalog();
        catalog.Register<EmptyArgs, int>("test.ok", _ => new OkModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.ok\",\"with\":{},\"shadow\":{\"sample\":1}}," +
            "{\"id\":\"m2\",\"use\":\"test.ok\",\"with\":{},\"shadow\":{\"sample\":1}}" +
            "]}}}}}");

        var finding = GetSingleFinding(report, "CFG_STAGE_SHADOW_MODULE_COUNT_HARD_EXCEEDED");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldRejectShadowSampleBps_WhenExceedingStageContractMax()
    {
        var registry = new FlowRegistry();
        registry.Register(
            "HomeFeed",
            FlowBlueprint.Define<int, int>("TestFlow")
                .Stage(
                    "s1",
                    contract => contract.AllowDynamicModules().MaxShadowSampleBps(5000),
                    stage =>
                        stage.Join<int>(
                            "final",
                            _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
                .Build());

        var catalog = new ModuleCatalog();
        catalog.Register<EmptyArgs, int>("test.ok", _ => new OkModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.ok\",\"with\":{},\"shadow\":{\"sample\":1}}" +
            "]}}}}}");

        var finding = GetSingleFinding(report, "CFG_STAGE_SHADOW_SAMPLE_BPS_EXCEEDED");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].shadow.sample", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldRejectFanoutMax_WhenOutsideStageContractRange()
    {
        var registry = new FlowRegistry();
        registry.Register(
            "HomeFeed",
            FlowBlueprint.Define<int, int>("TestFlow")
                .Stage(
                    "s1",
                    contract => contract.AllowDynamicModules().FanoutMaxRange(min: 2, max: 4),
                    stage =>
                        stage.Join<int>(
                            "final",
                            _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
                .Build());

        var catalog = new ModuleCatalog();
        catalog.Register<EmptyArgs, int>("test.ok", _ => new OkModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.ok\",\"with\":{}}" +
            "]}}}}}");

        var finding = GetSingleFinding(report, "CFG_STAGE_FANOUT_MAX_OUT_OF_RANGE");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.fanoutMax", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldAllowEmergencyPatchParams_AndValidateBinding()
    {
        var registry = new FlowRegistry();
        registry.Register<int, int, DefaultParams, NestedParamsPatch>(
            "HomeFeed",
            FlowBlueprint.Define<int, int>("TestFlow")
                .Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0)))
                .Build(),
            defaultParams: new DefaultParams());

        var validator = new ConfigValidator(registry, new ModuleCatalog());

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"emergency\":{\"reason\":\"r\",\"operator\":\"op\",\"ttl_minutes\":30," +
            "\"patch\":{\"params\":{\"Sub\":{\"MaxCandidate\":1}}}}}}}");

        Assert.True(report.IsValid);
        Assert.Empty(report.Findings);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportParamsBindFailed_WhenEmergencyPatchParamsCannotBind_AndMapDeepJsonExceptionPath()
    {
        var registry = new FlowRegistry();
        registry.Register<int, int, DefaultParams, NestedParamsPatch>(
            "HomeFeed",
            FlowBlueprint.Define<int, int>("TestFlow")
                .Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0)))
                .Build(),
            defaultParams: new DefaultParams());

        var validator = new ConfigValidator(registry, new ModuleCatalog());

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"emergency\":{\"reason\":\"r\",\"operator\":\"op\",\"ttl_minutes\":30," +
            "\"patch\":{\"params\":{\"Sub\":{\"MaxCandidate\":\"oops\"}}}}}}}");

        var finding = GetSingleFinding(report, "CFG_PARAMS_BIND_FAILED");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.emergency.patch.params.Sub.MaxCandidate", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldValidateParamsUnknownFields_Recursively_AndReportPreciseArrayPaths()
    {
        var registry = new FlowRegistry();
        registry.Register<int, int, DefaultParams, NestedParamsPatch>(
            "HomeFeed",
            FlowBlueprint.Define<int, int>("TestFlow")
                .Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0)))
                .Build(),
            defaultParams: new DefaultParams());

        var validator = new ConfigValidator(registry, new ModuleCatalog());

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"params\":{\"Sub\":{\"Items\":[{\"Name\":\"a\",\"Extra\":1}]}}}}}");

        var finding = GetSingleFinding(report, "CFG_PARAMS_UNKNOWN_FIELD");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.params.Sub.Items[0].Extra", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_RecursiveParamsUnknownFieldValidation_ShouldTreatDictionaryAndJsonElementAsOpaque()
    {
        var registry = new FlowRegistry();
        registry.Register<int, int, DefaultParams, OpaqueParamsPatch>(
            "HomeFeed",
            FlowBlueprint.Define<int, int>("TestFlow")
                .Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0)))
                .Build(),
            defaultParams: new DefaultParams());

        var validator = new ConfigValidator(registry, new ModuleCatalog());

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"params\":{\"Bag\":{\"x\":1,\"y\":2},\"Raw\":{\"a\":{\"b\":1}}}}}}");

        Assert.True(report.IsValid);
        Assert.Empty(report.Findings);
    }

    [Fact]
    public void ValidatePatchJson_ShouldValidateModuleArgsUnknownFields_Recursively()
    {
        var registry = new FlowRegistry();
        registry.Register(
            "HomeFeed",
            FlowBlueprint.Define<int, int>("TestFlow")
                .Stage(
                    "s1",
                    contract => contract.AllowDynamicModules(),
                    stage =>
                        stage.Join<int>(
                            "final",
                            _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
                .Build());

        var catalog = new ModuleCatalog();
        catalog.Register<ComplexArgs, int>("test.ok", _ => new ComplexArgsModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.ok\",\"with\":{\"Sub\":{\"Items\":[{\"A\":1,\"Extra\":2}]}}}" +
            "]}}}}}");

        var finding = GetSingleFinding(report, "CFG_MODULE_ARGS_UNKNOWN_FIELD");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].with.Sub.Items[0].Extra", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_RecursiveModuleArgsUnknownFieldValidation_ShouldTreatDictionaryAsOpaque()
    {
        var registry = new FlowRegistry();
        registry.Register(
            "HomeFeed",
            FlowBlueprint.Define<int, int>("TestFlow")
                .Stage(
                    "s1",
                    contract => contract.AllowDynamicModules(),
                    stage =>
                        stage.Join<int>(
                            "final",
                            _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
                .Build());

        var catalog = new ModuleCatalog();
        catalog.Register<ArgsWithBag, int>("test.ok", _ => new BagModule());

        var validator = new ConfigValidator(registry, catalog);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.ok\",\"with\":{\"Bag\":{\"any\":1,\"other\":2}}}" +
            "]}}}}}");

        Assert.True(report.IsValid);
        Assert.Empty(report.Findings);
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

        ValidationFinding? finding = null;
        var findings = report.Findings;

        for (var i = 0; i < findings.Count; i++)
        {
            if (!string.Equals(code, findings[i].Code, StringComparison.Ordinal))
            {
                continue;
            }

            if (finding is not null)
            {
                Assert.Fail($"Expected a single finding with code '{code}', but found multiple.");
            }

            finding = findings[i];
        }

        if (finding is null)
        {
            Assert.Fail($"Expected finding with code '{code}', but none was found.");
        }

        return finding.Value;
    }

    private sealed class EmptyArgs
    {
    }

    private sealed class OkModule : IModule<EmptyArgs, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<EmptyArgs> context)
        {
            _ = context;
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(0));
        }
    }

    private sealed class DefaultParams
    {
    }

    private sealed class NestedParamsPatch
    {
        public SubPatch? Sub { get; set; }
    }

    private sealed class SubPatch
    {
        public int MaxCandidate { get; set; }

        public ItemPatch[]? Items { get; set; }
    }

    private sealed class ItemPatch
    {
        public string? Name { get; set; }
    }

    private sealed class OpaqueParamsPatch
    {
        public Dictionary<string, int>? Bag { get; set; }

        public JsonElement Raw { get; set; }
    }

    private sealed class ComplexArgs
    {
        public ComplexSubArgs? Sub { get; set; }
    }

    private sealed class ComplexSubArgs
    {
        public ComplexItemArgs[]? Items { get; set; }
    }

    private sealed class ComplexItemArgs
    {
        public int A { get; set; }
    }

    private sealed class ComplexArgsModule : IModule<ComplexArgs, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<ComplexArgs> context)
        {
            _ = context;
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(0));
        }
    }

    private sealed class ArgsWithBag
    {
        public Dictionary<string, int>? Bag { get; set; }
    }

    private sealed class BagModule : IModule<ArgsWithBag, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<ArgsWithBag> context)
        {
            _ = context;
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(0));
        }
    }
}
