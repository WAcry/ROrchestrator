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
        var validator = new ConfigValidator(registry);

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
        var validator = new ConfigValidator(registry);

        var report = validator.ValidatePatchJson("{\"flows\":{}}");

        var finding = GetSingleFinding(report, "CFG_SCHEMA_VERSION_UNSUPPORTED");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.schemaVersion", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportUnknownField_WhenTopLevelFieldIsUnknown()
    {
        var registry = new FlowRegistry();
        var validator = new ConfigValidator(registry);

        var report = validator.ValidatePatchJson("{\"schemaVersion\":\"v1\",\"flows\":{},\"unknown\":123}");

        var finding = GetSingleFinding(report, "CFG_UNKNOWN_FIELD");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.unknown", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportFlowNotRegistered_WhenFlowIsNotInRegistry()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprint<int, int>("TestFlow", okValue: 0));

        var validator = new ConfigValidator(registry);

        var report = validator.ValidatePatchJson("{\"schemaVersion\":\"v1\",\"flows\":{\"NotAFlow\":{}}}");

        var finding = GetSingleFinding(report, "CFG_FLOW_NOT_REGISTERED");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.NotAFlow", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportStageNotInBlueprint_WhenStageKeyIsNotInBlueprint()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var validator = new ConfigValidator(registry);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s2\":{}}}}}");

        Assert.Single(report.Findings);

        var finding = GetSingleFinding(report, "CFG_STAGE_NOT_IN_BLUEPRINT");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.stages.s2", finding.Path);
    }

    [Fact]
    public void ValidatePatchJson_ShouldReportStageNotInBlueprint_WhenFlowIsRegisteredWithParams()
    {
        var registry = new FlowRegistry();
        var blueprint = CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0);
        registry.Register<int, int, TestParams, TestPatch>("HomeFeed", blueprint, new TestParams());

        var validator = new ConfigValidator(registry);

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

        var validator = new ConfigValidator(registry);

        var report = validator.ValidatePatchJson("{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{}}}");

        Assert.True(report.IsValid);
    }

    [Fact]
    public void ValidatePatchJson_ShouldBeValid_WhenStageKeysExistInBlueprint()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));

        var validator = new ConfigValidator(registry);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{}}}}}");

        Assert.True(report.IsValid);
    }

    [Fact]
    public void ValidatePatchJson_ShouldBeValid_WhenParamsPatchIsValid()
    {
        var registry = new FlowRegistry();
        var blueprint = CreateBlueprint<int, int>("TestFlow", okValue: 0);
        registry.Register<int, int, TestParams, ParamsPatchWithMaxCandidate>("HomeFeed", blueprint, new TestParams());

        var validator = new ConfigValidator(registry);

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

        var validator = new ConfigValidator(registry);

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

        var validator = new ConfigValidator(registry);

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

        var validator = new ConfigValidator(registry);

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

        var validator = new ConfigValidator(registry);

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

        var validator = new ConfigValidator(registry);

        var report = validator.ValidatePatchJson(
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"params\":{\"MaxCandidate\":10}}}}");

        var finding = GetSingleFinding(report, "CFG_PARAMS_BIND_FAILED");
        Assert.Equal(ValidationSeverity.Error, finding.Severity);
        Assert.Equal("$.flows.HomeFeed.params", finding.Path);
        Assert.False(string.IsNullOrEmpty(finding.Message));
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
}
