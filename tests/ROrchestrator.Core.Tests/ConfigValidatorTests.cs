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
}

