using ROrchestrator.Core;

namespace ROrchestrator.Core.Tests;

public sealed class ValidationReportTests
{
    [Fact]
    public void IsValid_ShouldBeTrue_WhenOnlyWarningsOrInfo()
    {
        var report = new ValidationReport(
            new[]
            {
                new ValidationFinding(ValidationSeverity.Warn, code: "TEST_WARN", path: "$", message: "warn"),
                new ValidationFinding(ValidationSeverity.Info, code: "TEST_INFO", path: "$", message: "info"),
            });

        Assert.True(report.IsValid);
    }

    [Fact]
    public void IsValid_ShouldBeFalse_WhenContainsError()
    {
        var report = new ValidationReport(
            new[]
            {
                new ValidationFinding(ValidationSeverity.Warn, code: "TEST_WARN", path: "$", message: "warn"),
                new ValidationFinding(ValidationSeverity.Error, code: "TEST_ERROR", path: "$", message: "error"),
            });

        Assert.False(report.IsValid);
    }
}

