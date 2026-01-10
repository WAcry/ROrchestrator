using System.Globalization;
using Rockestra.Runner;

namespace Rockestra.Runner.Tests;

public sealed class RunnerSmokeTests
{
    [Fact]
    public async Task RunAsync_ShouldSucceed_AndProduceExpectedArtifacts()
    {
        using var writer = new StringWriter(CultureInfo.InvariantCulture);

        var exitCode = await RunnerApp.RunAsync(writer);

        Assert.Equal(0, exitCode);

        var output = writer.ToString();

        Assert.Contains("validate:", output, StringComparison.Ordinal);
        Assert.Contains("explain_flow:", output, StringComparison.Ordinal);
        Assert.Contains("explain_patch:", output, StringComparison.Ordinal);
        Assert.Contains("diff_patch:", output, StringComparison.Ordinal);
        Assert.Contains("exec_explain:", output, StringComparison.Ordinal);

        Assert.Contains("RunnerFlow", output, StringComparison.Ordinal);
        Assert.Contains("mermaid", output, StringComparison.Ordinal);
    }
}

