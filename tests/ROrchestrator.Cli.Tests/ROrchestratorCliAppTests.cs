using System.Text.Json;
using ROrchestrator.Core;
using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Cli.Tests;

public sealed class ROrchestratorCliAppTests
{
    [Fact]
    public void Help_ShouldReturnJsonAndExitCode0()
    {
        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var exitCode = ROrchestratorCliApp.Run(new[] { "--help" }, stdout, stderr);

        Assert.Equal(0, exitCode);
        Assert.Equal(string.Empty, stderr.ToString());

        using var doc = JsonDocument.Parse(stdout.ToString());
        Assert.Equal("help", doc.RootElement.GetProperty("kind").GetString());
        Assert.True(doc.RootElement.GetProperty("commands").GetArrayLength() > 0);
    }

    [Fact]
    public void CommandHelp_ShouldReturnJsonAndExitCode0()
    {
        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var exitCode = ROrchestratorCliApp.Run(new[] { "validate", "--help" }, stdout, stderr);

        Assert.Equal(0, exitCode);
        Assert.Equal(string.Empty, stderr.ToString());

        using var doc = JsonDocument.Parse(stdout.ToString());
        Assert.Equal("help", doc.RootElement.GetProperty("kind").GetString());

        var command = doc.RootElement.GetProperty("command");
        Assert.Equal("validate", command.GetProperty("name").GetString());
        Assert.NotEqual(string.Empty, command.GetProperty("usage").GetString());
    }

    [Fact]
    public void Validate_ShouldReturnJsonAndExitCode0_WhenPatchValid()
    {
        const string patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}]}}}}}";

        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var exitCode = ROrchestratorCliApp.Run(
            new[]
            {
                "validate",
                "--bootstrapper-type",
                typeof(TestBootstrapper).FullName!,
                "--patch-json",
                patchJson,
            },
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.Equal(string.Empty, stderr.ToString());

        using var doc = JsonDocument.Parse(stdout.ToString());
        Assert.Equal("validate", doc.RootElement.GetProperty("kind").GetString());
        Assert.True(doc.RootElement.GetProperty("is_valid").GetBoolean());
    }

    [Fact]
    public void ExplainFlow_ShouldReturnJsonAndExitCode0()
    {
        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var exitCode = ROrchestratorCliApp.Run(
            new[]
            {
                "explain-flow",
                "--bootstrapper-type",
                typeof(TestBootstrapper).FullName!,
                "--flow",
                "HomeFeed",
            },
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.Equal(string.Empty, stderr.ToString());

        using var doc = JsonDocument.Parse(stdout.ToString());
        Assert.Equal("explain", doc.RootElement.GetProperty("kind").GetString());
    }

    [Fact]
    public void ExplainPatch_ShouldReturnJsonAndExitCode0()
    {
        const string patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}]}}}}}";

        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var exitCode = ROrchestratorCliApp.Run(
            new[]
            {
                "explain-patch",
                "--flow",
                "HomeFeed",
                "--patch-json",
                patchJson,
            },
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.Equal(string.Empty, stderr.ToString());

        using var doc = JsonDocument.Parse(stdout.ToString());
        Assert.Equal("explain_patch", doc.RootElement.GetProperty("kind").GetString());
    }

    [Fact]
    public void DiffPatch_ShouldReturnJsonAndExitCode0()
    {
        const string oldPatchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}]}}}}}";

        const string newPatchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}},{\"id\":\"m2\",\"use\":\"test.module\",\"with\":{}}]}}}}}";

        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var exitCode = ROrchestratorCliApp.Run(
            new[]
            {
                "diff-patch",
                "--old-json",
                oldPatchJson,
                "--new-json",
                newPatchJson,
            },
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.Equal(string.Empty, stderr.ToString());

        using var doc = JsonDocument.Parse(stdout.ToString());
        Assert.Equal("diff", doc.RootElement.GetProperty("kind").GetString());
    }

    [Fact]
    public void PreviewMatrix_ShouldReturnJsonAndExitCode0()
    {
        const string patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"fanoutMax\":1,\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}]}}}}}";

        const string matrixJson = "[{\"l1\":\"A\"},{\"l1\":\"B\"}]";

        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var exitCode = ROrchestratorCliApp.Run(
            new[]
            {
                "preview-matrix",
                "--flow",
                "HomeFeed",
                "--patch-json",
                patchJson,
                "--matrix-json",
                matrixJson,
            },
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.Equal(string.Empty, stderr.ToString());

        using var doc = JsonDocument.Parse(stdout.ToString());
        Assert.Equal("preview_matrix", doc.RootElement.GetProperty("kind").GetString());
        Assert.True(doc.RootElement.GetProperty("previews").GetArrayLength() == 2);
    }

    [Fact]
    public void UnknownCommand_ShouldReturnJsonErrorAndExitCode2()
    {
        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var exitCode = ROrchestratorCliApp.Run(new[] { "unknown" }, stdout, stderr);

        Assert.Equal(2, exitCode);
        Assert.NotEqual(string.Empty, stderr.ToString());

        using var doc = JsonDocument.Parse(stdout.ToString());
        Assert.Equal("cli_error", doc.RootElement.GetProperty("kind").GetString());
    }

    public static class TestBootstrapper
    {
        public static void Configure(FlowRegistry registry, ModuleCatalog catalog)
        {
            registry.Register(
                "HomeFeed",
                FlowBlueprint
                    .Define<TestArgs, int>("HomeFeed")
                    .Stage(
                        "s1",
                        stage =>
                        {
                            stage
                                .Step("n1", moduleType: "test.module")
                                .Join(
                                    "join",
                                    join: _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0)));
                        })
                    .Build());

            catalog.Register<TestArgs, int>("test.module", _ => new DummyModule());
        }
    }

    public sealed class TestArgs
    {
    }

    public sealed class DummyModule : IModule<TestArgs, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<TestArgs> context)
        {
            _ = context;
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(0));
        }
    }
}
