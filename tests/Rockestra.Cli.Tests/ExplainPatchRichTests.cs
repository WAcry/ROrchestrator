using System.Text.Json;
using Rockestra.Core;
using Rockestra.Core.Blueprint;
using Rockestra.Core.Selectors;

namespace Rockestra.Cli.Tests;

public sealed class ExplainPatchRichTests
{
    [Fact]
    public void ExplainPatchRich_ShouldMatchGolden_GateFalse()
    {
        const string patchJson =
            """
            {"schemaVersion":"v1","flows":{"RichFlow":{"stages":{"s1":{"fanoutMax":2,"modules":[{"id":"m_gate_false","use":"test.module","with":{},"gate":{"selector":"always_false"}},{"id":"m_ok","use":"test.module","with":{}}]}},"params":{"A":2}}}}
            """;

        var (exitCode, stdout, stderr) = RunExplainPatchRich(
            flowName: "RichFlow",
            patchJson: patchJson,
            qosTier: "full");

        Assert.Equal(0, exitCode);
        Assert.Equal(string.Empty, stderr);
        Assert.Equal(ReadGoldenFile("explain_patch_rich_gate_false.json"), stdout);

        using var doc = JsonDocument.Parse(stdout);
        Assert.Equal("explain_patch_rich", doc.RootElement.GetProperty("kind").GetString());
        Assert.True(doc.RootElement.GetProperty("validation").GetProperty("is_valid").GetBoolean());
    }

    [Fact]
    public void ExplainPatchRich_ShouldMatchGolden_FanoutTrim()
    {
        const string patchJson =
            """
            {"schemaVersion":"v1","flows":{"RichFlow":{"stages":{"s1":{"fanoutMax":1,"modules":[{"id":"m_high","use":"test.module","with":{},"priority":10},{"id":"m_mid","use":"test.module","with":{},"priority":5},{"id":"m_low","use":"test.module","with":{}}]}}}}}
            """;

        var (exitCode, stdout, stderr) = RunExplainPatchRich(
            flowName: "RichFlow",
            patchJson: patchJson,
            qosTier: "full");

        Assert.Equal(0, exitCode);
        Assert.Equal(string.Empty, stderr);
        Assert.Equal(ReadGoldenFile("explain_patch_rich_fanout_trim.json"), stdout);
    }

    [Fact]
    public void ExplainPatchRich_ShouldMatchGolden_QosFull()
    {
        const string patchJson =
            """
            {"schemaVersion":"v1","flows":{"RichFlow":{"params":{"A":2,"Token":"abc"},"stages":{"s1":{"fanoutMax":2,"modules":[{"id":"m1","use":"test.module","with":{}},{"id":"m2","use":"test.module","with":{}}]}},"qos":{"tiers":{"emergency":{"patch":{"params":{"A":3},"stages":{"s1":{"modules":[{"id":"m2","enabled":false}]}}}}}}}}}
            """;

        var (exitCode, stdout, stderr) = RunExplainPatchRich(
            flowName: "RichFlow",
            patchJson: patchJson,
            qosTier: "full");

        Assert.Equal(0, exitCode);
        Assert.Equal(string.Empty, stderr);
        Assert.Equal(ReadGoldenFile("explain_patch_rich_qos_full.json"), stdout);
    }

    [Fact]
    public void ExplainPatchRich_ShouldMatchGolden_QosEmergency()
    {
        const string patchJson =
            """
            {"schemaVersion":"v1","flows":{"RichFlow":{"params":{"A":2,"Token":"abc"},"stages":{"s1":{"fanoutMax":2,"modules":[{"id":"m1","use":"test.module","with":{}},{"id":"m2","use":"test.module","with":{}}]}},"qos":{"tiers":{"emergency":{"patch":{"params":{"A":3},"stages":{"s1":{"modules":[{"id":"m2","enabled":false}]}}}}}}}}}
            """;

        var (exitCode, stdout, stderr) = RunExplainPatchRich(
            flowName: "RichFlow",
            patchJson: patchJson,
            qosTier: "emergency");

        Assert.Equal(0, exitCode);
        Assert.Equal(string.Empty, stderr);
        Assert.Equal(ReadGoldenFile("explain_patch_rich_qos_emergency.json"), stdout);
    }

    [Fact]
    public void ExplainPatchRich_ShouldMatchGolden_StageContractDeny()
    {
        const string patchJson =
            """
            {"schemaVersion":"v1","flows":{"NoDynamicFlow":{"stages":{"s1":{"fanoutMax":1,"modules":[{"id":"m1","use":"test.module","with":{}}]}}}}}
            """;

        var (exitCode, stdout, stderr) = RunExplainPatchRich(
            flowName: "NoDynamicFlow",
            patchJson: patchJson,
            qosTier: "full");

        Assert.Equal(2, exitCode);
        Assert.NotEqual(string.Empty, stderr);
        Assert.Equal(ReadGoldenFile("explain_patch_rich_stage_contract_deny.json"), stdout);

        using var doc = JsonDocument.Parse(stdout);
        Assert.Equal("explain_patch_rich", doc.RootElement.GetProperty("kind").GetString());
        Assert.False(doc.RootElement.GetProperty("validation").GetProperty("is_valid").GetBoolean());
    }

    private static (int exitCode, string stdout, string stderr) RunExplainPatchRich(string flowName, string patchJson, string qosTier)
    {
        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var exitCode = RockestraCliApp.Run(
            new[]
            {
                "explain-patch",
                "--rich",
                "--bootstrapper-type",
                typeof(RichExplainPatchBootstrapper).FullName!,
                "--flow",
                flowName,
                "--patch-json",
                patchJson,
                "--qos-tier",
                qosTier,
            },
            stdout,
            stderr);

        return (exitCode, stdout.ToString().TrimEnd(), stderr.ToString());
    }

    private static string ReadGoldenFile(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Golden", fileName);
        return File.ReadAllText(path).TrimEnd();
    }

    public static class RichExplainPatchBootstrapper
    {
        public static void Configure(FlowRegistry registry, ModuleCatalog catalog, SelectorRegistry selectors)
        {
            registry.Register<int, int, RichParams, RichParamsPatch>(
                "RichFlow",
                FlowBlueprint
                    .Define<int, int>("RichFlow")
                    .Stage(
                        "s1",
                        contract => contract.AllowDynamicModules(),
                        stage => stage.Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
                    .Build(),
                defaultParams: new RichParams());

            registry.Register(
                "NoDynamicFlow",
                FlowBlueprint
                    .Define<int, int>("NoDynamicFlow")
                    .Stage(
                        "s1",
                        stage => stage.Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
                    .Build());

            catalog.Register<RichModuleArgs, int>("test.module", _ => new DummyModule());

            selectors.Register("always_false", _ => false);
        }
    }

    public sealed class RichModuleArgs
    {
    }

    public sealed class RichParams
    {
        public int A { get; set; }

        public string? Token { get; set; }
    }

    public sealed class RichParamsPatch
    {
        public int? A { get; set; }

        public string? Token { get; set; }
    }

    public sealed class DummyModule : IModule<RichModuleArgs, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<RichModuleArgs> context)
        {
            _ = context;
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(0));
        }
    }
}
