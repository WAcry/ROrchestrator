using System.Text.Json;
using Rockestra.BootstrapperFixture;
using Rockestra.Core;
using Rockestra.Core.Blueprint;
using Rockestra.Core.Selectors;

namespace Rockestra.Cli.Tests;

public sealed class RockestraCliAppTests
{
    [Fact]
    public void Help_ShouldReturnJsonAndExitCode0()
    {
        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var exitCode = RockestraCliApp.Run(new[] { "--help" }, stdout, stderr);

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

        var exitCode = RockestraCliApp.Run(new[] { "validate", "--help" }, stdout, stderr);

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
            """{"schemaVersion":"v1","flows":{"HomeFeed":{"stages":{"s1":{"fanoutMax":1,"modules":[{"id":"m1","use":"test.module","with":{}}]}}}}}""";

        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var exitCode = RockestraCliApp.Run(
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
    public void Validate_ShouldReturnJsonAndExitCode0_WhenSelectorGateIsValid()
    {
        const string patchJson =
            """{"schemaVersion":"v1","flows":{"HomeFeed":{"stages":{"s1":{"fanoutMax":1,"modules":[{"id":"m1","use":"test.module","with":{},"gate":{"selector":"is_allowed"}}]}}}}}""";

        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var exitCode = RockestraCliApp.Run(
            new[]
            {
                "validate",
                "--bootstrapper-type",
                typeof(SelectorTestBootstrapper).FullName!,
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
    public void Validate_ShouldReturnJsonErrorAndNonZeroExitCode_WhenSelectorNotRegistered()
    {
        const string patchJson =
            """{"schemaVersion":"v1","flows":{"HomeFeed":{"stages":{"s1":{"fanoutMax":1,"modules":[{"id":"m1","use":"test.module","with":{},"gate":{"selector":"is_allowed"}}]}}}}}""";

        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var exitCode = RockestraCliApp.Run(
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

        Assert.NotEqual(0, exitCode);
        Assert.NotEqual(string.Empty, stderr.ToString());

        using var doc = JsonDocument.Parse(stdout.ToString());
        Assert.Equal("validate", doc.RootElement.GetProperty("kind").GetString());
        Assert.False(doc.RootElement.GetProperty("is_valid").GetBoolean());

        var findings = doc.RootElement.GetProperty("findings");
        var enumerator = findings.EnumerateArray();
        Assert.True(enumerator.MoveNext());

        var finding = enumerator.Current;
        Assert.Equal("CFG_SELECTOR_NOT_REGISTERED", finding.GetProperty("code").GetString());

        var message = finding.GetProperty("message").GetString();
        Assert.NotNull(message);
        Assert.Contains("selector is not registered", message, StringComparison.Ordinal);
    }

    [Fact]
    public void Validate_ShouldSupportBootstrapperAssemblyPath()
    {
        const string patchJson =
            """{"schemaVersion":"v1","flows":{"HomeFeed":{"stages":{"s1":{"fanoutMax":1,"modules":[{"id":"m1","use":"fixture.module","with":{},"gate":{"selector":"is_allowed"}}]}}}}}""";

        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var assemblyPath = GetFixtureAssemblyPath();

        var exitCode = RockestraCliApp.Run(
            new[]
            {
                "validate",
                "--bootstrapper",
                assemblyPath,
                "--bootstrapper-type",
                typeof(FixtureBootstrapper).FullName!,
                "--patch-json",
                patchJson,
            },
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.Equal(string.Empty, stderr.ToString());

        using var doc = JsonDocument.Parse(stdout.ToString());
        Assert.Equal("validate", doc.RootElement.GetProperty("kind").GetString());
        Assert.Equal("v1", doc.RootElement.GetProperty("tooling_json_version").GetString());
        Assert.True(doc.RootElement.GetProperty("is_valid").GetBoolean());
    }

    [Fact]
    public void Validate_ShouldReturnJsonErrorAndExitCode2_WhenBootstrapperAssemblyMissing()
    {
        const string patchJson =
            """{"schemaVersion":"v1","flows":{"HomeFeed":{"stages":{"s1":{"fanoutMax":1,"modules":[{"id":"m1","use":"fixture.module","with":{},"gate":{"selector":"is_allowed"}}]}}}}}""";

        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var assemblyPath = GetFixtureAssemblyPath();
        var missingAssemblyPath = Path.Combine(Path.GetDirectoryName(assemblyPath)!, $"missing_{Guid.NewGuid():N}.dll");

        var exitCode = RockestraCliApp.Run(
            new[]
            {
                "validate",
                "--bootstrapper",
                missingAssemblyPath,
                "--bootstrapper-type",
                typeof(FixtureBootstrapper).FullName!,
                "--patch-json",
                patchJson,
            },
            stdout,
            stderr);

        Assert.Equal(2, exitCode);
        Assert.NotEqual(string.Empty, stderr.ToString());

        using var doc = JsonDocument.Parse(stdout.ToString());
        Assert.Equal("cli_error", doc.RootElement.GetProperty("kind").GetString());
        Assert.Equal("CLI_BOOTSTRAP_FAILED", doc.RootElement.GetProperty("code").GetString());

        var message = doc.RootElement.GetProperty("message").GetString();
        Assert.NotNull(message);
        Assert.Contains("Bootstrapper assembly not found", message, StringComparison.Ordinal);
    }

    [Fact]
    public void Validate_ShouldReturnJsonErrorAndExitCode2_WhenBootstrapperTypeNotFound()
    {
        const string patchJson =
            """{"schemaVersion":"v1","flows":{"HomeFeed":{"stages":{"s1":{"fanoutMax":1,"modules":[{"id":"m1","use":"fixture.module","with":{},"gate":{"selector":"is_allowed"}}]}}}}}""";

        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var assemblyPath = GetFixtureAssemblyPath();

        var exitCode = RockestraCliApp.Run(
            new[]
            {
                "validate",
                "--bootstrapper",
                assemblyPath,
                "--bootstrapper-type",
                "Rockestra.BootstrapperFixture.TypeDoesNotExist",
                "--patch-json",
                patchJson,
            },
            stdout,
            stderr);

        Assert.Equal(2, exitCode);
        Assert.NotEqual(string.Empty, stderr.ToString());

        using var doc = JsonDocument.Parse(stdout.ToString());
        Assert.Equal("cli_error", doc.RootElement.GetProperty("kind").GetString());
        Assert.Equal("CLI_BOOTSTRAP_FAILED", doc.RootElement.GetProperty("code").GetString());
        Assert.Equal(
            "Bootstrapper type not found: 'Rockestra.BootstrapperFixture.TypeDoesNotExist'.",
            doc.RootElement.GetProperty("message").GetString());
    }

    [Fact]
    public void Validate_ShouldReturnJsonErrorAndExitCode2_WhenBootstrapperSignatureMismatch()
    {
        const string patchJson =
            """{"schemaVersion":"v1","flows":{"HomeFeed":{"stages":{"s1":{"fanoutMax":1,"modules":[{"id":"m1","use":"fixture.module","with":{},"gate":{"selector":"is_allowed"}}]}}}}}""";

        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var assemblyPath = GetFixtureAssemblyPath();

        var exitCode = RockestraCliApp.Run(
            new[]
            {
                "validate",
                "--bootstrapper",
                assemblyPath,
                "--bootstrapper-type",
                typeof(SignatureMismatchBootstrapper).FullName!,
                "--patch-json",
                patchJson,
            },
            stdout,
            stderr);

        Assert.Equal(2, exitCode);
        Assert.NotEqual(string.Empty, stderr.ToString());

        using var doc = JsonDocument.Parse(stdout.ToString());
        Assert.Equal("cli_error", doc.RootElement.GetProperty("kind").GetString());
        Assert.Equal("CLI_BOOTSTRAP_FAILED", doc.RootElement.GetProperty("code").GetString());

        var message = doc.RootElement.GetProperty("message").GetString();
        Assert.NotNull(message);
        Assert.Contains("must define a public static method", message, StringComparison.Ordinal);
    }

    [Fact]
    public void ExplainFlow_ShouldReturnJsonAndExitCode0()
    {
        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var exitCode = RockestraCliApp.Run(
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
    public void ExplainPatch_ShouldSupportBootstrapperAssemblyPath()
    {
        const string patchJson =
            """{"schemaVersion":"v1","flows":{"HomeFeed":{"stages":{"s1":{"fanoutMax":1,"modules":[{"id":"m1","use":"fixture.module","with":{},"gate":{"selector":"is_allowed"}}]}}}}}""";

        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var assemblyPath = GetFixtureAssemblyPath();

        var exitCode = RockestraCliApp.Run(
            new[]
            {
                "explain-patch",
                "--flow",
                "HomeFeed",
                "--bootstrapper",
                assemblyPath,
                "--bootstrapper-type",
                typeof(FixtureBootstrapper).FullName!,
                "--patch-json",
                patchJson,
            },
            stdout,
            stderr);

        Assert.Equal(0, exitCode);
        Assert.Equal(string.Empty, stderr.ToString());

        using var doc = JsonDocument.Parse(stdout.ToString());
        var root = doc.RootElement;
        Assert.Equal("explain_patch", root.GetProperty("kind").GetString());
        Assert.Equal("v1", root.GetProperty("tooling_json_version").GetString());
        Assert.Equal("HomeFeed", root.GetProperty("flow_name").GetString());
        Assert.True(root.GetProperty("stages").GetArrayLength() > 0);

        var stage = root.GetProperty("stages")[0];
        var module = stage.GetProperty("modules")[0];
        Assert.Equal("GATE_TRUE", module.GetProperty("gate_decision_code").GetString());
    }

    [Fact]
    public void ExplainPatch_ShouldReturnJsonAndExitCode0()
    {
        const string patchJson =
            """{"schemaVersion":"v1","flows":{"HomeFeed":{"stages":{"s1":{"fanoutMax":1,"modules":[{"id":"m1","use":"test.module","with":{}}]}}}}}""";

        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var exitCode = RockestraCliApp.Run(
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
    public void ExplainPatch_ShouldUseUserIdRequestAttributesAndQosTier()
    {
        const string patchJson = """
            {
              "schemaVersion": "v1",
              "flows": {
                "HomeFeed": {
                  "stages": {
                    "s1": {
                      "fanoutMax": 10,
                      "modules": [
                        {
                          "id": "m_rollout",
                          "use": "test.module",
                          "with": {},
                          "gate": { "rollout": { "percent": 100, "salt": "m_rollout" } }
                        },
                        {
                          "id": "m_region",
                          "use": "test.module",
                          "with": {},
                          "gate": { "request": { "field": "region", "in": [ "US" ] } }
                        }
                      ]
                    }
                  },
                  "qos": {
                    "tiers": {
                      "emergency": {
                        "patch": {
                          "stages": {
                            "s1": {
                              "modules": [ { "id": "m_region", "enabled": false } ]
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            """;

        using var stdoutFull = new StringWriter();
        using var stderrFull = new StringWriter();

        var exitCodeFull = RockestraCliApp.Run(
            new[]
            {
                "explain-patch",
                "--flow",
                "HomeFeed",
                "--patch-json",
                patchJson,
                "--user-id",
                "u1",
                "--request-attr",
                "region=US",
                "--qos-tier",
                "full",
                "--include-mermaid",
            },
            stdoutFull,
            stderrFull);

        Assert.Equal(0, exitCodeFull);
        Assert.Equal(string.Empty, stderrFull.ToString());

        using var docFull = JsonDocument.Parse(stdoutFull.ToString());
        var rootFull = docFull.RootElement;
        Assert.True(rootFull.TryGetProperty("mermaid", out var mermaid));
        Assert.Equal(JsonValueKind.String, mermaid.ValueKind);
        Assert.NotEqual(string.Empty, mermaid.GetString());

        Assert.Equal("GATE_TRUE", FindModule(rootFull, "m_rollout").GetProperty("gate_decision_code").GetString());
        Assert.Equal("GATE_TRUE", FindModule(rootFull, "m_region").GetProperty("gate_decision_code").GetString());
        Assert.Equal("full", rootFull.GetProperty("qos").GetProperty("selected_tier").GetString());

        using var stdoutNoUser = new StringWriter();
        using var stderrNoUser = new StringWriter();

        var exitCodeNoUser = RockestraCliApp.Run(
            new[]
            {
                "explain-patch",
                "--flow",
                "HomeFeed",
                "--patch-json",
                patchJson,
                "--request-attr",
                "region=US",
                "--qos-tier",
                "full",
            },
            stdoutNoUser,
            stderrNoUser);

        Assert.Equal(0, exitCodeNoUser);
        Assert.Equal(string.Empty, stderrNoUser.ToString());

        using var docNoUser = JsonDocument.Parse(stdoutNoUser.ToString());
        var rootNoUser = docNoUser.RootElement;
        Assert.Equal("GATE_FALSE", FindModule(rootNoUser, "m_rollout").GetProperty("gate_decision_code").GetString());

        using var stdoutEmergency = new StringWriter();
        using var stderrEmergency = new StringWriter();

        var exitCodeEmergency = RockestraCliApp.Run(
            new[]
            {
                "explain-patch",
                "--flow",
                "HomeFeed",
                "--patch-json",
                patchJson,
                "--user-id",
                "u1",
                "--request-attr",
                "region=US",
                "--qos-tier",
                "emergency",
            },
            stdoutEmergency,
            stderrEmergency);

        Assert.Equal(0, exitCodeEmergency);
        Assert.Equal(string.Empty, stderrEmergency.ToString());

        using var docEmergency = JsonDocument.Parse(stdoutEmergency.ToString());
        var rootEmergency = docEmergency.RootElement;
        Assert.Equal("emergency", rootEmergency.GetProperty("qos").GetProperty("selected_tier").GetString());
        var emergencyModule = FindModule(rootEmergency, "m_region");
        Assert.False(emergencyModule.GetProperty("enabled").GetBoolean());
        Assert.Equal("DISABLED", emergencyModule.GetProperty("decision_code").GetString());

        static JsonElement FindModule(JsonElement root, string moduleId)
        {
            foreach (var stage in root.GetProperty("stages").EnumerateArray())
            {
                foreach (var module in stage.GetProperty("modules").EnumerateArray())
                {
                    if (string.Equals(module.GetProperty("module_id").GetString(), moduleId, StringComparison.Ordinal))
                    {
                        return module;
                    }
                }
            }

            throw new InvalidOperationException($"Module not found: '{moduleId}'.");
        }
    }

    [Fact]
    public void DiffPatch_ShouldReturnJsonAndExitCode0()
    {
        const string oldPatchJson =
            """{"schemaVersion":"v1","flows":{"HomeFeed":{"stages":{"s1":{"modules":[{"id":"m1","use":"test.module","with":{}}]}}}}}""";

        const string newPatchJson =
            """{"schemaVersion":"v1","flows":{"HomeFeed":{"stages":{"s1":{"modules":[{"id":"m1","use":"test.module","with":{}},{"id":"m2","use":"test.module","with":{}}]}}}}}""";

        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var exitCode = RockestraCliApp.Run(
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
            """{"schemaVersion":"v1","flows":{"HomeFeed":{"stages":{"s1":{"fanoutMax":1,"modules":[{"id":"m1","use":"test.module","with":{}}]}}}}}""";

        const string matrixJson = """[{"l1":"A"},{"l1":"B"}]""";

        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var exitCode = RockestraCliApp.Run(
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
    public void PreviewMatrix_ShouldSupportBootstrapperAssemblyPath()
    {
        const string patchJson =
            """{"schemaVersion":"v1","flows":{"HomeFeed":{"stages":{"s1":{"fanoutMax":1,"modules":[{"id":"m1","use":"fixture.module","with":{},"priority":0,"gate":{"selector":"is_allowed"}}]}}}}}""";

        const string matrixJson = "[{}]";

        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var assemblyPath = GetFixtureAssemblyPath();

        var exitCode = RockestraCliApp.Run(
            new[]
            {
                "preview-matrix",
                "--flow",
                "HomeFeed",
                "--bootstrapper",
                assemblyPath,
                "--bootstrapper-type",
                typeof(FixtureBootstrapper).FullName!,
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
        var root = doc.RootElement;
        Assert.Equal("preview_matrix", root.GetProperty("kind").GetString());
        Assert.Equal("v1", root.GetProperty("tooling_json_version").GetString());
        Assert.Equal("HomeFeed", root.GetProperty("flow_name").GetString());

        var preview = root.GetProperty("previews")[0];
        var stage = preview.GetProperty("stages")[0];
        var selected = stage.GetProperty("selected_modules")[0];
        Assert.Equal("m1", selected.GetProperty("module_id").GetString());
    }

    [Fact]
    public void PreviewMatrix_ShouldUseUserIdRequestAttributesAndQosTier()
    {
        const string patchJson = """
            {
              "schemaVersion": "v1",
              "flows": {
                "HomeFeed": {
                  "stages": {
                    "s1": {
                      "fanoutMax": 1,
                      "modules": [
                        { "id": "m_default", "use": "test.module", "with": {}, "priority": 0 },
                        {
                          "id": "m_user",
                          "use": "test.module",
                          "with": {},
                          "priority": 10,
                          "gate": { "rollout": { "percent": 100, "salt": "m_user" } }
                        },
                        {
                          "id": "m_region",
                          "use": "test.module",
                          "with": {},
                          "priority": 20,
                          "gate": { "request": { "field": "region", "in": [ "US" ] } }
                        }
                      ]
                    }
                  },
                  "qos": {
                    "tiers": {
                      "emergency": {
                        "patch": {
                          "stages": {
                            "s1": {
                              "modules": [ { "id": "m_region", "enabled": false } ]
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            """;

        const string matrixJson = "[{}]";

        Assert.Equal("m_default", PreviewSelectedModuleId(patchJson, matrixJson));
        Assert.Equal("m_user", PreviewSelectedModuleId(patchJson, matrixJson, userId: "u1"));
        Assert.Equal("m_region", PreviewSelectedModuleId(patchJson, matrixJson, userId: "u1", requestAttr: "region=US"));
        Assert.Equal("m_user", PreviewSelectedModuleId(patchJson, matrixJson, userId: "u1", requestAttr: "region=US", qosTier: "emergency"));

        static string PreviewSelectedModuleId(
            string patchJson,
            string matrixJson,
            string? userId = null,
            string? requestAttr = null,
            string qosTier = "full")
        {
            using var stdout = new StringWriter();
            using var stderr = new StringWriter();

            var args = new List<string>(capacity: 16)
            {
                "preview-matrix",
                "--flow",
                "HomeFeed",
                "--patch-json",
                patchJson,
                "--matrix-json",
                matrixJson,
                "--qos-tier",
                qosTier,
            };

            if (!string.IsNullOrEmpty(userId))
            {
                args.Add("--user-id");
                args.Add(userId);
            }

            if (!string.IsNullOrEmpty(requestAttr))
            {
                args.Add("--request-attr");
                args.Add(requestAttr);
            }

            var exitCode = RockestraCliApp.Run(args.ToArray(), stdout, stderr);

            Assert.Equal(0, exitCode);
            Assert.Equal(string.Empty, stderr.ToString());

            using var doc = JsonDocument.Parse(stdout.ToString());
            var preview = doc.RootElement.GetProperty("previews")[0];
            var stage = preview.GetProperty("stages")[0];
            var selected = stage.GetProperty("selected_modules")[0];
            return selected.GetProperty("module_id").GetString() ?? string.Empty;
        }
    }

    [Fact]
    public void UnknownCommand_ShouldReturnJsonErrorAndExitCode2()
    {
        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        var exitCode = RockestraCliApp.Run(new[] { "unknown" }, stdout, stderr);

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
                        contract => contract.AllowDynamicModules(),
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

    public static class SelectorTestBootstrapper
    {
        public static void Configure(FlowRegistry registry, ModuleCatalog catalog, SelectorRegistry selectors)
        {
            TestBootstrapper.Configure(registry, catalog);
            selectors.Register("is_allowed", _ => true);
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

    private static string GetFixtureAssemblyPath()
    {
        var assemblyPath = typeof(FixtureBootstrapper).Assembly.Location;
        Assert.NotEqual(string.Empty, assemblyPath);
        Assert.True(File.Exists(assemblyPath), $"Fixture assembly not found: '{assemblyPath}'.");
        return assemblyPath;
    }
}

