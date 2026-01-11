using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Rockestra.Core;
using Rockestra.Core.Blueprint;
using Rockestra.Tooling;

namespace Rockestra.Tooling.Tests;

public sealed class ExecExplainJsonV3Tests
{
    private static readonly DateTimeOffset FutureDeadline = new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset SnapshotTimestampUtc = new(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset SnapshotTimestampUtcFresh = new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExportJson_ShouldMatchGoldenFile_AfterNormalization()
    {
        const string flowName = "ExecExplainFlowV3";

        using var listener = new ActivityListener
        {
            ShouldListenTo = source => string.Equals(source.Name, "Rockestra", StringComparison.Ordinal),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            SampleUsingParentId = static (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
        };

        ActivitySource.AddActivityListener(listener);

        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();

        catalog.Register<EmptyArgs, int>("test.ok", _ => new OkModule());
        catalog.Register<int, int>("test.compute", _ => new ComputeModule());

        var blueprint = FlowBlueprint
            .Define<int, int>(flowName)
            .Stage(
                "s1",
                contract => contract.AllowDynamicModules(),
                stage =>
                    stage
                        .Step("compute", moduleType: "test.compute")
                        .Join<int>(
                            "final",
                            ctx =>
                            {
                                if (!ctx.TryGetNodeOutcome<int>("compute", out var outcome))
                                {
                                    throw new InvalidOperationException("Outcome for node 'compute' has not been recorded.");
                                }

                                return new ValueTask<Outcome<int>>(outcome);
                            }))
            .Build();

        registry.Register<int, int, DefaultParams, DefaultParamsPatch>(
            flowName,
            blueprint,
            defaultParams: new DefaultParams { Token = "secret", A = 1 });

        var patchJson = $$"""
        {
          "schemaVersion": "v1",
          "flows": {
            "{{flowName}}": {
              "params": {
                "Token": "secret",
                "A": 1
              },
              "stages": {
                "s1": {
                  "fanoutMax": 3,
                  "modules": [
                    { "id": "m_disabled", "use": "test.ok", "with": {} },
                    { "id": "m_gate_false", "use": "test.ok", "with": {}, "gate": { "experiment": { "layer": "l1", "in": [ "B" ] } } },
                    { "id": "m_high", "use": "test.ok", "with": {}, "limitKey": "depA", "priority": 10 },
                    { "id": "m_low", "use": "test.ok", "with": {}, "priority": 0 },
                    { "id": "m_shadow", "use": "test.ok", "with": {}, "shadow": { "sample": 1 } }
                  ]
                }
              },
              "experiments": [
                {
                  "layer": "l1",
                  "variant": "A",
                  "patch": {
                    "params": { "Token": "secret2" },
                    "stages": {
                      "s1": {
                        "modules": [
                          { "id": "m_exp", "use": "test.ok", "with": {}, "priority": 5 }
                        ]
                      }
                    }
                  }
                }
              ],
              "emergency": {
                "reason": "r",
                "operator": "op",
                "ttl_minutes": 30,
                "patch": {
                  "params": { "Token": "secret3" },
                  "stages": {
                    "s1": {
                      "fanoutMax": 1,
                      "modules": [
                        { "id": "m_disabled", "enabled": false }
                      ]
                    }
                  }
                }
              }
            }
          }
        }
        """;

        var host = new FlowHost(registry, catalog, new StaticConfigProvider(configVersion: 42, patchJson, SnapshotTimestampUtcFresh));

        var requestOptions = new FlowRequestOptions(
            variants: new Dictionary<string, string>
            {
                { "l2", "ignored" },
                { "l1", "A" },
            },
            userId: "u1");

        var flowContext = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline, requestOptions);
        flowContext.EnableExecExplain(new ExplainOptions(ExplainLevel.Standard));

        var outcome = await host.ExecuteAsync<int, int>(flowName, request: 5, flowContext);
        Assert.True(outcome.IsOk);

        Assert.True(flowContext.TryGetExecExplain(out var explain));
        Assert.True(explain.TryGetConfigVersion(out var configVersion));
        Assert.Equal(42UL, configVersion);
        Assert.True(explain.OverlaysApplied.Count > 0);
        Assert.True(explain.StageModules.Count > 0);

        var json = ExecExplainJsonV3.ExportJson(explain);
        var normalized = JsonExecExplainV3Normalizer.Normalize(json);
        var expected = ReadGoldenFile("exec_explain_json_v3.json");

        Assert.Equal(expected, normalized);
    }

    [Fact]
    public async Task ExportJson_ShouldIncludeEffectiveParamsAndSources_WhenFullExplainIsAllowed()
    {
        const string flowName = "ExecExplainFlowV3Full";

        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();

        catalog.Register<EmptyArgs, int>("test.ok", _ => new OkModule());

        var blueprint = FlowBlueprint
            .Define<int, int>(flowName)
            .Stage(
                "s1",
                contract => contract.AllowDynamicModules(),
                stage =>
                    stage.Join<int>(
                        "final",
                        _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
            .Build();

        registry.Register<int, int, DefaultParams, DefaultParamsPatch>(
            flowName,
            blueprint,
            defaultParams: new DefaultParams { Token = "secret", A = 1 });

        var patchJson = $$"""
        {
          "schemaVersion": "v1",
          "flows": {
            "{{flowName}}": {
              "params": {
                "Token": "secret2",
                "A": 2
              },
              "experiments": [
                {
                  "layer": "l1",
                  "variant": "A",
                  "patch": { "params": { "Token": "secret3" } }
                }
              ]
            }
          }
        }
        """;

        var host = new FlowHost(registry, catalog, new StaticConfigProvider(configVersion: 1, patchJson));

        var requestOptions = new FlowRequestOptions(
            variants: new Dictionary<string, string>
            {
                { "l1", "A" },
            });

        var flowContext = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline, requestOptions);
        flowContext.EnableExecExplain(new ExplainOptions(ExplainLevel.Full, reason: "test_reason"));

        var outcome = await host.ExecuteAsync<int, int>(flowName, request: 0, flowContext);
        Assert.True(outcome.IsOk);

        Assert.True(flowContext.TryGetExecExplain(out var explain));
        Assert.True(explain.TryGetConfigVersion(out var configVersion));
        Assert.Equal(1UL, configVersion);

        var json = ExecExplainJsonV3.ExportJson(explain);

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        var paramsElement = root.GetProperty("params");
        Assert.True(paramsElement.TryGetProperty("hash", out _));
        Assert.True(paramsElement.TryGetProperty("effective", out var effective));
        Assert.True(effective.ValueKind == JsonValueKind.Object);
        Assert.Equal("[REDACTED]", effective.GetProperty("Token").GetString());

        var sources = paramsElement.GetProperty("sources");
        Assert.True(sources.ValueKind == JsonValueKind.Array);
        Assert.True(sources.GetArrayLength() > 0);
    }

    [Fact]
    public async Task ExportJson_ShouldIncludeEmergencyTtlExpiredReason_WhenEmergencyOverlayIsExpired()
    {
        const string flowName = "ExecExplainFlowV3TtlExpired";

        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();

        var blueprint = FlowBlueprint
            .Define<int, int>(flowName)
            .Stage(
                "s1",
                stage =>
                    stage.Join<int>(
                        "final",
                        _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
            .Build();

        registry.Register<int, int, DefaultParams, DefaultParamsPatch>(
            flowName,
            blueprint,
            defaultParams: new DefaultParams { Token = "secret", A = 1 });

        var patchJson = $$"""
        {
          "schemaVersion": "v1",
          "flows": {
            "{{flowName}}": {
              "params": { "A": 1 },
              "emergency": {
                "reason": "r",
                "operator": "op",
                "ttl_minutes": 30,
                "patch": { "params": { "A": 2 } }
              }
            }
          }
        }
        """;

        var host = new FlowHost(registry, catalog, new StaticConfigProvider(configVersion: 1, patchJson, SnapshotTimestampUtc));

        var flowContext = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline);
        flowContext.EnableExecExplain(new ExplainOptions(ExplainLevel.Standard));

        var outcome = await host.ExecuteAsync<int, int>(flowName, request: 0, flowContext);
        Assert.True(outcome.IsOk);

        Assert.True(flowContext.TryGetExecExplain(out var explain));

        var json = ExecExplainJsonV3.ExportJson(explain);

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.Equal("EMERGENCY_TTL_EXPIRED", root.GetProperty("emergency_ignored_reason_code").GetString());

        var overlays = root.GetProperty("overlays_applied").EnumerateArray();
        foreach (var overlay in overlays)
        {
            Assert.NotEqual("emergency", overlay.GetProperty("layer").GetString());
        }
    }

    [Fact]
    public async Task ExportJson_ShouldNotIncludeEmergencyTtlExpiredReason_WhenEmergencyOverlayIsApplied()
    {
        const string flowName = "ExecExplainFlowV3TtlApplied";

        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();

        var blueprint = FlowBlueprint
            .Define<int, int>(flowName)
            .Stage(
                "s1",
                stage =>
                    stage.Join<int>(
                        "final",
                        _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
            .Build();

        registry.Register<int, int, DefaultParams, DefaultParamsPatch>(
            flowName,
            blueprint,
            defaultParams: new DefaultParams { Token = "secret", A = 1 });

        var patchJson = $$"""
        {
          "schemaVersion": "v1",
          "flows": {
            "{{flowName}}": {
              "params": { "A": 1 },
              "emergency": {
                "reason": "r",
                "operator": "op",
                "ttl_minutes": 30,
                "patch": { "params": { "A": 2 } }
              }
            }
          }
        }
        """;

        var host = new FlowHost(registry, catalog, new StaticConfigProvider(configVersion: 1, patchJson, SnapshotTimestampUtcFresh));

        var flowContext = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline);
        flowContext.EnableExecExplain(new ExplainOptions(ExplainLevel.Standard));

        var outcome = await host.ExecuteAsync<int, int>(flowName, request: 0, flowContext);
        Assert.True(outcome.IsOk);

        Assert.True(flowContext.TryGetExecExplain(out var explain));

        var json = ExecExplainJsonV3.ExportJson(explain);

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.Equal(JsonValueKind.Null, root.GetProperty("emergency_ignored_reason_code").ValueKind);

        var hasEmergencyOverlay = false;
        var overlays = root.GetProperty("overlays_applied").EnumerateArray();
        foreach (var overlay in overlays)
        {
            if (string.Equals("emergency", overlay.GetProperty("layer").GetString(), StringComparison.Ordinal))
            {
                hasEmergencyOverlay = true;
                break;
            }
        }

        Assert.True(hasEmergencyOverlay);
    }

    [Fact]
    public async Task ExportJson_ShouldIncludeLkgFallbackEvidence_WhenConfigSnapshotIsFromLkgFallback()
    {
        const string flowName = "ExecExplainFlowV3LkgFallback";

        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();

        catalog.Register<int, int>("test.compute", _ => new ComputeModule());

        var blueprint = FlowBlueprint
            .Define<int, int>(flowName)
            .Stage(
                "s1",
                contract => contract.DisallowDynamicModules(),
                stage =>
                    stage
                        .Step("compute", moduleType: "test.compute")
                        .Join<int>(
                            "final",
                            ctx =>
                            {
                                if (!ctx.TryGetNodeOutcome<int>("compute", out var outcome))
                                {
                                    throw new InvalidOperationException("Outcome for node 'compute' has not been recorded.");
                                }

                                return new ValueTask<Outcome<int>>(outcome);
                            }))
            .Build();

        registry.Register<int, int>(flowName, blueprint);

        var validPatch = "{\"schemaVersion\":\"v1\",\"flows\":{}}";
        var invalidPatch = "{\"flows\":{}}";

        var configProvider = new SequenceConfigProvider(
            new ConfigSnapshot(configVersion: 1, validPatch, meta: new ConfigSnapshotMeta(source: "static", timestampUtc: SnapshotTimestampUtc)),
            new ConfigSnapshot(configVersion: 2, invalidPatch, meta: new ConfigSnapshotMeta(source: "static", timestampUtc: SnapshotTimestampUtc)));

        var host = new FlowHost(registry, catalog, configProvider);

        var contextA = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline);
        var outcomeA = await host.ExecuteAsync<int, int>(flowName, request: 1, contextA);
        Assert.True(outcomeA.IsOk);

        var contextB = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline);
        contextB.EnableExecExplain(new ExplainOptions(ExplainLevel.Standard));

        var outcomeB = await host.ExecuteAsync<int, int>(flowName, request: 1, contextB);
        Assert.True(outcomeB.IsOk);

        Assert.True(contextB.TryGetExecExplain(out var explain));
        Assert.True(explain.TryGetConfigVersion(out var configVersion));
        Assert.Equal(1UL, configVersion);

        var json = ExecExplainJsonV3.ExportJson(explain);
        using var doc = JsonDocument.Parse(json);

        var meta = doc.RootElement.GetProperty("config_snapshot_meta");
        Assert.Equal("lkg", meta.GetProperty("source").GetString());
        Assert.Equal(SnapshotTimestampUtc, meta.GetProperty("timestamp_utc").GetDateTimeOffset());

        var lkg = meta.GetProperty("lkg");
        Assert.Equal(JsonValueKind.Object, lkg.ValueKind);
        Assert.True(lkg.GetProperty("fallback").GetBoolean());
        Assert.Equal(1UL, lkg.GetProperty("last_good_config_version").GetUInt64());
        Assert.Equal(2UL, lkg.GetProperty("candidate_config_version").GetUInt64());
    }

    private static string ReadGoldenFile(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Golden", fileName);
        return File.ReadAllText(path, Encoding.UTF8).TrimEnd('\r', '\n');
    }

    private sealed class DefaultParams
    {
        public string? Token { get; set; }

        public int A { get; set; }
    }

    private sealed class DefaultParamsPatch
    {
        public string? Token { get; set; }

        public int A { get; set; }
    }

    private sealed class EmptyArgs
    {
    }

    private sealed class OkModule : IModule<EmptyArgs, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<EmptyArgs> context)
        {
            _ = context;
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(1));
        }
    }

    private sealed class ComputeModule : IModule<int, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(context.Args + 1));
        }
    }

    private sealed class StaticConfigProvider : IConfigProvider
    {
        private readonly ConfigSnapshot _snapshot;

        public StaticConfigProvider(ulong configVersion, string patchJson)
            : this(configVersion, patchJson, SnapshotTimestampUtc)
        {
        }

        public StaticConfigProvider(ulong configVersion, string patchJson, DateTimeOffset timestampUtc)
        {
            _snapshot = new ConfigSnapshot(
                configVersion,
                patchJson,
                new ConfigSnapshotMeta(source: "static", timestampUtc: timestampUtc));
        }

        public ValueTask<ConfigSnapshot> GetSnapshotAsync(FlowContext context)
        {
            _ = context;
            return new ValueTask<ConfigSnapshot>(_snapshot);
        }
    }

    private sealed class EmptyServiceProvider : IServiceProvider
    {
        public static readonly EmptyServiceProvider Instance = new();

        private EmptyServiceProvider()
        {
        }

        public object? GetService(Type serviceType)
        {
            _ = serviceType;
            return null;
        }
    }

    private sealed class SequenceConfigProvider : IConfigProvider
    {
        private readonly ConfigSnapshot[] _snapshots;
        private int _nextIndex;

        public SequenceConfigProvider(params ConfigSnapshot[] snapshots)
        {
            _snapshots = snapshots ?? throw new ArgumentNullException(nameof(snapshots));

            if (snapshots.Length == 0)
            {
                throw new ArgumentException("Snapshots must be non-empty.", nameof(snapshots));
            }
        }

        public ValueTask<ConfigSnapshot> GetSnapshotAsync(FlowContext context)
        {
            _ = context;

            var index = Interlocked.Increment(ref _nextIndex) - 1;
            if ((uint)index < (uint)_snapshots.Length)
            {
                return new ValueTask<ConfigSnapshot>(_snapshots[index]);
            }

            return new ValueTask<ConfigSnapshot>(_snapshots[_snapshots.Length - 1]);
        }
    }
}
