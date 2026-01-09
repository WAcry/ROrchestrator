using ROrchestrator.Core;
using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Core.Tests;

public sealed class LkgConfigProviderTests
{
    private static readonly DateTimeOffset FutureDeadline = new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_ShouldFallbackToLastKnownGood_WhenConfigProviderReturnsInvalidPatch()
    {
        var services = new DummyServiceProvider();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        var registry = new FlowRegistry();
        registry.Register<int, int>("test_flow", CreateTestFlowBlueprint());

        var validPatch = "{\"schemaVersion\":\"v1\",\"flows\":{}}";
        var invalidPatch = "{\"flows\":{}}";

        var configProvider = new SequenceConfigProvider(
            new ConfigSnapshot(configVersion: 1, validPatch),
            new ConfigSnapshot(configVersion: 2, invalidPatch));

        var host = new FlowHost(registry, catalog, configProvider);

        var contextA = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var outcomeA = await host.ExecuteAsync<int, int>("test_flow", request: 1, contextA);
        Assert.True(outcomeA.IsOk);

        var explainSink = new ListExplainSink();
        var contextB = new FlowContext(services, CancellationToken.None, FutureDeadline, explainSink: explainSink);
        var outcomeB = await host.ExecuteAsync<int, int>("test_flow", request: 1, contextB);
        Assert.True(outcomeB.IsOk);

        Assert.True(contextB.TryGetConfigVersion(out var configVersion));
        Assert.Equal((ulong)1, configVersion);

        Assert.True(explainSink.TryGet("config_lkg_fallback", out var fallback));
        Assert.Equal("true", fallback);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldAcceptPatch_WithNoFlowsObject()
    {
        var services = new DummyServiceProvider();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        var registry = new FlowRegistry();
        registry.Register<int, int>("test_flow", CreateTestFlowBlueprint());

        var patchWithFlows = "{\"schemaVersion\":\"v1\",\"flows\":{}}";
        var patchWithoutFlows = "{\"schemaVersion\":\"v1\"}";

        var configProvider = new SequenceConfigProvider(
            new ConfigSnapshot(configVersion: 1, patchWithFlows),
            new ConfigSnapshot(configVersion: 2, patchWithoutFlows));

        var host = new FlowHost(registry, catalog, configProvider);

        var contextA = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var outcomeA = await host.ExecuteAsync<int, int>("test_flow", request: 1, contextA);
        Assert.True(outcomeA.IsOk);

        var explainSink = new ListExplainSink();
        var contextB = new FlowContext(services, CancellationToken.None, FutureDeadline, explainSink: explainSink);
        var outcomeB = await host.ExecuteAsync<int, int>("test_flow", request: 1, contextB);
        Assert.True(outcomeB.IsOk);

        Assert.True(contextB.TryGetConfigVersion(out var configVersion));
        Assert.Equal((ulong)2, configVersion);

        Assert.False(explainSink.TryGet("config_lkg_fallback", out _));
    }

    [Fact]
    public async Task GetSnapshotAsync_ShouldValidateOnlyOnce_WhenConfigVersionIsUnchanged()
    {
        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();
        var validator = new ConfigValidator(registry, catalog);

        var innerProvider = new CountingConfigProvider(configVersion: 10, patchJson: "{\"schemaVersion\":\"v1\",\"flows\":{}}");
        var provider = new LkgConfigProvider(innerProvider, validator);

        var services = new DummyServiceProvider();

        for (var i = 0; i < 5; i++)
        {
            var context = new FlowContext(services, CancellationToken.None, FutureDeadline);
            var snapshot = await provider.GetSnapshotAsync(context);
            Assert.Equal((ulong)10, snapshot.ConfigVersion);
        }

        Assert.Equal(1, provider.ValidationAttemptCount);
    }

    private sealed class DummyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType)
        {
            _ = serviceType;
            return null;
        }
    }

    private static FlowBlueprint<int, int> CreateTestFlowBlueprint()
    {
        return FlowBlueprint.Define<int, int>("TestFlow")
            .Step("step_a", "m.add_one")
            .Join<int>(
                "final",
                ctx =>
                {
                    Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var stepOutcome));
                    Assert.True(stepOutcome.IsOk);
                    return new ValueTask<Outcome<int>>(Outcome<int>.Ok(stepOutcome.Value));
                })
            .Build();
    }

    private sealed class AddOneModule : IModule<int, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(context.Args + 1));
        }
    }

    private sealed class SequenceConfigProvider : IConfigProvider
    {
        private readonly ConfigSnapshot[] _snapshots;
        private int _nextIndex;

        public SequenceConfigProvider(params ConfigSnapshot[] snapshots)
        {
            _snapshots = snapshots;
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

    private sealed class CountingConfigProvider : IConfigProvider
    {
        private readonly ulong _configVersion;
        private readonly string _patchJson;
        private int _callCount;

        public int CallCount => Volatile.Read(ref _callCount);

        public CountingConfigProvider(ulong configVersion, string patchJson)
        {
            _configVersion = configVersion;
            _patchJson = patchJson;
        }

        public ValueTask<ConfigSnapshot> GetSnapshotAsync(FlowContext context)
        {
            _ = context;
            Interlocked.Increment(ref _callCount);
            return new ValueTask<ConfigSnapshot>(new ConfigSnapshot(_configVersion, new string(_patchJson.AsSpan())));
        }
    }

    private sealed class ListExplainSink : IExplainSink
    {
        private readonly Dictionary<string, string> _values = new();

        public void Add(string key, string value)
        {
            _values[key] = value;
        }

        public bool TryGet(string key, out string value)
        {
            return _values.TryGetValue(key, out value!);
        }
    }
}
