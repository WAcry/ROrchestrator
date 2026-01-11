using Rockestra.Core;

namespace Rockestra.Core.Tests;

public sealed class PersistedLkgConfigProviderTests
{
    private static readonly DateTimeOffset FutureDeadline = new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task GetSnapshotAsync_WhenInnerThrowsAndStoreHasLkg_ShouldFallbackToStoreSnapshot()
    {
        var validPatch = """{"schemaVersion":"v1","flows":{}}""";

        var store = new FakeLkgSnapshotStore(
            loadResult: LkgSnapshotLoadResultKind.Loaded,
            loadedSnapshot: new ConfigSnapshot(configVersion: 10, validPatch, meta: new ConfigSnapshotMeta(source: "static", timestampUtc: DateTimeOffset.UtcNow)));

        var validator = new ConfigValidator(new FlowRegistry(), new ModuleCatalog());
        var provider = new PersistedLkgConfigProvider(new ThrowingConfigProvider(), validator, store);

        var explainSink = new ListExplainSink();
        var context = new FlowContext(new DummyServiceProvider(), CancellationToken.None, FutureDeadline, explainSink: explainSink);

        var snapshot = await provider.GetSnapshotAsync(context);
        Assert.Equal((ulong)10, snapshot.ConfigVersion);

        Assert.Equal("lkg", snapshot.Meta.Source);
        Assert.True(snapshot.Meta.TryGetLkgFallbackEvidence(out var evidence));
        Assert.True(evidence.Fallback);
        Assert.False(evidence.HasCandidateConfigVersion);
        Assert.Equal((ulong)10, evidence.LastGoodConfigVersion);

        Assert.True(explainSink.TryGet("config_lkg_fallback", out var fallback));
        Assert.Equal("true", fallback);
    }

    [Fact]
    public async Task GetSnapshotAsync_WhenStoreCorrupt_ShouldNotCrash()
    {
        var validPatch = """{"schemaVersion":"v1","flows":{}}""";

        var store = new FakeLkgSnapshotStore(loadResult: LkgSnapshotLoadResultKind.Corrupt, loadedSnapshot: default);
        var validator = new ConfigValidator(new FlowRegistry(), new ModuleCatalog());
        var provider = new PersistedLkgConfigProvider(new StaticConfigProvider(1, validPatch), validator, store);

        var context = new FlowContext(new DummyServiceProvider(), CancellationToken.None, FutureDeadline);
        var snapshot = await provider.GetSnapshotAsync(context);

        Assert.Equal((ulong)1, snapshot.ConfigVersion);
    }

    [Fact]
    public async Task GetSnapshotAsync_ShouldPersistOnlyWhenCandidateIsAccepted()
    {
        var validPatch = """{"schemaVersion":"v1","flows":{}}""";
        var invalidPatch = """{"flows":{}}""";

        var store = new FakeLkgSnapshotStore(loadResult: LkgSnapshotLoadResultKind.NotFound, loadedSnapshot: default);
        var validator = new ConfigValidator(new FlowRegistry(), new ModuleCatalog());
        var provider = new PersistedLkgConfigProvider(
            new SequenceConfigProvider(
                new ConfigSnapshot(configVersion: 1, validPatch, meta: new ConfigSnapshotMeta(source: "static", timestampUtc: DateTimeOffset.UtcNow)),
                new ConfigSnapshot(configVersion: 2, invalidPatch, meta: new ConfigSnapshotMeta(source: "static", timestampUtc: DateTimeOffset.UtcNow))),
            validator,
            store);

        var services = new DummyServiceProvider();

        var contextA = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var snapshotA = await provider.GetSnapshotAsync(contextA);
        Assert.Equal((ulong)1, snapshotA.ConfigVersion);
        Assert.Equal(1, store.StoreCallCount);
        Assert.Equal((ulong)1, store.LastStoredSnapshot.ConfigVersion);

        var contextB = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var snapshotB = await provider.GetSnapshotAsync(contextB);
        Assert.Equal((ulong)1, snapshotB.ConfigVersion);
        Assert.Equal(1, store.StoreCallCount);
        Assert.Equal((ulong)1, store.LastStoredSnapshot.ConfigVersion);
    }

    private sealed class DummyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType)
        {
            _ = serviceType;
            return null;
        }
    }

    private sealed class ThrowingConfigProvider : IConfigProvider
    {
        public ValueTask<ConfigSnapshot> GetSnapshotAsync(FlowContext context)
        {
            _ = context;
            throw new InvalidOperationException("boom");
        }
    }

    private sealed class StaticConfigProvider : IConfigProvider
    {
        private readonly ConfigSnapshot _snapshot;

        public StaticConfigProvider(ulong configVersion, string patchJson)
        {
            _snapshot = new ConfigSnapshot(
                configVersion,
                patchJson,
                new ConfigSnapshotMeta(source: "static", timestampUtc: DateTimeOffset.UtcNow));
        }

        public ValueTask<ConfigSnapshot> GetSnapshotAsync(FlowContext context)
        {
            _ = context;
            return new ValueTask<ConfigSnapshot>(_snapshot);
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

    private sealed class FakeLkgSnapshotStore : ILkgSnapshotStore
    {
        private readonly LkgSnapshotLoadResultKind _loadResult;
        private readonly ConfigSnapshot _loadedSnapshot;

        private int _storeCallCount;
        private ConfigSnapshot _lastStoredSnapshot;

        public int StoreCallCount => Volatile.Read(ref _storeCallCount);

        public ConfigSnapshot LastStoredSnapshot => _lastStoredSnapshot;

        public FakeLkgSnapshotStore(LkgSnapshotLoadResultKind loadResult, ConfigSnapshot loadedSnapshot)
        {
            _loadResult = loadResult;
            _loadedSnapshot = loadedSnapshot;
            _lastStoredSnapshot = new ConfigSnapshot(
                configVersion: 0,
                patchJson: string.Empty,
                meta: new ConfigSnapshotMeta(source: "static", timestampUtc: DateTimeOffset.UtcNow));
        }

        public LkgSnapshotLoadResultKind TryLoad(out ConfigSnapshot snapshot)
        {
            snapshot = _loadedSnapshot;
            return _loadResult;
        }

        public bool TryStore(in ConfigSnapshot snapshot)
        {
            Interlocked.Increment(ref _storeCallCount);
            _lastStoredSnapshot = snapshot;
            return true;
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


