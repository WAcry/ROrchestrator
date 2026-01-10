using Rockestra.Core;
using Rockestra.Core.Blueprint;

namespace Rockestra.Core.Tests;

public sealed class ModuleConcurrencyContractTests
{
    private static readonly DateTimeOffset FutureDeadline = new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_ShouldFailFast_WhenSingletonNotThreadSafeRunsConcurrently()
    {
        const string flowName = "singleton_not_thread_safe_concurrent";

        var catalog = new ModuleCatalog();
        catalog.Register<EmptyArgs, int>(
            "test.singleton",
            _ => new DelayedModule(delayMs: 50),
            lifetime: ModuleLifetime.Singleton,
            threadSafety: ModuleThreadSafety.NotThreadSafe);

        var registry = new FlowRegistry();
        registry.Register(
            flowName,
            FlowBlueprint.Define<int, int>(flowName)
                .Stage(
                    "s1",
                    contract => contract.AllowDynamicModules(),
                    stage =>
                        stage.Join<int>(
                            "final",
                            _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
                .Build());

        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"" + flowName + "\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":2,\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.singleton\",\"with\":{}}," +
            "{\"id\":\"m2\",\"use\":\"test.singleton\",\"with\":{}}" +
            "]}}}}}";

        var host = new FlowHost(registry, catalog, new StaticConfigProvider(configVersion: 1, patchJson));

        var context = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline);

        var ex = await Assert.ThrowsAsync<ModuleConcurrencyViolationException>(
            async () => await host.ExecuteAsync<int, int>(flowName, request: 0, context));

        Assert.Contains(flowName, ex.Message, StringComparison.Ordinal);
        Assert.Contains("m2", ex.Message, StringComparison.Ordinal);
        Assert.Contains("test.singleton", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldAllowConcurrentExecution_WhenSingletonIsThreadSafe()
    {
        const string flowName = "singleton_thread_safe_concurrent";

        var catalog = new ModuleCatalog();
        catalog.Register<EmptyArgs, int>(
            "test.singleton",
            _ => new DelayedModule(delayMs: 50),
            lifetime: ModuleLifetime.Singleton,
            threadSafety: ModuleThreadSafety.ThreadSafe);

        var registry = new FlowRegistry();
        registry.Register(
            flowName,
            FlowBlueprint.Define<int, int>(flowName)
                .Stage(
                    "s1",
                    contract => contract.AllowDynamicModules(),
                    stage =>
                        stage.Join<int>(
                            "final",
                            _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0))))
                .Build());

        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"" + flowName + "\":{" +
            "\"stages\":{\"s1\":{\"fanoutMax\":2,\"modules\":[" +
            "{\"id\":\"m1\",\"use\":\"test.singleton\",\"with\":{}}," +
            "{\"id\":\"m2\",\"use\":\"test.singleton\",\"with\":{}}" +
            "]}}}}}";

        var host = new FlowHost(registry, catalog, new StaticConfigProvider(configVersion: 1, patchJson));

        var context = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline);

        var outcome = await host.ExecuteAsync<int, int>(flowName, request: 0, context);
        Assert.True(outcome.IsOk);
    }

    private sealed class EmptyArgs
    {
    }

    private sealed class DelayedModule : IModule<EmptyArgs, int>
    {
        private readonly int _delayMs;

        public DelayedModule(int delayMs)
        {
            _delayMs = delayMs;
        }

        public async ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<EmptyArgs> context)
        {
            _ = context;
            await Task.Delay(_delayMs).ConfigureAwait(false);
            return Outcome<int>.Ok(1);
        }
    }

    private sealed class StaticConfigProvider : IConfigProvider
    {
        private readonly ConfigSnapshot _snapshot;

        public StaticConfigProvider(ulong configVersion, string patchJson)
        {
            _snapshot = new ConfigSnapshot(configVersion, patchJson);
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
}

