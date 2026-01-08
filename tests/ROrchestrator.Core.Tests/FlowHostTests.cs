using ROrchestrator.Core;
using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Core.Tests;

public sealed class FlowHostTests
{
    private static readonly DateTimeOffset FutureDeadline = new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_ShouldExecuteFlowAndReturnOutcome()
    {
        var services = new DummyServiceProvider();
        var context = new FlowContext(services, CancellationToken.None, FutureDeadline);

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        var registry = new FlowRegistry();
        registry.Register<int, int>(
            "test_flow",
            FlowBlueprint.Define<int, int>("TestFlow")
                .Step("step_a", "m.add_one")
                .Join<int>(
                    "final",
                    ctx =>
                    {
                        Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var stepOutcome));
                        Assert.True(stepOutcome.IsOk);
                        return new ValueTask<Outcome<int>>(Outcome<int>.Ok(stepOutcome.Value + 10));
                    })
                .Build());

        var host = new FlowHost(registry, catalog);

        var result = await host.ExecuteAsync<int, int>("test_flow", request: 5, context);

        Assert.True(result.IsOk);
        Assert.Equal(16, result.Value);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldExposeConfigSnapshotInFlowContext()
    {
        var services = new DummyServiceProvider();
        var context = new FlowContext(services, CancellationToken.None, FutureDeadline);

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        var registry = new FlowRegistry();
        registry.Register<int, int>("test_flow", CreateTestFlowBlueprint());

        var configProvider = new StaticConfigProvider(configVersion: 456, patchJson: "patch");
        var host = new FlowHost(registry, catalog, configProvider);

        var result = await host.ExecuteAsync<int, int>("test_flow", request: 1, context);
        Assert.True(result.IsOk);

        Assert.True(context.TryGetConfigVersion(out var configVersion));
        Assert.Equal((ulong)456, configVersion);

        Assert.True(context.TryGetConfigSnapshot(out var snapshot));
        Assert.Equal((ulong)456, snapshot.ConfigVersion);
        Assert.Equal("patch", snapshot.PatchJson);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldThrow_WhenFlowNameIsUnknown()
    {
        var services = new DummyServiceProvider();
        var context = new FlowContext(services, CancellationToken.None, FutureDeadline);

        var host = new FlowHost(new FlowRegistry(), new ModuleCatalog());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => host.ExecuteAsync<int, int>("unknown_flow", request: 1, context).AsTask());

        Assert.Contains("unknown_flow", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldReuseCachedPlanTemplate_ForSameFlowName()
    {
        var services = new DummyServiceProvider();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        var registry = new FlowRegistry();
        registry.Register<int, int>(
            "test_flow",
            FlowBlueprint.Define<int, int>("TestFlow")
                .Step("step_a", "m.add_one")
                .Join<int>(
                    "final",
                    ctx =>
                    {
                        Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var stepOutcome));
                        Assert.True(stepOutcome.IsOk);
                        return new ValueTask<Outcome<int>>(Outcome<int>.Ok(stepOutcome.Value));
                    })
                .Build());

        var host = new FlowHost(registry, catalog);

        Assert.Equal(0, host.CachedPlanTemplateCount);

        var contextA = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var resultA = await host.ExecuteAsync<int, int>("test_flow", request: 1, contextA);
        Assert.True(resultA.IsOk);
        Assert.Equal(2, resultA.Value);
        Assert.Equal(1, host.CachedPlanTemplateCount);

        var contextB = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var resultB = await host.ExecuteAsync<int, int>("test_flow", request: 2, contextB);
        Assert.True(resultB.IsOk);
        Assert.Equal(3, resultB.Value);
        Assert.Equal(1, host.CachedPlanTemplateCount);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldNotRecompile_WhenConfigVersionIsSame()
    {
        var services = new DummyServiceProvider();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        var registry = new FlowRegistry();
        registry.Register<int, int>("test_flow", CreateTestFlowBlueprint());

        var configProvider = new StaticConfigProvider(configVersion: 123, patchJson: string.Empty);
        var compiler = new CountingPlanCompiler();

        var host = new FlowHost(registry, catalog, configProvider, compiler);

        var contextA = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var resultA = await host.ExecuteAsync<int, int>("test_flow", request: 1, contextA);
        Assert.True(resultA.IsOk);
        Assert.Equal(2, resultA.Value);

        var contextB = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var resultB = await host.ExecuteAsync<int, int>("test_flow", request: 2, contextB);
        Assert.True(resultB.IsOk);
        Assert.Equal(3, resultB.Value);

        Assert.Equal(1, compiler.CompileCallCount);
        Assert.Equal(1, host.CachedPlanTemplateCount);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldRecompile_WhenConfigVersionChanges()
    {
        var services = new DummyServiceProvider();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        var registry = new FlowRegistry();
        registry.Register<int, int>("test_flow", CreateTestFlowBlueprint());

        var configProvider = new SequenceConfigProvider(
            new ConfigSnapshot(configVersion: 1, patchJson: string.Empty),
            new ConfigSnapshot(configVersion: 2, patchJson: string.Empty));
        var compiler = new CountingPlanCompiler();

        var host = new FlowHost(registry, catalog, configProvider, compiler);

        var contextA = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var resultA = await host.ExecuteAsync<int, int>("test_flow", request: 1, contextA);
        Assert.True(resultA.IsOk);
        Assert.Equal(2, resultA.Value);

        var contextB = new FlowContext(services, CancellationToken.None, FutureDeadline);
        var resultB = await host.ExecuteAsync<int, int>("test_flow", request: 1, contextB);
        Assert.True(resultB.IsOk);
        Assert.Equal(2, resultB.Value);

        Assert.Equal(2, compiler.CompileCallCount);
        Assert.Equal(2, host.CachedPlanTemplateCount);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldCompileOnce_ForConcurrentCallsWithSameConfigVersion()
    {
        var services = new DummyServiceProvider();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        var registry = new FlowRegistry();
        registry.Register<int, int>("test_flow", CreateTestFlowBlueprint());

        var configProvider = new StaticConfigProvider(configVersion: 123, patchJson: string.Empty);
        using var compiler = new BlockingCountingPlanCompiler();

        var host = new FlowHost(registry, catalog, configProvider, compiler);

        const int concurrency = 32;
        var startGate = new ManualResetEventSlim(initialState: false);

        var tasks = new Task<Outcome<int>>[concurrency];

        for (var i = 0; i < tasks.Length; i++)
        {
            tasks[i] = Task.Run(
                async () =>
                {
                    startGate.Wait(TimeSpan.FromSeconds(5));
                    var context = new FlowContext(services, CancellationToken.None, FutureDeadline);
                    return await host.ExecuteAsync<int, int>("test_flow", request: 1, context);
                });
        }

        startGate.Set();

        Assert.True(compiler.WaitForFirstCompile(TimeSpan.FromSeconds(5)));
        compiler.AllowCompilesToFinish();

        var results = await Task.WhenAll(tasks);
        for (var i = 0; i < results.Length; i++)
        {
            Assert.True(results[i].IsOk);
            Assert.Equal(2, results[i].Value);
        }

        Assert.Equal(1, compiler.CompileCallCount);
        Assert.Equal(1, host.CachedPlanTemplateCount);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldUseConfigSnapshot_WithinSameExecutionChain()
    {
        var services = new DummyServiceProvider();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        var registry = new FlowRegistry();
        registry.Register<int, int>("test_flow", CreateTestFlowBlueprint());

        var configProvider = new IncrementingConfigProvider();
        var compiler = new CountingPlanCompiler();

        var host = new FlowHost(registry, catalog, configProvider, compiler);

        var context = new FlowContext(services, CancellationToken.None, FutureDeadline);

        var resultA = await host.ExecuteAsync<int, int>("test_flow", request: 1, context);
        Assert.True(resultA.IsOk);
        Assert.Equal(2, resultA.Value);

        var resultB = await host.ExecuteAsync<int, int>("test_flow", request: 1, context);
        Assert.True(resultB.IsOk);
        Assert.Equal(2, resultB.Value);

        Assert.Equal(1, configProvider.CallCount);
        Assert.Equal(1, compiler.CompileCallCount);
    }

    private sealed class DummyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType)
        {
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

    private sealed class StaticConfigProvider : IConfigProvider
    {
        private readonly ConfigSnapshot _snapshot;

        public StaticConfigProvider(ulong configVersion, string patchJson)
        {
            _snapshot = new ConfigSnapshot(configVersion, patchJson);
        }

        public ValueTask<ConfigSnapshot> GetSnapshotAsync(FlowContext context)
        {
            return new ValueTask<ConfigSnapshot>(_snapshot);
        }
    }

    private sealed class SequenceConfigProvider : IConfigProvider
    {
        private readonly ConfigSnapshot[] _snapshots;
        private int _nextIndex;

        public SequenceConfigProvider(params ConfigSnapshot[] snapshots)
        {
            if (snapshots is null)
            {
                throw new ArgumentNullException(nameof(snapshots));
            }

            if (snapshots.Length == 0)
            {
                throw new ArgumentException("Snapshots must be non-empty.", nameof(snapshots));
            }

            _snapshots = snapshots;
        }

        public ValueTask<ConfigSnapshot> GetSnapshotAsync(FlowContext context)
        {
            var index = Interlocked.Increment(ref _nextIndex) - 1;

            if ((uint)index < (uint)_snapshots.Length)
            {
                return new ValueTask<ConfigSnapshot>(_snapshots[index]);
            }

            return new ValueTask<ConfigSnapshot>(_snapshots[_snapshots.Length - 1]);
        }
    }

    private sealed class IncrementingConfigProvider : IConfigProvider
    {
        private int _callCount;

        public int CallCount => Volatile.Read(ref _callCount);

        public ValueTask<ConfigSnapshot> GetSnapshotAsync(FlowContext context)
        {
            var version = (ulong)Interlocked.Increment(ref _callCount);
            return new ValueTask<ConfigSnapshot>(new ConfigSnapshot(version, patchJson: string.Empty));
        }
    }

    private sealed class CountingPlanCompiler : IPlanCompiler
    {
        private int _compileCallCount;

        public int CompileCallCount => Volatile.Read(ref _compileCallCount);

        public PlanTemplate<TReq, TResp> Compile<TReq, TResp>(FlowBlueprint<TReq, TResp> blueprint, ModuleCatalog catalog)
        {
            Interlocked.Increment(ref _compileCallCount);
            return PlanCompiler.Compile(blueprint, catalog);
        }
    }

    private sealed class BlockingCountingPlanCompiler : IPlanCompiler, IDisposable
    {
        private readonly ManualResetEventSlim _compileStarted;
        private readonly ManualResetEventSlim _allowCompile;
        private int _compileCallCount;

        public int CompileCallCount => Volatile.Read(ref _compileCallCount);

        public BlockingCountingPlanCompiler()
        {
            _compileStarted = new ManualResetEventSlim(initialState: false);
            _allowCompile = new ManualResetEventSlim(initialState: false);
        }

        public PlanTemplate<TReq, TResp> Compile<TReq, TResp>(FlowBlueprint<TReq, TResp> blueprint, ModuleCatalog catalog)
        {
            Interlocked.Increment(ref _compileCallCount);
            _compileStarted.Set();

            if (!_allowCompile.Wait(TimeSpan.FromSeconds(5)))
            {
                throw new TimeoutException("Timed out waiting for compile gate.");
            }

            return PlanCompiler.Compile(blueprint, catalog);
        }

        public bool WaitForFirstCompile(TimeSpan timeout)
        {
            return _compileStarted.Wait(timeout);
        }

        public void AllowCompilesToFinish()
        {
            _allowCompile.Set();
        }

        public void Dispose()
        {
            _compileStarted.Dispose();
            _allowCompile.Dispose();
        }
    }
}
