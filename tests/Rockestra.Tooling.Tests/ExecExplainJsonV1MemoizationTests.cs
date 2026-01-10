using System.Text.Json;
using Rockestra.Core;
using Rockestra.Core.Blueprint;
using Rockestra.Tooling;

namespace Rockestra.Tooling.Tests;

public sealed class ExecExplainJsonV1MemoizationTests
{
    private static readonly DateTimeOffset FutureDeadline = new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_WhenMemoDisabled_SameKey_ShouldNotReuse()
    {
        const string flowName = "MemoFlow_MemoDisabled";

        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();

        CountingMemoModule.Reset();

        catalog.Register<EmptyArgs, int>("test.memo", _ => new CountingMemoModule(delayMs: 10, outcome: Outcome<int>.Ok(1)));

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
            "{\"id\":\"m1\",\"use\":\"test.memo\",\"with\":{},\"memoKey\":\"k\"}," +
            "{\"id\":\"m2\",\"use\":\"test.memo\",\"with\":{},\"memoKey\":\"k\"}" +
            "]}}}}}";

        var host = new FlowHost(registry, catalog, new StaticConfigProvider(configVersion: 1, patchJson));

        var context = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline);

        var outcome = await host.ExecuteAsync<int, int>(flowName, request: 0, context);
        Assert.True(outcome.IsOk);

        Assert.Equal(2, CountingMemoModule.ExecuteCount);
    }

    [Fact]
    public async Task ExecuteAsync_WhenMemoEnabled_ConcurrentSameKey_ShouldExecuteOnce_AndMarkMemoHit()
    {
        const string flowName = "MemoFlow_SameKey";

        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();

        CountingMemoModule.Reset();

        catalog.Register<EmptyArgs, int>("test.memo", _ => new CountingMemoModule(delayMs: 30, outcome: Outcome<int>.Ok(1)));

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
            "{\"id\":\"m1\",\"use\":\"test.memo\",\"with\":{},\"memoKey\":\"k\"}," +
            "{\"id\":\"m2\",\"use\":\"test.memo\",\"with\":{},\"memoKey\":\"k\"}" +
            "]}}}}}";

        var host = new FlowHost(registry, catalog, new StaticConfigProvider(configVersion: 1, patchJson));

        var context = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline);
        context.EnableExecExplain(ExplainLevel.Standard);
        context.EnableRequestMemo();

        var outcome = await host.ExecuteAsync<int, int>(flowName, request: 0, context);
        Assert.True(outcome.IsOk);

        Assert.Equal(1, CountingMemoModule.ExecuteCount);

        Assert.True(context.TryGetExecExplain(out var explain));
        var json = ExecExplainJsonV1.ExportJson(explain);

        using var document = JsonDocument.Parse(json);
        var stageModules = document.RootElement.GetProperty("stage_modules");

        Assert.Equal(2, stageModules.GetArrayLength());

        var m1 = stageModules[0];
        var m2 = stageModules[1];

        Assert.Equal("m1", m1.GetProperty("module_id").GetString());
        Assert.False(m1.GetProperty("memo_hit").GetBoolean());

        Assert.Equal("m2", m2.GetProperty("module_id").GetString());
        Assert.True(m2.GetProperty("memo_hit").GetBoolean());
    }

    [Fact]
    public async Task ExecuteAsync_WhenMemoEnabled_DifferentKeys_ShouldNotReuse()
    {
        const string flowName = "MemoFlow_DifferentKeys";

        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();

        CountingMemoModule.Reset();

        catalog.Register<EmptyArgs, int>("test.memo", _ => new CountingMemoModule(delayMs: 10, outcome: Outcome<int>.Ok(1)));

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
            "{\"id\":\"m1\",\"use\":\"test.memo\",\"with\":{},\"memoKey\":\"k1\"}," +
            "{\"id\":\"m2\",\"use\":\"test.memo\",\"with\":{},\"memoKey\":\"k2\"}" +
            "]}}}}}";

        var host = new FlowHost(registry, catalog, new StaticConfigProvider(configVersion: 1, patchJson));

        var context = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline);
        context.EnableExecExplain(ExplainLevel.Standard);
        context.EnableRequestMemo();

        var outcome = await host.ExecuteAsync<int, int>(flowName, request: 0, context);
        Assert.True(outcome.IsOk);

        Assert.Equal(2, CountingMemoModule.ExecuteCount);
    }

    [Fact]
    public async Task ExecuteAsync_WhenMemoEnabled_ShouldNotReuseAcrossRequests()
    {
        const string flowName = "MemoFlow_CrossRequest";

        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();

        CountingMemoModule.Reset();

        catalog.Register<EmptyArgs, int>("test.memo", _ => new CountingMemoModule(delayMs: 10, outcome: Outcome<int>.Ok(1)));

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
            "{\"id\":\"m1\",\"use\":\"test.memo\",\"with\":{},\"memoKey\":\"k\"}," +
            "{\"id\":\"m2\",\"use\":\"test.memo\",\"with\":{},\"memoKey\":\"k\"}" +
            "]}}}}}";

        var host = new FlowHost(registry, catalog, new StaticConfigProvider(configVersion: 1, patchJson));

        for (var i = 0; i < 2; i++)
        {
            var context = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline);
            context.EnableRequestMemo();
            var outcome = await host.ExecuteAsync<int, int>(flowName, request: 0, context);
            Assert.True(outcome.IsOk);
        }

        Assert.Equal(2, CountingMemoModule.ExecuteCount);
    }

    [Fact]
    public async Task ExecuteAsync_WhenMemoEnabled_ErrorOutcome_ShouldBeReused()
    {
        const string flowName = "MemoFlow_Error";

        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();

        CountingMemoModule.Reset();

        catalog.Register<EmptyArgs, int>("test.memo", _ => new CountingMemoModule(delayMs: 10, outcome: Outcome<int>.Error("X")));

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
            "{\"id\":\"m1\",\"use\":\"test.memo\",\"with\":{},\"memoKey\":\"k\"}," +
            "{\"id\":\"m2\",\"use\":\"test.memo\",\"with\":{},\"memoKey\":\"k\"}" +
            "]}}}}}";

        var host = new FlowHost(registry, catalog, new StaticConfigProvider(configVersion: 1, patchJson));

        var context = new FlowContext(services: EmptyServiceProvider.Instance, CancellationToken.None, FutureDeadline);
        context.EnableExecExplain(ExplainLevel.Standard);
        context.EnableRequestMemo();

        var outcome = await host.ExecuteAsync<int, int>(flowName, request: 0, context);
        Assert.True(outcome.IsOk);

        Assert.Equal(1, CountingMemoModule.ExecuteCount);

        Assert.True(context.TryGetExecExplain(out var explain));
        var json = ExecExplainJsonV1.ExportJson(explain);

        using var document = JsonDocument.Parse(json);
        var stageModules = document.RootElement.GetProperty("stage_modules");

        Assert.Equal(2, stageModules.GetArrayLength());
        Assert.Equal("X", stageModules[0].GetProperty("outcome_code").GetString());
        Assert.Equal("X", stageModules[1].GetProperty("outcome_code").GetString());
    }

    private sealed class EmptyArgs
    {
    }

    private sealed class CountingMemoModule : IModule<EmptyArgs, int>
    {
        private readonly int _delayMs;
        private readonly Outcome<int> _outcome;

        public static int ExecuteCount;

        public CountingMemoModule(int delayMs, Outcome<int> outcome)
        {
            _delayMs = delayMs;
            _outcome = outcome;
        }

        public static void Reset()
        {
            ExecuteCount = 0;
        }

        public async ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<EmptyArgs> context)
        {
            _ = context;
            Interlocked.Increment(ref ExecuteCount);
            await Task.Delay(_delayMs).ConfigureAwait(false);
            return _outcome;
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

