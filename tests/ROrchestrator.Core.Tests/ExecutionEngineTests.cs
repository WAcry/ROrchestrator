using System.Reflection;
using ROrchestrator.Core;
using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Core.Tests;

public sealed class ExecutionEngineTests
{
    private static readonly DateTimeOffset FutureDeadline = new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_ShouldExecuteStepsAndJoinsSequentially_AndRecordOutcomes()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(
            services,
            CancellationToken.None,
            FutureDeadline);

        var module = new AddOneModule();
        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => module);

        var blueprint = FlowBlueprint.Define<int, int>("TestFlow")
            .Step("step_a", "m.add_one")
            .Join<int>(
                "final",
                ctx =>
                {
                    Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var stepOutcome));
                    Assert.True(stepOutcome.IsOk);
                    return new ValueTask<Outcome<int>>(Outcome<int>.Ok(stepOutcome.Value + 10));
                })
            .Build();

        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(blueprint, request: 5, flowContext);

        Assert.True(result.IsOk);
        Assert.Equal(16, result.Value);

        Assert.True(flowContext.TryGetNodeOutcome<int>("step_a", out var recordedStep));
        Assert.True(recordedStep.IsOk);
        Assert.Equal(6, recordedStep.Value);

        Assert.True(flowContext.TryGetNodeOutcome<int>("final", out var recordedFinal));
        Assert.True(recordedFinal.IsOk);
        Assert.Equal(16, recordedFinal.Value);

        Assert.Equal(5, module.LastArgs);
        Assert.Equal("step_a", module.LastModuleId);
        Assert.Equal("m.add_one", module.LastTypeName);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldThrow_WhenStepModuleTypeIsNotRegistered()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(
            services,
            CancellationToken.None,
            FutureDeadline);

        var blueprint = FlowBlueprint.Define<int, int>("TestFlow")
            .Step("step_a", "m.not_registered")
            .Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(1)))
            .Build();

        var engine = new ExecutionEngine(new ModuleCatalog());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await engine.ExecuteAsync(blueprint, request: 1, flowContext));

        Assert.Contains("m.not_registered", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldConvertModuleException_ToOutcomeError()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(
            services,
            CancellationToken.None,
            FutureDeadline);

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.boom", _ => new ThrowingModule());

        var blueprint = FlowBlueprint.Define<int, string>("TestFlow")
            .Step("step_a", "m.boom")
            .Join<string>(
                "final",
                ctx =>
                {
                    Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var outcome));
                    return new ValueTask<Outcome<string>>(
                        Outcome<string>.Ok(outcome.IsError ? "error:" + outcome.Code : "ok"));
                })
            .Build();

        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(blueprint, request: 1, flowContext);

        Assert.True(flowContext.TryGetNodeOutcome<int>("step_a", out var stepOutcome));
        Assert.True(stepOutcome.IsError);
        Assert.Equal(ExecutionEngine.UnhandledExceptionCode, stepOutcome.Code);

        Assert.True(result.IsOk);
        Assert.Equal("error:" + ExecutionEngine.UnhandledExceptionCode, result.Value);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldRethrowFatalException_FromModule()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(
            services,
            CancellationToken.None,
            FutureDeadline);

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.fatal", _ => new FatalModule());

        var blueprint = FlowBlueprint.Define<int, int>("TestFlow")
            .Step("step_a", "m.fatal")
            .Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(1)))
            .Build();

        var engine = new ExecutionEngine(catalog);

        await Assert.ThrowsAsync<OutOfMemoryException>(
            async () => await engine.ExecuteAsync(blueprint, request: 1, flowContext));
    }

    [Fact]
    public async Task ExecuteAsync_ShouldShortCircuit_WhenCancellationIsAlreadyRequested()
    {
        var services = new DummyServiceProvider();

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var flowContext = new FlowContext(services, cts.Token, FutureDeadline);

        var module = new CountingModule();
        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.counting", _ => module);

        var joinCalled = false;

        var blueprint = FlowBlueprint.Define<int, int>("TestFlow")
            .Step("step_a", "m.counting")
            .Join<int>(
                "final",
                _ =>
                {
                    joinCalled = true;
                    return new ValueTask<Outcome<int>>(Outcome<int>.Ok(123));
                })
            .Build();

        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(blueprint, request: 1, flowContext);

        Assert.True(result.IsCanceled);
        Assert.Equal(ExecutionEngine.UpstreamCanceledCode, result.Code);

        Assert.Equal(0, module.ExecuteCallCount);
        Assert.False(joinCalled);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldShortCircuit_WhenDeadlineIsAlreadyExceeded()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(
            services,
            CancellationToken.None,
            new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero));

        var module = new CountingModule();
        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.counting", _ => module);

        var joinCalled = false;

        var blueprint = FlowBlueprint.Define<int, int>("TestFlow")
            .Step("step_a", "m.counting")
            .Join<int>(
                "final",
                _ =>
                {
                    joinCalled = true;
                    return new ValueTask<Outcome<int>>(Outcome<int>.Ok(123));
                })
            .Build();

        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(blueprint, request: 1, flowContext);

        Assert.True(result.IsTimeout);
        Assert.Equal(ExecutionEngine.DeadlineExceededCode, result.Code);

        Assert.Equal(0, module.ExecuteCallCount);
        Assert.False(joinCalled);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldConvertOperationCanceledException_FromModule_ToCanceledOutcome()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.cancel", _ => new CancelingModule());

        var blueprint = FlowBlueprint.Define<int, string>("TestFlow")
            .Step("step_a", "m.cancel")
            .Join<string>("final", _ => new ValueTask<Outcome<string>>(Outcome<string>.Ok("done")))
            .Build();

        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(blueprint, request: 1, flowContext);

        Assert.True(result.IsOk);
        Assert.Equal("done", result.Value);

        Assert.True(flowContext.TryGetNodeOutcome<int>("step_a", out var stepOutcome));
        Assert.True(stepOutcome.IsCanceled);
        Assert.Equal(ExecutionEngine.UpstreamCanceledCode, stepOutcome.Code);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldConvertOperationCanceledException_FromJoin_ToCanceledOutcome()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);

        var blueprint = FlowBlueprint.Define<int, int>("TestFlow")
            .Join<int>("final", _ => throw new OperationCanceledException())
            .Build();

        var engine = new ExecutionEngine(new ModuleCatalog());

        var result = await engine.ExecuteAsync(blueprint, request: 1, flowContext);

        Assert.True(result.IsCanceled);
        Assert.Equal(ExecutionEngine.UpstreamCanceledCode, result.Code);

        Assert.True(flowContext.TryGetNodeOutcome<int>("final", out var recordedFinal));
        Assert.True(recordedFinal.IsCanceled);
        Assert.Equal(ExecutionEngine.UpstreamCanceledCode, recordedFinal.Code);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldShortCircuit_BeforeNextNode_WhenCancellationIsRequestedMidFlow()
    {
        var services = new DummyServiceProvider();

        using var cts = new CancellationTokenSource();

        var flowContext = new FlowContext(services, cts.Token, FutureDeadline);

        var cancelingModule = new CancelingTokenModule(cts);
        var countingModule = new CountingModule();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.cancel_token", _ => cancelingModule);
        catalog.Register<int, int>("m.counting", _ => countingModule);

        var joinCalled = false;

        var blueprint = FlowBlueprint.Define<int, int>("TestFlow")
            .Step("step_a", "m.cancel_token")
            .Step("step_b", "m.counting")
            .Join<int>(
                "final",
                _ =>
                {
                    joinCalled = true;
                    return new ValueTask<Outcome<int>>(Outcome<int>.Ok(123));
                })
            .Build();

        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(blueprint, request: 1, flowContext);

        Assert.True(result.IsCanceled);
        Assert.Equal(ExecutionEngine.UpstreamCanceledCode, result.Code);

        Assert.Equal(1, cancelingModule.ExecuteCallCount);
        Assert.Equal(0, countingModule.ExecuteCallCount);
        Assert.False(joinCalled);

        Assert.True(flowContext.TryGetNodeOutcome<int>("step_a", out var stepAOutcome));
        Assert.True(stepAOutcome.IsOk);

        Assert.False(flowContext.TryGetNodeOutcome<int>("step_b", out _));
        Assert.False(flowContext.TryGetNodeOutcome<int>("final", out _));
    }

    [Fact]
    public async Task ExecuteAsync_ShouldShortCircuit_BeforeNextNode_WhenDeadlineIsExceededMidFlow()
    {
        var services = new DummyServiceProvider();

        var delayingModule = new DelayingModule(delayMs: 750);
        var countingModule = new CountingModule();

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.delay", _ => delayingModule);
        catalog.Register<int, int>("m.counting", _ => countingModule);

        var joinCalled = false;

        var blueprint = FlowBlueprint.Define<int, int>("TestFlow")
            .Step("step_a", "m.delay")
            .Step("step_b", "m.counting")
            .Join<int>(
                "final",
                _ =>
                {
                    joinCalled = true;
                    return new ValueTask<Outcome<int>>(Outcome<int>.Ok(123));
                })
            .Build();

        var engine = new ExecutionEngine(catalog);
        var flowContext = new FlowContext(services, CancellationToken.None, DateTimeOffset.UtcNow.AddMilliseconds(500));

        var result = await engine.ExecuteAsync(blueprint, request: 1, flowContext);

        Assert.True(result.IsTimeout);
        Assert.Equal(ExecutionEngine.DeadlineExceededCode, result.Code);

        Assert.Equal(1, delayingModule.ExecuteCallCount);
        Assert.Equal(0, countingModule.ExecuteCallCount);
        Assert.False(joinCalled);

        Assert.True(flowContext.TryGetNodeOutcome<int>("step_a", out var stepAOutcome));
        Assert.True(stepAOutcome.IsOk);

        Assert.False(flowContext.TryGetNodeOutcome<int>("step_b", out _));
        Assert.False(flowContext.TryGetNodeOutcome<int>("final", out _));
    }

    [Fact]
    public async Task ExecuteAsync_ShouldConvertOperationCanceledException_FromModule_ToTimeout_WhenDeadlineExceeded()
    {
        var services = new DummyServiceProvider();

        var cancelingModule = new DelayedCancelingModule(delayMs: 750);

        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.cancel", _ => cancelingModule);

        var joinCalled = false;

        var blueprint = FlowBlueprint.Define<int, int>("TestFlow")
            .Step("step_a", "m.cancel")
            .Join<int>(
                "final",
                _ =>
                {
                    joinCalled = true;
                    return new ValueTask<Outcome<int>>(Outcome<int>.Ok(123));
                })
            .Build();

        var engine = new ExecutionEngine(catalog);
        var flowContext = new FlowContext(services, CancellationToken.None, DateTimeOffset.UtcNow.AddMilliseconds(500));

        var result = await engine.ExecuteAsync(blueprint, request: 1, flowContext);

        Assert.True(result.IsTimeout);
        Assert.Equal(ExecutionEngine.DeadlineExceededCode, result.Code);

        Assert.Equal(1, cancelingModule.ExecuteCallCount);
        Assert.False(joinCalled);

        Assert.True(flowContext.TryGetNodeOutcome<int>("step_a", out var recordedStep));
        Assert.True(recordedStep.IsTimeout);
        Assert.Equal(ExecutionEngine.DeadlineExceededCode, recordedStep.Code);

        Assert.False(flowContext.TryGetNodeOutcome<int>("final", out _));
    }

    [Fact]
    public async Task ExecuteAsync_ShouldConvertOperationCanceledException_FromJoin_ToTimeout_WhenDeadlineExceeded()
    {
        var services = new DummyServiceProvider();

        var joinCalled = false;

        var blueprint = FlowBlueprint.Define<int, int>("TestFlow")
            .Join<int>(
                "final",
                async _ =>
                {
                    joinCalled = true;
                    await Task.Delay(750);
                    throw new OperationCanceledException();
                })
            .Build();

        var engine = new ExecutionEngine(new ModuleCatalog());
        var flowContext = new FlowContext(services, CancellationToken.None, DateTimeOffset.UtcNow.AddMilliseconds(500));

        var result = await engine.ExecuteAsync(blueprint, request: 1, flowContext);

        Assert.True(joinCalled);
        Assert.True(result.IsTimeout);
        Assert.Equal(ExecutionEngine.DeadlineExceededCode, result.Code);

        Assert.True(flowContext.TryGetNodeOutcome<int>("final", out var recordedFinal));
        Assert.True(recordedFinal.IsTimeout);
        Assert.Equal(ExecutionEngine.DeadlineExceededCode, recordedFinal.Code);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldReturnTimeout_WhenDeadlineIsExceededDuringFinalJoin_EvenIfJoinReturnsOk()
    {
        var services = new DummyServiceProvider();

        var joinCalled = false;

        var blueprint = FlowBlueprint.Define<int, int>("TestFlow")
            .Join<int>(
                "final",
                async _ =>
                {
                    joinCalled = true;
                    await Task.Delay(750);
                    return Outcome<int>.Ok(123);
                })
            .Build();

        var engine = new ExecutionEngine(new ModuleCatalog());
        var flowContext = new FlowContext(services, CancellationToken.None, DateTimeOffset.UtcNow.AddMilliseconds(500));

        var result = await engine.ExecuteAsync(blueprint, request: 1, flowContext);

        Assert.True(joinCalled);
        Assert.True(result.IsTimeout);
        Assert.Equal(ExecutionEngine.DeadlineExceededCode, result.Code);

        Assert.True(flowContext.TryGetNodeOutcome<int>("final", out var recordedFinal));
        Assert.True(recordedFinal.IsOk);
        Assert.Equal(123, recordedFinal.Value);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldReturnCanceled_WhenCancellationIsRequestedDuringFinalJoin_EvenIfJoinReturnsOk()
    {
        var services = new DummyServiceProvider();

        using var cts = new CancellationTokenSource();
        var flowContext = new FlowContext(services, cts.Token, FutureDeadline);

        var joinCalled = false;

        var blueprint = FlowBlueprint.Define<int, int>("TestFlow")
            .Join<int>(
                "final",
                _ =>
                {
                    joinCalled = true;
                    cts.Cancel();
                    return new ValueTask<Outcome<int>>(Outcome<int>.Ok(123));
                })
            .Build();

        var engine = new ExecutionEngine(new ModuleCatalog());

        var result = await engine.ExecuteAsync(blueprint, request: 1, flowContext);

        Assert.True(joinCalled);
        Assert.True(result.IsCanceled);
        Assert.Equal(ExecutionEngine.UpstreamCanceledCode, result.Code);

        Assert.True(flowContext.TryGetNodeOutcome<int>("final", out var recordedFinal));
        Assert.True(recordedFinal.IsOk);
        Assert.Equal(123, recordedFinal.Value);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldEnsureNodeOutcomesCapacity_BeforeExecutingNodes()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(
            services,
            CancellationToken.None,
            FutureDeadline);

        var module = new ObservingOutcomesCapacityModule();
        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.observe_outcomes_capacity", _ => module);

        var blueprint = FlowBlueprint.Define<int, int>("TestFlow")
            .Step("step_a", "m.observe_outcomes_capacity")
            .Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(1)))
            .Build();

        module.ExpectedMinimumCapacity = blueprint.Nodes.Count;

        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(blueprint, request: 1, flowContext);

        Assert.True(result.IsOk);
        Assert.Null(module.ObservedException);
        Assert.False(module.ObservedWasNull);
        Assert.True(module.ObservedCapacity >= module.ExpectedMinimumCapacity);
    }

    private sealed class DummyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType)
        {
            return null;
        }
    }

    private sealed class AddOneModule : IModule<int, int>
    {
        public int LastArgs { get; private set; }

        public string? LastModuleId { get; private set; }

        public string? LastTypeName { get; private set; }

        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            LastArgs = context.Args;
            LastModuleId = context.ModuleId;
            LastTypeName = context.TypeName;
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(context.Args + 1));
        }
    }

    private sealed class ThrowingModule : IModule<int, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            throw new InvalidOperationException("boom");
        }
    }

    private sealed class FatalModule : IModule<int, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            throw new OutOfMemoryException("fatal");
        }
    }

    private sealed class CountingModule : IModule<int, int>
    {
        public int ExecuteCallCount { get; private set; }

        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            ExecuteCallCount++;
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(context.Args));
        }
    }

    private sealed class CancelingModule : IModule<int, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            throw new OperationCanceledException();
        }
    }

    private sealed class CancelingTokenModule : IModule<int, int>
    {
        private readonly CancellationTokenSource _cts;

        public int ExecuteCallCount { get; private set; }

        public CancelingTokenModule(CancellationTokenSource cts)
        {
            _cts = cts ?? throw new ArgumentNullException(nameof(cts));
        }

        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            ExecuteCallCount++;
            _cts.Cancel();
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(context.Args));
        }
    }

    private sealed class DelayingModule : IModule<int, int>
    {
        private readonly int _delayMs;

        public int ExecuteCallCount { get; private set; }

        public DelayingModule(int delayMs)
        {
            if (delayMs < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(delayMs));
            }

            _delayMs = delayMs;
        }

        public async ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            ExecuteCallCount++;
            await Task.Delay(_delayMs);
            return Outcome<int>.Ok(context.Args);
        }
    }

    private sealed class DelayedCancelingModule : IModule<int, int>
    {
        private readonly int _delayMs;

        public int ExecuteCallCount { get; private set; }

        public DelayedCancelingModule(int delayMs)
        {
            if (delayMs < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(delayMs));
            }

            _delayMs = delayMs;
        }

        public async ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            ExecuteCallCount++;
            await Task.Delay(_delayMs);
            throw new OperationCanceledException();
        }
    }

    private sealed class ObservingOutcomesCapacityModule : IModule<int, int>
    {
        private static readonly FieldInfo NodeOutcomesField =
            typeof(FlowContext).GetField("_nodeOutcomes", BindingFlags.NonPublic | BindingFlags.Instance)!;

        public int ExpectedMinimumCapacity { get; set; }

        public Exception? ObservedException { get; private set; }

        public bool ObservedWasNull { get; private set; }

        public int ObservedCapacity { get; private set; }

        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            try
            {
                var outcomes = NodeOutcomesField.GetValue(context.FlowContext);

                if (outcomes is null)
                {
                    ObservedWasNull = true;
                    ObservedCapacity = -1;
                }
                else
                {
                    ObservedWasNull = false;
                    ObservedCapacity = ((Array)outcomes).Length;
                }
            }
            catch (Exception ex)
            {
                ObservedException = ex;
            }

            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(context.Args));
        }
    }
}
