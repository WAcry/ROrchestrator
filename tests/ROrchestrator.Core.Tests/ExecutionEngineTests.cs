using ROrchestrator.Core;
using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Core.Tests;

public sealed class ExecutionEngineTests
{
    [Fact]
    public async Task ExecuteAsync_ShouldExecuteStepsAndJoinsSequentially_AndRecordOutcomes()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(
            services,
            CancellationToken.None,
            new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));

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
            new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));

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
            new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));

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
}
