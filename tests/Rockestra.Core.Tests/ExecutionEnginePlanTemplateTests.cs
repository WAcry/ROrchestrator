using Rockestra.Core;
using Rockestra.Core.Blueprint;

namespace Rockestra.Core.Tests;

public sealed class ExecutionEnginePlanTemplateTests
{
    private static readonly DateTimeOffset FutureDeadline = new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_Template_ShouldExecuteStepsAndJoinsSequentially_AndRecordOutcomes()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);

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

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(template, request: 5, flowContext);

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
    public async Task ExecuteAsync_Template_ShouldConvertModuleException_ToOutcomeError()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);

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

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(template, request: 1, flowContext);

        Assert.True(flowContext.TryGetNodeOutcome<int>("step_a", out var stepOutcome));
        Assert.True(stepOutcome.IsError);
        Assert.Equal(ExecutionEngine.UnhandledExceptionCode, stepOutcome.Code);

        Assert.True(result.IsOk);
        Assert.Equal("error:" + ExecutionEngine.UnhandledExceptionCode, result.Value);
    }

    [Fact]
    public async Task ExecuteAsync_Template_ShouldConvertOperationCanceledException_FromJoin_ToCanceledOutcome()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);

        var blueprint = FlowBlueprint.Define<int, int>("TestFlow")
            .Join<int>("final", (Func<FlowContext, ValueTask<Outcome<int>>>)(_ => throw new OperationCanceledException()))
            .Build();

        var template = PlanCompiler.Compile(blueprint, new ModuleCatalog());
        var engine = new ExecutionEngine(new ModuleCatalog());

        var result = await engine.ExecuteAsync(template, request: 1, flowContext);

        Assert.True(result.IsCanceled);
        Assert.Equal(ExecutionEngine.UpstreamCanceledCode, result.Code);

        Assert.True(flowContext.TryGetNodeOutcome<int>("final", out var recordedFinal));
        Assert.True(recordedFinal.IsCanceled);
        Assert.Equal(ExecutionEngine.UpstreamCanceledCode, recordedFinal.Code);
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


