using Rockestra.Core;
using Rockestra.Core.Blueprint;

namespace Rockestra.Core.Tests;

public sealed class JoinSyncTests
{
    private static readonly DateTimeOffset FutureDeadline = new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task ExecuteAsync_Blueprint_ShouldAllowSyncJoin()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);

        var module = new AddOneModule();
        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => module);

        var blueprint = FlowBlueprint
            .Define<int, int>("TestFlow")
            .Stage(
                "s1",
                stage => stage
                    .Step("step_a", "m.add_one")
                    .Join<int>(
                        "final",
                        ctx =>
                        {
                            Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var stepOutcome));
                            Assert.True(stepOutcome.IsOk);
                            return Outcome<int>.Ok(stepOutcome.Value + 10);
                        }))
            .Build();

        var engine = new ExecutionEngine(catalog);
        var result = await engine.ExecuteAsync(blueprint, request: 5, flowContext);

        Assert.True(result.IsOk);
        Assert.Equal(16, result.Value);
    }

    [Fact]
    public async Task ExecuteAsync_Template_ShouldAllowSyncJoin()
    {
        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);

        var module = new AddOneModule();
        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => module);

        var blueprint = FlowBlueprint
            .Define<int, int>("TestFlow")
            .Stage(
                "s1",
                stage => stage
                    .Step("step_a", "m.add_one")
                    .Join<int>(
                        "final",
                        ctx =>
                        {
                            Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var stepOutcome));
                            Assert.True(stepOutcome.IsOk);
                            return Outcome<int>.Ok(stepOutcome.Value + 10);
                        }))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);
        var engine = new ExecutionEngine(catalog);

        var result = await engine.ExecuteAsync(template, request: 5, flowContext);

        Assert.True(result.IsOk);
        Assert.Equal(16, result.Value);
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
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(context.Args + 1));
        }
    }
}

