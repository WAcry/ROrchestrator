using System.Globalization;
using ROrchestrator.Core;
using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Core.Tests;

public sealed class PlanCompilerTests
{
    [Fact]
    public void Compile_ShouldReturnTemplate_WithResolvedOutputTypes_AndDeterministicHash()
    {
        var catalog = new ModuleCatalog();
        catalog.Register<int, string>("m.int_to_string", _ => new IntToStringModule());
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        var blueprint = FlowBlueprint.Define<int, string>("TestFlow")
            .Step("s1", "m.int_to_string")
            .Step("s2", "m.add_one")
            .Join<string>("final", _ => new ValueTask<Outcome<string>>(Outcome<string>.Ok("ok")))
            .Build();

        var templateA = PlanCompiler.Compile(blueprint, catalog);
        var templateB = PlanCompiler.Compile(blueprint, catalog);

        Assert.Equal("TestFlow", templateA.Name);
        Assert.Equal(templateA.PlanHash, templateB.PlanHash);
        Assert.NotEqual(0ul, templateA.PlanHash);

        Assert.Equal(3, templateA.Nodes.Count);

        Assert.Equal(BlueprintNodeKind.Step, templateA.Nodes[0].Kind);
        Assert.Equal("s1", templateA.Nodes[0].Name);
        Assert.Equal("m.int_to_string", templateA.Nodes[0].ModuleType);
        Assert.Equal(typeof(string), templateA.Nodes[0].OutputType);

        Assert.Equal(BlueprintNodeKind.Step, templateA.Nodes[1].Kind);
        Assert.Equal("s2", templateA.Nodes[1].Name);
        Assert.Equal("m.add_one", templateA.Nodes[1].ModuleType);
        Assert.Equal(typeof(int), templateA.Nodes[1].OutputType);

        Assert.Equal(BlueprintNodeKind.Join, templateA.Nodes[2].Kind);
        Assert.Equal("final", templateA.Nodes[2].Name);
        Assert.Null(templateA.Nodes[2].ModuleType);
        Assert.NotNull(templateA.Nodes[2].Join);
        Assert.Equal(typeof(string), templateA.Nodes[2].OutputType);

        var changedBlueprint = FlowBlueprint.Define<int, string>("TestFlow")
            .Step("s1", "m.int_to_string")
            .Step("s2_modified", "m.add_one")
            .Join<string>("final", _ => new ValueTask<Outcome<string>>(Outcome<string>.Ok("ok")))
            .Build();

        var changedTemplate = PlanCompiler.Compile(changedBlueprint, catalog);
        Assert.NotEqual(templateA.PlanHash, changedTemplate.PlanHash);
    }

    [Fact]
    public void Compile_ShouldThrow_WhenModuleTypeIsNotRegistered()
    {
        var blueprint = FlowBlueprint.Define<int, string>("TestFlow")
            .Step("s1", "m.not_registered")
            .Join<string>("final", _ => new ValueTask<Outcome<string>>(Outcome<string>.Ok("ok")))
            .Build();

        var ex = Assert.Throws<InvalidOperationException>(
            () => PlanCompiler.Compile(blueprint, new ModuleCatalog()));

        Assert.Contains("TestFlow", ex.Message, StringComparison.Ordinal);
        Assert.Contains("s1", ex.Message, StringComparison.Ordinal);
        Assert.Contains("m.not_registered", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_ShouldThrow_WhenModuleSignatureDoesNotMatchRequestType()
    {
        var catalog = new ModuleCatalog();
        catalog.Register<string, int>("m.bad_signature", _ => new StringToIntModule());

        var blueprint = FlowBlueprint.Define<int, string>("TestFlow")
            .Step("s1", "m.bad_signature")
            .Join<string>("final", _ => new ValueTask<Outcome<string>>(Outcome<string>.Ok("ok")))
            .Build();

        var ex = Assert.Throws<InvalidOperationException>(() => PlanCompiler.Compile(blueprint, catalog));

        Assert.Contains("TestFlow", ex.Message, StringComparison.Ordinal);
        Assert.Contains("s1", ex.Message, StringComparison.Ordinal);
        Assert.Contains("m.bad_signature", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_ShouldThrow_WhenFinalNodeIsNotAJoin()
    {
        var catalog = new ModuleCatalog();
        catalog.Register<int, string>("m.int_to_string", _ => new IntToStringModule());

        var blueprint = FlowBlueprint.Define<int, string>("TestFlow")
            .Step("final_step", "m.int_to_string")
            .Build();

        var ex = Assert.Throws<InvalidOperationException>(() => PlanCompiler.Compile(blueprint, catalog));

        Assert.Contains("TestFlow", ex.Message, StringComparison.Ordinal);
        Assert.Contains("final_step", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_ShouldThrow_WhenFinalJoinOutputTypeDoesNotMatchResponseType()
    {
        var blueprint = FlowBlueprint.Define<int, string>("TestFlow")
            .Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(1)))
            .Build();

        var ex = Assert.Throws<InvalidOperationException>(
            () => PlanCompiler.Compile(blueprint, new ModuleCatalog()));

        Assert.Contains("TestFlow", ex.Message, StringComparison.Ordinal);
        Assert.Contains("final", ex.Message, StringComparison.Ordinal);
    }

    private sealed class IntToStringModule : IModule<int, string>
    {
        public ValueTask<Outcome<string>> ExecuteAsync(ModuleContext<int> context)
        {
            return new ValueTask<Outcome<string>>(Outcome<string>.Ok(context.Args.ToString()));
        }
    }

    private sealed class AddOneModule : IModule<int, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(context.Args + 1));
        }
    }

    private sealed class StringToIntModule : IModule<string, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<string> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(int.Parse(context.Args, CultureInfo.InvariantCulture)));
        }
    }
}
