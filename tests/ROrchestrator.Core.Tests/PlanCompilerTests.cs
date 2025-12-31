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
    public void CompileWithExplain_ShouldReturnTemplateAndExplain_WithDeterministicExplain()
    {
        var catalog = new ModuleCatalog();
        catalog.Register<int, string>("m.int_to_string", _ => new IntToStringModule());
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        var ok = "ok";

        var blueprintA = FlowBlueprint.Define<int, string>("TestFlow")
            .Stage("stage1", stage =>
            {
                stage.Step("s1", "m.int_to_string");
                stage.Step("s2", "m.add_one");
                stage.Join<string>("final", _ => new ValueTask<Outcome<string>>(Outcome<string>.Ok(ok)));
            })
            .Build();

        var blueprintB = FlowBlueprint.Define<int, string>("TestFlow")
            .Stage("stage1", stage =>
            {
                stage.Step("s1", "m.int_to_string");
                stage.Step("s2", "m.add_one");
                stage.Join<string>("final", _ => new ValueTask<Outcome<string>>(Outcome<string>.Ok(ok)));
            })
            .Build();

        var resultA = PlanCompiler.CompileWithExplain(blueprintA, catalog);
        var resultB = PlanCompiler.CompileWithExplain(blueprintB, catalog);

        Assert.Equal(resultA.Template.PlanHash, resultB.Template.PlanHash);
        Assert.NotEqual(0ul, resultA.Template.PlanHash);

        Assert.Equal("TestFlow", resultA.Explain.FlowName);
        Assert.Equal(resultA.Template.PlanHash, resultA.Explain.PlanTemplateHash);
        Assert.Equal(resultB.Template.PlanHash, resultB.Explain.PlanTemplateHash);

        Assert.True(resultA.Explain.Nodes is PlanExplainNode[]);

        Assert.Equal(resultA.Explain.Nodes.Count, resultB.Explain.Nodes.Count);

        for (var i = 0; i < resultA.Explain.Nodes.Count; i++)
        {
            var nodeA = resultA.Explain.Nodes[i];
            var nodeB = resultB.Explain.Nodes[i];

            Assert.Equal(nodeA.Kind, nodeB.Kind);
            Assert.Equal(nodeA.Name, nodeB.Name);
            Assert.Equal(nodeA.StageName, nodeB.StageName);
            Assert.Equal(nodeA.ModuleType, nodeB.ModuleType);
            Assert.Equal(nodeA.OutputType, nodeB.OutputType);
        }

        Assert.Equal(3, resultA.Explain.Nodes.Count);

        Assert.Equal(BlueprintNodeKind.Step, resultA.Explain.Nodes[0].Kind);
        Assert.Equal("s1", resultA.Explain.Nodes[0].Name);
        Assert.Equal("stage1", resultA.Explain.Nodes[0].StageName);
        Assert.Equal("m.int_to_string", resultA.Explain.Nodes[0].ModuleType);
        Assert.Equal(typeof(string), resultA.Explain.Nodes[0].OutputType);

        Assert.Equal(BlueprintNodeKind.Step, resultA.Explain.Nodes[1].Kind);
        Assert.Equal("s2", resultA.Explain.Nodes[1].Name);
        Assert.Equal("stage1", resultA.Explain.Nodes[1].StageName);
        Assert.Equal("m.add_one", resultA.Explain.Nodes[1].ModuleType);
        Assert.Equal(typeof(int), resultA.Explain.Nodes[1].OutputType);

        Assert.Equal(BlueprintNodeKind.Join, resultA.Explain.Nodes[2].Kind);
        Assert.Equal("final", resultA.Explain.Nodes[2].Name);
        Assert.Equal("stage1", resultA.Explain.Nodes[2].StageName);
        Assert.Null(resultA.Explain.Nodes[2].ModuleType);
        Assert.Equal(typeof(string), resultA.Explain.Nodes[2].OutputType);
    }

    [Fact]
    public void CompileWithExplain_ShouldChangeHash_WhenNodeFieldsChange()
    {
        var catalog = new ModuleCatalog();
        catalog.Register<int, string>("m.type_a", _ => new IntToStringModule());
        catalog.Register<int, string>("m.type_b", _ => new IntToStringModule());
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        static ValueTask<Outcome<string>> JoinString(FlowContext _)
        {
            return new ValueTask<Outcome<string>>(Outcome<string>.Ok("ok"));
        }

        static ValueTask<Outcome<int>> JoinInt(FlowContext _)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(1));
        }

        static ValueTask<Outcome<long>> JoinLong(FlowContext _)
        {
            return new ValueTask<Outcome<long>>(Outcome<long>.Ok(1L));
        }

        var blueprint = FlowBlueprint.Define<int, string>("TestFlow")
            .Step("s1", "m.type_a")
            .Stage("stage1", stage =>
            {
                stage.Join<int>("j1", JoinInt);
                stage.Step("s2", "m.add_one");
            })
            .Join<string>("final", JoinString)
            .Build();

        var baseHash = PlanCompiler.CompileWithExplain(blueprint, catalog).Explain.PlanTemplateHash;

        var nameChanged = FlowBlueprint.Define<int, string>("TestFlow")
            .Step("s1", "m.type_a")
            .Stage("stage1", stage =>
            {
                stage.Join<int>("j1", JoinInt);
                stage.Step("s2_modified", "m.add_one");
            })
            .Join<string>("final", JoinString)
            .Build();

        Assert.NotEqual(baseHash, PlanCompiler.CompileWithExplain(nameChanged, catalog).Explain.PlanTemplateHash);

        var stageNameChanged = FlowBlueprint.Define<int, string>("TestFlow")
            .Step("s1", "m.type_a")
            .Stage("stage2", stage =>
            {
                stage.Join<int>("j1", JoinInt);
                stage.Step("s2", "m.add_one");
            })
            .Join<string>("final", JoinString)
            .Build();

        Assert.NotEqual(baseHash, PlanCompiler.CompileWithExplain(stageNameChanged, catalog).Explain.PlanTemplateHash);

        var moduleTypeChanged = FlowBlueprint.Define<int, string>("TestFlow")
            .Step("s1", "m.type_b")
            .Stage("stage1", stage =>
            {
                stage.Join<int>("j1", JoinInt);
                stage.Step("s2", "m.add_one");
            })
            .Join<string>("final", JoinString)
            .Build();

        Assert.NotEqual(baseHash, PlanCompiler.CompileWithExplain(moduleTypeChanged, catalog).Explain.PlanTemplateHash);

        var outputTypeChanged = FlowBlueprint.Define<int, string>("TestFlow")
            .Step("s1", "m.type_a")
            .Stage("stage1", stage =>
            {
                stage.Join<long>("j1", JoinLong);
                stage.Step("s2", "m.add_one");
            })
            .Join<string>("final", JoinString)
            .Build();

        Assert.NotEqual(baseHash, PlanCompiler.CompileWithExplain(outputTypeChanged, catalog).Explain.PlanTemplateHash);

        var kindChanged = FlowBlueprint.Define<int, string>("TestFlow")
            .Step("s1", "m.type_a")
            .Stage("stage1", stage =>
            {
                stage.Step("j1", "m.add_one");
                stage.Step("s2", "m.add_one");
            })
            .Join<string>("final", JoinString)
            .Build();

        Assert.NotEqual(baseHash, PlanCompiler.CompileWithExplain(kindChanged, catalog).Explain.PlanTemplateHash);
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
