using System.Collections.Frozen;
using Rockestra.Core.Blueprint;

namespace Rockestra.Core.Tests;

public sealed class NodeNameToIndexFrozenDictionaryTests
{
    [Fact]
    public void Build_ShouldUseFrozenDictionary_ForNodeNameToIndex()
    {
        var blueprint = FlowBlueprint.Define<int, int>("TestFlow")
            .Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(123)))
            .Build();

        Assert.IsAssignableFrom<FrozenDictionary<string, int>>(blueprint.NodeNameToIndex);
        Assert.True(blueprint.NodeNameToIndex.TryGetValue("final", out var index));
        Assert.Equal(0, index);
    }

    [Fact]
    public void PlanCompiler_ShouldUseFrozenDictionary_ForNodeNameToIndex()
    {
        var catalog = new ModuleCatalog();
        catalog.Register<int, int>("m.add_one", _ => new AddOneModule());

        var blueprint = FlowBlueprint.Define<int, int>("TestFlow")
            .Step("step_a", "m.add_one")
            .Join<int>("final", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(123)))
            .Build();

        var template = PlanCompiler.Compile(blueprint, catalog);

        Assert.IsAssignableFrom<FrozenDictionary<string, int>>(template.NodeNameToIndex);
        Assert.True(template.NodeNameToIndex.TryGetValue("step_a", out var stepIndex));
        Assert.Equal(0, stepIndex);
        Assert.True(template.NodeNameToIndex.TryGetValue("final", out var finalIndex));
        Assert.Equal(1, finalIndex);
    }

    private sealed class AddOneModule : IModule<int, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<int> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(context.Args + 1));
        }
    }
}

