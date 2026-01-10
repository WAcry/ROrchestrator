using Rockestra.Core.Blueprint;

namespace Rockestra.Core.Tests;

public sealed class FlowBlueprintDslTests
{
    [Fact]
    public void Build_ShouldCaptureStagesStepsJoinsInOrder()
    {
        var blueprint = FlowBlueprint.Define<int, string>("TestFlow")
            .Stage("stage_a", st => st
                .Step("s1", "m1")
                .Join<int>("j1", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(1))))
            .Join<string>("final", _ => new ValueTask<Outcome<string>>(Outcome<string>.Ok("ok")))
            .Build();

        Assert.Equal("TestFlow", blueprint.Name);
        Assert.Equal(3, blueprint.Nodes.Count);

        var n0 = blueprint.Nodes[0];
        Assert.Equal(BlueprintNodeKind.Step, n0.Kind);
        Assert.Equal("s1", n0.Name);
        Assert.Equal("stage_a", n0.StageName);
        Assert.Equal("m1", n0.ModuleType);

        var n1 = blueprint.Nodes[1];
        Assert.Equal(BlueprintNodeKind.Join, n1.Kind);
        Assert.Equal("j1", n1.Name);
        Assert.Equal("stage_a", n1.StageName);
        Assert.Equal(typeof(int), n1.JoinOutputType);
        Assert.NotNull(n1.Join);

        var n2 = blueprint.Nodes[2];
        Assert.Equal(BlueprintNodeKind.Join, n2.Kind);
        Assert.Equal("final", n2.Name);
        Assert.Null(n2.StageName);
        Assert.Equal(typeof(string), n2.JoinOutputType);
        Assert.NotNull(n2.Join);
    }

    [Fact]
    public void Stage_ShouldRejectDuplicateStageName()
    {
        var builder = FlowBlueprint.Define<int, string>("TestFlow")
            .Stage("stage_a", st => st.Step("s1", "m1"));

        var ex = Assert.Throws<ArgumentException>(
            () => builder.Stage("stage_a", st => st.Step("s2", "m2")));

        Assert.Contains("TestFlow", ex.Message, StringComparison.Ordinal);
        Assert.Contains("stage_a", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Stage_ShouldRollbackStageName_WhenStageIsEmpty()
    {
        var builder = FlowBlueprint.Define<int, string>("TestFlow");

        Assert.Throws<InvalidOperationException>(() => builder.Stage("stage_a", _ => { }));

        builder.Stage("stage_a", st => st.Step("s1", "m1"));
    }

    [Fact]
    public void Stage_ShouldRollbackNodes_WhenConfigureThrows()
    {
        var builder = FlowBlueprint.Define<int, string>("TestFlow");

        Assert.Throws<InvalidOperationException>(
            () => builder.Stage("stage_a", st =>
            {
                st.Step("s1", "m1");
                throw new InvalidOperationException("boom");
            }));

        builder.Stage("stage_b", st => st.Step("s1", "m1"));

        var blueprint = builder.Build();
        Assert.Single(blueprint.Nodes);
        Assert.Equal("stage_b", blueprint.Nodes[0].StageName);
        Assert.Equal("s1", blueprint.Nodes[0].Name);
    }

    [Fact]
    public void Step_ShouldRejectDuplicateNodeName_AcrossStepAndJoin()
    {
        var ex = Assert.Throws<ArgumentException>(
            () => FlowBlueprint.Define<int, string>("TestFlow")
                .Stage("stage_a", st => st
                    .Step("dup", "m1")
                    .Join<int>("dup", _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(1))))
                .Build());

        Assert.Contains("TestFlow", ex.Message, StringComparison.Ordinal);
        Assert.Contains("dup", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Step_ShouldRejectEmptyNameOrModuleType()
    {
        Assert.Throws<ArgumentException>(() => FlowBlueprint.Define<int, string>("TestFlow").Step("", "m1"));
        Assert.Throws<ArgumentException>(() => FlowBlueprint.Define<int, string>("TestFlow").Step("s1", ""));
        Assert.Throws<ArgumentException>(() => FlowBlueprint.Define<int, string>("TestFlow").Step(null!, "m1"));
        Assert.Throws<ArgumentException>(() => FlowBlueprint.Define<int, string>("TestFlow").Step("s1", null!));
    }

    [Fact]
    public void Join_ShouldRejectNullNameOrJoinFunc()
    {
        Assert.Throws<ArgumentException>(
            () => FlowBlueprint.Define<int, string>("TestFlow")
                .Join<string>("", _ => new ValueTask<Outcome<string>>(Outcome<string>.Ok("ok"))));

        Assert.Throws<ArgumentNullException>(
            () => FlowBlueprint.Define<int, string>("TestFlow").Join<string>("j1", null!));
    }

    [Fact]
    public void Build_ShouldRejectEmptyFlow()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => FlowBlueprint.Define<int, string>("TestFlow").Build());

        Assert.Contains("TestFlow", ex.Message, StringComparison.Ordinal);
    }
}

