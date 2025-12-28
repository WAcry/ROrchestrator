using ROrchestrator.Core;
using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Core.Tests;

public sealed class FlowRegistryTests
{
    [Fact]
    public void Register_ShouldThrow_WhenFlowNameIsDuplicated()
    {
        var registry = new FlowRegistry();
        var blueprint = CreateBlueprint<int, int>("TestFlow", okValue: 0);

        registry.Register("cg.test_flow", blueprint);

        var ex = Assert.Throws<ArgumentException>(() => registry.Register("cg.test_flow", blueprint));

        Assert.Equal("flowName", ex.ParamName);
    }

    [Fact]
    public void TryGet_ShouldReturnBlueprint_WhenTypesMatch()
    {
        var registry = new FlowRegistry();
        var blueprint = CreateBlueprint<int, string>("TestFlow", okValue: "ok");

        registry.Register("cg.test_flow", blueprint);

        var found = registry.TryGet<int, string>("cg.test_flow", out var observed);

        Assert.True(found);
        Assert.Same(blueprint, observed);

        var observedViaGet = registry.Get<int, string>("cg.test_flow");

        Assert.Same(blueprint, observedViaGet);
    }

    [Fact]
    public void Get_ShouldThrow_WhenSignatureDoesNotMatch()
    {
        var registry = new FlowRegistry();
        var blueprint = CreateBlueprint<int, string>("TestFlow", okValue: "ok");

        registry.Register("cg.test_flow", blueprint);

        var ex = Assert.Throws<InvalidOperationException>(() => registry.Get<int, int>("cg.test_flow"));

        Assert.Contains("cg.test_flow", ex.Message, StringComparison.Ordinal);

        var ex2 = Assert.Throws<InvalidOperationException>(() => registry.TryGet<int, int>("cg.test_flow", out _));

        Assert.Contains("cg.test_flow", ex2.Message, StringComparison.Ordinal);
    }

    private static FlowBlueprint<TReq, TResp> CreateBlueprint<TReq, TResp>(string name, TResp okValue)
    {
        return FlowBlueprint.Define<TReq, TResp>(name)
            .Join<TResp>(
                name: "j1",
                join: _ => new ValueTask<Outcome<TResp>>(Outcome<TResp>.Ok(okValue)))
            .Build();
    }
}

