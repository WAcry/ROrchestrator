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
    public void Register_ShouldThrow_WhenFlowNameIsDuplicated_WithParams()
    {
        var registry = new FlowRegistry();
        var blueprint = CreateBlueprint<int, int>("TestFlow", okValue: 0);

        registry.Register<int, int, TestParams, TestPatch>("cg.test_flow", blueprint, new TestParams());

        var ex = Assert.Throws<ArgumentException>(
            () => registry.Register<int, int, TestParams, TestPatch>("cg.test_flow", blueprint, new TestParams()));

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
    public void TryGet_ShouldReturnDefinition_WhenTypesMatch_WithParams()
    {
        var registry = new FlowRegistry();
        var blueprint = CreateBlueprint<int, string>("TestFlow", okValue: "ok");
        var defaultParams = new TestParams();

        registry.Register<int, string, TestParams, TestPatch>("cg.test_flow", blueprint, defaultParams);

        var found = registry.TryGet<int, string, TestParams, TestPatch>("cg.test_flow", out var observed);

        Assert.True(found);
        Assert.Same(blueprint, observed.Blueprint);
        Assert.Same(defaultParams, observed.DefaultParams);
        Assert.Equal(typeof(TestParams), observed.ParamsType);
        Assert.Equal(typeof(TestPatch), observed.PatchType);

        var observedViaGet = registry.Get<int, string, TestParams, TestPatch>("cg.test_flow");

        Assert.Same(blueprint, observedViaGet.Blueprint);
        Assert.Same(defaultParams, observedViaGet.DefaultParams);
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

    [Fact]
    public void Get_ShouldThrow_WhenSignatureDoesNotMatch_WithParams()
    {
        var registry = new FlowRegistry();
        var blueprint = CreateBlueprint<int, string>("TestFlow", okValue: "ok");

        registry.Register<int, string, TestParams, TestPatch>("cg.test_flow", blueprint, new TestParams());

        var ex = Assert.Throws<InvalidOperationException>(
            () => registry.Get<int, string, OtherParams, TestPatch>("cg.test_flow"));

        Assert.Contains("cg.test_flow", ex.Message, StringComparison.Ordinal);

        var ex2 = Assert.Throws<InvalidOperationException>(
            () => registry.TryGet<int, string, OtherParams, TestPatch>("cg.test_flow", out _));

        Assert.Contains("cg.test_flow", ex2.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Get_ShouldThrow_AndTryGetReturnFalse_WhenFlowIsNotRegistered()
    {
        var registry = new FlowRegistry();

        Assert.False(registry.TryGet<int, string>("cg.unknown_flow", out _));

        var ex = Assert.Throws<InvalidOperationException>(() => registry.Get<int, string>("cg.unknown_flow"));

        Assert.Contains("cg.unknown_flow", ex.Message, StringComparison.Ordinal);

        Assert.False(registry.TryGet<int, string, TestParams, TestPatch>("cg.unknown_flow", out _));

        var ex2 = Assert.Throws<InvalidOperationException>(
            () => registry.Get<int, string, TestParams, TestPatch>("cg.unknown_flow"));

        Assert.Contains("cg.unknown_flow", ex2.Message, StringComparison.Ordinal);
    }

    private static FlowBlueprint<TReq, TResp> CreateBlueprint<TReq, TResp>(string name, TResp okValue)
    {
        return FlowBlueprint.Define<TReq, TResp>(name)
            .Join<TResp>(
                name: "j1",
                join: _ => new ValueTask<Outcome<TResp>>(Outcome<TResp>.Ok(okValue)))
            .Build();
    }

    private sealed class TestParams
    {
    }

    private sealed class OtherParams
    {
    }

    private sealed class TestPatch
    {
    }
}
