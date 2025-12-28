using ROrchestrator.Core.Gates;

namespace ROrchestrator.Core.Tests;

public sealed class GateEvaluatorTests
{
    [Fact]
    public void ExperimentGate_ShouldAllow_WhenVariantMatches()
    {
        var variants = new Dictionary<string, string>(1)
        {
            ["recall_layer"] = "B",
        };

        var gate = new ExperimentGate("recall_layer", ["B"]);
        var decision = GateEvaluator.Evaluate(gate, variants);

        Assert.True(decision.Allowed);
        Assert.Equal(GateDecision.AllowedCode, decision.Code);
    }

    [Fact]
    public void ExperimentGate_ShouldDeny_WhenLayerIsMissing()
    {
        var variants = new Dictionary<string, string>(0);

        var gate = new ExperimentGate("recall_layer", ["B"]);
        var decision = GateEvaluator.Evaluate(gate, variants);

        Assert.False(decision.Allowed);
        Assert.Equal(GateDecision.DeniedCode, decision.Code);
    }

    [Fact]
    public void ExperimentGate_ShouldDeny_WhenVariantDoesNotMatch()
    {
        var variants = new Dictionary<string, string>(1)
        {
            ["recall_layer"] = "A",
        };

        var gate = new ExperimentGate("recall_layer", ["B"]);
        var decision = GateEvaluator.Evaluate(gate, variants);

        Assert.False(decision.Allowed);
        Assert.Equal(GateDecision.DeniedCode, decision.Code);
    }

    [Fact]
    public void AllGate_ShouldAllow_WhenAllChildrenAllow()
    {
        var variants = new Dictionary<string, string>(2)
        {
            ["layer1"] = "A",
            ["layer2"] = "B",
        };

        var gate = new AllGate(
            new ExperimentGate("layer1", ["A"]),
            new ExperimentGate("layer2", ["B"]));

        var decision = GateEvaluator.Evaluate(gate, variants);

        Assert.True(decision.Allowed);
        Assert.Equal(GateDecision.AllowedCode, decision.Code);
    }

    [Fact]
    public void AllGate_ShouldDeny_WhenAnyChildDenies()
    {
        var variants = new Dictionary<string, string>(2)
        {
            ["layer1"] = "A",
            ["layer2"] = "B",
        };

        var gate = new AllGate(
            new ExperimentGate("layer1", ["A"]),
            new ExperimentGate("layer2", ["C"]));

        var decision = GateEvaluator.Evaluate(gate, variants);

        Assert.False(decision.Allowed);
        Assert.Equal(GateDecision.DeniedCode, decision.Code);
    }

    [Fact]
    public void AnyGate_ShouldAllow_WhenAnyChildAllows()
    {
        var variants = new Dictionary<string, string>(2)
        {
            ["layer1"] = "A",
            ["layer2"] = "B",
        };

        var gate = new AnyGate(
            new ExperimentGate("layer2", ["C"]),
            new ExperimentGate("layer1", ["A"]));

        var decision = GateEvaluator.Evaluate(gate, variants);

        Assert.True(decision.Allowed);
        Assert.Equal(GateDecision.AllowedCode, decision.Code);
    }

    [Fact]
    public void AnyGate_ShouldDeny_WhenAllChildrenDeny()
    {
        var variants = new Dictionary<string, string>(1)
        {
            ["layer1"] = "A",
        };

        var gate = new AnyGate(
            new ExperimentGate("layer1", ["B"]),
            new ExperimentGate("layer1", ["C"]));

        var decision = GateEvaluator.Evaluate(gate, variants);

        Assert.False(decision.Allowed);
        Assert.Equal(GateDecision.DeniedCode, decision.Code);
    }

    [Fact]
    public void NotGate_ShouldInvertDecision()
    {
        var variants = new Dictionary<string, string>(1)
        {
            ["layer1"] = "A",
        };

        var gate = new NotGate(new ExperimentGate("layer1", ["A"]));
        var decision = GateEvaluator.Evaluate(gate, variants);

        Assert.False(decision.Allowed);
        Assert.Equal(GateDecision.DeniedCode, decision.Code);
    }

    [Fact]
    public void AllGate_ShouldRejectNullOrEmptyChildren()
    {
        Assert.Throws<ArgumentException>(() => new AllGate());
        Assert.Throws<ArgumentNullException>(() => new AllGate(null!));
        Assert.Throws<ArgumentException>(() => new AllGate(null!, new ExperimentGate("layer1", ["A"])));
    }

    [Fact]
    public void AnyGate_ShouldRejectNullOrEmptyChildren()
    {
        Assert.Throws<ArgumentException>(() => new AnyGate());
        Assert.Throws<ArgumentNullException>(() => new AnyGate(null!));
        Assert.Throws<ArgumentException>(() => new AnyGate(null!, new ExperimentGate("layer1", ["A"])));
    }

    [Fact]
    public void NotGate_ShouldRejectNullChild()
    {
        Assert.Throws<ArgumentNullException>(() => new NotGate(null!));
    }
}

