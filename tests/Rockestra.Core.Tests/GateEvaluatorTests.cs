using Rockestra.Core.Gates;
using Rockestra.Core.Selectors;

namespace Rockestra.Core.Tests;

public sealed class GateEvaluatorTests
{
    private static readonly DateTimeOffset FutureDeadline = new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

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
        Assert.Equal("VARIANT_MATCH", decision.ReasonCode);
    }

    [Fact]
    public void ExperimentGate_ShouldDeny_WhenLayerIsMissing()
    {
        var variants = new Dictionary<string, string>(0);

        var gate = new ExperimentGate("recall_layer", ["B"]);
        var decision = GateEvaluator.Evaluate(gate, variants);

        Assert.False(decision.Allowed);
        Assert.Equal(GateDecision.DeniedCode, decision.Code);
        Assert.Equal("MISSING_VARIANT", decision.ReasonCode);
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
        Assert.Equal("VARIANT_MISMATCH", decision.ReasonCode);
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

    [Fact]
    public void RolloutGate_ShouldMatchStableHashAlgorithm()
    {
        var variants = new Dictionary<string, string>(0);
        var gate = new RolloutGate(percent: 50, salt: "m1");

        var context = new GateEvaluationContext(new VariantSet(variants), userId: "user_1");
        var decision = GateEvaluator.Evaluate(gate, in context);

        var bucket = ComputeRolloutBucket("user_1", "m1");
        Assert.Equal(bucket < 50, decision.Allowed);
        Assert.Equal(decision.Allowed ? GateDecision.AllowedCode : GateDecision.DeniedCode, decision.Code);
        Assert.Equal(decision.Allowed ? "ROLLOUT_TRUE" : "ROLLOUT_FALSE", decision.ReasonCode);
    }

    [Fact]
    public void RolloutGate_ShouldDeny_WhenUserIdIsMissing()
    {
        var variants = new Dictionary<string, string>(0);
        var gate = new RolloutGate(percent: 100, salt: "m1");

        var context = new GateEvaluationContext(new VariantSet(variants), userId: null);
        var decision = GateEvaluator.Evaluate(gate, in context);

        Assert.False(decision.Allowed);
        Assert.Equal(GateDecision.DeniedCode, decision.Code);
        Assert.Equal("MISSING_USER_ID", decision.ReasonCode);
    }

    [Fact]
    public void RequestAttrGate_ShouldAllow_WhenFieldMatches()
    {
        var variants = new Dictionary<string, string>(0);
        var requestAttrs = new Dictionary<string, string>(1)
        {
            ["region"] = "US",
        };

        var gate = new RequestAttrGate("region", ["US"]);
        var context = new GateEvaluationContext(new VariantSet(variants), requestAttributes: requestAttrs);

        var decision = GateEvaluator.Evaluate(gate, in context);

        Assert.True(decision.Allowed);
        Assert.Equal(GateDecision.AllowedCode, decision.Code);
        Assert.Equal("REQUEST_ATTR_MATCH", decision.ReasonCode);
    }

    [Fact]
    public void RequestAttrGate_ShouldDeny_WhenFieldIsMissing()
    {
        var variants = new Dictionary<string, string>(0);
        var requestAttrs = new Dictionary<string, string>(0);

        var gate = new RequestAttrGate("region", ["US"]);
        var context = new GateEvaluationContext(new VariantSet(variants), requestAttributes: requestAttrs);

        var decision = GateEvaluator.Evaluate(gate, in context);

        Assert.False(decision.Allowed);
        Assert.Equal(GateDecision.DeniedCode, decision.Code);
        Assert.Equal("MISSING_REQUEST_ATTR", decision.ReasonCode);
    }

    [Fact]
    public void RequestAttrGate_ShouldDeny_WhenFieldDoesNotMatch()
    {
        var variants = new Dictionary<string, string>(0);
        var requestAttrs = new Dictionary<string, string>(1)
        {
            ["region"] = "US",
        };

        var gate = new RequestAttrGate("region", ["CN"]);
        var context = new GateEvaluationContext(new VariantSet(variants), requestAttributes: requestAttrs);

        var decision = GateEvaluator.Evaluate(gate, in context);

        Assert.False(decision.Allowed);
        Assert.Equal(GateDecision.DeniedCode, decision.Code);
        Assert.Equal("REQUEST_ATTR_MISMATCH", decision.ReasonCode);
    }

    [Fact]
    public void SelectorGate_ShouldAllow_WhenSelectorReturnsTrue()
    {
        var gate = new SelectorGate("is_allowed");

        var selectors = new SelectorRegistry();
        selectors.Register("is_allowed", _ => true);

        var variants = new Dictionary<string, string>(0);
        var context = new FlowContext(new DummyServiceProvider(), CancellationToken.None, FutureDeadline);
        var evalContext = new GateEvaluationContext(new VariantSet(variants), selectorRegistry: selectors, flowContext: context);

        var decision = GateEvaluator.Evaluate(gate, in evalContext);

        Assert.True(decision.Allowed);
        Assert.Equal(GateDecision.AllowedCode, decision.Code);
        Assert.Equal("SELECTOR_TRUE", decision.ReasonCode);
    }

    [Fact]
    public void SelectorGate_ShouldDeny_WhenSelectorReturnsFalse()
    {
        var gate = new SelectorGate("is_denied");

        var selectors = new SelectorRegistry();
        selectors.Register("is_denied", _ => false);

        var variants = new Dictionary<string, string>(0);
        var context = new FlowContext(new DummyServiceProvider(), CancellationToken.None, FutureDeadline);
        var evalContext = new GateEvaluationContext(new VariantSet(variants), selectorRegistry: selectors, flowContext: context);

        var decision = GateEvaluator.Evaluate(gate, in evalContext);

        Assert.False(decision.Allowed);
        Assert.Equal(GateDecision.DeniedCode, decision.Code);
        Assert.Equal("SELECTOR_FALSE", decision.ReasonCode);
    }

    private static int ComputeRolloutBucket(string userId, string salt)
    {
        const ulong offsetBasis = 14695981039346656037;
        const ulong prime = 1099511628211;

        var hash = offsetBasis;

        hash = HashChars(hash, userId);
        hash = HashChar(hash, '\0');
        hash = HashChars(hash, salt);

        return (int)(hash % 100);

        static ulong HashChars(ulong hash, string value)
        {
            for (var i = 0; i < value.Length; i++)
            {
                hash = HashChar(hash, value[i]);
            }

            return hash;
        }

        static ulong HashChar(ulong hash, char c)
        {
            var u = (ushort)c;

            hash ^= (byte)u;
            hash *= prime;
            hash ^= (byte)(u >> 8);
            hash *= prime;

            return hash;
        }
    }

    private sealed class DummyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }
}

