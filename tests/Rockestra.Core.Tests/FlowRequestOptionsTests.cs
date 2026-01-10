using Rockestra.Core.Gates;
using Rockestra.Core.Selectors;

namespace Rockestra.Core.Tests;

public sealed class FlowRequestOptionsTests
{
    private static readonly DateTimeOffset FutureDeadline = new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public void FlowContext_ShouldExposeVariantsUserIdAndRequestAttributes()
    {
        var variants = new Dictionary<string, string>(1)
        {
            ["recall_layer"] = "B",
        };

        var requestAttributes = new Dictionary<string, string>(1)
        {
            ["region"] = "US",
        };

        var opt = new FlowRequestOptions(variants, userId: "user_1", requestAttributes);

        var ctx = new FlowContext(new DummyServiceProvider(), CancellationToken.None, FutureDeadline, opt);

        Assert.Equal("B", ctx.Variants["recall_layer"]);
        Assert.Equal("user_1", ctx.UserId);
        Assert.Equal("US", ctx.RequestAttributes["region"]);
    }

    [Fact]
    public void GateEvaluator_Evaluate_WithFlowContext_ShouldUseUnifiedRoutingInputs()
    {
        var variants = new Dictionary<string, string>(1)
        {
            ["recall_layer"] = "B",
        };

        var requestAttributes = new Dictionary<string, string>(1)
        {
            ["region"] = "US",
        };

        var opt = new FlowRequestOptions(variants, userId: "user_1", requestAttributes);
        var ctx = new FlowContext(new DummyServiceProvider(), CancellationToken.None, FutureDeadline, opt);

        Assert.True(GateEvaluator.Evaluate(new ExperimentGate("recall_layer", ["B"]), ctx).Allowed);
        Assert.True(GateEvaluator.Evaluate(new RequestAttrGate("region", ["US"]), ctx).Allowed);

        var rolloutGate = new RolloutGate(percent: 50, salt: "m1");
        var rolloutDecision = GateEvaluator.Evaluate(rolloutGate, ctx);
        var expectedBucket = ComputeRolloutBucket("user_1", "m1");
        Assert.Equal(expectedBucket < 50, rolloutDecision.Allowed);

        var selectors = new SelectorRegistry();
        selectors.Register("is_us", flowContext => flowContext.RequestAttributes.TryGetValue("region", out var v) && v == "US");

        var selectorDecision = GateEvaluator.Evaluate(new SelectorGate("is_us"), ctx, selectors);
        Assert.True(selectorDecision.Allowed);
    }

    private static int ComputeRolloutBucket(string userId, string salt)
    {
        const ulong offsetBasis = 14695981039346656037;
        const ulong prime = 1099511628211;

        var hash = offsetBasis;

        for (var i = 0; i < userId.Length; i++)
        {
            hash = HashChar(hash, userId[i]);
        }

        hash = HashChar(hash, '\0');

        for (var i = 0; i < salt.Length; i++)
        {
            hash = HashChar(hash, salt[i]);
        }

        return (int)(hash % 100);

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


