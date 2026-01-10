using Rockestra.Core;

namespace Rockestra.Core.Tests;

public sealed class FlowContextTests
{
    [Fact]
    public void Ctor_ShouldExposeCoreFields()
    {
        var services = new DummyServiceProvider();
        using var cts = new CancellationTokenSource();
        var deadline = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        var ctx = new FlowContext(services, cts.Token, deadline);

        Assert.Same(services, ctx.Services);
        Assert.Equal(cts.Token, ctx.CancellationToken);
        Assert.Equal(deadline, ctx.Deadline);
    }

    [Fact]
    public void Ctor_ShouldRejectNullServices()
    {
        var deadline = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        var ex = Assert.Throws<ArgumentNullException>(() => new FlowContext(null!, CancellationToken.None, deadline));
        Assert.Equal("services", ex.ParamName);
    }

    [Fact]
    public void Ctor_ShouldRejectDefaultDeadline()
    {
        var services = new DummyServiceProvider();

        var ex = Assert.Throws<ArgumentException>(() => new FlowContext(services, CancellationToken.None, default));
        Assert.Equal("deadline", ex.ParamName);
    }

    [Fact]
    public void NodeOutcomes_ShouldSupportWriteAndRead()
    {
        var ctx = new FlowContext(
            new DummyServiceProvider(),
            CancellationToken.None,
            new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));

        ctx.RecordNodeOutcome("step_a", Outcome<int>.Ok(42));

        Assert.True(ctx.TryGetNodeOutcome<int>("step_a", out var outcome));
        Assert.True(outcome.IsOk);
        Assert.Equal(42, outcome.Value);
    }

    [Fact]
    public void NodeOutcomes_ShouldRejectDuplicateWrites()
    {
        var ctx = new FlowContext(
            new DummyServiceProvider(),
            CancellationToken.None,
            new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));

        ctx.RecordNodeOutcome("step_a", Outcome<int>.Ok(1));

        var ex = Assert.Throws<InvalidOperationException>(() => ctx.RecordNodeOutcome("step_a", Outcome<int>.Ok(2)));
        Assert.Contains("step_a", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void NodeOutcomes_ShouldRejectTypeMismatchOnRead()
    {
        var ctx = new FlowContext(
            new DummyServiceProvider(),
            CancellationToken.None,
            new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));

        ctx.RecordNodeOutcome("step_a", Outcome<int>.Ok(42));

        var ex = Assert.Throws<InvalidOperationException>(() => ctx.TryGetNodeOutcome<string>("step_a", out _));
        Assert.Contains("step_a", ex.Message, StringComparison.Ordinal);
    }

    private sealed class DummyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType)
        {
            return null;
        }
    }
}

