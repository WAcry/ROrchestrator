using ROrchestrator.Core;

namespace ROrchestrator.Core.Tests;

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

    private sealed class DummyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType)
        {
            return null;
        }
    }
}

