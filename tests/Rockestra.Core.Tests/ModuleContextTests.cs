using Rockestra.Core;

namespace Rockestra.Core.Tests;

public sealed class ModuleContextTests
{
    [Fact]
    public void Ctor_ShouldExposeCoreFields()
    {
        var services = new DummyServiceProvider();
        using var cts = new CancellationTokenSource();
        var deadline = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        var flowContext = new FlowContext(services, cts.Token, deadline);
        var ctx = new ModuleContext<int>("recall/cg_user_follow", "cg.offline_key_list", 123, flowContext);

        Assert.Equal("recall/cg_user_follow", ctx.ModuleId);
        Assert.Equal("cg.offline_key_list", ctx.TypeName);
        Assert.Equal(123, ctx.Args);

        Assert.Same(flowContext, ctx.FlowContext);
        Assert.Same(services, ctx.Services);
        Assert.Equal(cts.Token, ctx.CancellationToken);
        Assert.Equal(deadline, ctx.Deadline);
    }

    [Fact]
    public void Ctor_ShouldRejectNullFlowContext()
    {
        var ex = Assert.Throws<ArgumentNullException>(
            () => new ModuleContext<int>("recall/cg_user_follow", "cg.offline_key_list", 123, null!));

        Assert.Equal("flowContext", ex.ParamName);
    }

    [Theory]
    [InlineData("")]
    [InlineData(null)]
    public void Ctor_ShouldRejectNullOrEmptyModuleId(string? moduleId)
    {
        var flowContext = new FlowContext(new DummyServiceProvider(), CancellationToken.None, new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));

        var ex = Assert.Throws<ArgumentException>(() => new ModuleContext<int>(moduleId!, "cg.offline_key_list", 123, flowContext));
        Assert.Equal("moduleId", ex.ParamName);
    }

    [Theory]
    [InlineData("")]
    [InlineData(null)]
    public void Ctor_ShouldRejectNullOrEmptyTypeName(string? typeName)
    {
        var flowContext = new FlowContext(new DummyServiceProvider(), CancellationToken.None, new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));

        var ex = Assert.Throws<ArgumentException>(() => new ModuleContext<int>("recall/cg_user_follow", typeName!, 123, flowContext));
        Assert.Equal("typeName", ex.ParamName);
    }

    [Fact]
    public void Ctor_ShouldRejectNullArgs_ForReferenceTypes()
    {
        var flowContext = new FlowContext(new DummyServiceProvider(), CancellationToken.None, new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));

        var ex = Assert.Throws<ArgumentNullException>(
            () => new ModuleContext<string>("recall/cg_user_follow", "cg.offline_key_list", null!, flowContext));

        Assert.Equal("args", ex.ParamName);
    }

    private sealed class DummyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType)
        {
            return null;
        }
    }
}


