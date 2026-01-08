using ROrchestrator.Core;

namespace ROrchestrator.Core.Tests;

public sealed class ExceptionGuardTests
{
    [Fact]
    public void ShouldHandle_ShouldReturnFalse_ForFatalExceptions()
    {
        Assert.False(ExceptionGuard.ShouldHandle(new OutOfMemoryException()));
        Assert.False(ExceptionGuard.ShouldHandle(new StackOverflowException()));
        Assert.False(ExceptionGuard.ShouldHandle(new AccessViolationException()));
    }

    [Fact]
    public void ShouldHandle_ShouldReturnTrue_ForNonFatalExceptions()
    {
        Assert.True(ExceptionGuard.ShouldHandle(new InvalidOperationException("boom")));
    }
}
