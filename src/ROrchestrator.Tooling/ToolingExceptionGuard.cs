namespace ROrchestrator.Tooling;

internal static class ToolingExceptionGuard
{
    public static bool ShouldHandle(Exception exception)
    {
        if (exception is OutOfMemoryException
            || exception is StackOverflowException
            || exception is AccessViolationException)
        {
            return false;
        }

        return true;
    }
}

