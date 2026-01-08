namespace ROrchestrator.Core;

internal static class ExceptionGuard
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
