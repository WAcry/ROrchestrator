namespace Rockestra.Core;

internal static class ExceptionGuard
{
    public static bool ShouldHandle(Exception exception)
    {
        if (exception is OutOfMemoryException
            || exception is StackOverflowException
            || exception is AccessViolationException
            || exception is ModuleConcurrencyViolationException)
        {
            return false;
        }

        return true;
    }
}

