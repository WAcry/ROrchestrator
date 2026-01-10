namespace Rockestra.Core;

public enum ModuleLifetime : byte
{
    Transient = 0,
    Singleton = 1,
}

public enum ModuleThreadSafety : byte
{
    ThreadSafe = 0,
    NotThreadSafe = 1,
}

public sealed class ModuleConcurrencyViolationException : InvalidOperationException
{
    public ModuleConcurrencyViolationException(string message)
        : base(message)
    {
    }
}


