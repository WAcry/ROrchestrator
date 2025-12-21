namespace ROrchestrator.Core;

public enum OutcomeKind : byte
{
    Unspecified = 0,
    Ok = 1,
    Error = 2,
    Timeout = 3,
    Skipped = 4,
    Fallback = 5,
    Canceled = 6,
}
