namespace ROrchestrator.Tooling;

public readonly struct ToolingCommandResult
{
    public int ExitCode { get; }

    public string Json { get; }

    public ToolingCommandResult(int exitCode, string json)
    {
        if (exitCode < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(exitCode));
        }

        Json = json ?? throw new ArgumentNullException(nameof(json));
        ExitCode = exitCode;
    }
}

