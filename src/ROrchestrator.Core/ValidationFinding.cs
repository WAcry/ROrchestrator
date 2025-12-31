namespace ROrchestrator.Core;

public readonly struct ValidationFinding
{
    public ValidationSeverity Severity { get; }

    public string Code { get; }

    public string Path { get; }

    public string Message { get; }

    public ValidationFinding(ValidationSeverity severity, string code, string path, string message)
    {
        if (string.IsNullOrEmpty(code))
        {
            throw new ArgumentException("Code must be non-empty.", nameof(code));
        }

        Path = path ?? throw new ArgumentNullException(nameof(path));
        Message = message ?? throw new ArgumentNullException(nameof(message));

        Severity = severity;
        Code = code;
    }
}

