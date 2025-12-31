namespace ROrchestrator.Core;

public sealed class ValidationReport
{
    public static ValidationReport Empty { get; } = new(Array.Empty<ValidationFinding>());

    public IReadOnlyList<ValidationFinding> Findings { get; }

    public bool IsValid => Findings.Count == 0;

    internal ValidationReport(ValidationFinding[] findings)
    {
        Findings = findings ?? throw new ArgumentNullException(nameof(findings));
    }
}

