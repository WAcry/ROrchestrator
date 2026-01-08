namespace ROrchestrator.Core;

public sealed class ValidationReport
{
    public static ValidationReport Empty { get; } = new(Array.Empty<ValidationFinding>());

    public IReadOnlyList<ValidationFinding> Findings { get; }

    public bool IsValid => _isValid;

    private readonly bool _isValid;

    internal ValidationReport(ValidationFinding[] findings)
    {
        Findings = findings ?? throw new ArgumentNullException(nameof(findings));

        var isValid = true;

        for (var i = 0; i < findings.Length; i++)
        {
            if (findings[i].Severity == ValidationSeverity.Error)
            {
                isValid = false;
                break;
            }
        }

        _isValid = isValid;
    }
}
