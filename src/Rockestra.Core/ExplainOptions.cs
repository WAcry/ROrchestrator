namespace Rockestra.Core;

public readonly struct ExplainOptions
{
    public ExplainLevel Level { get; }

    public string? Reason { get; }

    public ExplainRedactionPolicy Policy { get; }

    public ExplainOptions(
        ExplainLevel level = ExplainLevel.Minimal,
        string? reason = null,
        ExplainRedactionPolicy policy = ExplainRedactionPolicy.Default)
    {
        if ((uint)level > (uint)ExplainLevel.Full)
        {
            throw new ArgumentOutOfRangeException(nameof(level), level, "Unsupported explain level.");
        }

        if ((uint)policy > (uint)ExplainRedactionPolicy.Default)
        {
            throw new ArgumentOutOfRangeException(nameof(policy), policy, "Unsupported explain redaction policy.");
        }

        Level = level;
        Reason = string.IsNullOrWhiteSpace(reason) ? null : reason;
        Policy = policy;
    }
}

