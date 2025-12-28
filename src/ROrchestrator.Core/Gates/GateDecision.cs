namespace ROrchestrator.Core.Gates;

public readonly struct GateDecision
{
    public const string AllowedCode = "GATE_TRUE";
    public const string DeniedCode = "GATE_FALSE";

    private readonly string? _code;

    public bool Allowed { get; }

    public string Code => _code ?? string.Empty;

    private GateDecision(bool allowed, string code)
    {
        Allowed = allowed;
        _code = code;
    }

    public static readonly GateDecision AllowedDecision = new(allowed: true, AllowedCode);
    public static readonly GateDecision DeniedDecision = new(allowed: false, DeniedCode);
}

