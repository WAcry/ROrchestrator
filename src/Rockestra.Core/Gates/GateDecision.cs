namespace Rockestra.Core.Gates;

public readonly struct GateDecision
{
    public const string AllowedCode = "GATE_TRUE";
    public const string DeniedCode = "GATE_FALSE";

    private readonly string? _code;
    private readonly string? _reasonCode;

    public bool Allowed { get; }

    public string Code => _code ?? string.Empty;

    public string ReasonCode => _reasonCode ?? string.Empty;

    private GateDecision(bool allowed, string code, string reasonCode)
    {
        Allowed = allowed;
        _code = code;
        _reasonCode = reasonCode;
    }

    public static readonly GateDecision AllowedDecision = new(allowed: true, AllowedCode, reasonCode: string.Empty);
    public static readonly GateDecision DeniedDecision = new(allowed: false, DeniedCode, reasonCode: string.Empty);

    internal static readonly GateDecision AllowedVariantMatchDecision = new(allowed: true, AllowedCode, reasonCode: "VARIANT_MATCH");
    internal static readonly GateDecision DeniedMissingVariantDecision = new(allowed: false, DeniedCode, reasonCode: "MISSING_VARIANT");
    internal static readonly GateDecision DeniedVariantMismatchDecision = new(allowed: false, DeniedCode, reasonCode: "VARIANT_MISMATCH");

    internal static readonly GateDecision AllowedRolloutTrueDecision = new(allowed: true, AllowedCode, reasonCode: "ROLLOUT_TRUE");
    internal static readonly GateDecision DeniedRolloutFalseDecision = new(allowed: false, DeniedCode, reasonCode: "ROLLOUT_FALSE");
    internal static readonly GateDecision DeniedMissingUserIdDecision = new(allowed: false, DeniedCode, reasonCode: "MISSING_USER_ID");

    internal static readonly GateDecision AllowedRequestAttrMatchDecision = new(allowed: true, AllowedCode, reasonCode: "REQUEST_ATTR_MATCH");
    internal static readonly GateDecision DeniedRequestAttrMismatchDecision = new(allowed: false, DeniedCode, reasonCode: "REQUEST_ATTR_MISMATCH");
    internal static readonly GateDecision DeniedMissingRequestAttrDecision = new(allowed: false, DeniedCode, reasonCode: "MISSING_REQUEST_ATTR");

    internal static readonly GateDecision AllowedSelectorTrueDecision = new(allowed: true, AllowedCode, reasonCode: "SELECTOR_TRUE");
    internal static readonly GateDecision DeniedSelectorFalseDecision = new(allowed: false, DeniedCode, reasonCode: "SELECTOR_FALSE");

    internal static readonly GateDecision AllowedAllDecision = new(allowed: true, AllowedCode, reasonCode: "ALL_TRUE");
    internal static readonly GateDecision DeniedAnyDecision = new(allowed: false, DeniedCode, reasonCode: "ANY_FALSE");
    internal static readonly GateDecision AllowedNotDecision = new(allowed: true, AllowedCode, reasonCode: "NOT_TRUE");
    internal static readonly GateDecision DeniedNotDecision = new(allowed: false, DeniedCode, reasonCode: "NOT_FALSE");
}

