namespace ROrchestrator.Core.Gates;

public readonly struct GateEvaluationContext
{
    public VariantSet Variants { get; }

    public string? UserId { get; }

    public IReadOnlyDictionary<string, string>? RequestAttributes { get; }

    public GateEvaluationContext(
        VariantSet variants,
        string? userId = null,
        IReadOnlyDictionary<string, string>? requestAttributes = null)
    {
        Variants = variants;
        UserId = userId;
        RequestAttributes = requestAttributes;
    }
}

