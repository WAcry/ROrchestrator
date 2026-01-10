using Rockestra.Core.Selectors;

namespace Rockestra.Core.Gates;

public readonly struct GateEvaluationContext
{
    public VariantSet Variants { get; }

    public string? UserId { get; }

    public IReadOnlyDictionary<string, string>? RequestAttributes { get; }

    public SelectorRegistry? SelectorRegistry { get; }

    public FlowContext? FlowContext { get; }

    public GateEvaluationContext(
        VariantSet variants,
        string? userId = null,
        IReadOnlyDictionary<string, string>? requestAttributes = null,
        SelectorRegistry? selectorRegistry = null,
        FlowContext? flowContext = null)
    {
        Variants = variants;
        UserId = userId;
        RequestAttributes = requestAttributes;
        SelectorRegistry = selectorRegistry;
        FlowContext = flowContext;
    }
}

