namespace ROrchestrator.Core.Gates;

public static class GateEvaluator
{
    public static GateDecision Evaluate(Gate gate, IReadOnlyDictionary<string, string> variants)
    {
        if (gate is null)
        {
            throw new ArgumentNullException(nameof(gate));
        }

        if (variants is null)
        {
            throw new ArgumentNullException(nameof(variants));
        }

        return EvaluateAllowed(gate, variants)
            ? GateDecision.AllowedDecision
            : GateDecision.DeniedDecision;
    }

    public static GateDecision Evaluate(Gate gate, VariantSet variants)
    {
        if (gate is null)
        {
            throw new ArgumentNullException(nameof(gate));
        }

        return Evaluate(gate, variants.Dictionary);
    }

    private static bool EvaluateAllowed(Gate gate, IReadOnlyDictionary<string, string> variants)
    {
        if (gate is ExperimentGate experiment)
        {
            if (!variants.TryGetValue(experiment.Layer, out var currentVariant))
            {
                return false;
            }

            var allowedVariants = experiment.Variants.Span;
            for (var i = 0; i < allowedVariants.Length; i++)
            {
                if (currentVariant == allowedVariants[i])
                {
                    return true;
                }
            }

            return false;
        }

        if (gate is AllGate all)
        {
            var children = all.Children.Span;
            for (var i = 0; i < children.Length; i++)
            {
                if (!EvaluateAllowed(children[i], variants))
                {
                    return false;
                }
            }

            return true;
        }

        if (gate is AnyGate any)
        {
            var children = any.Children.Span;
            for (var i = 0; i < children.Length; i++)
            {
                if (EvaluateAllowed(children[i], variants))
                {
                    return true;
                }
            }

            return false;
        }

        if (gate is NotGate not)
        {
            return !EvaluateAllowed(not.Child, variants);
        }

        throw new InvalidOperationException($"Unsupported gate type: '{gate.GetType()}'.");
    }
}

public readonly struct VariantSet
{
    private static readonly IReadOnlyDictionary<string, string> EmptyDictionary =
        new System.Collections.ObjectModel.ReadOnlyDictionary<string, string>(
            new Dictionary<string, string>(0));

    private readonly IReadOnlyDictionary<string, string>? _variants;

    public IReadOnlyDictionary<string, string> Dictionary => _variants ?? EmptyDictionary;

    public VariantSet(IReadOnlyDictionary<string, string> variants)
    {
        _variants = variants ?? throw new ArgumentNullException(nameof(variants));
    }
}
