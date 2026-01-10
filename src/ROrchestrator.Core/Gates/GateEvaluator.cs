using ROrchestrator.Core.Selectors;

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

        var context = new GateEvaluationContext(new VariantSet(variants));

        return EvaluateDecision(gate, in context);
    }

    public static GateDecision Evaluate(Gate gate, VariantSet variants)
    {
        if (gate is null)
        {
            throw new ArgumentNullException(nameof(gate));
        }

        var context = new GateEvaluationContext(variants);

        return EvaluateDecision(gate, in context);
    }

    public static GateDecision Evaluate(Gate gate, in GateEvaluationContext context)
    {
        if (gate is null)
        {
            throw new ArgumentNullException(nameof(gate));
        }

        return EvaluateDecision(gate, in context);
    }

    public static GateDecision Evaluate(Gate gate, FlowContext flowContext)
    {
        if (gate is null)
        {
            throw new ArgumentNullException(nameof(gate));
        }

        if (flowContext is null)
        {
            throw new ArgumentNullException(nameof(flowContext));
        }

        var evalContext = new GateEvaluationContext(
            variants: new VariantSet(flowContext.Variants),
            userId: flowContext.UserId,
            requestAttributes: flowContext.RequestAttributes,
            selectorRegistry: null,
            flowContext: flowContext);

        return EvaluateDecision(gate, in evalContext);
    }

    public static GateDecision Evaluate(Gate gate, FlowContext flowContext, SelectorRegistry selectorRegistry)
    {
        if (gate is null)
        {
            throw new ArgumentNullException(nameof(gate));
        }

        if (flowContext is null)
        {
            throw new ArgumentNullException(nameof(flowContext));
        }

        if (selectorRegistry is null)
        {
            throw new ArgumentNullException(nameof(selectorRegistry));
        }

        var evalContext = new GateEvaluationContext(
            variants: new VariantSet(flowContext.Variants),
            userId: flowContext.UserId,
            requestAttributes: flowContext.RequestAttributes,
            selectorRegistry: selectorRegistry,
            flowContext: flowContext);

        return EvaluateDecision(gate, in evalContext);
    }

    private static GateDecision EvaluateDecision(Gate gate, in GateEvaluationContext context)
    {
        if (gate is ExperimentGate experiment)
        {
            var variants = context.Variants.Dictionary;

            if (!variants.TryGetValue(experiment.Layer, out var currentVariant))
            {
                return GateDecision.DeniedMissingVariantDecision;
            }

            var allowedVariants = experiment.Variants.Span;
            for (var i = 0; i < allowedVariants.Length; i++)
            {
                if (currentVariant == allowedVariants[i])
                {
                    return GateDecision.AllowedVariantMatchDecision;
                }
            }

            return GateDecision.DeniedVariantMismatchDecision;
        }

        if (gate is RolloutGate rollout)
        {
            if (string.IsNullOrEmpty(context.UserId))
            {
                return GateDecision.DeniedMissingUserIdDecision;
            }

            var bucket = ComputeRolloutBucket(context.UserId, rollout.Salt);
            return bucket < rollout.Percent
                ? GateDecision.AllowedRolloutTrueDecision
                : GateDecision.DeniedRolloutFalseDecision;
        }

        if (gate is RequestAttrGate request)
        {
            var requestAttributes = context.RequestAttributes;
            if (requestAttributes is null || !requestAttributes.TryGetValue(request.Field, out var value))
            {
                return GateDecision.DeniedMissingRequestAttrDecision;
            }

            var allowedValues = request.Values.Span;
            for (var i = 0; i < allowedValues.Length; i++)
            {
                if (string.Equals(value, allowedValues[i], StringComparison.Ordinal))
                {
                    return GateDecision.AllowedRequestAttrMatchDecision;
                }
            }

            return GateDecision.DeniedRequestAttrMismatchDecision;
        }

        if (gate is AllGate all)
        {
            var children = all.Children.Span;
            for (var i = 0; i < children.Length; i++)
            {
                var childDecision = EvaluateDecision(children[i], in context);
                if (!childDecision.Allowed)
                {
                    return childDecision;
                }
            }

            return GateDecision.AllowedAllDecision;
        }

        if (gate is AnyGate any)
        {
            var children = any.Children.Span;
            for (var i = 0; i < children.Length; i++)
            {
                var childDecision = EvaluateDecision(children[i], in context);
                if (childDecision.Allowed)
                {
                    return childDecision;
                }
            }

            return GateDecision.DeniedAnyDecision;
        }

        if (gate is NotGate not)
        {
            var childDecision = EvaluateDecision(not.Child, in context);
            return childDecision.Allowed
                ? GateDecision.DeniedNotDecision
                : GateDecision.AllowedNotDecision;
        }

        if (gate is SelectorGate selectorGate)
        {
            var registry = context.SelectorRegistry;
            if (registry is null)
            {
                throw new InvalidOperationException("SelectorRegistry is required to evaluate SelectorGate.");
            }

            var flowContext = context.FlowContext;
            if (flowContext is null)
            {
                throw new InvalidOperationException("FlowContext is required to evaluate SelectorGate.");
            }

            if (!registry.TryGet(selectorGate.SelectorName, out var selector))
            {
                throw new InvalidOperationException($"Selector '{selectorGate.SelectorName}' is not registered.");
            }

            return selector(flowContext)
                ? GateDecision.AllowedSelectorTrueDecision
                : GateDecision.DeniedSelectorFalseDecision;
        }

        throw new InvalidOperationException($"Unsupported gate type: '{gate.GetType()}'.");
    }

    private static ulong ComputeRolloutBucket(string userId, string salt)
    {
        const ulong offsetBasis = 14695981039346656037;
        const ulong prime = 1099511628211;

        var hash = offsetBasis;
        hash = HashChars(hash, userId);
        hash = HashChar(hash, '\0');
        hash = HashChars(hash, salt);

        return hash % 100;

        static ulong HashChars(ulong hash, string value)
        {
            for (var i = 0; i < value.Length; i++)
            {
                hash = HashChar(hash, value[i]);
            }

            return hash;
        }

        static ulong HashChar(ulong hash, char c)
        {
            var u = (ushort)c;

            hash ^= (byte)u;
            hash *= prime;
            hash ^= (byte)(u >> 8);
            hash *= prime;

            return hash;
        }
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
