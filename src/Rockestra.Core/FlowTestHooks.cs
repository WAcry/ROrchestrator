namespace Rockestra.Core;

internal interface IFlowTestOverrideProvider
{
    bool TryGetOverride(string moduleId, out object overrideEntry);
}

internal interface IFlowTestInvocationSink
{
    void Record(string moduleId, string moduleType, bool isOverride, OutcomeKind outcomeKind, string outcomeCode);
}

internal sealed class FlowTestOverrideOutcome<TOut>
{
    public Outcome<TOut> Outcome { get; }

    public FlowTestOverrideOutcome(Outcome<TOut> outcome)
    {
        Outcome = outcome;
    }
}

internal sealed class FlowTestOverrideCompute<TArgs, TOut>
{
    public Func<ModuleContext<TArgs>, ValueTask<Outcome<TOut>>> Compute { get; }

    public FlowTestOverrideCompute(Func<ModuleContext<TArgs>, ValueTask<Outcome<TOut>>> compute)
    {
        Compute = compute ?? throw new ArgumentNullException(nameof(compute));
    }
}


