namespace Rockestra.Core.Testing;

public sealed class Overrides : IFlowTestOverrideProvider
{
    private readonly Dictionary<string, object> _overrides;

    public Overrides()
    {
        _overrides = new Dictionary<string, object>();
    }

    public Overrides OverrideOutcome<TOut>(string moduleId, Outcome<TOut> outcome)
    {
        if (string.IsNullOrEmpty(moduleId))
        {
            throw new ArgumentException("ModuleId must be non-empty.", nameof(moduleId));
        }

        if (!_overrides.TryAdd(moduleId, new FlowTestOverrideOutcome<TOut>(outcome)))
        {
            throw new ArgumentException($"An override for moduleId '{moduleId}' has already been configured.", nameof(moduleId));
        }

        return this;
    }

    public Overrides OverrideCompute<TArgs, TOut>(
        string moduleId,
        Func<ModuleContext<TArgs>, ValueTask<Outcome<TOut>>> compute)
    {
        if (string.IsNullOrEmpty(moduleId))
        {
            throw new ArgumentException("ModuleId must be non-empty.", nameof(moduleId));
        }

        if (compute is null)
        {
            throw new ArgumentNullException(nameof(compute));
        }

        if (!_overrides.TryAdd(moduleId, new FlowTestOverrideCompute<TArgs, TOut>(compute)))
        {
            throw new ArgumentException($"An override for moduleId '{moduleId}' has already been configured.", nameof(moduleId));
        }

        return this;
    }

    bool IFlowTestOverrideProvider.TryGetOverride(string moduleId, out object overrideEntry)
    {
        if (_overrides.TryGetValue(moduleId, out var entry))
        {
            overrideEntry = entry;
            return true;
        }

        overrideEntry = null!;
        return false;
    }
}

