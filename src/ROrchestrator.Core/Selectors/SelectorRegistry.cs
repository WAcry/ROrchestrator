using ROrchestrator.Core;

namespace ROrchestrator.Core.Selectors;

public sealed class SelectorRegistry
{
    public static SelectorRegistry Empty { get; } = new(isReadOnly: true);

    private readonly Dictionary<string, Func<FlowContext, bool>> _selectors;
    private readonly bool _isReadOnly;

    public SelectorRegistry()
    {
        _selectors = new Dictionary<string, Func<FlowContext, bool>>();
        _isReadOnly = false;
    }

    private SelectorRegistry(bool isReadOnly)
    {
        _selectors = new Dictionary<string, Func<FlowContext, bool>>(0);
        _isReadOnly = isReadOnly;
    }

    public void Register(string name, Func<FlowContext, bool> selector)
    {
        if (_isReadOnly)
        {
            throw new InvalidOperationException("The selector registry is read-only.");
        }

        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentException("Name must be non-empty.", nameof(name));
        }

        if (selector is null)
        {
            throw new ArgumentNullException(nameof(selector));
        }

        if (!_selectors.TryAdd(name, selector))
        {
            throw new ArgumentException($"Selector '{name}' is already registered.", nameof(name));
        }
    }

    public bool IsRegistered(string name)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentException("Name must be non-empty.", nameof(name));
        }

        return _selectors.ContainsKey(name);
    }

    internal bool TryGet(string name, out Func<FlowContext, bool> selector)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentException("Name must be non-empty.", nameof(name));
        }

        return _selectors.TryGetValue(name, out selector!);
    }
}
