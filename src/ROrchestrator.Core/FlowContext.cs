namespace ROrchestrator.Core;

public sealed class FlowContext
{
    private readonly object _nodeOutcomesLock;
    private Dictionary<string, NodeOutcomeEntry>? _nodeOutcomes;

    public IServiceProvider Services { get; }

    public CancellationToken CancellationToken { get; }

    public DateTimeOffset Deadline { get; }

    public FlowContext(IServiceProvider services, CancellationToken cancellationToken, DateTimeOffset deadline)
    {
        Services = services ?? throw new ArgumentNullException(nameof(services));

        if (deadline == default)
        {
            throw new ArgumentException("Deadline must be non-default.", nameof(deadline));
        }

        _nodeOutcomesLock = new object();
        CancellationToken = cancellationToken;
        Deadline = deadline;
    }

    public void RecordNodeOutcome<T>(string nodeName, Outcome<T> outcome)
    {
        if (string.IsNullOrEmpty(nodeName))
        {
            throw new ArgumentException("Node name must be non-empty.", nameof(nodeName));
        }

        var entry = NodeOutcomeEntry.Create(outcome);

        lock (_nodeOutcomesLock)
        {
            _nodeOutcomes ??= new Dictionary<string, NodeOutcomeEntry>();

            if (!_nodeOutcomes.TryAdd(nodeName, entry))
            {
                throw new InvalidOperationException($"Outcome for node '{nodeName}' has already been recorded.");
            }
        }
    }

    internal void EnsureNodeOutcomesCapacity(int capacity)
    {
        if (capacity <= 0)
        {
            return;
        }

        lock (_nodeOutcomesLock)
        {
            if (_nodeOutcomes is null)
            {
                _nodeOutcomes = new Dictionary<string, NodeOutcomeEntry>(capacity);
                return;
            }

            _nodeOutcomes.EnsureCapacity(capacity);
        }
    }

    public bool TryGetNodeOutcome<T>(string nodeName, out Outcome<T> outcome)
    {
        if (string.IsNullOrEmpty(nodeName))
        {
            throw new ArgumentException("Node name must be non-empty.", nameof(nodeName));
        }

        NodeOutcomeEntry entry;

        lock (_nodeOutcomesLock)
        {
            if (_nodeOutcomes is null || !_nodeOutcomes.TryGetValue(nodeName, out entry))
            {
                outcome = default;
                return false;
            }
        }

        if (entry.OutputType != typeof(T))
        {
            throw new InvalidOperationException(
                $"Outcome for node '{nodeName}' has type '{entry.OutputType}', not '{typeof(T)}'.");
        }

        outcome = entry.ToOutcome<T>();
        return true;
    }

    private readonly struct NodeOutcomeEntry
    {
        public Type OutputType { get; }

        public OutcomeKind Kind { get; }

        public object? Value { get; }

        public string Code { get; }

        private NodeOutcomeEntry(Type outputType, OutcomeKind kind, object? value, string code)
        {
            OutputType = outputType;
            Kind = kind;
            Value = value;
            Code = code;
        }

        public static NodeOutcomeEntry Create<T>(Outcome<T> outcome)
        {
            object? value = null;
            var kind = outcome.Kind;
            var code = outcome.Code;

            if (kind == OutcomeKind.Ok || kind == OutcomeKind.Fallback)
            {
                value = outcome.Value;
            }

            return new NodeOutcomeEntry(typeof(T), kind, value, code);
        }

        public Outcome<T> ToOutcome<T>()
        {
            return Kind switch
            {
                OutcomeKind.Unspecified => default,
                OutcomeKind.Ok => Outcome<T>.Ok((T)Value!),
                OutcomeKind.Error => Outcome<T>.Error(Code),
                OutcomeKind.Timeout => Outcome<T>.Timeout(Code),
                OutcomeKind.Skipped => Outcome<T>.Skipped(Code),
                OutcomeKind.Fallback => Outcome<T>.Fallback((T)Value!, Code),
                OutcomeKind.Canceled => Outcome<T>.Canceled(Code),
                _ => throw new InvalidOperationException($"Unsupported outcome kind: '{Kind}'."),
            };
        }
    }
}
