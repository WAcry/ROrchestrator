namespace ROrchestrator.Core;

public sealed class FlowContext
{
    private static readonly IReadOnlyDictionary<string, string> EmptyStringDictionary =
        new System.Collections.ObjectModel.ReadOnlyDictionary<string, string>(new Dictionary<string, string>(0));

    private readonly Lock _configSnapshotGate;
    private readonly Lock _nodeOutcomeGate;
    private readonly IReadOnlyDictionary<string, string> _variants;
    private readonly string? _userId;
    private readonly IReadOnlyDictionary<string, string> _requestAttributes;
    private readonly IExplainSink _explainSink;
    private ExecExplainCollectorV1? _execExplainCollector;
    private ConfigSnapshot _configSnapshot;
    private Task<ConfigSnapshot>? _configSnapshotTask;
    private int _configSnapshotState;
    private Dictionary<string, int>? _nodeNameToIndex;
    private IReadOnlyDictionary<string, int>? _nodeNameToIndexView;
    private NodeOutcomeEntry[]? _nodeOutcomes;
    private int[]? _nodeOutcomeStates;
    private int _nodeCount;
    private int _nextDynamicIndex;
    private bool _hasRecordedOutcomes;
    private IFlowTestOverrideProvider? _flowTestOverrideProvider;
    private IFlowTestInvocationSink? _flowTestInvocationSink;

    public IServiceProvider Services { get; }

    public CancellationToken CancellationToken { get; }

    public DateTimeOffset Deadline { get; }

    public IReadOnlyDictionary<string, string> Variants => _variants;

    public string? UserId => _userId;

    public IReadOnlyDictionary<string, string> RequestAttributes => _requestAttributes;

    public IExplainSink ExplainSink => _explainSink;

    public FlowContext(IServiceProvider services, CancellationToken cancellationToken, DateTimeOffset deadline, IExplainSink? explainSink = null)
    {
        Services = services ?? throw new ArgumentNullException(nameof(services));

        if (deadline == default)
        {
            throw new ArgumentException("Deadline must be non-default.", nameof(deadline));
        }

        _configSnapshotGate = new();
        _nodeOutcomeGate = new();
        _variants = EmptyStringDictionary;
        _userId = null;
        _requestAttributes = EmptyStringDictionary;
        _explainSink = explainSink ?? NullExplainSink.Instance;
        CancellationToken = cancellationToken;
        Deadline = deadline;
    }

    public FlowContext(
        IServiceProvider services,
        CancellationToken cancellationToken,
        DateTimeOffset deadline,
        FlowRequestOptions requestOptions,
        IExplainSink? explainSink = null)
    {
        Services = services ?? throw new ArgumentNullException(nameof(services));

        if (deadline == default)
        {
            throw new ArgumentException("Deadline must be non-default.", nameof(deadline));
        }

        _configSnapshotGate = new();
        _nodeOutcomeGate = new();
        _variants = requestOptions.Variants ?? EmptyStringDictionary;
        _userId = requestOptions.UserId;
        _requestAttributes = requestOptions.RequestAttributes ?? EmptyStringDictionary;
        _explainSink = explainSink ?? NullExplainSink.Instance;
        CancellationToken = cancellationToken;
        Deadline = deadline;
    }

    public void EnableExecExplain()
    {
        _execExplainCollector ??= new ExecExplainCollectorV1();
    }

    public bool TryGetExecExplain(out ExecExplain explain)
    {
        var collector = _execExplainCollector;
        if (collector is null)
        {
            explain = null!;
            return false;
        }

        return collector.TryGetExplain(out explain);
    }

    internal ExecExplainCollectorV1? ExecExplainCollector => _execExplainCollector;

    internal IFlowTestOverrideProvider? FlowTestOverrideProvider => _flowTestOverrideProvider;

    internal IFlowTestInvocationSink? FlowTestInvocationSink => _flowTestInvocationSink;

    internal void ConfigureForTesting(IFlowTestOverrideProvider? overrideProvider, IFlowTestInvocationSink? invocationSink)
    {
        _flowTestOverrideProvider = overrideProvider;
        _flowTestInvocationSink = invocationSink;
    }

    public bool TryGetConfigVersion(out ulong configVersion)
    {
        if (Volatile.Read(ref _configSnapshotState) == 2)
        {
            configVersion = _configSnapshot.ConfigVersion;
            return true;
        }

        configVersion = default;
        return false;
    }

    public bool TryGetConfigSnapshot(out ConfigSnapshot snapshot)
    {
        if (Volatile.Read(ref _configSnapshotState) == 2)
        {
            snapshot = _configSnapshot;
            return true;
        }

        snapshot = default;
        return false;
    }

    internal ValueTask<ConfigSnapshot> GetConfigSnapshotAsync(IConfigProvider provider)
    {
        if (provider is null)
        {
            throw new ArgumentNullException(nameof(provider));
        }

        var state = Volatile.Read(ref _configSnapshotState);

        if (state == 2)
        {
            return new ValueTask<ConfigSnapshot>(_configSnapshot);
        }

        if (state == 1)
        {
            var task = Volatile.Read(ref _configSnapshotTask);
            if (task is not null)
            {
                return new ValueTask<ConfigSnapshot>(task);
            }

            return GetConfigSnapshotSlowAsync(provider);
        }

        return GetConfigSnapshotSlowAsync(provider);
    }

    private ValueTask<ConfigSnapshot> GetConfigSnapshotSlowAsync(IConfigProvider provider)
    {
        lock (_configSnapshotGate)
        {
            var state = _configSnapshotState;

            if (state == 2)
            {
                return new ValueTask<ConfigSnapshot>(_configSnapshot);
            }

            if (state == 1)
            {
                return new ValueTask<ConfigSnapshot>(_configSnapshotTask!);
            }

            var snapshotTask = provider.GetSnapshotAsync(this);

            if (snapshotTask.IsCompletedSuccessfully)
            {
                var snapshot = snapshotTask.Result;
                _configSnapshot = snapshot;
                Volatile.Write(ref _configSnapshotState, 2);
                return new ValueTask<ConfigSnapshot>(snapshot);
            }

            var task = FetchAndStoreConfigSnapshotAsync(snapshotTask);
            _configSnapshotTask = task;
            Volatile.Write(ref _configSnapshotState, 1);
            return new ValueTask<ConfigSnapshot>(task);
        }
    }

    private async Task<ConfigSnapshot> FetchAndStoreConfigSnapshotAsync(ValueTask<ConfigSnapshot> snapshotTask)
    {
        try
        {
            var snapshot = await snapshotTask.ConfigureAwait(false);

            lock (_configSnapshotGate)
            {
                _configSnapshot = snapshot;
                _configSnapshotTask = null;
                Volatile.Write(ref _configSnapshotState, 2);
            }

            return snapshot;
        }
        catch
        {
            lock (_configSnapshotGate)
            {
                _configSnapshotTask = null;
                Volatile.Write(ref _configSnapshotState, 0);
            }

            throw;
        }
    }

    internal void RecordNodeOutcome<T>(string nodeName, Outcome<T> outcome)
    {
        if (string.IsNullOrEmpty(nodeName))
        {
            throw new ArgumentException("Node name must be non-empty.", nameof(nodeName));
        }

        var index = GetOrAddNodeIndex(nodeName);
        RecordNodeOutcome(index, nodeName, outcome);
    }

    // Must be called by the execution engine before recording outcomes for a compiled plan.
    internal void PrepareForExecution(IReadOnlyDictionary<string, int> nodeNameToIndex, int nodeCount)
    {
        if (nodeNameToIndex is null)
        {
            throw new ArgumentNullException(nameof(nodeNameToIndex));
        }

        if (nodeCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(nodeCount), nodeCount, "NodeCount must be greater than zero.");
        }

        lock (_nodeOutcomeGate)
        {
            var previousCount = _nodeCount;
            _nodeNameToIndexView = nodeNameToIndex;
            _nodeCount = nodeCount;
            _nextDynamicIndex = nodeCount;
            _nodeNameToIndex?.Clear();
            EnsureOutcomeCapacity(nodeCount);

            if (_hasRecordedOutcomes)
            {
                var clearCount = previousCount > nodeCount ? previousCount : nodeCount;
                Array.Clear(_nodeOutcomeStates!, 0, clearCount);
                Array.Clear(_nodeOutcomes!, 0, clearCount);
                _hasRecordedOutcomes = false;
            }
        }
    }

    public bool TryGetNodeOutcome<T>(string nodeName, out Outcome<T> outcome)
    {
        if (string.IsNullOrEmpty(nodeName))
        {
            throw new ArgumentException("Node name must be non-empty.", nameof(nodeName));
        }

        if (!TryGetNodeIndex(nodeName, out var index))
        {
            outcome = default;
            return false;
        }

        var states = _nodeOutcomeStates;
        if (states is null || (uint)index >= (uint)states.Length)
        {
            outcome = default;
            return false;
        }

        if (Volatile.Read(ref states[index]) != 2)
        {
            outcome = default;
            return false;
        }

        var entry = _nodeOutcomes![index];

        if (entry.OutputType != typeof(T))
        {
            throw new InvalidOperationException(
                $"Outcome for node '{nodeName}' has type '{entry.OutputType}', not '{typeof(T)}'.");
        }

        outcome = entry.ToOutcome<T>();
        return true;
    }

    internal bool TryGetNodeOutcomeMetadata(int nodeIndex, out OutcomeKind kind, out string code)
    {
        if (nodeIndex < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(nodeIndex), nodeIndex, "NodeIndex must be non-negative.");
        }

        var states = _nodeOutcomeStates;
        if (states is null || (uint)nodeIndex >= (uint)states.Length)
        {
            kind = default;
            code = string.Empty;
            return false;
        }

        if (Volatile.Read(ref states[nodeIndex]) != 2)
        {
            kind = default;
            code = string.Empty;
            return false;
        }

        var entry = _nodeOutcomes![nodeIndex];
        kind = entry.Kind;
        code = entry.Code;
        return true;
    }

    internal void RecordNodeOutcome<T>(int nodeIndex, string nodeName, Outcome<T> outcome)
    {
        if (nodeIndex < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(nodeIndex), nodeIndex, "NodeIndex must be non-negative.");
        }

        if (nodeIndex >= _nodeCount)
        {
            throw new InvalidOperationException($"Node index '{nodeIndex}' is out of range for the current execution plan.");
        }

        var entry = NodeOutcomeEntry.Create(outcome);

        var states = _nodeOutcomeStates;
        var entries = _nodeOutcomes;

        if (states is null || entries is null || (uint)nodeIndex >= (uint)states.Length)
        {
            lock (_nodeOutcomeGate)
            {
                states = _nodeOutcomeStates;
                entries = _nodeOutcomes;

                if (states is null || entries is null || (uint)nodeIndex >= (uint)states.Length)
                {
                    EnsureOutcomeCapacity(nodeIndex + 1);
                    states = _nodeOutcomeStates!;
                    entries = _nodeOutcomes!;
                }
            }
        }

        if (Interlocked.CompareExchange(ref states[nodeIndex], 1, 0) != 0)
        {
            throw new InvalidOperationException($"Outcome for node '{nodeName}' has already been recorded.");
        }

        entries[nodeIndex] = entry;
        Volatile.Write(ref states[nodeIndex], 2);
        _hasRecordedOutcomes = true;
    }

    private int GetOrAddNodeIndex(string nodeName)
    {
        var view = _nodeNameToIndexView;
        if (view is not null)
        {
            if (view.TryGetValue(nodeName, out var index))
            {
                return index;
            }

            lock (_nodeOutcomeGate)
            {
                _nodeNameToIndex ??= new Dictionary<string, int>();

                if (_nodeNameToIndex.TryGetValue(nodeName, out index))
                {
                    return index;
                }

                index = _nextDynamicIndex;
                _nextDynamicIndex++;
                _nodeCount = _nextDynamicIndex;
                _nodeNameToIndex.Add(nodeName, index);
                EnsureOutcomeCapacity(_nodeCount);
                return index;
            }
        }

        lock (_nodeOutcomeGate)
        {
            _nodeNameToIndex ??= new Dictionary<string, int>();

            if (_nodeNameToIndex.TryGetValue(nodeName, out var index))
            {
                return index;
            }

            index = _nextDynamicIndex;
            _nextDynamicIndex++;
            _nodeCount = _nextDynamicIndex;
            _nodeNameToIndex.Add(nodeName, index);
            EnsureOutcomeCapacity(_nodeCount);
            return index;
        }
    }

    private bool TryGetNodeIndex(string nodeName, out int index)
    {
        var view = _nodeNameToIndexView;
        if (view is not null)
        {
            if (view.TryGetValue(nodeName, out index))
            {
                return true;
            }

            lock (_nodeOutcomeGate)
            {
                if (_nodeNameToIndex is null || !_nodeNameToIndex.TryGetValue(nodeName, out index))
                {
                    index = default;
                    return false;
                }

                return true;
            }
        }

        lock (_nodeOutcomeGate)
        {
            if (_nodeNameToIndex is null || !_nodeNameToIndex.TryGetValue(nodeName, out index))
            {
                index = default;
                return false;
            }

            return true;
        }
    }

    private void EnsureOutcomeCapacity(int requiredCount)
    {
        if (requiredCount <= 0)
        {
            return;
        }

        if (_nodeOutcomes is null || _nodeOutcomeStates is null)
        {
            _nodeOutcomes = new NodeOutcomeEntry[requiredCount];
            _nodeOutcomeStates = new int[requiredCount];
            return;
        }

        if (_nodeOutcomes.Length >= requiredCount)
        {
            return;
        }

        var newSize = _nodeOutcomes.Length;
        while (newSize < requiredCount)
        {
            newSize = newSize < 256 ? newSize * 2 : newSize + (newSize >> 1);
        }

        var newOutcomes = new NodeOutcomeEntry[newSize];
        var newStates = new int[newSize];

        Array.Copy(_nodeOutcomes, newOutcomes, _nodeOutcomes.Length);
        Array.Copy(_nodeOutcomeStates, newStates, _nodeOutcomeStates.Length);

        _nodeOutcomes = newOutcomes;
        _nodeOutcomeStates = newStates;
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

    private sealed class NullExplainSink : IExplainSink
    {
        public static readonly NullExplainSink Instance = new();

        private NullExplainSink()
        {
        }

        public void Add(string key, string value)
        {
        }
    }
}
