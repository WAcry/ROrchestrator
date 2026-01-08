using System.Diagnostics;
using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Core;

public sealed class ExecExplain
{
    private readonly ExecExplainNode[] _nodes;
    private readonly ulong _configVersion;
    private readonly bool _hasConfigVersion;

    public string FlowName { get; }

    public ulong PlanHash { get; }

    public long StartTimestamp { get; }

    public long EndTimestamp { get; }

    public long DurationStopwatchTicks => EndTimestamp - StartTimestamp;

    public IReadOnlyList<ExecExplainNode> Nodes => _nodes;

    internal ExecExplain(
        string flowName,
        ulong planHash,
        bool hasConfigVersion,
        ulong configVersion,
        long startTimestamp,
        long endTimestamp,
        ExecExplainNode[] nodes)
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        if (nodes is null)
        {
            throw new ArgumentNullException(nameof(nodes));
        }

        if (nodes.Length == 0)
        {
            throw new ArgumentException("Nodes must be non-empty.", nameof(nodes));
        }

        if (startTimestamp < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(startTimestamp), startTimestamp, "StartTimestamp must be non-negative.");
        }

        if (endTimestamp < startTimestamp)
        {
            throw new ArgumentOutOfRangeException(nameof(endTimestamp), endTimestamp, "EndTimestamp must be >= StartTimestamp.");
        }

        FlowName = flowName;
        PlanHash = planHash;
        _hasConfigVersion = hasConfigVersion;
        _configVersion = configVersion;
        StartTimestamp = startTimestamp;
        EndTimestamp = endTimestamp;
        _nodes = nodes;
    }

    public bool TryGetConfigVersion(out ulong configVersion)
    {
        if (_hasConfigVersion)
        {
            configVersion = _configVersion;
            return true;
        }

        configVersion = default;
        return false;
    }

    public TimeSpan GetDuration()
    {
        return Stopwatch.GetElapsedTime(StartTimestamp, EndTimestamp);
    }
}

public readonly struct ExecExplainNode
{
    public BlueprintNodeKind Kind { get; }

    public string Name { get; }

    public string? StageName { get; }

    public string? ModuleType { get; }

    public long StartTimestamp { get; }

    public long EndTimestamp { get; }

    public long DurationStopwatchTicks => EndTimestamp - StartTimestamp;

    public OutcomeKind OutcomeKind { get; }

    public string OutcomeCode { get; }

    internal ExecExplainNode(
        BlueprintNodeKind kind,
        string name,
        string? stageName,
        string? moduleType,
        long startTimestamp,
        long endTimestamp,
        OutcomeKind outcomeKind,
        string outcomeCode)
    {
        Kind = kind;
        Name = name;
        StageName = stageName;
        ModuleType = moduleType;
        StartTimestamp = startTimestamp;
        EndTimestamp = endTimestamp;
        OutcomeKind = outcomeKind;
        OutcomeCode = outcomeCode;
    }

    public TimeSpan GetDuration()
    {
        return Stopwatch.GetElapsedTime(StartTimestamp, EndTimestamp);
    }
}
