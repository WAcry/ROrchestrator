using System.Diagnostics;
using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Core;

public sealed class ExecExplain
{
    private static readonly IReadOnlyDictionary<string, string> EmptyVariants =
        new System.Collections.ObjectModel.ReadOnlyDictionary<string, string>(new Dictionary<string, string>(0));

    private readonly ExecExplainNode[] _nodes;
    private readonly ExecExplainStageModule[] _stageModules;
    private readonly PatchEvaluatorV1.PatchOverlayAppliedV1[] _overlaysApplied;
    private readonly IReadOnlyDictionary<string, string> _variants;
    private readonly ulong _configVersion;
    private readonly bool _hasConfigVersion;
    private readonly QosTier _qosSelectedTier;

    public string FlowName { get; }

    public ExplainLevel Level { get; }

    public ulong PlanHash { get; }

    public long StartTimestamp { get; }

    public long EndTimestamp { get; }

    public long DurationStopwatchTicks => EndTimestamp - StartTimestamp;

    public IReadOnlyList<ExecExplainNode> Nodes => _nodes;

    public IReadOnlyList<ExecExplainStageModule> StageModules => _stageModules;

    public IReadOnlyList<PatchEvaluatorV1.PatchOverlayAppliedV1> OverlaysApplied => _overlaysApplied;

    public IReadOnlyDictionary<string, string> Variants => _variants;

    public QosTier QosSelectedTier => _qosSelectedTier;

    internal ExecExplain(
        string flowName,
        ExplainLevel level,
        ulong planHash,
        bool hasConfigVersion,
        ulong configVersion,
        QosTier qosSelectedTier,
        PatchEvaluatorV1.PatchOverlayAppliedV1[]? overlaysApplied,
        IReadOnlyDictionary<string, string>? variants,
        long startTimestamp,
        long endTimestamp,
        ExecExplainNode[] nodes,
        ExecExplainStageModule[] stageModules)
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

        _stageModules = stageModules ?? throw new ArgumentNullException(nameof(stageModules));
        _overlaysApplied = overlaysApplied ?? Array.Empty<PatchEvaluatorV1.PatchOverlayAppliedV1>();
        _variants = variants ?? EmptyVariants;

        if (startTimestamp < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(startTimestamp), startTimestamp, "StartTimestamp must be non-negative.");
        }

        if (endTimestamp < startTimestamp)
        {
            throw new ArgumentOutOfRangeException(nameof(endTimestamp), endTimestamp, "EndTimestamp must be >= StartTimestamp.");
        }

        FlowName = flowName;
        Level = level;
        PlanHash = planHash;
        _hasConfigVersion = hasConfigVersion;
        _configVersion = configVersion;
        _qosSelectedTier = qosSelectedTier;
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

public readonly struct ExecExplainStageModule
{
    public string StageName { get; }

    public string ModuleId { get; }

    public string ModuleType { get; }

    public int Priority { get; }

    public long StartTimestamp { get; }

    public long EndTimestamp { get; }

    public long DurationStopwatchTicks => EndTimestamp - StartTimestamp;

    public OutcomeKind OutcomeKind { get; }

    public string OutcomeCode { get; }

    public string GateDecisionCode { get; }

    public string GateSelectorName { get; }

    public bool IsShadow { get; }

    public ushort ShadowSampleBps { get; }

    public bool IsOverride { get; }

    internal ExecExplainStageModule(
        string stageName,
        string moduleId,
        string moduleType,
        int priority,
        long startTimestamp,
        long endTimestamp,
        OutcomeKind outcomeKind,
        string outcomeCode,
        string gateDecisionCode,
        string gateSelectorName,
        bool isShadow,
        ushort shadowSampleBps,
        bool isOverride)
    {
        StageName = stageName;
        ModuleId = moduleId;
        ModuleType = moduleType;
        Priority = priority;
        StartTimestamp = startTimestamp;
        EndTimestamp = endTimestamp;
        OutcomeKind = outcomeKind;
        OutcomeCode = outcomeCode;
        GateDecisionCode = gateDecisionCode;
        GateSelectorName = gateSelectorName;
        IsShadow = isShadow;
        ShadowSampleBps = shadowSampleBps;
        IsOverride = isOverride;
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
