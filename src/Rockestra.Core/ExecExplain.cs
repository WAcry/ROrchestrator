using System.Diagnostics;
using Rockestra.Core.Blueprint;

namespace Rockestra.Core;

public sealed class ExecExplain
{
    private static readonly IReadOnlyDictionary<string, string> EmptyVariants =
        new System.Collections.ObjectModel.ReadOnlyDictionary<string, string>(new Dictionary<string, string>(0));

    private readonly ExecExplainNode[] _nodes;
    private readonly ExecExplainStageModule[] _stageModules;
    private readonly PatchEvaluatorV1.PatchOverlayAppliedV1[] _overlaysApplied;
    private readonly IReadOnlyDictionary<string, string> _variants;
    private readonly bool _hasTrace;
    private readonly ActivityTraceId _traceId;
    private readonly ActivitySpanId _spanId;
    private readonly bool _hasParamsExplain;
    private readonly ParamsExplain _paramsExplain;
    private readonly ExplainLevel _requestedLevel;
    private readonly string? _explainReason;
    private readonly ExplainRedactionPolicy _policy;
    private readonly string? _levelDowngradeReasonCode;
    private readonly ulong _configVersion;
    private readonly bool _hasConfigVersion;
    private readonly bool _hasConfigSnapshotMeta;
    private readonly ConfigSnapshotMeta _configSnapshotMeta;
    private readonly QosTier _qosSelectedTier;
    private readonly string? _qosReasonCode;
    private readonly IReadOnlyDictionary<string, string>? _qosSignals;

    public string FlowName { get; }

    public ExplainLevel Level { get; }

    public ExplainLevel RequestedLevel => _requestedLevel;

    public string? ExplainReason => _explainReason;

    public ExplainRedactionPolicy Policy => _policy;

    public string? LevelDowngradeReasonCode => _levelDowngradeReasonCode;

    public ulong PlanHash { get; }

    public DateTimeOffset DeadlineUtc { get; }

    public long BudgetRemainingMsAtStart { get; }

    public long StartTimestamp { get; }

    public long EndTimestamp { get; }

    public long DurationStopwatchTicks => EndTimestamp - StartTimestamp;

    public IReadOnlyList<ExecExplainNode> Nodes => _nodes;

    public IReadOnlyList<ExecExplainStageModule> StageModules => _stageModules;

    public IReadOnlyList<PatchEvaluatorV1.PatchOverlayAppliedV1> OverlaysApplied => _overlaysApplied;

    public IReadOnlyDictionary<string, string> Variants => _variants;

    public QosTier QosSelectedTier => _qosSelectedTier;

    public string? QosReasonCode => _qosReasonCode;

    public IReadOnlyDictionary<string, string>? QosSignals => _qosSignals;

    internal ExecExplain(
        string flowName,
        ExplainLevel requestedLevel,
        ExplainLevel level,
        string? explainReason,
        ExplainRedactionPolicy policy,
        string? levelDowngradeReasonCode,
        ulong planHash,
        bool hasConfigVersion,
        ulong configVersion,
        bool hasConfigSnapshotMeta,
        ConfigSnapshotMeta configSnapshotMeta,
        QosTier qosSelectedTier,
        string? qosReasonCode,
        IReadOnlyDictionary<string, string>? qosSignals,
        PatchEvaluatorV1.PatchOverlayAppliedV1[]? overlaysApplied,
        IReadOnlyDictionary<string, string>? variants,
        bool hasTrace,
        ActivityTraceId traceId,
        ActivitySpanId spanId,
        DateTimeOffset deadlineUtc,
        long budgetRemainingMsAtStart,
        bool hasParamsExplain,
        ParamsExplain paramsExplain,
        long startTimestamp,
        long endTimestamp,
        ExecExplainNode[] nodes,
        ExecExplainStageModule[] stageModules)
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        if ((uint)requestedLevel > (uint)ExplainLevel.Full)
        {
            throw new ArgumentOutOfRangeException(nameof(requestedLevel), requestedLevel, "Unsupported explain level.");
        }

        if ((uint)level > (uint)ExplainLevel.Full)
        {
            throw new ArgumentOutOfRangeException(nameof(level), level, "Unsupported explain level.");
        }

        if ((uint)policy > (uint)ExplainRedactionPolicy.Default)
        {
            throw new ArgumentOutOfRangeException(nameof(policy), policy, "Unsupported explain redaction policy.");
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

        if (deadlineUtc == default)
        {
            throw new ArgumentException("DeadlineUtc must be non-default.", nameof(deadlineUtc));
        }

        if (budgetRemainingMsAtStart < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(budgetRemainingMsAtStart), budgetRemainingMsAtStart, "BudgetRemainingMsAtStart must be >= 0.");
        }

        FlowName = flowName;
        Level = level;
        _requestedLevel = requestedLevel;
        _explainReason = explainReason;
        _policy = policy;
        _levelDowngradeReasonCode = levelDowngradeReasonCode;
        PlanHash = planHash;
        _hasConfigVersion = hasConfigVersion;
        _configVersion = configVersion;
        _hasConfigSnapshotMeta = hasConfigSnapshotMeta;
        _configSnapshotMeta = configSnapshotMeta;
        _qosSelectedTier = qosSelectedTier;
        _qosReasonCode = qosReasonCode;
        _qosSignals = qosSignals;
        _hasTrace = hasTrace;
        _traceId = traceId;
        _spanId = spanId;
        DeadlineUtc = deadlineUtc;
        BudgetRemainingMsAtStart = budgetRemainingMsAtStart;
        _hasParamsExplain = hasParamsExplain;
        _paramsExplain = paramsExplain;
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

    public bool TryGetConfigSnapshotMeta(out ConfigSnapshotMeta meta)
    {
        if (_hasConfigSnapshotMeta)
        {
            meta = _configSnapshotMeta;
            return true;
        }

        meta = default;
        return false;
    }

    public TimeSpan GetDuration()
    {
        return Stopwatch.GetElapsedTime(StartTimestamp, EndTimestamp);
    }

    public bool TryGetTrace(out ActivityTraceId traceId, out ActivitySpanId spanId)
    {
        if (_hasTrace)
        {
            traceId = _traceId;
            spanId = _spanId;
            return true;
        }

        traceId = default;
        spanId = default;
        return false;
    }

    public bool TryGetParamsExplain(out ParamsExplain explain)
    {
        if (_hasParamsExplain)
        {
            explain = _paramsExplain;
            return true;
        }

        explain = default;
        return false;
    }
}

public readonly struct ExecExplainStageModule
{
    public string StageName { get; }

    public string ModuleId { get; }

    public string ModuleType { get; }

    public string? LimitKey { get; }

    public int Priority { get; }

    public long StartTimestamp { get; }

    public long EndTimestamp { get; }

    public long DurationStopwatchTicks => EndTimestamp - StartTimestamp;

    public OutcomeKind OutcomeKind { get; }

    public string OutcomeCode { get; }

    public string GateDecisionCode { get; }

    public string GateReasonCode { get; }

    public string GateSelectorName { get; }

    public bool IsShadow { get; }

    public ushort ShadowSampleBps { get; }

    public bool IsOverride { get; }

    public bool MemoHit { get; }

    internal ExecExplainStageModule(
        string stageName,
        string moduleId,
        string moduleType,
        string? limitKey,
        int priority,
        long startTimestamp,
        long endTimestamp,
        OutcomeKind outcomeKind,
        string outcomeCode,
        string gateDecisionCode,
        string gateReasonCode,
        string gateSelectorName,
        bool isShadow,
        ushort shadowSampleBps,
        bool isOverride,
        bool memoHit)
    {
        StageName = stageName;
        ModuleId = moduleId;
        ModuleType = moduleType;
        LimitKey = limitKey;
        Priority = priority;
        StartTimestamp = startTimestamp;
        EndTimestamp = endTimestamp;
        OutcomeKind = outcomeKind;
        OutcomeCode = outcomeCode;
        GateDecisionCode = gateDecisionCode;
        GateReasonCode = gateReasonCode;
        GateSelectorName = gateSelectorName;
        IsShadow = isShadow;
        ShadowSampleBps = shadowSampleBps;
        IsOverride = isOverride;
        MemoHit = memoHit;
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

