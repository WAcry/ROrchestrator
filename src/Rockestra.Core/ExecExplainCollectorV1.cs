using System.Diagnostics;

namespace Rockestra.Core;

internal sealed class ExecExplainCollectorV1
{
    private static readonly IReadOnlyDictionary<string, string> EmptyVariants =
        new System.Collections.ObjectModel.ReadOnlyDictionary<string, string>(new Dictionary<string, string>(0));

    private ExecExplainNode[]? _nodes;
    private List<ExecExplainStageModule>? _stageModules;
    private ExecExplain? _explain;
    private string? _flowName;
    private ExplainLevel _requestedLevel;
    private ExplainLevel _level;
    private string? _reason;
    private ExplainRedactionPolicy _policy;
    private string? _levelDowngradeReasonCode;
    private ulong _planHash;
    private PatchEvaluatorV1.PatchOverlayAppliedV1[]? _overlaysApplied;
    private string? _emergencyOverlayIgnoredReasonCode;
    private IReadOnlyDictionary<string, string>? _variants;
    private bool _hasTrace;
    private ActivityTraceId _traceId;
    private ActivitySpanId _spanId;
    private DateTimeOffset _deadlineUtc;
    private long _budgetRemainingMsAtStart;
    private long _flowStartTimestamp;
    private long _flowEndTimestamp;
    private bool _active;

    public bool IsActive => _active;

    public ExplainLevel Level => _level;

    public ExplainLevel RequestedLevel => _requestedLevel;

    public string? ExplainReason => _reason;

    public ExplainRedactionPolicy Policy => _policy;

    public string? LevelDowngradeReasonCode => _levelDowngradeReasonCode;

    public ExecExplainCollectorV1(ExplainOptions options)
    {
        SetOptions(options);
    }

    public void SetOptions(ExplainOptions options)
    {
        _requestedLevel = options.Level;
        _reason = options.Reason;
        _policy = options.Policy;
        _level = ApplyReasonGate(_requestedLevel, _reason, out _levelDowngradeReasonCode);
    }

    public void Clear()
    {
        _nodes = null;
        _stageModules = null;
        _explain = null;
        _flowName = null;
        _planHash = 0;
        _overlaysApplied = null;
        _emergencyOverlayIgnoredReasonCode = null;
        _variants = null;
        _hasTrace = false;
        _traceId = default;
        _spanId = default;
        _deadlineUtc = default;
        _budgetRemainingMsAtStart = 0;
        _flowStartTimestamp = 0;
        _flowEndTimestamp = 0;
        _active = false;
    }

    private static ExplainLevel ApplyReasonGate(ExplainLevel requestedLevel, string? reason, out string? downgradeReasonCode)
    {
        if (requestedLevel == ExplainLevel.Full && string.IsNullOrWhiteSpace(reason))
        {
            downgradeReasonCode = "FULL_REASON_REQUIRED";
            return ExplainLevel.Standard;
        }

        downgradeReasonCode = null;
        return requestedLevel;
    }

    public void Start(string flowName, ulong planHash, IReadOnlyList<PlanNodeTemplate> nodes, DateTimeOffset deadlineUtc)
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        if (nodes is null)
        {
            throw new ArgumentNullException(nameof(nodes));
        }

        if (deadlineUtc == default)
        {
            throw new ArgumentException("Deadline must be non-default.", nameof(deadlineUtc));
        }

        var nodeCount = nodes.Count;
        if (nodeCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(nodes), nodeCount, "Nodes must be non-empty.");
        }

        _flowName = flowName;
        _planHash = planHash;
        _deadlineUtc = deadlineUtc.ToUniversalTime();
        var remainingTicks = _deadlineUtc.UtcTicks - DateTimeOffset.UtcNow.UtcTicks;
        _budgetRemainingMsAtStart = remainingTicks <= 0 ? 0 : remainingTicks / TimeSpan.TicksPerMillisecond;
        _hasTrace = false;
        _traceId = default;
        _spanId = default;
        _flowStartTimestamp = Stopwatch.GetTimestamp();
        _flowEndTimestamp = 0;
        _active = true;
        _explain = null;
        _overlaysApplied = Array.Empty<PatchEvaluatorV1.PatchOverlayAppliedV1>();
        _emergencyOverlayIgnoredReasonCode = null;
        _variants = EmptyVariants;

        if (_stageModules is not null)
        {
            _stageModules.Clear();
        }

        var explainNodes = new ExecExplainNode[nodeCount];
        for (var i = 0; i < nodeCount; i++)
        {
            var node = nodes[i];
            explainNodes[i] = new ExecExplainNode(
                node.Kind,
                node.Name,
                node.StageName,
                node.ModuleType,
                startTimestamp: 0,
                endTimestamp: 0,
                outcomeKind: OutcomeKind.Unspecified,
                outcomeCode: string.Empty);
        }

        _nodes = explainNodes;
    }

    public void RecordTrace(ActivityTraceId traceId, ActivitySpanId spanId)
    {
        _hasTrace = true;
        _traceId = traceId;
        _spanId = spanId;
    }

    public void RecordRouting(
        IReadOnlyDictionary<string, string> variants,
        IReadOnlyList<PatchEvaluatorV1.PatchOverlayAppliedV1>? overlaysApplied,
        string? emergencyOverlayIgnoredReasonCode)
    {
        if (!_active)
        {
            return;
        }

        if (_level == ExplainLevel.Minimal)
        {
            return;
        }

        if (variants is null || variants.Count == 0)
        {
            _variants = EmptyVariants;
        }
        else
        {
            var copy = new Dictionary<string, string>(capacity: variants.Count);
            foreach (var pair in variants)
            {
                copy.Add(pair.Key, pair.Value);
            }

            _variants = new System.Collections.ObjectModel.ReadOnlyDictionary<string, string>(copy);
        }

        if (overlaysApplied is null || overlaysApplied.Count == 0)
        {
            _overlaysApplied = Array.Empty<PatchEvaluatorV1.PatchOverlayAppliedV1>();
        }
        else if (overlaysApplied is PatchEvaluatorV1.PatchOverlayAppliedV1[] overlayArray)
        {
            _overlaysApplied = overlayArray;
        }
        else
        {
            var array = new PatchEvaluatorV1.PatchOverlayAppliedV1[overlaysApplied.Count];
            for (var i = 0; i < overlaysApplied.Count; i++)
            {
                array[i] = overlaysApplied[i];
            }

            _overlaysApplied = array;
        }

        _emergencyOverlayIgnoredReasonCode = string.IsNullOrEmpty(emergencyOverlayIgnoredReasonCode) ? null : emergencyOverlayIgnoredReasonCode;
    }

    public void RecordStageModule(
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
        if (!_active)
        {
            return;
        }

        _stageModules ??= new List<ExecExplainStageModule>(capacity: 8);
        _stageModules.Add(
            new ExecExplainStageModule(
                stageName,
                moduleId,
                moduleType,
                limitKey,
                priority,
                startTimestamp,
                endTimestamp,
                outcomeKind,
                outcomeCode,
                gateDecisionCode,
                gateReasonCode,
                gateSelectorName,
                isShadow,
                shadowSampleBps,
                isOverride,
                memoHit));
    }

    public void RecordNode(PlanNodeTemplate node, long startTimestamp, long endTimestamp, OutcomeKind outcomeKind, string outcomeCode)
    {
        if (!_active)
        {
            return;
        }

        _nodes![node.Index] = new ExecExplainNode(
            node.Kind,
            node.Name,
            node.StageName,
            node.ModuleType,
            startTimestamp,
            endTimestamp,
            outcomeKind,
            outcomeCode);
    }

    public void Finish(FlowContext context)
    {
        if (!_active)
        {
            return;
        }

        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        _flowEndTimestamp = Stopwatch.GetTimestamp();
        var hasConfigVersion = context.TryGetConfigVersion(out var configVersion);
        var hasConfigSnapshotMeta = context.TryGetConfigSnapshot(out var configSnapshot);
        var configSnapshotMeta = hasConfigSnapshotMeta ? configSnapshot.Meta : default;
        var qosSelectedTier = context.QosSelectedTier;
        var qosReasonCode = context.QosReasonCode;
        var qosSignals = context.QosSignals;

        ParamsExplain paramsExplain = default;
        var hasParamsExplain = _level != ExplainLevel.Minimal && context.TryGetParamsExplain(out paramsExplain);

        var nodes = _nodes;
        if (nodes is null)
        {
            throw new InvalidOperationException("ExecExplainCollectorV1 has not been started.");
        }

        var stageModules = _stageModules;
        var stageModulesArray = stageModules is null || stageModules.Count == 0
            ? Array.Empty<ExecExplainStageModule>()
            : stageModules.ToArray();

        _explain = new ExecExplain(
            _flowName!,
            _requestedLevel,
            _level,
            _reason,
            _policy,
            _levelDowngradeReasonCode,
            _planHash,
            hasConfigVersion,
            configVersion,
            hasConfigSnapshotMeta,
            configSnapshotMeta,
            qosSelectedTier,
            qosReasonCode,
            qosSignals,
            _overlaysApplied,
            _emergencyOverlayIgnoredReasonCode,
            _variants,
            _hasTrace,
            _traceId,
            _spanId,
            _deadlineUtc,
            _budgetRemainingMsAtStart,
            hasParamsExplain,
            paramsExplain,
            _flowStartTimestamp,
            _flowEndTimestamp,
            nodes,
            stageModulesArray);
        _active = false;
    }

    public bool TryGetExplain(out ExecExplain explain)
    {
        var current = _explain;
        if (current is null)
        {
            explain = null!;
            return false;
        }

        explain = current;
        return true;
    }
}

