using System.Text.Json;

namespace Rockestra.Core;

public sealed class FlowContext
{
    private static readonly IReadOnlyDictionary<string, string> EmptyStringDictionary =
        new System.Collections.ObjectModel.ReadOnlyDictionary<string, string>(new Dictionary<string, string>(0));

    private readonly Lock _configSnapshotGate;
    private readonly Lock _nodeOutcomeGate;
    private readonly Lock _paramsGate;
    private readonly Lock _stageFanoutGate;
    private readonly IReadOnlyDictionary<string, string> _variants;
    private readonly string? _userId;
    private readonly IReadOnlyDictionary<string, string> _requestAttributes;
    private QosTier _qosSelectedTier;
    private string? _qosReasonCode;
    private IReadOnlyDictionary<string, string>? _qosSignals;
    private int _qosSignalsEnabled;
    private readonly IExplainSink _explainSink;
    private ExecExplainCollectorV1? _execExplainCollector;
    private PatchEvaluatorV1.FlowPatchEvaluationV1? _activePatchEvaluation;
    private ulong _activePatchEvaluationConfigVersion;
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
    private string? _flowNameHint;
    private string? _currentFlowName;
    private Type? _flowParamsType;
    private Type? _flowParamsPatchType;
    private object? _flowDefaultParams;
    private string? _paramsCacheFlowName;
    private Type? _paramsCacheType;
    private ulong _paramsCacheConfigVersion;
    private object? _paramsCacheValue;
    private int _paramsCacheState;
    private Dictionary<string, StageFanoutSnapshot>? _stageFanoutSnapshots;
    private RequestMemo? _requestMemo;
    private int _requestMemoEnabled;

    public IServiceProvider Services { get; }

    public CancellationToken CancellationToken { get; }

    public DateTimeOffset Deadline { get; }

    public IReadOnlyDictionary<string, string> Variants => _variants;

    public string? UserId => _userId;

    public IReadOnlyDictionary<string, string> RequestAttributes => _requestAttributes;

    internal QosTier QosSelectedTier => _qosSelectedTier;

    internal string? QosReasonCode => _qosReasonCode;

    internal IReadOnlyDictionary<string, string>? QosSignals => _qosSignals;

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
        _paramsGate = new();
        _stageFanoutGate = new();
        _variants = EmptyStringDictionary;
        _userId = null;
        _requestAttributes = EmptyStringDictionary;
        _qosSelectedTier = QosTier.Full;
        _qosReasonCode = null;
        _qosSignals = null;
        _qosSignalsEnabled = 0;
        _explainSink = explainSink ?? NullExplainSink.Instance;
        CancellationToken = cancellationToken;
        Deadline = deadline;
        _requestMemo = null;
        _requestMemoEnabled = 0;
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
        _paramsGate = new();
        _stageFanoutGate = new();
        _variants = requestOptions.Variants ?? EmptyStringDictionary;
        _userId = requestOptions.UserId;
        _requestAttributes = requestOptions.RequestAttributes ?? EmptyStringDictionary;
        _qosSelectedTier = QosTier.Full;
        _qosReasonCode = null;
        _qosSignals = null;
        _qosSignalsEnabled = 0;
        _explainSink = explainSink ?? NullExplainSink.Instance;
        CancellationToken = cancellationToken;
        Deadline = deadline;
        _requestMemo = null;
        _requestMemoEnabled = 0;
    }

    public void EnableQosSignals()
    {
        Volatile.Write(ref _qosSignalsEnabled, 1);
    }

    internal bool ShouldCollectQosSignals()
    {
        if (Volatile.Read(ref _qosSignalsEnabled) != 0)
        {
            return true;
        }

        var collector = _execExplainCollector;
        return collector is not null && collector.Level == ExplainLevel.Full;
    }

    public void EnableRequestMemo()
    {
        Volatile.Write(ref _requestMemoEnabled, 1);
    }

    internal bool TryGetOrCreateRequestMemo(out RequestMemo memo)
    {
        if (Volatile.Read(ref _requestMemoEnabled) == 0)
        {
            memo = null!;
            return false;
        }

        var existing = Volatile.Read(ref _requestMemo);
        if (existing is not null)
        {
            memo = existing;
            return true;
        }

        var created = new RequestMemo();
        var prior = Interlocked.CompareExchange(ref _requestMemo, created, null);
        memo = prior ?? created;
        return true;
    }

    internal void SetQosDecision(QosTier tier, string? reasonCode, IReadOnlyDictionary<string, string>? signals)
    {
        _qosSelectedTier = tier;
        _qosReasonCode = reasonCode;
        _qosSignals = signals;
    }

    public void EnableExecExplain()
    {
        EnableExecExplain(new ExplainOptions(ExplainLevel.Minimal));
    }

    public void EnableExecExplain(ExplainOptions options)
    {
        var collector = _execExplainCollector;

        if (collector is null)
        {
            _execExplainCollector = new ExecExplainCollectorV1(options);
            return;
        }

        collector.SetOptions(options);
    }

    [Obsolete("Use EnableExecExplain(ExplainOptions) instead. Full explain requires a reason and may be downgraded.")]
    public void EnableExecExplain(ExplainLevel level = ExplainLevel.Minimal)
    {
        EnableExecExplain(new ExplainOptions(level));
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

    internal void SetActivePatchEvaluation(PatchEvaluatorV1.FlowPatchEvaluationV1 evaluation, ulong configVersion)
    {
        if (evaluation is null)
        {
            throw new ArgumentNullException(nameof(evaluation));
        }

        _activePatchEvaluationConfigVersion = configVersion;
        Volatile.Write(ref _activePatchEvaluation, evaluation);
    }

    internal void ClearActivePatchEvaluation(PatchEvaluatorV1.FlowPatchEvaluationV1 evaluation)
    {
        if (evaluation is null)
        {
            throw new ArgumentNullException(nameof(evaluation));
        }

        var current = Volatile.Read(ref _activePatchEvaluation);

        if (!ReferenceEquals(current, evaluation))
        {
            return;
        }

        Volatile.Write(ref _activePatchEvaluation, null);
        _activePatchEvaluationConfigVersion = 0;
    }

    internal IFlowTestOverrideProvider? FlowTestOverrideProvider => _flowTestOverrideProvider;

    internal IFlowTestInvocationSink? FlowTestInvocationSink => _flowTestInvocationSink;

    internal void ConfigureForTesting(IFlowTestOverrideProvider? overrideProvider, IFlowTestInvocationSink? invocationSink)
    {
        _flowTestOverrideProvider = overrideProvider;
        _flowTestInvocationSink = invocationSink;
    }

    internal void SetFlowNameHint(string flowName)
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        Volatile.Write(ref _flowNameHint, flowName);
    }

    internal bool TryGetFlowNameHint(out string flowName)
    {
        var value = Volatile.Read(ref _flowNameHint);

        if (!string.IsNullOrEmpty(value))
        {
            flowName = value;
            return true;
        }

        flowName = string.Empty;
        return false;
    }

    internal void ConfigureFlowBinding(string flowName, Type? paramsType, Type? patchType, object? defaultParams)
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        if (paramsType is null)
        {
            throw new ArgumentNullException(nameof(paramsType));
        }

        if (patchType is null)
        {
            throw new ArgumentNullException(nameof(patchType));
        }

        if (defaultParams is null)
        {
            throw new ArgumentNullException(nameof(defaultParams));
        }

        _currentFlowName = flowName;
        _flowParamsType = paramsType;
        _flowParamsPatchType = patchType;
        _flowDefaultParams = defaultParams;
        Volatile.Write(ref _paramsCacheState, 0);
    }

    public bool TryGetStageFanoutSnapshot(string stageName, out StageFanoutSnapshot snapshot)
    {
        if (string.IsNullOrEmpty(stageName))
        {
            throw new ArgumentException("StageName must be non-empty.", nameof(stageName));
        }

        lock (_stageFanoutGate)
        {
            var snapshots = _stageFanoutSnapshots;
            if (snapshots is not null
                && snapshots.TryGetValue(stageName, out var found)
                && found is not null)
            {
                snapshot = found;
                return true;
            }
        }

        snapshot = null!;
        return false;
    }

    internal void RecordStageFanoutSnapshot(string stageName, string[] enabledModuleIds, StageFanoutSkippedModule[] skippedModules)
    {
        if (string.IsNullOrEmpty(stageName))
        {
            throw new ArgumentException("StageName must be non-empty.", nameof(stageName));
        }

        if (enabledModuleIds is null)
        {
            throw new ArgumentNullException(nameof(enabledModuleIds));
        }

        if (skippedModules is null)
        {
            throw new ArgumentNullException(nameof(skippedModules));
        }

        var snapshot = new StageFanoutSnapshot(stageName, enabledModuleIds, skippedModules);

        lock (_stageFanoutGate)
        {
            _stageFanoutSnapshots ??= new Dictionary<string, StageFanoutSnapshot>(capacity: 4);
            _stageFanoutSnapshots[stageName] = snapshot;
        }
    }

    public TParams Params<TParams>()
        where TParams : notnull
    {
        var flowName = _currentFlowName;
        if (string.IsNullOrEmpty(flowName))
        {
            throw new InvalidOperationException("FlowContext is not configured with a flow binding.");
        }

        var boundType = _flowParamsType;
        if (boundType is null)
        {
            throw new InvalidOperationException("FlowContext is not configured with a params type.");
        }

        if (boundType != typeof(TParams))
        {
            throw new InvalidOperationException(
                $"FlowContext params type is '{boundType}', not '{typeof(TParams)}'.");
        }

        var configVersion = 0UL;
        var patchJson = string.Empty;

        if (TryGetConfigSnapshot(out var snapshot))
        {
            configVersion = snapshot.ConfigVersion;
            patchJson = snapshot.PatchJson;
        }

        if (Volatile.Read(ref _paramsCacheState) == 1
            && _paramsCacheConfigVersion == configVersion
            && string.Equals(_paramsCacheFlowName, flowName, StringComparison.Ordinal)
            && _paramsCacheType == typeof(TParams))
        {
            return (TParams)_paramsCacheValue!;
        }

        lock (_paramsGate)
        {
            if (_paramsCacheState == 1
                && _paramsCacheConfigVersion == configVersion
                && string.Equals(_paramsCacheFlowName, flowName, StringComparison.Ordinal)
                && _paramsCacheType == typeof(TParams))
            {
                return (TParams)_paramsCacheValue!;
            }

            var computed = ComputeParams<TParams>(flowName, patchJson, configVersion);

            _paramsCacheFlowName = flowName;
            _paramsCacheType = typeof(TParams);
            _paramsCacheConfigVersion = configVersion;
            _paramsCacheValue = computed;
            Volatile.Write(ref _paramsCacheState, 1);
            return computed;
        }
    }

    public bool TryGetParamsExplain(out ParamsExplain explain)
    {
        var collector = _execExplainCollector;

        if (collector is null || collector.Level == ExplainLevel.Minimal)
        {
            explain = default;
            return false;
        }

        var flowName = _currentFlowName;
        var paramsType = _flowParamsType;
        var defaultParams = _flowDefaultParams;

        if (string.IsNullOrEmpty(flowName) || paramsType is null || defaultParams is null)
        {
            explain = default;
            return false;
        }

        var configVersion = 0UL;
        var patchJson = string.Empty;

        if (TryGetConfigSnapshot(out var snapshot))
        {
            configVersion = snapshot.ConfigVersion;
            patchJson = snapshot.PatchJson;
        }

        var defaultParamsJson = JsonSerializer.SerializeToUtf8Bytes(defaultParams, paramsType);

        using var defaultDocument = JsonDocument.Parse(defaultParamsJson);
        var defaultElement = defaultDocument.RootElement;

        if (defaultElement.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException($"Default params JSON must be an object. Flow: '{flowName}'.");
        }

        var variants = _variants;
        var qosTier = _qosSelectedTier;

        if (TryGetParamsExplainFromActiveEvaluation(flowName, configVersion, defaultElement, variants, qosTier, collector, out explain))
        {
            return true;
        }

        return TryGetParamsExplainFromPatchJson(flowName, patchJson, defaultElement, variants, qosTier, collector, out explain);
    }

    private bool TryGetParamsExplainFromActiveEvaluation(
        string flowName,
        ulong configVersion,
        JsonElement defaultElement,
        IReadOnlyDictionary<string, string> variants,
        QosTier qosTier,
        ExecExplainCollectorV1 collector,
        out ParamsExplain explain)
    {
        var evaluation = Volatile.Read(ref _activePatchEvaluation);

        if (evaluation is null
            || Volatile.Read(ref _activePatchEvaluationConfigVersion) != configVersion
            || !string.Equals(evaluation.FlowName, flowName, StringComparison.Ordinal))
        {
            explain = default;
            return false;
        }

        if (!evaluation.TryGetFlowPatch(out var flowPatch) || flowPatch.ValueKind != JsonValueKind.Object)
        {
            return TryGetParamsExplainFromFlowPatch(defaultElement, flowPatch: default, variants, qosTier, collector, out explain);
        }

        return TryGetParamsExplainFromFlowPatch(defaultElement, flowPatch, variants, qosTier, collector, out explain);
    }

    private static bool TryGetParamsExplainFromPatchJson(
        string flowName,
        string patchJson,
        JsonElement defaultElement,
        IReadOnlyDictionary<string, string> variants,
        QosTier qosTier,
        ExecExplainCollectorV1 collector,
        out ParamsExplain explain)
    {
        if (string.IsNullOrEmpty(patchJson))
        {
            return TryGetParamsExplainFromFlowPatch(defaultElement, flowPatch: default, variants, qosTier, collector, out explain);
        }

        using var patchDocument = JsonDocument.Parse(patchJson);
        var root = patchDocument.RootElement;

        if (root.ValueKind != JsonValueKind.Object
            || !root.TryGetProperty("schemaVersion", out var schemaVersion)
            || schemaVersion.ValueKind != JsonValueKind.String
            || !schemaVersion.ValueEquals("v1")
            || !root.TryGetProperty("flows", out var flowsElement)
            || flowsElement.ValueKind != JsonValueKind.Object
            || !flowsElement.TryGetProperty(flowName, out var flowPatch)
            || flowPatch.ValueKind != JsonValueKind.Object)
        {
            return TryGetParamsExplainFromFlowPatch(defaultElement, flowPatch: default, variants, qosTier, collector, out explain);
        }

        return TryGetParamsExplainFromFlowPatch(defaultElement, flowPatch, variants, qosTier, collector, out explain);
    }

    private static bool TryGetParamsExplainFromFlowPatch(
        JsonElement defaultElement,
        JsonElement flowPatch,
        IReadOnlyDictionary<string, string> variants,
        QosTier qosTier,
        ExecExplainCollectorV1 collector,
        out ParamsExplain explain)
    {
        if (collector.Level == ExplainLevel.Standard)
        {
            if (!FlowParamsResolver.TryComputeParamsHash(defaultElement, flowPatch, variants, qosTier, out var hash))
            {
                explain = default;
                return false;
            }

            explain = new ParamsExplain(paramsHash: hash.ToString("X16"), effectiveJsonUtf8: null, sources: null);
            return true;
        }

        if (collector.Level == ExplainLevel.Full)
        {
            if (!FlowParamsResolver.TryComputeExplainFull(defaultElement, flowPatch, variants, qosTier, out var effectiveJsonUtf8, out var sources, out var hash))
            {
                explain = default;
                return false;
            }

            var redacted = ExplainRedactor.RedactParamsEffective(effectiveJsonUtf8.AsMemory(), collector.Policy);
            explain = new ParamsExplain(paramsHash: hash.ToString("X16"), redacted, sources);
            return true;
        }

        explain = default;
        return false;
    }

    private TParams ComputeParams<TParams>(string flowName, string patchJson, ulong configVersion)
        where TParams : notnull
    {
        var evaluation = Volatile.Read(ref _activePatchEvaluation);

        if (evaluation is not null
            && Volatile.Read(ref _activePatchEvaluationConfigVersion) == configVersion
            && string.Equals(evaluation.FlowName, flowName, StringComparison.Ordinal))
        {
            if (!evaluation.TryGetFlowPatch(out var flowPatch))
            {
                return ComputeParams<TParams>(flowName, patchJson: string.Empty);
            }

            return ComputeParams<TParams>(flowName, flowPatch);
        }

        return ComputeParams<TParams>(flowName, patchJson);
    }

    private TParams ComputeParams<TParams>(string flowName, string patchJson)
        where TParams : notnull
    {
        var defaultParams = (TParams)_flowDefaultParams!;

        var defaultParamsJson = JsonSerializer.SerializeToUtf8Bytes(defaultParams);

        using var defaultDocument = JsonDocument.Parse(defaultParamsJson);
        var baseElement = defaultDocument.RootElement;

        if (baseElement.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException($"Default params JSON must be an object. Flow: '{flowName}'.");
        }

        if (string.IsNullOrEmpty(patchJson))
        {
            var cloned = JsonSerializer.Deserialize<TParams>(defaultParamsJson);
            if (!typeof(TParams).IsValueType && cloned is null)
            {
                throw new InvalidOperationException($"Default params binding produced null. Flow: '{flowName}'.");
            }

            return cloned!;
        }

        using var patchDocument = JsonDocument.Parse(patchJson);
        var root = patchDocument.RootElement;

        if (root.ValueKind != JsonValueKind.Object)
        {
            return DeserializeDefaultParams<TParams>(defaultParamsJson, flowName);
        }

        if (!root.TryGetProperty("schemaVersion", out var schemaVersion)
            || schemaVersion.ValueKind != JsonValueKind.String
            || !schemaVersion.ValueEquals("v1"))
        {
            return DeserializeDefaultParams<TParams>(defaultParamsJson, flowName);
        }

        if (!root.TryGetProperty("flows", out var flowsElement) || flowsElement.ValueKind != JsonValueKind.Object)
        {
            return DeserializeDefaultParams<TParams>(defaultParamsJson, flowName);
        }

        if (!flowsElement.TryGetProperty(flowName, out var flowPatch) || flowPatch.ValueKind != JsonValueKind.Object)
        {
            return DeserializeDefaultParams<TParams>(defaultParamsJson, flowName);
        }

        return ApplyParamsPatchesAndMerge<TParams>(flowName, defaultParamsJson, baseElement, flowPatch);
    }

    private TParams ComputeParams<TParams>(string flowName, JsonElement flowPatch)
        where TParams : notnull
    {
        var defaultParams = (TParams)_flowDefaultParams!;

        var defaultParamsJson = JsonSerializer.SerializeToUtf8Bytes(defaultParams);

        using var defaultDocument = JsonDocument.Parse(defaultParamsJson);
        var baseElement = defaultDocument.RootElement;

        if (baseElement.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException($"Default params JSON must be an object. Flow: '{flowName}'.");
        }

        if (flowPatch.ValueKind != JsonValueKind.Object)
        {
            return DeserializeDefaultParams<TParams>(defaultParamsJson, flowName);
        }

        return ApplyParamsPatchesAndMerge<TParams>(flowName, defaultParamsJson, baseElement, flowPatch);
    }

    private TParams ApplyParamsPatchesAndMerge<TParams>(
        string flowName,
        byte[] defaultParamsJson,
        JsonElement baseElement,
        JsonElement flowPatch)
        where TParams : notnull
    {
        var buffer = new JsonElementBuffer(initialCapacity: 4);

        if (flowPatch.TryGetProperty("params", out var baseParamsPatch))
        {
            if (baseParamsPatch.ValueKind == JsonValueKind.Object)
            {
                buffer.Add(baseParamsPatch);
            }
            else if (baseParamsPatch.ValueKind != JsonValueKind.Undefined)
            {
                throw new InvalidOperationException($"params must be an object. Flow: '{flowName}'.");
            }
        }

        var variants = _variants;
        if (variants.Count != 0
            && flowPatch.TryGetProperty("experiments", out var experimentsPatch)
            && experimentsPatch.ValueKind == JsonValueKind.Array)
        {
            CollectExperimentParamsPatches(flowName, experimentsPatch, variants, ref buffer);
        }

        if (flowPatch.TryGetProperty("qos", out var qosElement)
            && qosElement.ValueKind == JsonValueKind.Object
            && qosElement.TryGetProperty("tiers", out var qosTiersElement)
            && qosTiersElement.ValueKind == JsonValueKind.Object)
        {
            var tierName = GetQosTierPatchKey(_qosSelectedTier);

            if (qosTiersElement.TryGetProperty(tierName, out var qosTierElement)
                && qosTierElement.ValueKind == JsonValueKind.Object
                && qosTierElement.TryGetProperty("patch", out var qosTierPatch)
                && qosTierPatch.ValueKind == JsonValueKind.Object
                && qosTierPatch.TryGetProperty("params", out var qosParamsPatch))
            {
                if (qosParamsPatch.ValueKind == JsonValueKind.Object)
                {
                    buffer.Add(qosParamsPatch);
                }
                else if (qosParamsPatch.ValueKind != JsonValueKind.Undefined)
                {
                    throw new InvalidOperationException($"qos.tiers[].patch.params must be an object. Flow: '{flowName}' tier: '{tierName}'.");
                }
            }
        }

        if (flowPatch.TryGetProperty("emergency", out var emergencyPatch)
            && emergencyPatch.ValueKind == JsonValueKind.Object
            && emergencyPatch.TryGetProperty("patch", out var emergencyPatchBody)
            && emergencyPatchBody.ValueKind == JsonValueKind.Object
            && emergencyPatchBody.TryGetProperty("params", out var emergencyParamsPatch))
        {
            if (emergencyParamsPatch.ValueKind == JsonValueKind.Object)
            {
                buffer.Add(emergencyParamsPatch);
            }
            else if (emergencyParamsPatch.ValueKind != JsonValueKind.Undefined)
            {
                throw new InvalidOperationException($"emergency.patch.params must be an object. Flow: '{flowName}'.");
            }
        }

        if (buffer.Count == 0)
        {
            return DeserializeDefaultParams<TParams>(defaultParamsJson, flowName);
        }

        var patchType = _flowParamsPatchType!;

        for (var i = 0; i < buffer.Count; i++)
        {
            var bound = buffer.Items[i].Deserialize(patchType);
            if (!patchType.IsValueType && bound is null)
            {
                throw new InvalidOperationException($"Params patch binding produced null. Flow: '{flowName}'.");
            }
        }

        var mergedJson = JsonMergeV1.Merge(baseElement, buffer.Items, buffer.Count);

        var merged = JsonSerializer.Deserialize<TParams>(mergedJson);
        if (!typeof(TParams).IsValueType && merged is null)
        {
            throw new InvalidOperationException($"Params binding produced null. Flow: '{flowName}'.");
        }

        return merged!;
    }

    private static string GetQosTierPatchKey(QosTier tier)
    {
        return tier switch
        {
            QosTier.Full => "full",
            QosTier.Conserve => "conserve",
            QosTier.Emergency => "emergency",
            QosTier.Fallback => "fallback",
            _ => "full",
        };
    }

    private static TParams DeserializeDefaultParams<TParams>(byte[] defaultParamsJson, string flowName)
        where TParams : notnull
    {
        var cloned = JsonSerializer.Deserialize<TParams>(defaultParamsJson);
        if (!typeof(TParams).IsValueType && cloned is null)
        {
            throw new InvalidOperationException($"Default params binding produced null. Flow: '{flowName}'.");
        }

        return cloned!;
    }

    private static void CollectExperimentParamsPatches(
        string flowName,
        JsonElement experimentsPatch,
        IReadOnlyDictionary<string, string> variants,
        ref JsonElementBuffer buffer)
    {
        foreach (var experimentMapping in experimentsPatch.EnumerateArray())
        {
            if (experimentMapping.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            string? layer = null;
            string? variant = null;
            JsonElement patch = default;
            var hasPatch = false;

            foreach (var field in experimentMapping.EnumerateObject())
            {
                if (field.NameEquals("layer"))
                {
                    layer = field.Value.ValueKind == JsonValueKind.String ? field.Value.GetString() : null;
                    continue;
                }

                if (field.NameEquals("variant"))
                {
                    variant = field.Value.ValueKind == JsonValueKind.String ? field.Value.GetString() : null;
                    continue;
                }

                if (field.NameEquals("patch"))
                {
                    hasPatch = true;
                    patch = field.Value;
                }
            }

            if (string.IsNullOrEmpty(layer) || variant is null || !hasPatch || patch.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            if (!variants.TryGetValue(layer!, out var currentVariant)
                || !string.Equals(currentVariant, variant, StringComparison.Ordinal))
            {
                continue;
            }

            if (!patch.TryGetProperty("params", out var experimentParamsPatch))
            {
                continue;
            }

            if (experimentParamsPatch.ValueKind == JsonValueKind.Object)
            {
                buffer.Add(experimentParamsPatch);
            }
            else if (experimentParamsPatch.ValueKind != JsonValueKind.Undefined)
            {
                throw new InvalidOperationException(
                    $"experiments[].patch.params must be an object. Flow: '{flowName}' layer: '{layer}' variant: '{variant}'.");
            }
        }
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

    internal void SetConfigSnapshotForTesting(ConfigSnapshot snapshot)
    {
        lock (_configSnapshotGate)
        {
            _configSnapshot = snapshot;
            _configSnapshotTask = null;
            Volatile.Write(ref _configSnapshotState, 2);
        }
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

        lock (_stageFanoutGate)
        {
            _stageFanoutSnapshots?.Clear();
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

    private struct JsonElementBuffer
    {
        private JsonElement[] _items;
        private int _count;

        public int Count => _count;

        public JsonElement[] Items => _items;

        public JsonElementBuffer(int initialCapacity)
        {
            _items = initialCapacity <= 0 ? Array.Empty<JsonElement>() : new JsonElement[initialCapacity];
            _count = 0;
        }

        public void Add(JsonElement item)
        {
            if ((uint)_count >= (uint)_items.Length)
            {
                var newSize = _items.Length == 0 ? 4 : _items.Length * 2;
                var newItems = new JsonElement[newSize];
                Array.Copy(_items, 0, newItems, 0, _items.Length);
                _items = newItems;
            }

            _items[_count] = item;
            _count++;
        }
    }
}

