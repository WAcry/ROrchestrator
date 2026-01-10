using System.Text.Json;

namespace ROrchestrator.Core;

public static class PatchEvaluatorV1
{
    private const string SupportedSchemaVersion = "v1";

    private const int ParsedPatchCacheSize = 32;

    private static readonly Lock ParsedPatchCacheGate = new();
    private static readonly CachedPatchDocument?[] ParsedPatchCache = new CachedPatchDocument?[ParsedPatchCacheSize];

    public static FlowPatchEvaluationV1 Evaluate(string flowName, string patchJson, FlowRequestOptions requestOptions, ulong configVersion = 0)
    {
        return Evaluate(flowName, patchJson, requestOptions, qosTier: QosTier.Full, configVersion: configVersion);
    }

    public static FlowPatchEvaluationV1 Evaluate(
        string flowName,
        string patchJson,
        FlowRequestOptions requestOptions,
        QosTier qosTier,
        ulong configVersion = 0)
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        if (patchJson is null)
        {
            throw new ArgumentNullException(nameof(patchJson));
        }

        if (patchJson.Length == 0)
        {
            return FlowPatchEvaluationV1.CreateEmpty(flowName);
        }

        qosTier = NormalizeQosTier(qosTier);

        JsonDocument document;
        CachedPatchDocument? cachedDocument = null;
        var ownsDocument = true;

        if (configVersion == 0)
        {
            document = ParsePatchDocument(patchJson);
        }
        else
        {
            cachedDocument = AcquireCachedPatchDocument(patchJson, configVersion);
            document = cachedDocument.Document;
            ownsDocument = false;
        }

        try
        {
            var root = document.RootElement;

            if (!root.TryGetProperty("flows", out var flowsElement) || flowsElement.ValueKind != JsonValueKind.Object)
            {
                ReleasePatchDocument(document, ownsDocument, cachedDocument);
                return FlowPatchEvaluationV1.CreateEmpty(flowName);
            }

            if (!flowsElement.TryGetProperty(flowName, out var flowPatch) || flowPatch.ValueKind != JsonValueKind.Object)
            {
                ReleasePatchDocument(document, ownsDocument, cachedDocument);
                return FlowPatchEvaluationV1.CreateEmpty(flowName);
            }

            var stageMap = new StageMap();
            var overlays = new List<PatchOverlayAppliedV1>(capacity: 5);
            overlays.Add(PatchOverlayAppliedV1.Base);

            if (flowPatch.TryGetProperty("stages", out var baseStagesPatch) && baseStagesPatch.ValueKind == JsonValueKind.Object)
            {
                ApplyStagesPatch(flowName, baseStagesPatch, ref stageMap);
            }

            var variants = requestOptions.Variants;

            if (variants is not null
                && variants.Count != 0
                && flowPatch.TryGetProperty("experiments", out var experimentsPatch)
                && experimentsPatch.ValueKind == JsonValueKind.Array)
            {
                ApplyExperimentsPatch(flowName, experimentsPatch, variants, overlays, ref stageMap);
            }

            if (TryGetQosTierPatchBody(flowPatch, qosTier, out var qosPatchBody))
            {
                overlays.Add(PatchOverlayAppliedV1.Qos);

                if (qosPatchBody.TryGetProperty("stages", out var qosStagesPatch)
                    && qosStagesPatch.ValueKind == JsonValueKind.Object)
                {
                    ApplyQosStagesPatch(flowName, qosStagesPatch, ref stageMap);
                }
            }

            if (flowPatch.TryGetProperty("emergency", out var emergencyPatch)
                && emergencyPatch.ValueKind == JsonValueKind.Object
                && emergencyPatch.TryGetProperty("patch", out var emergencyPatchBody)
                && emergencyPatchBody.ValueKind == JsonValueKind.Object)
            {
                overlays.Add(PatchOverlayAppliedV1.Emergency);

                if (emergencyPatchBody.TryGetProperty("stages", out var emergencyStagesPatch)
                    && emergencyStagesPatch.ValueKind == JsonValueKind.Object)
                {
                    ApplyEmergencyStagesPatch(emergencyStagesPatch, ref stageMap);
                }
            }

            var stageArray = stageMap.Build();
            var overlayArray = overlays.ToArray();
            return new FlowPatchEvaluationV1(flowName, document, flowPatch, overlayArray, stageArray, ownsDocument, cachedDocument);
        }
        catch
        {
            ReleasePatchDocument(document, ownsDocument, cachedDocument);
            throw;
        }
    }

    private static QosTier NormalizeQosTier(QosTier tier)
    {
        return tier switch
        {
            QosTier.Full => QosTier.Full,
            QosTier.Conserve => QosTier.Conserve,
            QosTier.Emergency => QosTier.Emergency,
            QosTier.Fallback => QosTier.Fallback,
            _ => QosTier.Full,
        };
    }

    private static JsonDocument ParsePatchDocument(string patchJson)
    {
        JsonDocument document;

        try
        {
            document = JsonDocument.Parse(patchJson);
        }
        catch (JsonException ex)
        {
            throw new FormatException("patchJson is not a valid JSON document.", ex);
        }

        try
        {
            var root = document.RootElement;

            if (root.ValueKind != JsonValueKind.Object)
            {
                throw new FormatException("patchJson must be a JSON object.");
            }

            if (!root.TryGetProperty("schemaVersion", out var schemaVersion)
                || schemaVersion.ValueKind != JsonValueKind.String
                || !schemaVersion.ValueEquals(SupportedSchemaVersion))
            {
                throw new FormatException("patchJson schemaVersion is missing or unsupported.");
            }

            return document;
        }
        catch
        {
            document.Dispose();
            throw;
        }
    }

    private static CachedPatchDocument AcquireCachedPatchDocument(string patchJson, ulong configVersion)
    {
        var index = (int)(configVersion & (ParsedPatchCacheSize - 1));

        var entry = Volatile.Read(ref ParsedPatchCache[index]);
        if (entry is not null
            && entry.ConfigVersion == configVersion
            && ReferenceEquals(entry.PatchJson, patchJson)
            && entry.TryAcquireLease())
        {
            return entry;
        }

        lock (ParsedPatchCacheGate)
        {
            entry = ParsedPatchCache[index];
            if (entry is not null
                && entry.ConfigVersion == configVersion
                && ReferenceEquals(entry.PatchJson, patchJson)
                && entry.TryAcquireLease())
            {
                return entry;
            }

            var document = ParsePatchDocument(patchJson);
            var newEntry = new CachedPatchDocument(configVersion, patchJson, document);
            var previous = ParsedPatchCache[index];
            ParsedPatchCache[index] = newEntry;

            previous?.MarkEvicted();

            return newEntry;
        }
    }

    private static void ReleasePatchDocument(JsonDocument document, bool ownsDocument, CachedPatchDocument? cachedDocument)
    {
        if (ownsDocument)
        {
            document.Dispose();
            return;
        }

        cachedDocument?.ReleaseLease();
    }

    internal sealed class CachedPatchDocument
    {
        private readonly string _patchJson;
        private readonly JsonDocument _document;
        private int _leaseCount;
        private int _evicted;
        private int _disposed;

        public ulong ConfigVersion { get; }

        public string PatchJson => _patchJson;

        public JsonDocument Document => _document;

        public CachedPatchDocument(ulong configVersion, string patchJson, JsonDocument document)
        {
            ConfigVersion = configVersion;
            _patchJson = patchJson ?? throw new ArgumentNullException(nameof(patchJson));
            _document = document ?? throw new ArgumentNullException(nameof(document));
            _leaseCount = 1;
            _evicted = 0;
            _disposed = 0;
        }

        public bool TryAcquireLease()
        {
            if (Volatile.Read(ref _evicted) != 0)
            {
                return false;
            }

            Interlocked.Increment(ref _leaseCount);

            if (Volatile.Read(ref _evicted) != 0)
            {
                ReleaseLease();
                return false;
            }

            return true;
        }

        public void ReleaseLease()
        {
            if (Interlocked.Decrement(ref _leaseCount) == 0 && Volatile.Read(ref _evicted) != 0)
            {
                DisposeOnce();
            }
        }

        public void MarkEvicted()
        {
            Volatile.Write(ref _evicted, 1);

            if (Volatile.Read(ref _leaseCount) == 0)
            {
                DisposeOnce();
            }
        }

        private void DisposeOnce()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _document.Dispose();
        }
    }

    private static void ApplyStagesPatch(string flowName, JsonElement stagesPatch, ref StageMap stageMap)
    {
        foreach (var stageProperty in stagesPatch.EnumerateObject())
        {
            if (stageProperty.Value.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            var stageName = stageProperty.Name;
            var stageBuilder = stageMap.GetOrAdd(stageName);
            stageBuilder.ApplyFullStagePatch(flowName, stageName, stageProperty.Value);
        }
    }

    private static void ApplyExperimentsPatch(
        string flowName,
        JsonElement experimentsPatch,
        IReadOnlyDictionary<string, string> variants,
        List<PatchOverlayAppliedV1> overlays,
        ref StageMap stageMap)
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
                    continue;
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

            overlays.Add(new PatchOverlayAppliedV1(layer: "experiment", experimentLayer: layer, experimentVariant: variant));

            if (!patch.TryGetProperty("stages", out var experimentStagesPatch)
                || experimentStagesPatch.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            foreach (var stageProperty in experimentStagesPatch.EnumerateObject())
            {
                if (stageProperty.Value.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                var stageName = stageProperty.Name;
                var stageBuilder = stageMap.GetOrAdd(stageName);
                stageBuilder.ApplyFullStagePatch(flowName, stageName, stageProperty.Value);
            }
        }
    }

    private static void ApplyEmergencyStagesPatch(JsonElement emergencyStagesPatch, ref StageMap stageMap)
    {
        foreach (var stageProperty in emergencyStagesPatch.EnumerateObject())
        {
            if (stageProperty.Value.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            var stageName = stageProperty.Name;
            var stageBuilder = stageMap.GetOrAdd(stageName);
            stageBuilder.ApplyEmergencyStagePatch(stageName, stageProperty.Value);
        }
    }

    private static void ApplyQosStagesPatch(string flowName, JsonElement qosStagesPatch, ref StageMap stageMap)
    {
        foreach (var stageProperty in qosStagesPatch.EnumerateObject())
        {
            if (stageProperty.Value.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            var stageName = stageProperty.Name;
            var stageBuilder = stageMap.GetOrAdd(stageName);
            stageBuilder.ApplyQosStagePatch(flowName, stageName, stageProperty.Value);
        }
    }

    private static bool TryGetQosTierPatchBody(JsonElement flowPatch, QosTier qosTier, out JsonElement qosPatchBody)
    {
        qosPatchBody = default;

        if (!flowPatch.TryGetProperty("qos", out var qosElement) || qosElement.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        if (!qosElement.TryGetProperty("tiers", out var tiersElement) || tiersElement.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        var tierName = GetQosTierPatchKey(qosTier);

        if (!tiersElement.TryGetProperty(tierName, out var tierElement) || tierElement.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        if (!tierElement.TryGetProperty("patch", out var patchElement) || patchElement.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        qosPatchBody = patchElement;
        return true;
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

    public readonly struct PatchOverlayAppliedV1
    {
        public static readonly PatchOverlayAppliedV1 Base = new(layer: "base", experimentLayer: null, experimentVariant: null);

        public static readonly PatchOverlayAppliedV1 Qos = new(layer: "qos", experimentLayer: null, experimentVariant: null);

        public static readonly PatchOverlayAppliedV1 Emergency = new(layer: "emergency", experimentLayer: null, experimentVariant: null);

        public string Layer { get; }

        public string? ExperimentLayer { get; }

        public string? ExperimentVariant { get; }

        public PatchOverlayAppliedV1(string layer, string? experimentLayer, string? experimentVariant)
        {
            Layer = layer;
            ExperimentLayer = experimentLayer;
            ExperimentVariant = experimentVariant;
        }
    }

    public readonly struct StagePatchV1
    {
        private readonly StageModulePatchV1[] _modules;
        private readonly StageModulePatchV1[] _shadowModules;

        public string StageName { get; }

        public bool HasFanoutMax { get; }

        public int FanoutMax { get; }

        public IReadOnlyList<StageModulePatchV1> Modules => _modules;

        public IReadOnlyList<StageModulePatchV1> ShadowModules => _shadowModules;

        internal StagePatchV1(
            string stageName,
            bool hasFanoutMax,
            int fanoutMax,
            StageModulePatchV1[] modules,
            StageModulePatchV1[] shadowModules)
        {
            StageName = stageName;
            HasFanoutMax = hasFanoutMax;
            FanoutMax = fanoutMax;
            _modules = modules;
            _shadowModules = shadowModules;
        }
    }

    public readonly struct StageModulePatchV1
    {
        public string ModuleId { get; }

        public string ModuleType { get; }

        public string? LimitKey { get; }

        public string? MemoKey { get; }

        public JsonElement Args { get; }

        public bool Enabled { get; }

        public int Priority { get; }

        public bool HasGate { get; }

        public JsonElement Gate { get; }

        public bool DisabledByEmergency { get; }

        public bool IsShadow { get; }

        public ushort ShadowSampleBps { get; }

        internal StageModulePatchV1(
            string moduleId,
            string moduleType,
            string? limitKey,
            string? memoKey,
            JsonElement args,
            bool enabled,
            int priority,
            bool hasGate,
            JsonElement gate,
            bool disabledByEmergency,
            bool isShadow,
            ushort shadowSampleBps)
        {
            ModuleId = moduleId;
            ModuleType = moduleType;
            LimitKey = limitKey;
            MemoKey = memoKey;
            Args = args;
            Enabled = enabled;
            Priority = priority;
            HasGate = hasGate;
            Gate = gate;
            DisabledByEmergency = disabledByEmergency;
            IsShadow = isShadow;
            ShadowSampleBps = shadowSampleBps;
        }
    }

    public sealed class FlowPatchEvaluationV1 : IDisposable
    {
        private static readonly PatchOverlayAppliedV1[] EmptyOverlayArray = Array.Empty<PatchOverlayAppliedV1>();
        private static readonly StagePatchV1[] EmptyStageArray = Array.Empty<StagePatchV1>();

        private readonly JsonDocument? _document;
        private readonly JsonElement _flowPatch;
        private readonly PatchOverlayAppliedV1[] _overlaysApplied;
        private readonly StagePatchV1[] _stages;
        private readonly bool _ownsDocument;
        private CachedPatchDocument? _cachedDocument;

        public string FlowName { get; }

        public IReadOnlyList<PatchOverlayAppliedV1> OverlaysApplied => _overlaysApplied;

        public IReadOnlyList<StagePatchV1> Stages => _stages;

        internal JsonDocument GetDocumentForTesting()
        {
            if (_document is null)
            {
                throw new InvalidOperationException("No patch document is associated with this evaluation.");
            }

            return _document;
        }

        internal FlowPatchEvaluationV1(
            string flowName,
            JsonDocument document,
            JsonElement flowPatch,
            PatchOverlayAppliedV1[] overlaysApplied,
            StagePatchV1[] stages,
            bool ownsDocument,
            CachedPatchDocument? cachedDocument)
        {
            FlowName = flowName;
            _document = document;
            _flowPatch = flowPatch;
            _overlaysApplied = overlaysApplied;
            _stages = stages;
            _ownsDocument = ownsDocument;
            _cachedDocument = cachedDocument;
        }

        private FlowPatchEvaluationV1(string flowName, PatchOverlayAppliedV1[] overlaysApplied, StagePatchV1[] stages)
        {
            FlowName = flowName;
            _document = null;
            _flowPatch = default;
            _overlaysApplied = overlaysApplied;
            _stages = stages;
            _ownsDocument = false;
            _cachedDocument = null;
        }

        internal static FlowPatchEvaluationV1 CreateEmpty(string flowName)
        {
            return new FlowPatchEvaluationV1(flowName, EmptyOverlayArray, EmptyStageArray);
        }

        public void Dispose()
        {
            if (_ownsDocument)
            {
                _document?.Dispose();
                return;
            }

            var cachedDocument = Interlocked.Exchange(ref _cachedDocument, null);
            cachedDocument?.ReleaseLease();
        }

        internal bool TryGetFlowPatch(out JsonElement flowPatch)
        {
            if (_document is null || _flowPatch.ValueKind != JsonValueKind.Object)
            {
                flowPatch = default;
                return false;
            }

            flowPatch = _flowPatch;
            return true;
        }
    }

    private sealed class StageBuilder
    {
        private readonly List<ModuleBuilder> _modules;
        private Dictionary<string, int>? _moduleIndex;

        public string StageName { get; }

        public bool HasFanoutMax { get; private set; }

        public int FanoutMax { get; private set; }

        public StageBuilder(string stageName)
        {
            StageName = stageName;
            _modules = new List<ModuleBuilder>(capacity: 4);
        }

        public void ApplyFullStagePatch(string flowName, string stageName, JsonElement stagePatch)
        {
            foreach (var stageField in stagePatch.EnumerateObject())
            {
                if (stageField.NameEquals("fanoutMax"))
                {
                    if (TryGetNonNegativeInt32(stageField.Value, out var fanoutMax))
                    {
                        HasFanoutMax = true;
                        FanoutMax = fanoutMax;
                    }

                    continue;
                }

                if (stageField.NameEquals("modules"))
                {
                    if (stageField.Value.ValueKind == JsonValueKind.Array)
                    {
                        ApplyFullModulesPatch(flowName, stageName, stageField.Value);
                    }

                    continue;
                }
            }
        }

        private void ApplyFullModulesPatch(string flowName, string stageName, JsonElement modulesPatch)
        {
            foreach (var moduleElement in modulesPatch.EnumerateArray())
            {
                if (moduleElement.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                if (!TryParseModuleId(moduleElement, out var moduleId))
                {
                    continue;
                }

                if (!_TryGetOrAddModule(moduleId!, out var moduleBuilder))
                {
                    throw new InvalidOperationException(
                        $"Unexpected failure to allocate module '{moduleId}' for flow '{flowName}' stage '{stageName}'.");
                }

                moduleBuilder.ApplyFullModulePatch(flowName, stageName, moduleElement);
            }
        }

        public void ApplyEmergencyStagePatch(string stageName, JsonElement stagePatch)
        {
            foreach (var stageField in stagePatch.EnumerateObject())
            {
                if (stageField.NameEquals("fanoutMax"))
                {
                    if (TryGetNonNegativeInt32(stageField.Value, out var fanoutMax))
                    {
                        HasFanoutMax = true;
                        FanoutMax = fanoutMax;
                    }

                    continue;
                }

                if (stageField.NameEquals("modules"))
                {
                    if (stageField.Value.ValueKind == JsonValueKind.Array)
                    {
                        ApplyEmergencyModulesPatch(stageName, stageField.Value);
                    }

                    continue;
                }
            }
        }

        private void ApplyEmergencyModulesPatch(string stageName, JsonElement modulesPatch)
        {
            foreach (var moduleElement in modulesPatch.EnumerateArray())
            {
                if (moduleElement.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                if (!TryParseModuleId(moduleElement, out var moduleId))
                {
                    continue;
                }

                var index = TryGetModuleIndex(moduleId!);
                if (index < 0)
                {
                    continue;
                }

                _modules[index].ApplyEmergencyDisable();
            }
        }

        public void ApplyQosStagePatch(string flowName, string stageName, JsonElement stagePatch)
        {
            foreach (var stageField in stagePatch.EnumerateObject())
            {
                if (stageField.NameEquals("fanoutMax"))
                {
                    if (TryGetNonNegativeInt32(stageField.Value, out var fanoutMax))
                    {
                        HasFanoutMax = true;
                        FanoutMax = fanoutMax;
                    }

                    continue;
                }

                if (stageField.NameEquals("modules"))
                {
                    if (stageField.Value.ValueKind == JsonValueKind.Array)
                    {
                        ApplyQosModulesPatch(flowName, stageName, stageField.Value);
                    }

                    continue;
                }
            }
        }

        private void ApplyQosModulesPatch(string flowName, string stageName, JsonElement modulesPatch)
        {
            foreach (var moduleElement in modulesPatch.EnumerateArray())
            {
                if (moduleElement.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                if (!TryParseModuleId(moduleElement, out var moduleId))
                {
                    continue;
                }

                var index = TryGetModuleIndex(moduleId!);
                if (index < 0)
                {
                    continue;
                }

                _modules[index].ApplyQosModulePatch(flowName, stageName, moduleElement);
            }
        }

        private int TryGetModuleIndex(string moduleId)
        {
            if (_moduleIndex is null)
            {
                for (var i = 0; i < _modules.Count; i++)
                {
                    if (string.Equals(_modules[i].ModuleId, moduleId, StringComparison.Ordinal))
                    {
                        return i;
                    }
                }

                return -1;
            }

            return _moduleIndex.TryGetValue(moduleId, out var index) ? index : -1;
        }

        private bool _TryGetOrAddModule(string moduleId, out ModuleBuilder module)
        {
            module = null!;

            if (_moduleIndex is not null)
            {
                if (_moduleIndex.TryGetValue(moduleId, out var index))
                {
                    module = _modules[index];
                    return true;
                }
            }
            else
            {
                for (var i = 0; i < _modules.Count; i++)
                {
                    if (string.Equals(_modules[i].ModuleId, moduleId, StringComparison.Ordinal))
                    {
                        module = _modules[i];
                        return true;
                    }
                }
            }

            var builder = new ModuleBuilder(moduleId);
            var newIndex = _modules.Count;
            _modules.Add(builder);

            if (_moduleIndex is null && _modules.Count > 8)
            {
                _moduleIndex = new Dictionary<string, int>(capacity: _modules.Count);
                for (var i = 0; i < _modules.Count; i++)
                {
                    _moduleIndex.Add(_modules[i].ModuleId, i);
                }
            }
            else if (_moduleIndex is not null)
            {
                _moduleIndex.Add(moduleId, newIndex);
            }

            module = builder;
            return true;
        }

        public StagePatchV1 Build()
        {
            StageModulePatchV1[] modules;
            StageModulePatchV1[] shadowModules;

            if (_modules.Count == 0)
            {
                modules = Array.Empty<StageModulePatchV1>();
                shadowModules = Array.Empty<StageModulePatchV1>();
            }
            else
            {
                var primaryCount = 0;
                var shadowCount = 0;

                for (var i = 0; i < _modules.Count; i++)
                {
                    if (_modules[i].IsShadow)
                    {
                        shadowCount++;
                        continue;
                    }

                    primaryCount++;
                }

                modules = primaryCount == 0 ? Array.Empty<StageModulePatchV1>() : new StageModulePatchV1[primaryCount];
                shadowModules = shadowCount == 0 ? Array.Empty<StageModulePatchV1>() : new StageModulePatchV1[shadowCount];

                var moduleIndex = 0;
                var shadowIndex = 0;

                for (var i = 0; i < _modules.Count; i++)
                {
                    var built = _modules[i].Build();

                    if (built.IsShadow)
                    {
                        shadowModules[shadowIndex] = built;
                        shadowIndex++;
                        continue;
                    }

                    modules[moduleIndex] = built;
                    moduleIndex++;
                }

                if (moduleIndex != modules.Length || shadowIndex != shadowModules.Length)
                {
                    throw new InvalidOperationException("Stage builder produced inconsistent module counts.");
                }
            }

            return new StagePatchV1(StageName, HasFanoutMax, FanoutMax, modules, shadowModules);
        }
    }

    private sealed class ModuleBuilder
    {
        private JsonElement _args;
        private JsonElement _gate;

        public string ModuleId { get; }

        public string ModuleType { get; private set; }

        public bool Enabled { get; private set; }

        public int Priority { get; private set; }

        public bool HasGate { get; private set; }

        public bool DisabledByEmergency { get; private set; }

        public bool IsShadow { get; private set; }

        public ushort ShadowSampleBps { get; private set; }

        public string? LimitKey { get; private set; }

        public string? MemoKey { get; private set; }

        public ModuleBuilder(string moduleId)
        {
            ModuleId = moduleId;
            ModuleType = string.Empty;
            Enabled = true;
            Priority = 0;
            HasGate = false;
            DisabledByEmergency = false;
            IsShadow = false;
            ShadowSampleBps = 0;
            LimitKey = null;
            MemoKey = null;
        }

        public void ApplyFullModulePatch(string flowName, string stageName, JsonElement modulePatch)
        {
            string? use = null;
            var hasWith = false;
            JsonElement withElement = default;
            var hasEnabled = false;
            var enabled = true;
            var hasPriority = false;
            var priority = 0;
            var hasGate = false;
            JsonElement gate = default;
            var hasShadow = false;
            ushort shadowSampleBps = 0;
            var hasLimitKey = false;
            string? limitKey = null;
            var hasMemoKey = false;
            string? memoKey = null;

            foreach (var moduleField in modulePatch.EnumerateObject())
            {
                if (moduleField.NameEquals("use"))
                {
                    use = moduleField.Value.ValueKind == JsonValueKind.String ? moduleField.Value.GetString() : null;
                    continue;
                }

                if (moduleField.NameEquals("with"))
                {
                    hasWith = true;
                    withElement = moduleField.Value;
                    continue;
                }

                if (moduleField.NameEquals("enabled"))
                {
                    hasEnabled = true;
                    if (moduleField.Value.ValueKind == JsonValueKind.True || moduleField.Value.ValueKind == JsonValueKind.False)
                    {
                        enabled = moduleField.Value.GetBoolean();
                    }
                    continue;
                }

                if (moduleField.NameEquals("priority"))
                {
                    hasPriority = true;
                    if (TryGetInt32(moduleField.Value, out var parsed))
                    {
                        priority = parsed;
                    }
                    continue;
                }

                if (moduleField.NameEquals("gate"))
                {
                    hasGate = true;
                    gate = moduleField.Value;
                    continue;
                }

                if (moduleField.NameEquals("shadow"))
                {
                    hasShadow = true;
                    shadowSampleBps = ParseShadowSampleBps(moduleField.Value, flowName, stageName, ModuleId);
                    continue;
                }

                if (moduleField.NameEquals("limitKey"))
                {
                    hasLimitKey = true;
                    limitKey = moduleField.Value.ValueKind == JsonValueKind.String ? moduleField.Value.GetString() : null;
                    continue;
                }

                if (moduleField.NameEquals("memoKey"))
                {
                    hasMemoKey = true;
                    memoKey = moduleField.Value.ValueKind == JsonValueKind.String ? moduleField.Value.GetString() : null;
                    continue;
                }
            }

            if (string.IsNullOrEmpty(use))
            {
                throw new InvalidOperationException(
                    $"modules[].use is required for module '{ModuleId}' in flow '{flowName}' stage '{stageName}'.");
            }

            if (!hasWith || withElement.ValueKind == JsonValueKind.Null || withElement.ValueKind == JsonValueKind.Undefined)
            {
                throw new InvalidOperationException(
                    $"modules[].with is required for module '{ModuleId}' in flow '{flowName}' stage '{stageName}'.");
            }

            ModuleType = use!;
            _args = withElement;

            if (hasEnabled)
            {
                Enabled = enabled;
            }

            if (hasPriority)
            {
                Priority = priority;
            }

            if (hasGate)
            {
                HasGate = true;
                _gate = gate;
            }

            if (hasShadow)
            {
                IsShadow = true;
                ShadowSampleBps = shadowSampleBps;
            }

            if (hasLimitKey)
            {
                LimitKey = string.IsNullOrEmpty(limitKey) ? null : limitKey;
            }

            if (hasMemoKey)
            {
                MemoKey = string.IsNullOrEmpty(memoKey) ? null : memoKey;
            }
        }

        public void ApplyEmergencyDisable()
        {
            Enabled = false;
            DisabledByEmergency = true;
        }

        public void ApplyQosModulePatch(string flowName, string stageName, JsonElement modulePatch)
        {
            var hasEnabled = false;
            var enabled = true;

            var hasShadow = false;
            ushort shadowSampleBps = 0;

            foreach (var moduleField in modulePatch.EnumerateObject())
            {
                if (moduleField.NameEquals("enabled"))
                {
                    hasEnabled = true;
                    if (moduleField.Value.ValueKind == JsonValueKind.True || moduleField.Value.ValueKind == JsonValueKind.False)
                    {
                        enabled = moduleField.Value.GetBoolean();
                    }

                    continue;
                }

                if (moduleField.NameEquals("shadow"))
                {
                    hasShadow = true;
                    shadowSampleBps = ParseShadowSampleBps(moduleField.Value, flowName, stageName, ModuleId);
                    continue;
                }
            }

            if (hasEnabled)
            {
                Enabled = enabled;
            }

            if (hasShadow)
            {
                IsShadow = true;
                ShadowSampleBps = shadowSampleBps;
            }
        }

        public StageModulePatchV1 Build()
        {
            return new StageModulePatchV1(
                ModuleId,
                ModuleType,
                LimitKey,
                MemoKey,
                _args,
                Enabled,
                Priority,
                HasGate,
                _gate,
                DisabledByEmergency,
                IsShadow,
                ShadowSampleBps);
        }

        private static ushort ParseShadowSampleBps(JsonElement shadowElement, string flowName, string stageName, string moduleId)
        {
            if (shadowElement.ValueKind != JsonValueKind.Object)
            {
                throw new InvalidOperationException(
                    $"modules[].shadow must be an object for module '{moduleId}' in flow '{flowName}' stage '{stageName}'.");
            }

            var hasSample = false;
            double sample = 0;

            foreach (var property in shadowElement.EnumerateObject())
            {
                if (property.NameEquals("sample"))
                {
                    hasSample = true;

                    if (property.Value.ValueKind != JsonValueKind.Number || !property.Value.TryGetDouble(out sample))
                    {
                        throw new InvalidOperationException(
                            $"modules[].shadow.sample must be a number for module '{moduleId}' in flow '{flowName}' stage '{stageName}'.");
                    }

                    continue;
                }

                throw new InvalidOperationException(
                    $"Unknown field: {property.Name} in modules[].shadow for module '{moduleId}' in flow '{flowName}' stage '{stageName}'.");
            }

            if (!hasSample)
            {
                throw new InvalidOperationException(
                    $"modules[].shadow.sample is required for module '{moduleId}' in flow '{flowName}' stage '{stageName}'.");
            }

            if (sample < 0 || sample > 1)
            {
                throw new InvalidOperationException(
                    $"modules[].shadow.sample must be within range 0..1 for module '{moduleId}' in flow '{flowName}' stage '{stageName}'.");
            }

            if (sample == 0)
            {
                return 0;
            }

            if (sample == 1)
            {
                return 10000;
            }

            var bps = (int)Math.Round(sample * 10000.0, MidpointRounding.AwayFromZero);
            if ((uint)bps > 10000)
            {
                throw new InvalidOperationException(
                    $"modules[].shadow.sample must be within range 0..1 for module '{moduleId}' in flow '{flowName}' stage '{stageName}'.");
            }

            return (ushort)bps;
        }
    }

    private struct StageMap
    {
        private Dictionary<string, int>? _index;
        private List<StageBuilder>? _stages;

        public StageBuilder GetOrAdd(string stageName)
        {
            _stages ??= new List<StageBuilder>(capacity: 4);

            if (_index is not null)
            {
                if (_index.TryGetValue(stageName, out var index))
                {
                    return _stages[index];
                }
            }
            else
            {
                for (var i = 0; i < _stages.Count; i++)
                {
                    if (string.Equals(_stages[i].StageName, stageName, StringComparison.Ordinal))
                    {
                        return _stages[i];
                    }
                }
            }

            var builder = new StageBuilder(stageName);
            var newIndex = _stages.Count;
            _stages.Add(builder);

            if (_index is null && _stages.Count > 8)
            {
                _index = new Dictionary<string, int>(capacity: _stages.Count);
                for (var i = 0; i < _stages.Count; i++)
                {
                    _index.Add(_stages[i].StageName, i);
                }
            }
            else if (_index is not null)
            {
                _index.Add(stageName, newIndex);
            }

            return builder;
        }

        public StagePatchV1[] Build()
        {
            if (_stages is null || _stages.Count == 0)
            {
                return Array.Empty<StagePatchV1>();
            }

            var result = new StagePatchV1[_stages.Count];

            for (var i = 0; i < _stages.Count; i++)
            {
                result[i] = _stages[i].Build();
            }

            return result;
        }
    }

    private static bool TryParseModuleId(JsonElement modulePatch, out string? moduleId)
    {
        moduleId = null;

        if (!modulePatch.TryGetProperty("id", out var idElement) || idElement.ValueKind != JsonValueKind.String)
        {
            return false;
        }

        moduleId = idElement.GetString();
        return !string.IsNullOrEmpty(moduleId);
    }

    private static bool TryGetInt32(JsonElement element, out int value)
    {
        value = 0;

        if (element.ValueKind != JsonValueKind.Number)
        {
            return false;
        }

        if (element.TryGetInt32(out value))
        {
            return true;
        }

        if (element.TryGetInt64(out var value64))
        {
            if (value64 < int.MinValue || value64 > int.MaxValue)
            {
                return false;
            }

            value = (int)value64;
            return true;
        }

        return false;
    }

    private static bool TryGetNonNegativeInt32(JsonElement element, out int value)
    {
        value = 0;

        if (!TryGetInt32(element, out value))
        {
            return false;
        }

        return value >= 0;
    }
}
