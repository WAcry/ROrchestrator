using System.Text.Json;

namespace ROrchestrator.Core;

public static class PatchEvaluatorV1
{
    private const string SupportedSchemaVersion = "v1";

    public static FlowPatchEvaluationV1 Evaluate(string flowName, string patchJson, FlowRequestOptions requestOptions)
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

            if (!root.TryGetProperty("flows", out var flowsElement) || flowsElement.ValueKind != JsonValueKind.Object)
            {
                document.Dispose();
                return FlowPatchEvaluationV1.CreateEmpty(flowName);
            }

            if (!flowsElement.TryGetProperty(flowName, out var flowPatch) || flowPatch.ValueKind != JsonValueKind.Object)
            {
                document.Dispose();
                return FlowPatchEvaluationV1.CreateEmpty(flowName);
            }

            var stageMap = new StageMap();
            var overlays = new List<PatchOverlayAppliedV1>(capacity: 4);
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
            return new FlowPatchEvaluationV1(flowName, document, flowPatch, overlayArray, stageArray);
        }
        catch
        {
            document.Dispose();
            throw;
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

    public readonly struct PatchOverlayAppliedV1
    {
        public static readonly PatchOverlayAppliedV1 Base = new(layer: "base", experimentLayer: null, experimentVariant: null);

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

        public string StageName { get; }

        public bool HasFanoutMax { get; }

        public int FanoutMax { get; }

        public IReadOnlyList<StageModulePatchV1> Modules => _modules;

        internal StagePatchV1(string stageName, bool hasFanoutMax, int fanoutMax, StageModulePatchV1[] modules)
        {
            StageName = stageName;
            HasFanoutMax = hasFanoutMax;
            FanoutMax = fanoutMax;
            _modules = modules;
        }
    }

    public readonly struct StageModulePatchV1
    {
        public string ModuleId { get; }

        public string ModuleType { get; }

        public JsonElement Args { get; }

        public bool Enabled { get; }

        public int Priority { get; }

        public bool HasGate { get; }

        public JsonElement Gate { get; }

        public bool DisabledByEmergency { get; }

        internal StageModulePatchV1(
            string moduleId,
            string moduleType,
            JsonElement args,
            bool enabled,
            int priority,
            bool hasGate,
            JsonElement gate,
            bool disabledByEmergency)
        {
            ModuleId = moduleId;
            ModuleType = moduleType;
            Args = args;
            Enabled = enabled;
            Priority = priority;
            HasGate = hasGate;
            Gate = gate;
            DisabledByEmergency = disabledByEmergency;
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

        public string FlowName { get; }

        public IReadOnlyList<PatchOverlayAppliedV1> OverlaysApplied => _overlaysApplied;

        public IReadOnlyList<StagePatchV1> Stages => _stages;

        internal FlowPatchEvaluationV1(
            string flowName,
            JsonDocument document,
            JsonElement flowPatch,
            PatchOverlayAppliedV1[] overlaysApplied,
            StagePatchV1[] stages)
        {
            FlowName = flowName;
            _document = document;
            _flowPatch = flowPatch;
            _overlaysApplied = overlaysApplied;
            _stages = stages;
        }

        private FlowPatchEvaluationV1(string flowName, PatchOverlayAppliedV1[] overlaysApplied, StagePatchV1[] stages)
        {
            FlowName = flowName;
            _document = null;
            _flowPatch = default;
            _overlaysApplied = overlaysApplied;
            _stages = stages;
        }

        internal static FlowPatchEvaluationV1 CreateEmpty(string flowName)
        {
            return new FlowPatchEvaluationV1(flowName, EmptyOverlayArray, EmptyStageArray);
        }

        public void Dispose()
        {
            _document?.Dispose();
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

            if (_modules.Count == 0)
            {
                modules = Array.Empty<StageModulePatchV1>();
            }
            else
            {
                modules = new StageModulePatchV1[_modules.Count];

                for (var i = 0; i < _modules.Count; i++)
                {
                    modules[i] = _modules[i].Build();
                }
            }

            return new StagePatchV1(StageName, HasFanoutMax, FanoutMax, modules);
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

        public ModuleBuilder(string moduleId)
        {
            ModuleId = moduleId;
            ModuleType = string.Empty;
            Enabled = true;
            Priority = 0;
            HasGate = false;
            DisabledByEmergency = false;
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
        }

        public void ApplyEmergencyDisable()
        {
            Enabled = false;
            DisabledByEmergency = true;
        }

        public StageModulePatchV1 Build()
        {
            return new StageModulePatchV1(
                ModuleId,
                ModuleType,
                _args,
                Enabled,
                Priority,
                HasGate,
                _gate,
                DisabledByEmergency);
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
