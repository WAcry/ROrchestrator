using System.Globalization;
using System.Text.Json;

namespace ROrchestrator.Core;

public static class PatchDiffV1
{
    private const string SupportedSchemaVersion = "v1";

    public static PatchModuleDiffReport DiffModules(string oldPatchJson, string newPatchJson)
    {
        if (oldPatchJson is null)
        {
            throw new ArgumentNullException(nameof(oldPatchJson));
        }

        if (newPatchJson is null)
        {
            throw new ArgumentNullException(nameof(newPatchJson));
        }

        JsonDocument oldDocument;

        try
        {
            oldDocument = JsonDocument.Parse(oldPatchJson);
        }
        catch (JsonException ex)
        {
            throw new FormatException("oldPatchJson is not a valid JSON document.", ex);
        }

        using (oldDocument)
        {
            EnsureSupportedSchemaVersion(oldDocument.RootElement);

            JsonDocument newDocument;

            try
            {
                newDocument = JsonDocument.Parse(newPatchJson);
            }
            catch (JsonException ex)
            {
                throw new FormatException("newPatchJson is not a valid JSON document.", ex);
            }

            using (newDocument)
            {
                EnsureSupportedSchemaVersion(newDocument.RootElement);

                Dictionary<ModuleKey, ModuleInfo>? oldModuleMap = null;
                Dictionary<ModuleKey, ModuleInfo>? newModuleMap = null;

                CollectModules(oldDocument.RootElement, ref oldModuleMap);
                CollectModules(newDocument.RootElement, ref newModuleMap);

                if (oldModuleMap is null && newModuleMap is null)
                {
                    return PatchModuleDiffReport.Empty;
                }

                var buffer = new ModuleDiffBuffer();

                if (oldModuleMap is not null)
                {
                    foreach (var pair in oldModuleMap)
                    {
                        var key = pair.Key;
                        var oldModule = pair.Value;

                        if (newModuleMap is null || !newModuleMap.TryGetValue(key, out var newModule))
                        {
                            buffer.Add(
                                PatchModuleDiff.CreateRemoved(
                                    key.FlowName,
                                    key.StageName,
                                    key.ModuleId,
                                    BuildModulePath(key.FlowName, key.StageName, oldModule.Index, oldModule.ExperimentIndex),
                                    key.ExperimentLayer,
                                    key.ExperimentVariant));
                            continue;
                        }

                        var isUseChanged = !string.Equals(oldModule.Use, newModule.Use, StringComparison.Ordinal);
                        var isWithChanged = !JsonElementDeepEquals(oldModule.With, newModule.With);
                        var isEnabledChanged = oldModule.Enabled != newModule.Enabled;
                        var isPriorityChanged = oldModule.Priority != newModule.Priority;

                        var oldHasGate = oldModule.Gate.ValueKind != JsonValueKind.Undefined;
                        var newHasGate = newModule.Gate.ValueKind != JsonValueKind.Undefined;

                        var isGateAdded = !oldHasGate && newHasGate;
                        var isGateRemoved = oldHasGate && !newHasGate;
                        var isGateChanged = oldHasGate && newHasGate && !JsonElementDeepEquals(oldModule.Gate, newModule.Gate);

                        var oldHasShadow = oldModule.HasShadow;
                        var newHasShadow = newModule.HasShadow;

                        var isShadowAdded = !oldHasShadow && newHasShadow;
                        var isShadowRemoved = oldHasShadow && !newHasShadow;
                        var isShadowSampleChanged = oldHasShadow && newHasShadow && oldModule.ShadowSampleBps != newModule.ShadowSampleBps;

                        if (isUseChanged || isWithChanged || isEnabledChanged || isPriorityChanged || isGateAdded || isGateRemoved || isGateChanged
                            || isShadowAdded || isShadowRemoved || isShadowSampleChanged)
                        {
                            var modulePath = BuildModulePath(key.FlowName, key.StageName, newModule.Index, newModule.ExperimentIndex);

                            if (isUseChanged)
                            {
                                buffer.Add(
                                    PatchModuleDiff.CreateUseChanged(
                                        key.FlowName,
                                        key.StageName,
                                        key.ModuleId,
                                        string.Concat(modulePath, ".use"),
                                        key.ExperimentLayer,
                                        key.ExperimentVariant));
                            }

                            if (isEnabledChanged)
                            {
                                buffer.Add(
                                    PatchModuleDiff.CreateEnabledChanged(
                                        key.FlowName,
                                        key.StageName,
                                        key.ModuleId,
                                        string.Concat(modulePath, ".enabled"),
                                        key.ExperimentLayer,
                                        key.ExperimentVariant));
                            }

                            if (isPriorityChanged)
                            {
                                buffer.Add(
                                    PatchModuleDiff.CreatePriorityChanged(
                                        key.FlowName,
                                        key.StageName,
                                        key.ModuleId,
                                        string.Concat(modulePath, ".priority"),
                                        key.ExperimentLayer,
                                        key.ExperimentVariant));
                            }

                            if (isGateAdded)
                            {
                                buffer.Add(
                                    PatchModuleDiff.CreateGateAdded(
                                        key.FlowName,
                                        key.StageName,
                                        key.ModuleId,
                                        string.Concat(modulePath, ".gate"),
                                        key.ExperimentLayer,
                                        key.ExperimentVariant));
                            }
                            else if (isGateRemoved)
                            {
                                buffer.Add(
                                    PatchModuleDiff.CreateGateRemoved(
                                        key.FlowName,
                                        key.StageName,
                                        key.ModuleId,
                                        string.Concat(modulePath, ".gate"),
                                        key.ExperimentLayer,
                                        key.ExperimentVariant));
                            }
                            else if (isGateChanged)
                            {
                                buffer.Add(
                                    PatchModuleDiff.CreateGateChanged(
                                        key.FlowName,
                                        key.StageName,
                                        key.ModuleId,
                                        string.Concat(modulePath, ".gate"),
                                        key.ExperimentLayer,
                                        key.ExperimentVariant));
                            }

                            if (isShadowAdded)
                            {
                                buffer.Add(
                                    PatchModuleDiff.CreateShadowAdded(
                                        key.FlowName,
                                        key.StageName,
                                        key.ModuleId,
                                        string.Concat(modulePath, ".shadow"),
                                        key.ExperimentLayer,
                                        key.ExperimentVariant));
                            }
                            else if (isShadowRemoved)
                            {
                                buffer.Add(
                                    PatchModuleDiff.CreateShadowRemoved(
                                        key.FlowName,
                                        key.StageName,
                                        key.ModuleId,
                                        string.Concat(modulePath, ".shadow"),
                                        key.ExperimentLayer,
                                        key.ExperimentVariant));
                            }
                            else if (isShadowSampleChanged)
                            {
                                buffer.Add(
                                    PatchModuleDiff.CreateShadowSampleChanged(
                                        key.FlowName,
                                        key.StageName,
                                        key.ModuleId,
                                        string.Concat(modulePath, ".shadow.sample"),
                                        key.ExperimentLayer,
                                        key.ExperimentVariant));
                            }

                            if (isWithChanged)
                            {
                                DiffWithValuesKnownDifferent(key, string.Concat(modulePath, ".with"), oldModule.With, newModule.With, ref buffer);
                            }
                        }
                    }
                }

                if (newModuleMap is not null)
                {
                    foreach (var pair in newModuleMap)
                    {
                        var key = pair.Key;

                        if (oldModuleMap is not null && oldModuleMap.ContainsKey(key))
                        {
                            continue;
                        }

                        var newModule = pair.Value;

                        buffer.Add(
                            PatchModuleDiff.CreateAdded(
                                key.FlowName,
                                key.StageName,
                                key.ModuleId,
                                BuildModulePath(key.FlowName, key.StageName, newModule.Index, newModule.ExperimentIndex),
                                key.ExperimentLayer,
                                key.ExperimentVariant));
                    }
                }

                var diffs = buffer.ToArray();

                if (diffs.Length == 0)
                {
                    return PatchModuleDiffReport.Empty;
                }

                if (diffs.Length > 1)
                {
                    Array.Sort(diffs, PatchModuleDiffComparer.Instance);
                }

                return new PatchModuleDiffReport(diffs);
            }
        }
    }

    public static PatchParamDiffReport DiffParams(string oldPatchJson, string newPatchJson)
    {
        if (oldPatchJson is null)
        {
            throw new ArgumentNullException(nameof(oldPatchJson));
        }

        if (newPatchJson is null)
        {
            throw new ArgumentNullException(nameof(newPatchJson));
        }

        JsonDocument oldDocument;

        try
        {
            oldDocument = JsonDocument.Parse(oldPatchJson);
        }
        catch (JsonException ex)
        {
            throw new FormatException("oldPatchJson is not a valid JSON document.", ex);
        }

        using (oldDocument)
        {
            EnsureSupportedSchemaVersion(oldDocument.RootElement);

            JsonDocument newDocument;

            try
            {
                newDocument = JsonDocument.Parse(newPatchJson);
            }
            catch (JsonException ex)
            {
                throw new FormatException("newPatchJson is not a valid JSON document.", ex);
            }

            using (newDocument)
            {
                EnsureSupportedSchemaVersion(newDocument.RootElement);

                Dictionary<ParamsKey, ParamsInfo>? oldParamsMap = null;
                Dictionary<ParamsKey, ParamsInfo>? newParamsMap = null;

                CollectParams(oldDocument.RootElement, ref oldParamsMap);
                CollectParams(newDocument.RootElement, ref newParamsMap);

                if (oldParamsMap is null && newParamsMap is null)
                {
                    return PatchParamDiffReport.Empty;
                }

                var buffer = new ParamDiffBuffer();

                if (oldParamsMap is not null)
                {
                    foreach (var pair in oldParamsMap)
                    {
                        var key = pair.Key;
                        var oldParams = pair.Value;

                        if (newParamsMap is null || !newParamsMap.TryGetValue(key, out var newParams))
                        {
                            AddAllParamPropertiesAsRemoved(key, BuildParamsPath(key.FlowName, oldParams.ExperimentIndex), oldParams.Params, ref buffer);
                            continue;
                        }

                        DiffParamObjects(
                            key,
                            BuildParamsPath(key.FlowName, newParams.ExperimentIndex),
                            oldParams.Params,
                            newParams.Params,
                            ref buffer);
                    }
                }

                if (newParamsMap is not null)
                {
                    foreach (var pair in newParamsMap)
                    {
                        var key = pair.Key;

                        if (oldParamsMap is not null && oldParamsMap.ContainsKey(key))
                        {
                            continue;
                        }

                        var newParams = pair.Value;
                        AddAllParamPropertiesAsAdded(key, BuildParamsPath(key.FlowName, newParams.ExperimentIndex), newParams.Params, ref buffer);
                    }
                }

                var diffs = buffer.ToArray();

                if (diffs.Length == 0)
                {
                    return PatchParamDiffReport.Empty;
                }

                if (diffs.Length > 1)
                {
                    Array.Sort(diffs, PatchParamDiffComparer.Instance);
                }

                return new PatchParamDiffReport(diffs);
            }
        }
    }

    public static PatchFanoutMaxDiffReport DiffFanoutMax(string oldPatchJson, string newPatchJson)
    {
        if (oldPatchJson is null)
        {
            throw new ArgumentNullException(nameof(oldPatchJson));
        }

        if (newPatchJson is null)
        {
            throw new ArgumentNullException(nameof(newPatchJson));
        }

        JsonDocument oldDocument;

        try
        {
            oldDocument = JsonDocument.Parse(oldPatchJson);
        }
        catch (JsonException ex)
        {
            throw new FormatException("oldPatchJson is not a valid JSON document.", ex);
        }

        using (oldDocument)
        {
            EnsureSupportedSchemaVersion(oldDocument.RootElement);

            JsonDocument newDocument;

            try
            {
                newDocument = JsonDocument.Parse(newPatchJson);
            }
            catch (JsonException ex)
            {
                throw new FormatException("newPatchJson is not a valid JSON document.", ex);
            }

            using (newDocument)
            {
                EnsureSupportedSchemaVersion(newDocument.RootElement);

                Dictionary<FanoutMaxKey, FanoutMaxInfo>? oldFanoutMaxMap = null;
                Dictionary<FanoutMaxKey, FanoutMaxInfo>? newFanoutMaxMap = null;

                CollectFanoutMax(oldDocument.RootElement, ref oldFanoutMaxMap);
                CollectFanoutMax(newDocument.RootElement, ref newFanoutMaxMap);

                if (oldFanoutMaxMap is null && newFanoutMaxMap is null)
                {
                    return PatchFanoutMaxDiffReport.Empty;
                }

                var buffer = new FanoutMaxDiffBuffer();

                if (oldFanoutMaxMap is not null)
                {
                    foreach (var pair in oldFanoutMaxMap)
                    {
                        var key = pair.Key;
                        var oldValue = pair.Value;

                        if (newFanoutMaxMap is null || !newFanoutMaxMap.TryGetValue(key, out var newValue))
                        {
                            buffer.Add(
                                PatchFanoutMaxDiff.CreateRemoved(
                                    key.FlowName,
                                    key.StageName,
                                    BuildFanoutMaxPath(key.FlowName, key.StageName, oldValue.ExperimentIndex),
                                    key.ExperimentLayer,
                                    key.ExperimentVariant));
                            continue;
                        }

                        if (!JsonElementDeepEquals(oldValue.Value, newValue.Value))
                        {
                            buffer.Add(
                                PatchFanoutMaxDiff.CreateChanged(
                                    key.FlowName,
                                    key.StageName,
                                    BuildFanoutMaxPath(key.FlowName, key.StageName, newValue.ExperimentIndex),
                                    key.ExperimentLayer,
                                    key.ExperimentVariant));
                        }
                    }
                }

                if (newFanoutMaxMap is not null)
                {
                    foreach (var pair in newFanoutMaxMap)
                    {
                        var key = pair.Key;

                        if (oldFanoutMaxMap is not null && oldFanoutMaxMap.ContainsKey(key))
                        {
                            continue;
                        }

                        var newValue = pair.Value;

                        buffer.Add(
                            PatchFanoutMaxDiff.CreateAdded(
                                key.FlowName,
                                key.StageName,
                                BuildFanoutMaxPath(key.FlowName, key.StageName, newValue.ExperimentIndex),
                                key.ExperimentLayer,
                                key.ExperimentVariant));
                    }
                }

                var diffs = buffer.ToArray();

                if (diffs.Length == 0)
                {
                    return PatchFanoutMaxDiffReport.Empty;
                }

                if (diffs.Length > 1)
                {
                    Array.Sort(diffs, PatchFanoutMaxDiffComparer.Instance);
                }

                return new PatchFanoutMaxDiffReport(diffs);
            }
        }
    }

    public static PatchEmergencyDiffReport DiffEmergency(string oldPatchJson, string newPatchJson)
    {
        if (oldPatchJson is null)
        {
            throw new ArgumentNullException(nameof(oldPatchJson));
        }

        if (newPatchJson is null)
        {
            throw new ArgumentNullException(nameof(newPatchJson));
        }

        JsonDocument oldDocument;

        try
        {
            oldDocument = JsonDocument.Parse(oldPatchJson);
        }
        catch (JsonException ex)
        {
            throw new FormatException("oldPatchJson is not a valid JSON document.", ex);
        }

        using (oldDocument)
        {
            EnsureSupportedSchemaVersion(oldDocument.RootElement);

            JsonDocument newDocument;

            try
            {
                newDocument = JsonDocument.Parse(newPatchJson);
            }
            catch (JsonException ex)
            {
                throw new FormatException("newPatchJson is not a valid JSON document.", ex);
            }

            using (newDocument)
            {
                EnsureSupportedSchemaVersion(newDocument.RootElement);

                Dictionary<string, JsonElement>? oldEmergencyMap = null;
                Dictionary<string, JsonElement>? newEmergencyMap = null;

                CollectEmergency(oldDocument.RootElement, ref oldEmergencyMap);
                CollectEmergency(newDocument.RootElement, ref newEmergencyMap);

                if (oldEmergencyMap is null && newEmergencyMap is null)
                {
                    return PatchEmergencyDiffReport.Empty;
                }

                var buffer = new EmergencyDiffBuffer();

                if (oldEmergencyMap is not null)
                {
                    foreach (var pair in oldEmergencyMap)
                    {
                        var flowName = pair.Key;
                        var oldEmergency = pair.Value;

                        if (newEmergencyMap is null || !newEmergencyMap.TryGetValue(flowName, out var newEmergency))
                        {
                            buffer.Add(PatchEmergencyDiff.CreateRemoved(flowName, BuildEmergencyPath(flowName)));
                            continue;
                        }

                        DiffEmergencyObjects(flowName, BuildEmergencyPath(flowName), oldEmergency, newEmergency, ref buffer);
                    }
                }

                if (newEmergencyMap is not null)
                {
                    foreach (var pair in newEmergencyMap)
                    {
                        var flowName = pair.Key;

                        if (oldEmergencyMap is not null && oldEmergencyMap.ContainsKey(flowName))
                        {
                            continue;
                        }

                        buffer.Add(PatchEmergencyDiff.CreateAdded(flowName, BuildEmergencyPath(flowName)));
                    }
                }

                var diffs = buffer.ToArray();

                if (diffs.Length == 0)
                {
                    return PatchEmergencyDiffReport.Empty;
                }

                if (diffs.Length > 1)
                {
                    Array.Sort(diffs, PatchEmergencyDiffComparer.Instance);
                }

                return new PatchEmergencyDiffReport(diffs);
            }
        }
    }

    private static void EnsureSupportedSchemaVersion(JsonElement root)
    {
        if (root.ValueKind != JsonValueKind.Object)
        {
            throw new NotSupportedException("schemaVersion is missing or unsupported. Supported: v1.");
        }

        if (!root.TryGetProperty("schemaVersion", out var schemaVersionElement)
            || schemaVersionElement.ValueKind != JsonValueKind.String
            || !schemaVersionElement.ValueEquals(SupportedSchemaVersion))
        {
            throw new NotSupportedException("schemaVersion is missing or unsupported. Supported: v1.");
        }
    }

    private static void CollectEmergency(JsonElement root, ref Dictionary<string, JsonElement>? emergencyMap)
    {
        if (!root.TryGetProperty("flows", out var flowsElement))
        {
            return;
        }

        if (flowsElement.ValueKind != JsonValueKind.Object)
        {
            throw new FormatException("flows must be an object.");
        }

        foreach (var flowProperty in flowsElement.EnumerateObject())
        {
            var flowName = flowProperty.Name;
            var flowPatch = flowProperty.Value;

            if (flowPatch.ValueKind != JsonValueKind.Object)
            {
                throw new FormatException(string.Concat("Flow patch must be an object. Flow: ", flowName));
            }

            if (!flowPatch.TryGetProperty("emergency", out var emergencyElement) || emergencyElement.ValueKind == JsonValueKind.Null)
            {
                continue;
            }

            if (emergencyElement.ValueKind != JsonValueKind.Object)
            {
                throw new FormatException(string.Concat("emergency must be an object. Flow: ", flowName));
            }

            emergencyMap ??= new Dictionary<string, JsonElement>(4);
            emergencyMap.Add(flowName, emergencyElement);
        }
    }

    private static void DiffEmergencyObjects(
        string flowName,
        string pathPrefix,
        JsonElement oldEmergency,
        JsonElement newEmergency,
        ref EmergencyDiffBuffer buffer)
    {
        if (oldEmergency.ValueKind != JsonValueKind.Object || newEmergency.ValueKind != JsonValueKind.Object)
        {
            throw new FormatException(string.Concat("emergency must be an object. Flow: ", flowName));
        }

        foreach (var oldProperty in oldEmergency.EnumerateObject())
        {
            if (!newEmergency.TryGetProperty(oldProperty.Name, out var newValue))
            {
                buffer.Add(PatchEmergencyDiff.CreateRemoved(flowName, string.Concat(pathPrefix, ".", oldProperty.Name)));
                continue;
            }

            var oldValue = oldProperty.Value;

            if (oldValue.ValueKind == JsonValueKind.Object && newValue.ValueKind == JsonValueKind.Object)
            {
                DiffEmergencyObjects(flowName, string.Concat(pathPrefix, ".", oldProperty.Name), oldValue, newValue, ref buffer);
                continue;
            }

            if (!JsonElementDeepEquals(oldValue, newValue))
            {
                buffer.Add(PatchEmergencyDiff.CreateChanged(flowName, string.Concat(pathPrefix, ".", oldProperty.Name)));
            }
        }

        foreach (var newProperty in newEmergency.EnumerateObject())
        {
            if (!oldEmergency.TryGetProperty(newProperty.Name, out _))
            {
                buffer.Add(PatchEmergencyDiff.CreateAdded(flowName, string.Concat(pathPrefix, ".", newProperty.Name)));
            }
        }
    }

    private static string BuildEmergencyPath(string flowName)
    {
        return string.Concat("$.flows.", flowName, ".emergency");
    }

    private static void CollectParams(JsonElement root, ref Dictionary<ParamsKey, ParamsInfo>? paramsMap)
    {
        if (!root.TryGetProperty("flows", out var flowsElement))
        {
            return;
        }

        if (flowsElement.ValueKind != JsonValueKind.Object)
        {
            throw new FormatException("flows must be an object.");
        }

        foreach (var flowProperty in flowsElement.EnumerateObject())
        {
            var flowName = flowProperty.Name;
            var flowPatch = flowProperty.Value;

            if (flowPatch.ValueKind != JsonValueKind.Object)
            {
                throw new FormatException(string.Concat("Flow patch must be an object. Flow: ", flowName));
            }

            CollectFlowParams(flowName, flowPatch, experimentLayer: null, experimentVariant: null, experimentIndex: -1, ref paramsMap);

            if (!flowPatch.TryGetProperty("experiments", out var experimentsElement) || experimentsElement.ValueKind == JsonValueKind.Null)
            {
                continue;
            }

            if (experimentsElement.ValueKind != JsonValueKind.Array)
            {
                throw new FormatException(string.Concat("experiments must be an array. Flow: ", flowName));
            }

            var experimentIndex = 0;

            foreach (var experimentMapping in experimentsElement.EnumerateArray())
            {
                if (experimentMapping.ValueKind != JsonValueKind.Object)
                {
                    throw new FormatException(string.Concat("experiments must be an array of objects. Flow: ", flowName));
                }

                if (!experimentMapping.TryGetProperty("layer", out var layerElement) || layerElement.ValueKind != JsonValueKind.String)
                {
                    throw new FormatException(
                        string.Concat(
                            "experiments[].layer is required and must be a non-empty string. Flow: ",
                            flowName,
                            ", ExperimentIndex: ",
                            experimentIndex.ToString(CultureInfo.InvariantCulture)));
                }

                var layer = layerElement.GetString();

                if (string.IsNullOrEmpty(layer))
                {
                    throw new FormatException(
                        string.Concat(
                            "experiments[].layer is required and must be a non-empty string. Flow: ",
                            flowName,
                            ", ExperimentIndex: ",
                            experimentIndex.ToString(CultureInfo.InvariantCulture)));
                }

                if (!experimentMapping.TryGetProperty("variant", out var variantElement) || variantElement.ValueKind != JsonValueKind.String)
                {
                    throw new FormatException(
                        string.Concat(
                            "experiments[].variant is required and must be a non-empty string. Flow: ",
                            flowName,
                            ", ExperimentIndex: ",
                            experimentIndex.ToString(CultureInfo.InvariantCulture)));
                }

                var variant = variantElement.GetString();

                if (string.IsNullOrEmpty(variant))
                {
                    throw new FormatException(
                        string.Concat(
                            "experiments[].variant is required and must be a non-empty string. Flow: ",
                            flowName,
                            ", ExperimentIndex: ",
                            experimentIndex.ToString(CultureInfo.InvariantCulture)));
                }

                if (!experimentMapping.TryGetProperty("patch", out var patchElement) || patchElement.ValueKind == JsonValueKind.Null)
                {
                    throw new FormatException(
                        string.Concat(
                            "experiments[].patch is required. Flow: ",
                            flowName,
                            ", ExperimentIndex: ",
                            experimentIndex.ToString(CultureInfo.InvariantCulture)));
                }

                if (patchElement.ValueKind != JsonValueKind.Object)
                {
                    throw new FormatException(
                        string.Concat(
                            "experiments[].patch must be a non-null object. Flow: ",
                            flowName,
                            ", ExperimentIndex: ",
                            experimentIndex.ToString(CultureInfo.InvariantCulture)));
                }

                CollectFlowParams(flowName, patchElement, layer!, variant!, experimentIndex, ref paramsMap);

                experimentIndex++;
            }
        }
    }

    private static void CollectFlowParams(
        string flowName,
        JsonElement flowPatch,
        string? experimentLayer,
        string? experimentVariant,
        int experimentIndex,
        ref Dictionary<ParamsKey, ParamsInfo>? paramsMap)
    {
        if (!flowPatch.TryGetProperty("params", out var paramsElement) || paramsElement.ValueKind == JsonValueKind.Null)
        {
            return;
        }

        if (paramsElement.ValueKind != JsonValueKind.Object)
        {
            if (experimentIndex < 0)
            {
                throw new FormatException(string.Concat("params must be an object. Flow: ", flowName));
            }

            throw new FormatException(
                string.Concat(
                    "params must be an object. Flow: ",
                    flowName,
                    ", ExperimentIndex: ",
                    experimentIndex.ToString(CultureInfo.InvariantCulture)));
        }

        paramsMap ??= new Dictionary<ParamsKey, ParamsInfo>(4);

        var key = new ParamsKey(flowName, experimentLayer, experimentVariant);

        if (!paramsMap.TryAdd(key, new ParamsInfo(paramsElement, experimentIndex)))
        {
            throw new FormatException(
                string.Concat(
                    "Duplicate experiment mapping within flow. Flow: ",
                    flowName,
                    ", Layer: ",
                    experimentLayer,
                    ", Variant: ",
                    experimentVariant));
        }
    }

    private static void CollectModules(JsonElement root, ref Dictionary<ModuleKey, ModuleInfo>? moduleMap)
    {
        if (!root.TryGetProperty("flows", out var flowsElement))
        {
            return;
        }

        if (flowsElement.ValueKind != JsonValueKind.Object)
        {
            throw new FormatException("flows must be an object.");
        }

        foreach (var flowProperty in flowsElement.EnumerateObject())
        {
            var flowName = flowProperty.Name;
            var flowPatch = flowProperty.Value;

            if (flowPatch.ValueKind != JsonValueKind.Object)
            {
                throw new FormatException(string.Concat("Flow patch must be an object. Flow: ", flowName));
            }

            CollectFlowModules(flowName, flowPatch, experimentLayer: null, experimentVariant: null, experimentIndex: -1, ref moduleMap);

            if (!flowPatch.TryGetProperty("experiments", out var experimentsElement) || experimentsElement.ValueKind == JsonValueKind.Null)
            {
                continue;
            }

            if (experimentsElement.ValueKind != JsonValueKind.Array)
            {
                throw new FormatException(string.Concat("experiments must be an array. Flow: ", flowName));
            }

            var experimentIndex = 0;

            foreach (var experimentMapping in experimentsElement.EnumerateArray())
            {
                if (experimentMapping.ValueKind != JsonValueKind.Object)
                {
                    throw new FormatException(string.Concat("experiments must be an array of objects. Flow: ", flowName));
                }

                if (!experimentMapping.TryGetProperty("layer", out var layerElement) || layerElement.ValueKind != JsonValueKind.String)
                {
                    throw new FormatException(
                        string.Concat(
                            "experiments[].layer is required and must be a non-empty string. Flow: ",
                            flowName,
                            ", ExperimentIndex: ",
                            experimentIndex.ToString(CultureInfo.InvariantCulture)));
                }

                var layer = layerElement.GetString();

                if (string.IsNullOrEmpty(layer))
                {
                    throw new FormatException(
                        string.Concat(
                            "experiments[].layer is required and must be a non-empty string. Flow: ",
                            flowName,
                            ", ExperimentIndex: ",
                            experimentIndex.ToString(CultureInfo.InvariantCulture)));
                }

                if (!experimentMapping.TryGetProperty("variant", out var variantElement) || variantElement.ValueKind != JsonValueKind.String)
                {
                    throw new FormatException(
                        string.Concat(
                            "experiments[].variant is required and must be a non-empty string. Flow: ",
                            flowName,
                            ", ExperimentIndex: ",
                            experimentIndex.ToString(CultureInfo.InvariantCulture)));
                }

                var variant = variantElement.GetString();

                if (string.IsNullOrEmpty(variant))
                {
                    throw new FormatException(
                        string.Concat(
                            "experiments[].variant is required and must be a non-empty string. Flow: ",
                            flowName,
                            ", ExperimentIndex: ",
                            experimentIndex.ToString(CultureInfo.InvariantCulture)));
                }

                if (!experimentMapping.TryGetProperty("patch", out var patchElement) || patchElement.ValueKind == JsonValueKind.Null)
                {
                    throw new FormatException(
                        string.Concat(
                            "experiments[].patch is required. Flow: ",
                            flowName,
                            ", ExperimentIndex: ",
                            experimentIndex.ToString(CultureInfo.InvariantCulture)));
                }

                if (patchElement.ValueKind != JsonValueKind.Object)
                {
                    throw new FormatException(
                        string.Concat(
                            "experiments[].patch must be a non-null object. Flow: ",
                            flowName,
                            ", ExperimentIndex: ",
                            experimentIndex.ToString(CultureInfo.InvariantCulture)));
                }

                CollectFlowModules(flowName, patchElement, layer!, variant!, experimentIndex, ref moduleMap);

                experimentIndex++;
            }
        }
    }

    private static void CollectFanoutMax(JsonElement root, ref Dictionary<FanoutMaxKey, FanoutMaxInfo>? fanoutMaxMap)
    {
        if (!root.TryGetProperty("flows", out var flowsElement))
        {
            return;
        }

        if (flowsElement.ValueKind != JsonValueKind.Object)
        {
            throw new FormatException("flows must be an object.");
        }

        foreach (var flowProperty in flowsElement.EnumerateObject())
        {
            var flowName = flowProperty.Name;
            var flowPatch = flowProperty.Value;

            if (flowPatch.ValueKind != JsonValueKind.Object)
            {
                throw new FormatException(string.Concat("Flow patch must be an object. Flow: ", flowName));
            }

            CollectFlowFanoutMax(flowName, flowPatch, experimentLayer: null, experimentVariant: null, experimentIndex: -1, ref fanoutMaxMap);

            if (!flowPatch.TryGetProperty("experiments", out var experimentsElement) || experimentsElement.ValueKind == JsonValueKind.Null)
            {
                continue;
            }

            if (experimentsElement.ValueKind != JsonValueKind.Array)
            {
                throw new FormatException(string.Concat("experiments must be an array. Flow: ", flowName));
            }

            var experimentIndex = 0;

            foreach (var experimentMapping in experimentsElement.EnumerateArray())
            {
                if (experimentMapping.ValueKind != JsonValueKind.Object)
                {
                    throw new FormatException(string.Concat("experiments must be an array of objects. Flow: ", flowName));
                }

                if (!experimentMapping.TryGetProperty("layer", out var layerElement) || layerElement.ValueKind != JsonValueKind.String)
                {
                    throw new FormatException(
                        string.Concat(
                            "experiments[].layer is required and must be a non-empty string. Flow: ",
                            flowName,
                            ", ExperimentIndex: ",
                            experimentIndex.ToString(CultureInfo.InvariantCulture)));
                }

                var layer = layerElement.GetString();

                if (string.IsNullOrEmpty(layer))
                {
                    throw new FormatException(
                        string.Concat(
                            "experiments[].layer is required and must be a non-empty string. Flow: ",
                            flowName,
                            ", ExperimentIndex: ",
                            experimentIndex.ToString(CultureInfo.InvariantCulture)));
                }

                if (!experimentMapping.TryGetProperty("variant", out var variantElement) || variantElement.ValueKind != JsonValueKind.String)
                {
                    throw new FormatException(
                        string.Concat(
                            "experiments[].variant is required and must be a non-empty string. Flow: ",
                            flowName,
                            ", ExperimentIndex: ",
                            experimentIndex.ToString(CultureInfo.InvariantCulture)));
                }

                var variant = variantElement.GetString();

                if (string.IsNullOrEmpty(variant))
                {
                    throw new FormatException(
                        string.Concat(
                            "experiments[].variant is required and must be a non-empty string. Flow: ",
                            flowName,
                            ", ExperimentIndex: ",
                            experimentIndex.ToString(CultureInfo.InvariantCulture)));
                }

                if (!experimentMapping.TryGetProperty("patch", out var patchElement) || patchElement.ValueKind == JsonValueKind.Null)
                {
                    throw new FormatException(
                        string.Concat(
                            "experiments[].patch is required. Flow: ",
                            flowName,
                            ", ExperimentIndex: ",
                            experimentIndex.ToString(CultureInfo.InvariantCulture)));
                }

                if (patchElement.ValueKind != JsonValueKind.Object)
                {
                    throw new FormatException(
                        string.Concat(
                            "experiments[].patch must be a non-null object. Flow: ",
                            flowName,
                            ", ExperimentIndex: ",
                            experimentIndex.ToString(CultureInfo.InvariantCulture)));
                }

                CollectFlowFanoutMax(flowName, patchElement, layer!, variant!, experimentIndex, ref fanoutMaxMap);

                experimentIndex++;
            }
        }
    }

    private static void CollectFlowFanoutMax(
        string flowName,
        JsonElement flowPatch,
        string? experimentLayer,
        string? experimentVariant,
        int experimentIndex,
        ref Dictionary<FanoutMaxKey, FanoutMaxInfo>? fanoutMaxMap)
    {
        if (!flowPatch.TryGetProperty("stages", out var stagesElement) || stagesElement.ValueKind == JsonValueKind.Null)
        {
            return;
        }

        if (stagesElement.ValueKind != JsonValueKind.Object)
        {
            throw new FormatException(string.Concat("stages must be an object. Flow: ", flowName));
        }

        foreach (var stageProperty in stagesElement.EnumerateObject())
        {
            var stageName = stageProperty.Name;
            var stagePatch = stageProperty.Value;

            if (stagePatch.ValueKind != JsonValueKind.Object)
            {
                throw new FormatException(
                    string.Concat("Stage patch must be an object. Flow: ", flowName, ", Stage: ", stageName));
            }

            if (!stagePatch.TryGetProperty("fanoutMax", out var fanoutMaxElement) || fanoutMaxElement.ValueKind == JsonValueKind.Null)
            {
                continue;
            }

            fanoutMaxMap ??= new Dictionary<FanoutMaxKey, FanoutMaxInfo>(4);

            var key = new FanoutMaxKey(flowName, stageName, experimentLayer, experimentVariant);

            if (!fanoutMaxMap.TryAdd(key, new FanoutMaxInfo(fanoutMaxElement, experimentIndex)))
            {
                throw new FormatException(
                    string.Concat("Duplicate stage entry within flow. Flow: ", flowName, ", Stage: ", stageName));
            }
        }
    }

    private static string BuildParamsPath(string flowName, int experimentIndex)
    {
        if (experimentIndex < 0)
        {
            return string.Concat("$.flows.", flowName, ".params");
        }

        return string.Concat(
            "$.flows.",
            flowName,
            ".experiments[",
            experimentIndex.ToString(CultureInfo.InvariantCulture),
            "].patch.params");
    }

    private static string BuildFanoutMaxPath(string flowName, string stageName, int experimentIndex)
    {
        if (experimentIndex < 0)
        {
            return string.Concat("$.flows.", flowName, ".stages.", stageName, ".fanoutMax");
        }

        return string.Concat(
            "$.flows.",
            flowName,
            ".experiments[",
            experimentIndex.ToString(CultureInfo.InvariantCulture),
            "].patch.stages.",
            stageName,
            ".fanoutMax");
    }

    private static void AddAllParamPropertiesAsAdded(ParamsKey key, string pathPrefix, JsonElement paramsElement, ref ParamDiffBuffer buffer)
    {
        foreach (var property in paramsElement.EnumerateObject())
        {
            buffer.Add(PatchParamDiff.CreateAdded(key.FlowName, string.Concat(pathPrefix, ".", property.Name), key.ExperimentLayer, key.ExperimentVariant));
        }
    }

    private static void AddAllParamPropertiesAsRemoved(ParamsKey key, string pathPrefix, JsonElement paramsElement, ref ParamDiffBuffer buffer)
    {
        foreach (var property in paramsElement.EnumerateObject())
        {
            buffer.Add(PatchParamDiff.CreateRemoved(key.FlowName, string.Concat(pathPrefix, ".", property.Name), key.ExperimentLayer, key.ExperimentVariant));
        }
    }

    private static void DiffParamObjects(
        ParamsKey key,
        string pathPrefix,
        JsonElement oldParams,
        JsonElement newParams,
        ref ParamDiffBuffer buffer)
    {
        foreach (var oldProperty in oldParams.EnumerateObject())
        {
            var name = oldProperty.Name;
            var oldValue = oldProperty.Value;

            if (!newParams.TryGetProperty(name, out var newValue))
            {
                buffer.Add(
                    PatchParamDiff.CreateRemoved(
                        key.FlowName,
                        string.Concat(pathPrefix, ".", name),
                        key.ExperimentLayer,
                        key.ExperimentVariant));
                continue;
            }

            if (oldValue.ValueKind == JsonValueKind.Object && newValue.ValueKind == JsonValueKind.Object)
            {
                DiffParamObjects(key, string.Concat(pathPrefix, ".", name), oldValue, newValue, ref buffer);
                continue;
            }

            if (!JsonElementDeepEquals(oldValue, newValue))
            {
                buffer.Add(
                    PatchParamDiff.CreateChanged(
                        key.FlowName,
                        string.Concat(pathPrefix, ".", name),
                        key.ExperimentLayer,
                        key.ExperimentVariant));
            }
        }

        foreach (var newProperty in newParams.EnumerateObject())
        {
            var name = newProperty.Name;

            if (oldParams.TryGetProperty(name, out _))
            {
                continue;
            }

            buffer.Add(PatchParamDiff.CreateAdded(key.FlowName, string.Concat(pathPrefix, ".", name), key.ExperimentLayer, key.ExperimentVariant));
        }
    }

    private static void DiffWithValuesKnownDifferent(
        ModuleKey key,
        string pathPrefix,
        JsonElement oldValue,
        JsonElement newValue,
        ref ModuleDiffBuffer buffer)
    {
        if (oldValue.ValueKind == JsonValueKind.Object && newValue.ValueKind == JsonValueKind.Object)
        {
            DiffWithObjects(key, pathPrefix, oldValue, newValue, ref buffer);
            return;
        }

        if (oldValue.ValueKind == JsonValueKind.Array && newValue.ValueKind == JsonValueKind.Array)
        {
            DiffWithArrays(key, pathPrefix, oldValue, newValue, ref buffer);
            return;
        }

        buffer.Add(
            PatchModuleDiff.CreateWithChanged(
                key.FlowName,
                key.StageName,
                key.ModuleId,
                pathPrefix,
                key.ExperimentLayer,
                key.ExperimentVariant));
    }

    private static void DiffWithObjects(
        ModuleKey key,
        string pathPrefix,
        JsonElement oldObject,
        JsonElement newObject,
        ref ModuleDiffBuffer buffer)
    {
        foreach (var oldProperty in oldObject.EnumerateObject())
        {
            var name = oldProperty.Name;
            var oldValue = oldProperty.Value;

            if (!newObject.TryGetProperty(name, out var newValue))
            {
                buffer.Add(
                    PatchModuleDiff.CreateWithRemoved(
                        key.FlowName,
                        key.StageName,
                        key.ModuleId,
                        string.Concat(pathPrefix, ".", name),
                        key.ExperimentLayer,
                        key.ExperimentVariant));
                continue;
            }

            if (JsonElementDeepEquals(oldValue, newValue))
            {
                continue;
            }

            DiffWithValuesKnownDifferent(key, string.Concat(pathPrefix, ".", name), oldValue, newValue, ref buffer);
        }

        foreach (var newProperty in newObject.EnumerateObject())
        {
            var name = newProperty.Name;

            if (oldObject.TryGetProperty(name, out _))
            {
                continue;
            }

            buffer.Add(
                PatchModuleDiff.CreateWithAdded(
                    key.FlowName,
                    key.StageName,
                    key.ModuleId,
                    string.Concat(pathPrefix, ".", name),
                    key.ExperimentLayer,
                    key.ExperimentVariant));
        }
    }

    private static void DiffWithArrays(
        ModuleKey key,
        string pathPrefix,
        JsonElement oldArray,
        JsonElement newArray,
        ref ModuleDiffBuffer buffer)
    {
        var oldLength = oldArray.GetArrayLength();
        var newLength = newArray.GetArrayLength();
        var commonLength = oldLength < newLength ? oldLength : newLength;

        var oldEnumerator = oldArray.EnumerateArray();
        var newEnumerator = newArray.EnumerateArray();

        var index = 0;

        while (index < commonLength)
        {
            if (!oldEnumerator.MoveNext() || !newEnumerator.MoveNext())
            {
                break;
            }

            var oldValue = oldEnumerator.Current;
            var newValue = newEnumerator.Current;

            if (!JsonElementDeepEquals(oldValue, newValue))
            {
                DiffWithValuesKnownDifferent(
                    key,
                    string.Concat(pathPrefix, "[", index.ToString(CultureInfo.InvariantCulture), "]"),
                    oldValue,
                    newValue,
                    ref buffer);
            }

            index++;
        }

        while (index < oldLength)
        {
            if (!oldEnumerator.MoveNext())
            {
                break;
            }

            buffer.Add(
                PatchModuleDiff.CreateWithRemoved(
                    key.FlowName,
                    key.StageName,
                    key.ModuleId,
                    string.Concat(pathPrefix, "[", index.ToString(CultureInfo.InvariantCulture), "]"),
                    key.ExperimentLayer,
                    key.ExperimentVariant));

            index++;
        }

        index = commonLength;

        while (index < newLength)
        {
            if (!newEnumerator.MoveNext())
            {
                break;
            }

            buffer.Add(
                PatchModuleDiff.CreateWithAdded(
                    key.FlowName,
                    key.StageName,
                    key.ModuleId,
                    string.Concat(pathPrefix, "[", index.ToString(CultureInfo.InvariantCulture), "]"),
                    key.ExperimentLayer,
                    key.ExperimentVariant));

            index++;
        }
    }

    private static void CollectFlowModules(
        string flowName,
        JsonElement flowPatch,
        string? experimentLayer,
        string? experimentVariant,
        int experimentIndex,
        ref Dictionary<ModuleKey, ModuleInfo>? moduleMap)
    {
        if (!flowPatch.TryGetProperty("stages", out var stagesElement) || stagesElement.ValueKind == JsonValueKind.Null)
        {
            return;
        }

        if (stagesElement.ValueKind != JsonValueKind.Object)
        {
            throw new FormatException(string.Concat("stages must be an object. Flow: ", flowName));
        }

        foreach (var stageProperty in stagesElement.EnumerateObject())
        {
            var stageName = stageProperty.Name;
            var stagePatch = stageProperty.Value;

            if (stagePatch.ValueKind != JsonValueKind.Object)
            {
                throw new FormatException(
                    string.Concat("Stage patch must be an object. Flow: ", flowName, ", Stage: ", stageName));
            }

            if (!stagePatch.TryGetProperty("modules", out var modulesElement) || modulesElement.ValueKind == JsonValueKind.Null)
            {
                continue;
            }

            if (modulesElement.ValueKind != JsonValueKind.Array)
            {
                throw new FormatException(
                    string.Concat("modules must be an array. Flow: ", flowName, ", Stage: ", stageName));
            }

            var index = 0;

            foreach (var moduleElement in modulesElement.EnumerateArray())
            {
                if (moduleElement.ValueKind != JsonValueKind.Object)
                {
                    throw new FormatException(
                        string.Concat("modules must be an array of objects. Flow: ", flowName, ", Stage: ", stageName));
                }

                if (!moduleElement.TryGetProperty("id", out var idElement) || idElement.ValueKind != JsonValueKind.String)
                {
                    throw new FormatException(
                        string.Concat("modules[].id is required and must be a string. Flow: ", flowName, ", Stage: ", stageName));
                }

                var moduleId = idElement.GetString();

                if (string.IsNullOrEmpty(moduleId))
                {
                    throw new FormatException(
                        string.Concat("modules[].id is required and must be non-empty. Flow: ", flowName, ", Stage: ", stageName));
                }

                if (!moduleElement.TryGetProperty("use", out var useElement) || useElement.ValueKind != JsonValueKind.String)
                {
                    throw new FormatException(
                        string.Concat("modules[].use is required and must be a string. Flow: ", flowName, ", Stage: ", stageName, ", ModuleId: ", moduleId));
                }

                var moduleUse = useElement.GetString();

                if (string.IsNullOrEmpty(moduleUse))
                {
                    throw new FormatException(
                        string.Concat("modules[].use is required and must be non-empty. Flow: ", flowName, ", Stage: ", stageName, ", ModuleId: ", moduleId));
                }

                if (!moduleElement.TryGetProperty("with", out var withElement) || withElement.ValueKind == JsonValueKind.Null)
                {
                    throw new FormatException(
                        string.Concat("modules[].with is required. Flow: ", flowName, ", Stage: ", stageName, ", ModuleId: ", moduleId));
                }

                var enabled = true;

                if (moduleElement.TryGetProperty("enabled", out var enabledElement) && enabledElement.ValueKind != JsonValueKind.Null)
                {
                    if (enabledElement.ValueKind == JsonValueKind.True || enabledElement.ValueKind == JsonValueKind.False)
                    {
                        enabled = enabledElement.GetBoolean();
                    }
                }

                var priority = 0;

                if (moduleElement.TryGetProperty("priority", out var priorityElement) && priorityElement.ValueKind != JsonValueKind.Null)
                {
                    if (priorityElement.ValueKind == JsonValueKind.Number && priorityElement.TryGetInt32(out var priorityValue))
                    {
                        priority = priorityValue;
                    }
                }

                JsonElement gateElement = default;

                if (moduleElement.TryGetProperty("gate", out var rawGateElement) && rawGateElement.ValueKind != JsonValueKind.Null)
                {
                    gateElement = rawGateElement;
                }

                var hasShadow = false;
                ushort shadowSampleBps = 0;

                if (moduleElement.TryGetProperty("shadow", out var shadowElement))
                {
                    hasShadow = true;
                    shadowSampleBps = ParseShadowSampleBps(shadowElement, flowName, stageName, moduleId);
                }

                moduleMap ??= new Dictionary<ModuleKey, ModuleInfo>(4);

                var key = new ModuleKey(flowName, stageName, moduleId, experimentLayer, experimentVariant);

                if (!moduleMap.TryAdd(key, new ModuleInfo(moduleUse!, withElement, gateElement, enabled, priority, hasShadow, shadowSampleBps, index, experimentIndex)))
                {
                    throw new FormatException(
                        string.Concat("Duplicate module id within stage. Flow: ", flowName, ", Stage: ", stageName, ", ModuleId: ", moduleId));
                }

                index++;
            }
        }
    }

    private static string BuildModulePath(string flowName, string stageName, int moduleIndex, int experimentIndex)
    {
        if (experimentIndex < 0)
        {
            return string.Concat(
                "$.flows.",
                flowName,
                ".stages.",
                stageName,
                ".modules[",
                moduleIndex.ToString(CultureInfo.InvariantCulture),
                "]");
        }

        return string.Concat(
            "$.flows.",
            flowName,
            ".experiments[",
            experimentIndex.ToString(CultureInfo.InvariantCulture),
            "].patch.stages.",
            stageName,
            ".modules[",
            moduleIndex.ToString(CultureInfo.InvariantCulture),
            "]");
    }

    private static ushort ParseShadowSampleBps(JsonElement shadowElement, string flowName, string stageName, string moduleId)
    {
        if (shadowElement.ValueKind != JsonValueKind.Object)
        {
            throw new FormatException(
                string.Concat("modules[].shadow must be an object. Flow: ", flowName, ", Stage: ", stageName, ", ModuleId: ", moduleId));
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
                    throw new FormatException(
                        string.Concat("modules[].shadow.sample must be a number. Flow: ", flowName, ", Stage: ", stageName, ", ModuleId: ", moduleId));
                }

                continue;
            }

            throw new FormatException(
                string.Concat("Unknown field: ", property.Name, " in modules[].shadow. Flow: ", flowName, ", Stage: ", stageName, ", ModuleId: ", moduleId));
        }

        if (!hasSample)
        {
            throw new FormatException(
                string.Concat("modules[].shadow.sample is required. Flow: ", flowName, ", Stage: ", stageName, ", ModuleId: ", moduleId));
        }

        if (sample < 0 || sample > 1)
        {
            throw new FormatException(
                string.Concat("modules[].shadow.sample must be within range 0..1. Flow: ", flowName, ", Stage: ", stageName, ", ModuleId: ", moduleId));
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
            throw new FormatException(
                string.Concat("modules[].shadow.sample must be within range 0..1. Flow: ", flowName, ", Stage: ", stageName, ", ModuleId: ", moduleId));
        }

        return (ushort)bps;
    }

    private static bool JsonElementDeepEquals(JsonElement left, JsonElement right)
    {
        var leftKind = left.ValueKind;
        var rightKind = right.ValueKind;

        if (leftKind != rightKind)
        {
            return false;
        }

        switch (leftKind)
        {
            case JsonValueKind.Object:
            {
                var leftCount = 0;

                foreach (var leftProperty in left.EnumerateObject())
                {
                    leftCount++;

                    if (!right.TryGetProperty(leftProperty.Name, out var rightValue))
                    {
                        return false;
                    }

                    if (!JsonElementDeepEquals(leftProperty.Value, rightValue))
                    {
                        return false;
                    }
                }

                var rightCount = 0;

                foreach (var _ in right.EnumerateObject())
                {
                    rightCount++;
                }

                return leftCount == rightCount;
            }

            case JsonValueKind.Array:
            {
                if (left.GetArrayLength() != right.GetArrayLength())
                {
                    return false;
                }

                var leftEnumerator = left.EnumerateArray();
                var rightEnumerator = right.EnumerateArray();

                while (leftEnumerator.MoveNext())
                {
                    if (!rightEnumerator.MoveNext())
                    {
                        return false;
                    }

                    if (!JsonElementDeepEquals(leftEnumerator.Current, rightEnumerator.Current))
                    {
                        return false;
                    }
                }

                return !rightEnumerator.MoveNext();
            }

            case JsonValueKind.String:
                return string.Equals(left.GetString(), right.GetString(), StringComparison.Ordinal);

            case JsonValueKind.Number:
                return string.Equals(left.GetRawText(), right.GetRawText(), StringComparison.Ordinal);

            case JsonValueKind.True:
            case JsonValueKind.False:
                return left.GetBoolean() == right.GetBoolean();

            case JsonValueKind.Null:
            case JsonValueKind.Undefined:
                return true;

            default:
                return string.Equals(left.GetRawText(), right.GetRawText(), StringComparison.Ordinal);
        }
    }

    private readonly struct ParamsKey : IEquatable<ParamsKey>
    {
        public readonly string FlowName;
        public readonly string? ExperimentLayer;
        public readonly string? ExperimentVariant;

        public ParamsKey(string flowName, string? experimentLayer, string? experimentVariant)
        {
            FlowName = flowName;
            ExperimentLayer = experimentLayer;
            ExperimentVariant = experimentVariant;
        }

        public bool Equals(ParamsKey other)
        {
            return string.Equals(FlowName, other.FlowName, StringComparison.Ordinal)
                && string.Equals(ExperimentLayer, other.ExperimentLayer, StringComparison.Ordinal)
                && string.Equals(ExperimentVariant, other.ExperimentVariant, StringComparison.Ordinal);
        }

        public override bool Equals(object? obj)
        {
            return obj is ParamsKey other && Equals(other);
        }

        public override int GetHashCode()
        {
            var hash = new HashCode();
            hash.Add(FlowName);
            hash.Add(ExperimentLayer);
            hash.Add(ExperimentVariant);
            return hash.ToHashCode();
        }
    }

    private readonly struct ParamsInfo
    {
        public readonly JsonElement Params;
        public readonly int ExperimentIndex;

        public ParamsInfo(JsonElement @params, int experimentIndex)
        {
            Params = @params;
            ExperimentIndex = experimentIndex;
        }
    }

    private readonly struct ModuleKey : IEquatable<ModuleKey>
    {
        public readonly string FlowName;
        public readonly string? ExperimentLayer;
        public readonly string? ExperimentVariant;
        public readonly string StageName;
        public readonly string ModuleId;

        public ModuleKey(string flowName, string stageName, string moduleId, string? experimentLayer, string? experimentVariant)
        {
            FlowName = flowName;
            ExperimentLayer = experimentLayer;
            ExperimentVariant = experimentVariant;
            StageName = stageName;
            ModuleId = moduleId;
        }

        public bool Equals(ModuleKey other)
        {
            return string.Equals(FlowName, other.FlowName, StringComparison.Ordinal)
                && string.Equals(ExperimentLayer, other.ExperimentLayer, StringComparison.Ordinal)
                && string.Equals(ExperimentVariant, other.ExperimentVariant, StringComparison.Ordinal)
                && string.Equals(StageName, other.StageName, StringComparison.Ordinal)
                && string.Equals(ModuleId, other.ModuleId, StringComparison.Ordinal);
        }

        public override bool Equals(object? obj)
        {
            return obj is ModuleKey other && Equals(other);
        }

        public override int GetHashCode()
        {
            var hash = new HashCode();
            hash.Add(FlowName);
            hash.Add(ExperimentLayer);
            hash.Add(ExperimentVariant);
            hash.Add(StageName);
            hash.Add(ModuleId);
            return hash.ToHashCode();
        }
    }

    private readonly struct FanoutMaxKey : IEquatable<FanoutMaxKey>
    {
        public readonly string FlowName;
        public readonly string? ExperimentLayer;
        public readonly string? ExperimentVariant;
        public readonly string StageName;

        public FanoutMaxKey(string flowName, string stageName, string? experimentLayer, string? experimentVariant)
        {
            FlowName = flowName;
            ExperimentLayer = experimentLayer;
            ExperimentVariant = experimentVariant;
            StageName = stageName;
        }

        public bool Equals(FanoutMaxKey other)
        {
            return string.Equals(FlowName, other.FlowName, StringComparison.Ordinal)
                && string.Equals(ExperimentLayer, other.ExperimentLayer, StringComparison.Ordinal)
                && string.Equals(ExperimentVariant, other.ExperimentVariant, StringComparison.Ordinal)
                && string.Equals(StageName, other.StageName, StringComparison.Ordinal);
        }

        public override bool Equals(object? obj)
        {
            return obj is FanoutMaxKey other && Equals(other);
        }

        public override int GetHashCode()
        {
            var hash = new HashCode();
            hash.Add(FlowName);
            hash.Add(ExperimentLayer);
            hash.Add(ExperimentVariant);
            hash.Add(StageName);
            return hash.ToHashCode();
        }
    }

    private readonly struct FanoutMaxInfo
    {
        public readonly JsonElement Value;
        public readonly int ExperimentIndex;

        public FanoutMaxInfo(JsonElement value, int experimentIndex)
        {
            Value = value;
            ExperimentIndex = experimentIndex;
        }
    }

    private readonly struct ModuleInfo
    {
        public readonly string Use;
        public readonly JsonElement With;
        public readonly JsonElement Gate;
        public readonly bool Enabled;
        public readonly int Priority;
        public readonly bool HasShadow;
        public readonly ushort ShadowSampleBps;
        public readonly int Index;
        public readonly int ExperimentIndex;

        public ModuleInfo(
            string use,
            JsonElement with,
            JsonElement gate,
            bool enabled,
            int priority,
            bool hasShadow,
            ushort shadowSampleBps,
            int index,
            int experimentIndex)
        {
            Use = use;
            With = with;
            Gate = gate;
            Enabled = enabled;
            Priority = priority;
            HasShadow = hasShadow;
            ShadowSampleBps = shadowSampleBps;
            Index = index;
            ExperimentIndex = experimentIndex;
        }
    }

    private struct ModuleDiffBuffer
    {
        private PatchModuleDiff[]? _items;
        private int _count;

        public void Add(PatchModuleDiff item)
        {
            var items = _items;

            if (items is null)
            {
                items = new PatchModuleDiff[4];
                _items = items;
            }
            else if ((uint)_count >= (uint)items.Length)
            {
                var newItems = new PatchModuleDiff[items.Length * 2];
                Array.Copy(items, 0, newItems, 0, items.Length);
                items = newItems;
                _items = items;
            }

            items[_count] = item;
            _count++;
        }

        public PatchModuleDiff[] ToArray()
        {
            if (_count == 0)
            {
                return Array.Empty<PatchModuleDiff>();
            }

            var items = _items!;

            if (_count == items.Length)
            {
                return items;
            }

            var trimmed = new PatchModuleDiff[_count];
            Array.Copy(items, 0, trimmed, 0, _count);
            return trimmed;
        }
    }

    private struct ParamDiffBuffer
    {
        private PatchParamDiff[]? _items;
        private int _count;

        public void Add(PatchParamDiff item)
        {
            var items = _items;

            if (items is null)
            {
                items = new PatchParamDiff[4];
                _items = items;
            }
            else if ((uint)_count >= (uint)items.Length)
            {
                var newItems = new PatchParamDiff[items.Length * 2];
                Array.Copy(items, 0, newItems, 0, items.Length);
                items = newItems;
                _items = items;
            }

            items[_count] = item;
            _count++;
        }

        public PatchParamDiff[] ToArray()
        {
            if (_count == 0)
            {
                return Array.Empty<PatchParamDiff>();
            }

            var items = _items!;

            if (_count == items.Length)
            {
                return items;
            }

            var trimmed = new PatchParamDiff[_count];
            Array.Copy(items, 0, trimmed, 0, _count);
            return trimmed;
        }
    }

    private struct FanoutMaxDiffBuffer
    {
        private PatchFanoutMaxDiff[]? _items;
        private int _count;

        public void Add(PatchFanoutMaxDiff item)
        {
            var items = _items;

            if (items is null)
            {
                items = new PatchFanoutMaxDiff[4];
                _items = items;
            }
            else if ((uint)_count >= (uint)items.Length)
            {
                var newItems = new PatchFanoutMaxDiff[items.Length * 2];
                Array.Copy(items, 0, newItems, 0, items.Length);
                items = newItems;
                _items = items;
            }

            items[_count] = item;
            _count++;
        }

        public PatchFanoutMaxDiff[] ToArray()
        {
            if (_count == 0)
            {
                return Array.Empty<PatchFanoutMaxDiff>();
            }

            var items = _items!;

            if (_count == items.Length)
            {
                return items;
            }

            var trimmed = new PatchFanoutMaxDiff[_count];
            Array.Copy(items, 0, trimmed, 0, _count);
            return trimmed;
        }
    }

    private sealed class PatchModuleDiffComparer : IComparer<PatchModuleDiff>
    {
        public static PatchModuleDiffComparer Instance { get; } = new();

        public int Compare(PatchModuleDiff x, PatchModuleDiff y)
        {
            var c = string.CompareOrdinal(x.FlowName, y.FlowName);
            if (c != 0)
            {
                return c;
            }

            c = string.CompareOrdinal(x.ExperimentLayer, y.ExperimentLayer);
            if (c != 0)
            {
                return c;
            }

            c = string.CompareOrdinal(x.ExperimentVariant, y.ExperimentVariant);
            if (c != 0)
            {
                return c;
            }

            c = string.CompareOrdinal(x.StageName, y.StageName);
            if (c != 0)
            {
                return c;
            }

            c = string.CompareOrdinal(x.ModuleId, y.ModuleId);
            if (c != 0)
            {
                return c;
            }

            c = string.CompareOrdinal(x.Path, y.Path);
            if (c != 0)
            {
                return c;
            }

            return ((int)x.Kind).CompareTo((int)y.Kind);
        }
    }

    private sealed class PatchParamDiffComparer : IComparer<PatchParamDiff>
    {
        public static PatchParamDiffComparer Instance { get; } = new();

        public int Compare(PatchParamDiff x, PatchParamDiff y)
        {
            var c = string.CompareOrdinal(x.FlowName, y.FlowName);
            if (c != 0)
            {
                return c;
            }

            c = string.CompareOrdinal(x.ExperimentLayer, y.ExperimentLayer);
            if (c != 0)
            {
                return c;
            }

            c = string.CompareOrdinal(x.ExperimentVariant, y.ExperimentVariant);
            if (c != 0)
            {
                return c;
            }

            c = string.CompareOrdinal(x.Path, y.Path);
            if (c != 0)
            {
                return c;
            }

            return ((int)x.Kind).CompareTo((int)y.Kind);
        }
    }

    private sealed class PatchFanoutMaxDiffComparer : IComparer<PatchFanoutMaxDiff>
    {
        public static PatchFanoutMaxDiffComparer Instance { get; } = new();

        public int Compare(PatchFanoutMaxDiff x, PatchFanoutMaxDiff y)
        {
            var c = string.CompareOrdinal(x.FlowName, y.FlowName);
            if (c != 0)
            {
                return c;
            }

            c = string.CompareOrdinal(x.ExperimentLayer, y.ExperimentLayer);
            if (c != 0)
            {
                return c;
            }

            c = string.CompareOrdinal(x.ExperimentVariant, y.ExperimentVariant);
            if (c != 0)
            {
                return c;
            }

            c = string.CompareOrdinal(x.StageName, y.StageName);
            if (c != 0)
            {
                return c;
            }

            c = string.CompareOrdinal(x.Path, y.Path);
            if (c != 0)
            {
                return c;
            }

            return ((int)x.Kind).CompareTo((int)y.Kind);
        }
    }

    private sealed class PatchEmergencyDiffComparer : IComparer<PatchEmergencyDiff>
    {
        public static PatchEmergencyDiffComparer Instance { get; } = new();

        public int Compare(PatchEmergencyDiff x, PatchEmergencyDiff y)
        {
            var c = string.CompareOrdinal(x.FlowName, y.FlowName);
            if (c != 0)
            {
                return c;
            }

            c = string.CompareOrdinal(x.Path, y.Path);
            if (c != 0)
            {
                return c;
            }

            return ((int)x.Kind).CompareTo((int)y.Kind);
        }
    }

    private struct EmergencyDiffBuffer
    {
        private PatchEmergencyDiff[]? _items;
        private int _count;

        public void Add(PatchEmergencyDiff item)
        {
            var items = _items;

            if (items is null)
            {
                items = new PatchEmergencyDiff[4];
                _items = items;
            }
            else if ((uint)_count >= (uint)items.Length)
            {
                var newItems = new PatchEmergencyDiff[items.Length * 2];
                Array.Copy(items, 0, newItems, 0, items.Length);
                items = newItems;
                _items = items;
            }

            items[_count] = item;
            _count++;
        }

        public PatchEmergencyDiff[] ToArray()
        {
            if (_count == 0)
            {
                return Array.Empty<PatchEmergencyDiff>();
            }

            var items = _items!;

            if (_count == items.Length)
            {
                return items;
            }

            var trimmed = new PatchEmergencyDiff[_count];
            Array.Copy(items, 0, trimmed, 0, _count);
            return trimmed;
        }
    }
}

public sealed class PatchModuleDiffReport
{
    public static PatchModuleDiffReport Empty { get; } = new(Array.Empty<PatchModuleDiff>());

    private readonly PatchModuleDiff[] _diffs;

    public IReadOnlyList<PatchModuleDiff> Diffs => _diffs;

    internal PatchModuleDiffReport(PatchModuleDiff[] diffs)
    {
        _diffs = diffs ?? throw new ArgumentNullException(nameof(diffs));
    }
}

public enum PatchModuleDiffKind
{
    Added = 1,
    Removed = 2,
    UseChanged = 3,
    WithChanged = 4,
    WithAdded = 5,
    WithRemoved = 6,
    GateAdded = 7,
    GateRemoved = 8,
    GateChanged = 9,
    EnabledChanged = 10,
    PriorityChanged = 11,
    ShadowAdded = 12,
    ShadowRemoved = 13,
    ShadowSampleChanged = 14,
}

public readonly struct PatchModuleDiff
{
    public PatchModuleDiffKind Kind { get; }

    public string FlowName { get; }

    public string? ExperimentLayer { get; }

    public string? ExperimentVariant { get; }

    public string StageName { get; }

    public string ModuleId { get; }

    public string Path { get; }

    private PatchModuleDiff(
        PatchModuleDiffKind kind,
        string flowName,
        string stageName,
        string moduleId,
        string path,
        string? experimentLayer,
        string? experimentVariant)
    {
        Kind = kind;
        FlowName = flowName;
        ExperimentLayer = experimentLayer;
        ExperimentVariant = experimentVariant;
        StageName = stageName;
        ModuleId = moduleId;
        Path = path;
    }

    internal static PatchModuleDiff CreateAdded(
        string flowName,
        string stageName,
        string moduleId,
        string path,
        string? experimentLayer = null,
        string? experimentVariant = null)
    {
        return new PatchModuleDiff(PatchModuleDiffKind.Added, flowName, stageName, moduleId, path, experimentLayer, experimentVariant);
    }

    internal static PatchModuleDiff CreateRemoved(
        string flowName,
        string stageName,
        string moduleId,
        string path,
        string? experimentLayer = null,
        string? experimentVariant = null)
    {
        return new PatchModuleDiff(PatchModuleDiffKind.Removed, flowName, stageName, moduleId, path, experimentLayer, experimentVariant);
    }

    internal static PatchModuleDiff CreateUseChanged(
        string flowName,
        string stageName,
        string moduleId,
        string path,
        string? experimentLayer = null,
        string? experimentVariant = null)
    {
        return new PatchModuleDiff(PatchModuleDiffKind.UseChanged, flowName, stageName, moduleId, path, experimentLayer, experimentVariant);
    }

    internal static PatchModuleDiff CreateEnabledChanged(
        string flowName,
        string stageName,
        string moduleId,
        string path,
        string? experimentLayer = null,
        string? experimentVariant = null)
    {
        return new PatchModuleDiff(PatchModuleDiffKind.EnabledChanged, flowName, stageName, moduleId, path, experimentLayer, experimentVariant);
    }

    internal static PatchModuleDiff CreatePriorityChanged(
        string flowName,
        string stageName,
        string moduleId,
        string path,
        string? experimentLayer = null,
        string? experimentVariant = null)
    {
        return new PatchModuleDiff(PatchModuleDiffKind.PriorityChanged, flowName, stageName, moduleId, path, experimentLayer, experimentVariant);
    }

    internal static PatchModuleDiff CreateWithChanged(
        string flowName,
        string stageName,
        string moduleId,
        string path,
        string? experimentLayer = null,
        string? experimentVariant = null)
    {
        return new PatchModuleDiff(PatchModuleDiffKind.WithChanged, flowName, stageName, moduleId, path, experimentLayer, experimentVariant);
    }

    internal static PatchModuleDiff CreateWithAdded(
        string flowName,
        string stageName,
        string moduleId,
        string path,
        string? experimentLayer = null,
        string? experimentVariant = null)
    {
        return new PatchModuleDiff(PatchModuleDiffKind.WithAdded, flowName, stageName, moduleId, path, experimentLayer, experimentVariant);
    }

    internal static PatchModuleDiff CreateWithRemoved(
        string flowName,
        string stageName,
        string moduleId,
        string path,
        string? experimentLayer = null,
        string? experimentVariant = null)
    {
        return new PatchModuleDiff(PatchModuleDiffKind.WithRemoved, flowName, stageName, moduleId, path, experimentLayer, experimentVariant);
    }

    internal static PatchModuleDiff CreateGateAdded(
        string flowName,
        string stageName,
        string moduleId,
        string path,
        string? experimentLayer = null,
        string? experimentVariant = null)
    {
        return new PatchModuleDiff(PatchModuleDiffKind.GateAdded, flowName, stageName, moduleId, path, experimentLayer, experimentVariant);
    }

    internal static PatchModuleDiff CreateGateRemoved(
        string flowName,
        string stageName,
        string moduleId,
        string path,
        string? experimentLayer = null,
        string? experimentVariant = null)
    {
        return new PatchModuleDiff(PatchModuleDiffKind.GateRemoved, flowName, stageName, moduleId, path, experimentLayer, experimentVariant);
    }

    internal static PatchModuleDiff CreateGateChanged(
        string flowName,
        string stageName,
        string moduleId,
        string path,
        string? experimentLayer = null,
        string? experimentVariant = null)
    {
        return new PatchModuleDiff(PatchModuleDiffKind.GateChanged, flowName, stageName, moduleId, path, experimentLayer, experimentVariant);
    }

    internal static PatchModuleDiff CreateShadowAdded(
        string flowName,
        string stageName,
        string moduleId,
        string path,
        string? experimentLayer = null,
        string? experimentVariant = null)
    {
        return new PatchModuleDiff(PatchModuleDiffKind.ShadowAdded, flowName, stageName, moduleId, path, experimentLayer, experimentVariant);
    }

    internal static PatchModuleDiff CreateShadowRemoved(
        string flowName,
        string stageName,
        string moduleId,
        string path,
        string? experimentLayer = null,
        string? experimentVariant = null)
    {
        return new PatchModuleDiff(PatchModuleDiffKind.ShadowRemoved, flowName, stageName, moduleId, path, experimentLayer, experimentVariant);
    }

    internal static PatchModuleDiff CreateShadowSampleChanged(
        string flowName,
        string stageName,
        string moduleId,
        string path,
        string? experimentLayer = null,
        string? experimentVariant = null)
    {
        return new PatchModuleDiff(PatchModuleDiffKind.ShadowSampleChanged, flowName, stageName, moduleId, path, experimentLayer, experimentVariant);
    }
}

public sealed class PatchFanoutMaxDiffReport
{
    public static PatchFanoutMaxDiffReport Empty { get; } = new(Array.Empty<PatchFanoutMaxDiff>());

    private readonly PatchFanoutMaxDiff[] _diffs;

    public IReadOnlyList<PatchFanoutMaxDiff> Diffs => _diffs;

    internal PatchFanoutMaxDiffReport(PatchFanoutMaxDiff[] diffs)
    {
        _diffs = diffs ?? throw new ArgumentNullException(nameof(diffs));
    }
}

public enum PatchFanoutMaxDiffKind
{
    Added = 1,
    Removed = 2,
    Changed = 3,
}

public readonly struct PatchFanoutMaxDiff
{
    public PatchFanoutMaxDiffKind Kind { get; }

    public string FlowName { get; }

    public string? ExperimentLayer { get; }

    public string? ExperimentVariant { get; }

    public string StageName { get; }

    public string Path { get; }

    private PatchFanoutMaxDiff(
        PatchFanoutMaxDiffKind kind,
        string flowName,
        string stageName,
        string path,
        string? experimentLayer,
        string? experimentVariant)
    {
        Kind = kind;
        FlowName = flowName;
        ExperimentLayer = experimentLayer;
        ExperimentVariant = experimentVariant;
        StageName = stageName;
        Path = path;
    }

    internal static PatchFanoutMaxDiff CreateAdded(
        string flowName,
        string stageName,
        string path,
        string? experimentLayer = null,
        string? experimentVariant = null)
    {
        return new PatchFanoutMaxDiff(PatchFanoutMaxDiffKind.Added, flowName, stageName, path, experimentLayer, experimentVariant);
    }

    internal static PatchFanoutMaxDiff CreateRemoved(
        string flowName,
        string stageName,
        string path,
        string? experimentLayer = null,
        string? experimentVariant = null)
    {
        return new PatchFanoutMaxDiff(PatchFanoutMaxDiffKind.Removed, flowName, stageName, path, experimentLayer, experimentVariant);
    }

    internal static PatchFanoutMaxDiff CreateChanged(
        string flowName,
        string stageName,
        string path,
        string? experimentLayer = null,
        string? experimentVariant = null)
    {
        return new PatchFanoutMaxDiff(PatchFanoutMaxDiffKind.Changed, flowName, stageName, path, experimentLayer, experimentVariant);
    }
}

public sealed class PatchParamDiffReport
{
    public static PatchParamDiffReport Empty { get; } = new(Array.Empty<PatchParamDiff>());

    private readonly PatchParamDiff[] _diffs;

    public IReadOnlyList<PatchParamDiff> Diffs => _diffs;

    internal PatchParamDiffReport(PatchParamDiff[] diffs)
    {
        _diffs = diffs ?? throw new ArgumentNullException(nameof(diffs));
    }
}

public enum PatchParamDiffKind
{
    Added = 1,
    Removed = 2,
    Changed = 3,
}

public readonly struct PatchParamDiff
{
    public PatchParamDiffKind Kind { get; }

    public string FlowName { get; }

    public string? ExperimentLayer { get; }

    public string? ExperimentVariant { get; }

    public string Path { get; }

    private PatchParamDiff(
        PatchParamDiffKind kind,
        string flowName,
        string path,
        string? experimentLayer,
        string? experimentVariant)
    {
        Kind = kind;
        FlowName = flowName;
        Path = path;
        ExperimentLayer = experimentLayer;
        ExperimentVariant = experimentVariant;
    }

    internal static PatchParamDiff CreateAdded(
        string flowName,
        string path,
        string? experimentLayer = null,
        string? experimentVariant = null)
    {
        return new PatchParamDiff(PatchParamDiffKind.Added, flowName, path, experimentLayer, experimentVariant);
    }

    internal static PatchParamDiff CreateRemoved(
        string flowName,
        string path,
        string? experimentLayer = null,
        string? experimentVariant = null)
    {
        return new PatchParamDiff(PatchParamDiffKind.Removed, flowName, path, experimentLayer, experimentVariant);
    }

    internal static PatchParamDiff CreateChanged(
        string flowName,
        string path,
        string? experimentLayer = null,
        string? experimentVariant = null)
    {
        return new PatchParamDiff(PatchParamDiffKind.Changed, flowName, path, experimentLayer, experimentVariant);
    }
}

public sealed class PatchEmergencyDiffReport
{
    public static PatchEmergencyDiffReport Empty { get; } = new(Array.Empty<PatchEmergencyDiff>());

    private readonly PatchEmergencyDiff[] _diffs;

    public IReadOnlyList<PatchEmergencyDiff> Diffs => _diffs;

    internal PatchEmergencyDiffReport(PatchEmergencyDiff[] diffs)
    {
        _diffs = diffs ?? throw new ArgumentNullException(nameof(diffs));
    }
}

public enum PatchEmergencyDiffKind
{
    Added = 1,
    Removed = 2,
    Changed = 3,
}

public readonly struct PatchEmergencyDiff
{
    public PatchEmergencyDiffKind Kind { get; }

    public string FlowName { get; }

    public string Path { get; }

    private PatchEmergencyDiff(PatchEmergencyDiffKind kind, string flowName, string path)
    {
        Kind = kind;
        FlowName = flowName;
        Path = path;
    }

    internal static PatchEmergencyDiff CreateAdded(string flowName, string path)
    {
        return new PatchEmergencyDiff(PatchEmergencyDiffKind.Added, flowName, path);
    }

    internal static PatchEmergencyDiff CreateRemoved(string flowName, string path)
    {
        return new PatchEmergencyDiff(PatchEmergencyDiffKind.Removed, flowName, path);
    }

    internal static PatchEmergencyDiff CreateChanged(string flowName, string path)
    {
        return new PatchEmergencyDiff(PatchEmergencyDiffKind.Changed, flowName, path);
    }
}
