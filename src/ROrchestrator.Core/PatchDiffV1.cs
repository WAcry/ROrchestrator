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

                        var oldHasGate = oldModule.Gate.ValueKind != JsonValueKind.Undefined;
                        var newHasGate = newModule.Gate.ValueKind != JsonValueKind.Undefined;

                        var isGateAdded = !oldHasGate && newHasGate;
                        var isGateRemoved = oldHasGate && !newHasGate;
                        var isGateChanged = oldHasGate && newHasGate && !JsonElementDeepEquals(oldModule.Gate, newModule.Gate);

                        if (isUseChanged || isWithChanged || isGateAdded || isGateRemoved || isGateChanged)
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

                JsonElement gateElement = default;

                if (moduleElement.TryGetProperty("gate", out var rawGateElement) && rawGateElement.ValueKind != JsonValueKind.Null)
                {
                    gateElement = rawGateElement;
                }

                moduleMap ??= new Dictionary<ModuleKey, ModuleInfo>(4);

                var key = new ModuleKey(flowName, stageName, moduleId, experimentLayer, experimentVariant);

                if (!moduleMap.TryAdd(key, new ModuleInfo(moduleUse, withElement, gateElement, index, experimentIndex)))
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

    private readonly struct ModuleInfo
    {
        public readonly string Use;
        public readonly JsonElement With;
        public readonly JsonElement Gate;
        public readonly int Index;
        public readonly int ExperimentIndex;

        public ModuleInfo(string use, JsonElement with, JsonElement gate, int index, int experimentIndex)
        {
            Use = use;
            With = with;
            Gate = gate;
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
