using System.Text.Json;
using System.Text.Json.Serialization;
using System.Reflection;

namespace ROrchestrator.Core;

public sealed class ConfigValidator
{
    private const string SupportedSchemaVersion = "v1";

    private const string CodeParseError = "CFG_PARSE_ERROR";
    private const string CodeSchemaVersionUnsupported = "CFG_SCHEMA_VERSION_UNSUPPORTED";
    private const string CodeUnknownField = "CFG_UNKNOWN_FIELD";
    private const string CodeFlowsNotObject = "CFG_FLOWS_NOT_OBJECT";
    private const string CodeFlowPatchNotObject = "CFG_FLOW_PATCH_NOT_OBJECT";
    private const string CodeFlowNotRegistered = "CFG_FLOW_NOT_REGISTERED";
    private const string CodeStagesNotObject = "CFG_STAGES_NOT_OBJECT";
    private const string CodeStagePatchNotObject = "CFG_STAGE_PATCH_NOT_OBJECT";
    private const string CodeStageNotInBlueprint = "CFG_STAGE_NOT_IN_BLUEPRINT";
    private const string CodeParamsBindFailed = "CFG_PARAMS_BIND_FAILED";
    private const string CodeParamsUnknownField = "CFG_PARAMS_UNKNOWN_FIELD";
    private const string CodeModulesNotArray = "CFG_MODULES_NOT_ARRAY";
    private const string CodeModuleIdMissing = "CFG_MODULE_ID_MISSING";
    private const string CodeModuleIdDuplicate = "CFG_MODULE_ID_DUPLICATE";
    private const string CodeModuleIdInvalidFormat = "CFG_MODULE_ID_INVALID_FORMAT";
    private const string CodeModuleTypeMissing = "CFG_MODULE_TYPE_MISSING";
    private const string CodeModuleTypeNotRegistered = "CFG_MODULE_TYPE_NOT_REGISTERED";
    private const string CodeModuleArgsMissing = "CFG_MODULE_ARGS_MISSING";
    private const string CodeModuleArgsBindFailed = "CFG_MODULE_ARGS_BIND_FAILED";

    private readonly FlowRegistry _flowRegistry;
    private readonly ModuleCatalog _moduleCatalog;

    public ConfigValidator(FlowRegistry flowRegistry, ModuleCatalog moduleCatalog)
    {
        _flowRegistry = flowRegistry ?? throw new ArgumentNullException(nameof(flowRegistry));
        _moduleCatalog = moduleCatalog ?? throw new ArgumentNullException(nameof(moduleCatalog));
    }

    public ValidationReport ValidatePatchJson(string patchJson)
    {
        if (patchJson is null)
        {
            throw new ArgumentNullException(nameof(patchJson));
        }

        JsonDocument document;

        try
        {
            document = JsonDocument.Parse(patchJson);
        }
        catch (JsonException ex)
        {
            return new ValidationReport(
                new[]
                {
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeParseError,
                        path: "$",
                        message: ex.Message),
                });
        }

        using (document)
        {
            var root = document.RootElement;

            if (root.ValueKind != JsonValueKind.Object)
            {
                return new ValidationReport(
                    new[]
                    {
                        CreateSchemaVersionUnsupportedFinding(),
                    });
            }

            var findings = new FindingBuffer();

            var hasSchemaVersion = false;
            var schemaVersionSupported = false;

            var hasFlows = false;
            JsonElement flowsElement = default;

            foreach (var property in root.EnumerateObject())
            {
                if (property.NameEquals("schemaVersion"))
                {
                    hasSchemaVersion = true;

                    if (property.Value.ValueKind == JsonValueKind.String && property.Value.ValueEquals(SupportedSchemaVersion))
                    {
                        schemaVersionSupported = true;
                    }

                    continue;
                }

                if (property.NameEquals("flows"))
                {
                    hasFlows = true;
                    flowsElement = property.Value;
                    continue;
                }

                var name = property.Name;
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeUnknownField,
                        path: string.Concat("$.", name),
                        message: string.Concat("Unknown field: ", name)));
            }

            if (!hasSchemaVersion || !schemaVersionSupported)
            {
                findings.Add(CreateSchemaVersionUnsupportedFinding());
            }

            if (hasFlows && flowsElement.ValueKind != JsonValueKind.Object)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeFlowsNotObject,
                        path: "$.flows",
                        message: "flows must be an object."));
            }
            else if (hasFlows && flowsElement.ValueKind == JsonValueKind.Object)
            {
                foreach (var flowProperty in flowsElement.EnumerateObject())
                {
                    var flowName = flowProperty.Name;
                    string[] blueprintStageNameSet = Array.Empty<string>();
                    var flowRegistered = false;
                    Type? patchType = null;

                    if (flowName.Length != 0)
                    {
                        flowRegistered = _flowRegistry.TryGetStageNameSetAndPatchType(flowName, out blueprintStageNameSet, out patchType);
                    }

                    if (!flowRegistered)
                    {
                        findings.Add(
                            new ValidationFinding(
                                ValidationSeverity.Error,
                                code: CodeFlowNotRegistered,
                                path: string.Concat("$.flows.", flowName),
                                message: string.Concat("Flow is not registered: ", flowName)));
                    }

                    if (flowProperty.Value.ValueKind != JsonValueKind.Object)
                    {
                        findings.Add(
                            new ValidationFinding(
                                ValidationSeverity.Error,
                                code: CodeFlowPatchNotObject,
                                path: string.Concat("$.flows.", flowName),
                                message: "Flow patch must be an object."));
                        continue;
                    }

                    ValidateFlowPatchTopLevelFields(
                        flowName,
                        flowProperty.Value,
                        ref findings,
                        out var hasStagesPatch,
                        out var stagesPatch,
                        out var paramsPatch,
                        out var hasParamsPatch);

                    if (flowRegistered)
                    {
                        ValidateFlowParamsPatch(flowName, hasParamsPatch, paramsPatch, patchType, ref findings);
                    }

                    if (hasStagesPatch && stagesPatch.ValueKind != JsonValueKind.Object)
                    {
                        findings.Add(
                            new ValidationFinding(
                                ValidationSeverity.Error,
                                code: CodeStagesNotObject,
                                path: string.Concat("$.flows.", flowName, ".stages"),
                                message: "stages must be an object."));
                    }
                    else if (flowRegistered && stagesPatch.ValueKind == JsonValueKind.Object)
                    {
                        ValidateStagePatches(flowName, stagesPatch, blueprintStageNameSet, _moduleCatalog, ref findings);
                    }
                }
            }

            return findings.IsEmpty ? ValidationReport.Empty : new ValidationReport(findings.ToArray());
        }
    }

    private static void ValidateFlowPatchTopLevelFields(
        string flowName,
        JsonElement flowPatch,
        ref FindingBuffer findings,
        out bool hasStagesPatch,
        out JsonElement stagesPatch,
        out JsonElement paramsPatch,
        out bool hasParamsPatch)
    {
        hasStagesPatch = false;
        stagesPatch = default;
        paramsPatch = default;
        hasParamsPatch = false;

        foreach (var flowField in flowPatch.EnumerateObject())
        {
            if (flowField.NameEquals("params"))
            {
                hasParamsPatch = true;
                paramsPatch = flowField.Value;
                continue;
            }

            if (flowField.NameEquals("experiments")
                || flowField.NameEquals("emergency"))
            {
                continue;
            }

            if (flowField.NameEquals("stages"))
            {
                hasStagesPatch = true;
                stagesPatch = flowField.Value;
                continue;
            }

            var fieldName = flowField.Name;

            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeUnknownField,
                    path: string.Concat("$.flows.", flowName, ".", fieldName),
                    message: string.Concat("Unknown field: ", fieldName)));
        }
    }

    private static void ValidateFlowParamsPatch(
        string flowName,
        bool hasParamsPatch,
        JsonElement paramsPatch,
        Type? patchType,
        ref FindingBuffer findings)
    {
        if (!hasParamsPatch)
        {
            return;
        }

        var paramsPath = string.Concat("$.flows.", flowName, ".params");

        if (patchType is null)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeParamsBindFailed,
                    path: paramsPath,
                    message: "params is not allowed for this flow."));
            return;
        }

        if (paramsPatch.ValueKind == JsonValueKind.Object
            && patchType != typeof(JsonElement)
            && patchType != typeof(JsonDocument)
            && !typeof(System.Collections.IDictionary).IsAssignableFrom(patchType))
        {
            var knownFieldNameSet = BuildJsonPropertyNameSet(patchType);

            foreach (var property in paramsPatch.EnumerateObject())
            {
                var name = property.Name;

                if (NameSetContains(knownFieldNameSet, name))
                {
                    continue;
                }

                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeParamsUnknownField,
                        path: string.Concat(paramsPath, ".", name),
                        message: string.Concat("Unknown params field: ", name)));
            }
        }

        if (paramsPatch.ValueKind == JsonValueKind.Null)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeParamsBindFailed,
                    path: paramsPath,
                    message: "params must not be null."));
            return;
        }

        try
        {
            _ = paramsPatch.Deserialize(patchType);
        }
        catch (JsonException ex)
        {
            var path = BuildParamsBindFailedPath(paramsPath, ex.Path);

            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeParamsBindFailed,
                    path: path,
                    message: ex.Message));
        }
        catch (NotSupportedException ex)
        {
            var message = ex.Message;
            if (string.IsNullOrEmpty(message))
            {
                message = "params binding is not supported.";
            }

            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeParamsBindFailed,
                    path: paramsPath,
                    message: message));
        }
        catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
        {
            var message = ex.Message;
            if (string.IsNullOrEmpty(message))
            {
                message = "params binding failed.";
            }

            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeParamsBindFailed,
                    path: paramsPath,
                    message: message));
        }
    }

    private static void ValidateStagePatches(
        string flowName,
        JsonElement stagesPatch,
        string[] blueprintStageNameSet,
        ModuleCatalog moduleCatalog,
        ref FindingBuffer findings)
    {
        foreach (var stageProperty in stagesPatch.EnumerateObject())
        {
            var stageName = stageProperty.Name;
            var stageNameInBlueprint = StageNameSetContains(blueprintStageNameSet, stageName);

            if (!stageNameInBlueprint)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeStageNotInBlueprint,
                        path: string.Concat("$.flows.", flowName, ".stages.", stageName),
                        message: string.Concat("Stage is not in blueprint: ", stageName)));
            }

            if (stageProperty.Value.ValueKind != JsonValueKind.Object)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeStagePatchNotObject,
                        path: string.Concat("$.flows.", flowName, ".stages.", stageName),
                        message: "Stage patch must be an object."));
                continue;
            }

            if (!stageNameInBlueprint)
            {
                continue;
            }

            ValidateStagePatch(flowName, stageName, stageProperty.Value, moduleCatalog, ref findings);
        }
    }

    private static void ValidateStagePatch(
        string flowName,
        string stageName,
        JsonElement stagePatch,
        ModuleCatalog moduleCatalog,
        ref FindingBuffer findings)
    {
        var hasModules = false;
        JsonElement modulesPatch = default;

        foreach (var stageField in stagePatch.EnumerateObject())
        {
            if (stageField.NameEquals("modules"))
            {
                hasModules = true;
                modulesPatch = stageField.Value;
                continue;
            }

            var fieldName = stageField.Name;
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeUnknownField,
                    path: string.Concat("$.flows.", flowName, ".stages.", stageName, ".", fieldName),
                    message: string.Concat("Unknown field: ", fieldName)));
        }

        if (!hasModules)
        {
            return;
        }

        var modulesPathPrefix = string.Concat("$.flows.", flowName, ".stages.", stageName, ".modules");

        if (modulesPatch.ValueKind != JsonValueKind.Array)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeModulesNotArray,
                    path: modulesPathPrefix,
                    message: "modules must be an array."));
            return;
        }

        ValidateModulesPatch(flowName, stageName, modulesPathPrefix, modulesPatch, moduleCatalog, ref findings);
    }

    private static void ValidateModulesPatch(
        string flowName,
        string stageName,
        string modulesPathPrefix,
        JsonElement modulesPatch,
        ModuleCatalog moduleCatalog,
        ref FindingBuffer findings)
    {
        _ = flowName;
        _ = stageName;

        Dictionary<string, int>? moduleIdIndexMap = null;

        var index = 0;

        foreach (var modulePatch in modulesPatch.EnumerateArray())
        {
            if (modulePatch.ValueKind != JsonValueKind.Object)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeModulesNotArray,
                        path: string.Concat(modulesPathPrefix, "[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "]"),
                        message: "modules must be an array of objects."));
                index++;
                continue;
            }

            string? moduleId = null;
            string? moduleUse = null;
            var hasModuleWith = false;
            JsonElement moduleWith = default;

            foreach (var moduleField in modulePatch.EnumerateObject())
            {
                if (moduleField.NameEquals("id"))
                {
                    if (moduleField.Value.ValueKind == JsonValueKind.String)
                    {
                        moduleId = moduleField.Value.GetString();
                    }

                    continue;
                }

                if (moduleField.NameEquals("use"))
                {
                    if (moduleField.Value.ValueKind == JsonValueKind.String)
                    {
                        moduleUse = moduleField.Value.GetString();
                    }

                    continue;
                }

                if (moduleField.NameEquals("with"))
                {
                    hasModuleWith = true;
                    moduleWith = moduleField.Value;
                    continue;
                }

                var fieldName = moduleField.Name;
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeUnknownField,
                        path: string.Concat(
                            modulesPathPrefix,
                            "[",
                            index.ToString(System.Globalization.CultureInfo.InvariantCulture),
                            "].",
                            fieldName),
                        message: string.Concat("Unknown field: ", fieldName)));
            }

            if (string.IsNullOrEmpty(moduleId))
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeModuleIdMissing,
                        path: string.Concat(modulesPathPrefix, "[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "].id"),
                        message: "modules[].id is required."));
            }
            else
            {
                if (!IsValidModuleIdFormat(moduleId))
                {
                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Warn,
                            code: CodeModuleIdInvalidFormat,
                            path: string.Concat(modulesPathPrefix, "[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "].id"),
                            message: "modules[].id must match [a-z0-9_]+ and length <= 64."));
                }

                moduleIdIndexMap ??= new Dictionary<string, int>();

                if (moduleIdIndexMap.TryGetValue(moduleId, out var firstIndex))
                {
                    var normalizedFirstIndex = firstIndex;
                    if (normalizedFirstIndex < 0)
                    {
                        normalizedFirstIndex = -normalizedFirstIndex - 1;
                    }

                    if (firstIndex >= 0)
                    {
                        findings.Add(
                            new ValidationFinding(
                                ValidationSeverity.Error,
                                code: CodeModuleIdDuplicate,
                                path: string.Concat(modulesPathPrefix, "[", normalizedFirstIndex.ToString(System.Globalization.CultureInfo.InvariantCulture), "].id"),
                                message: string.Concat("Duplicate module id: ", moduleId)));

                        moduleIdIndexMap[moduleId] = -normalizedFirstIndex - 1;
                    }

                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Error,
                            code: CodeModuleIdDuplicate,
                            path: string.Concat(modulesPathPrefix, "[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "].id"),
                            message: string.Concat("Duplicate module id: ", moduleId)));
                }
                else
                {
                    moduleIdIndexMap.Add(moduleId, index);
                }
            }

            var moduleArgsType = (Type?)null;

            if (string.IsNullOrEmpty(moduleUse))
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeModuleTypeMissing,
                        path: string.Concat(modulesPathPrefix, "[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "].use"),
                        message: "modules[].use is required."));
            }
            else
            {
                if (!moduleCatalog.TryGetSignature(moduleUse, out var argsType, out _))
                {
                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Error,
                            code: CodeModuleTypeNotRegistered,
                            path: string.Concat(modulesPathPrefix, "[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "].use"),
                            message: string.Concat("Module type is not registered: ", moduleUse)));
                }
                else
                {
                    moduleArgsType = argsType;
                }
            }

            var moduleWithPath = string.Concat(modulesPathPrefix, "[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "].with");

            if (!hasModuleWith || moduleWith.ValueKind == JsonValueKind.Null)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeModuleArgsMissing,
                        path: moduleWithPath,
                        message: "modules[].with is required."));
            }
            else if (moduleArgsType is not null)
            {
                try
                {
                    _ = moduleWith.Deserialize(moduleArgsType);
                }
                catch (JsonException ex)
                {
                    var path = BuildParamsBindFailedPath(moduleWithPath, ex.Path);

                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Error,
                            code: CodeModuleArgsBindFailed,
                            path: path,
                            message: ex.Message));
                }
                catch (NotSupportedException ex)
                {
                    var message = ex.Message;
                    if (string.IsNullOrEmpty(message))
                    {
                        message = "module args binding is not supported.";
                    }

                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Error,
                            code: CodeModuleArgsBindFailed,
                            path: moduleWithPath,
                            message: message));
                }
                catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
                {
                    var message = ex.Message;
                    if (string.IsNullOrEmpty(message))
                    {
                        message = "module args binding failed.";
                    }

                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Error,
                            code: CodeModuleArgsBindFailed,
                            path: moduleWithPath,
                            message: message));
                }
            }

            index++;
        }
    }

    private static bool IsValidModuleIdFormat(string moduleId)
    {
        if (moduleId.Length == 0 || moduleId.Length > 64)
        {
            return false;
        }

        for (var i = 0; i < moduleId.Length; i++)
        {
            var c = moduleId[i];

            if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')
            {
                continue;
            }

            return false;
        }

        return true;
    }

    private static bool StageNameSetContains(string[] blueprintStageNameSet, string stageName)
    {
        for (var i = 0; i < blueprintStageNameSet.Length; i++)
        {
            if (string.Equals(blueprintStageNameSet[i], stageName, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static bool NameSetContains(string[] nameSet, string name)
    {
        for (var i = 0; i < nameSet.Length; i++)
        {
            if (string.Equals(nameSet[i], name, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static string[] BuildJsonPropertyNameSet(Type type)
    {
        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
        if (properties.Length == 0)
        {
            return Array.Empty<string>();
        }

        var buffer = new string[properties.Length];
        var count = 0;

        for (var i = 0; i < properties.Length; i++)
        {
            var property = properties[i];

            if (property.GetIndexParameters().Length != 0)
            {
                continue;
            }

            var ignoreAttribute = property.GetCustomAttribute<JsonIgnoreAttribute>(inherit: true);
            if (ignoreAttribute is not null && ignoreAttribute.Condition == JsonIgnoreCondition.Always)
            {
                continue;
            }

            var jsonNameAttribute = property.GetCustomAttribute<JsonPropertyNameAttribute>(inherit: true);
            var name = jsonNameAttribute is null ? property.Name : jsonNameAttribute.Name;

            if (string.IsNullOrEmpty(name))
            {
                continue;
            }

            if (count != 0)
            {
                var found = false;

                for (var j = 0; j < count; j++)
                {
                    if (string.Equals(buffer[j], name, StringComparison.Ordinal))
                    {
                        found = true;
                        break;
                    }
                }

                if (found)
                {
                    continue;
                }
            }

            buffer[count] = name;
            count++;
        }

        if (count == 0)
        {
            return Array.Empty<string>();
        }

        if (count == buffer.Length)
        {
            return buffer;
        }

        var trimmed = new string[count];
        Array.Copy(buffer, 0, trimmed, 0, count);
        return trimmed;
    }

    private static string BuildParamsBindFailedPath(string paramsPath, string? exceptionPath)
    {
        if (string.IsNullOrEmpty(exceptionPath) || string.Equals(exceptionPath, "$", StringComparison.Ordinal))
        {
            return paramsPath;
        }

        if (exceptionPath![0] != '$')
        {
            return paramsPath;
        }

        return string.Concat(paramsPath.AsSpan(), exceptionPath.AsSpan(1));
    }

    private static ValidationFinding CreateSchemaVersionUnsupportedFinding()
    {
        return new ValidationFinding(
            ValidationSeverity.Error,
            code: CodeSchemaVersionUnsupported,
            path: "$.schemaVersion",
            message: "schemaVersion is missing or unsupported.");
    }

    private struct FindingBuffer
    {
        private ValidationFinding[]? _items;
        private int _count;

        public bool IsEmpty => _count == 0;

        public void Add(ValidationFinding item)
        {
            var items = _items;

            if (items is null)
            {
                items = new ValidationFinding[4];
                _items = items;
            }
            else if ((uint)_count >= (uint)items.Length)
            {
                var newItems = new ValidationFinding[items.Length * 2];
                Array.Copy(items, 0, newItems, 0, items.Length);
                items = newItems;
                _items = items;
            }

            items[_count] = item;
            _count++;
        }

        public ValidationFinding[] ToArray()
        {
            if (_count == 0)
            {
                return Array.Empty<ValidationFinding>();
            }

            var items = _items!;

            if (_count == items.Length)
            {
                return items;
            }

            var trimmed = new ValidationFinding[_count];
            Array.Copy(items, 0, trimmed, 0, _count);
            return trimmed;
        }
    }
}
