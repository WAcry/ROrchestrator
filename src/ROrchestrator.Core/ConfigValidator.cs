using System.Text.Json;
using System.Text.Json.Serialization;
using System.Reflection;
using ROrchestrator.Core.Gates;

namespace ROrchestrator.Core;

public sealed class ConfigValidator
{
    private const string SupportedSchemaVersion = "v1";

    private const string CodeParseError = "CFG_PARSE_ERROR";
    private const string CodeSchemaVersionUnsupported = "CFG_SCHEMA_VERSION_UNSUPPORTED";
    private const string CodeUnknownField = "CFG_UNKNOWN_FIELD";
    private const string CodeFanoutMaxInvalid = "CFG_FANOUT_MAX_INVALID";
    private const string CodeFanoutMaxExceeded = "CFG_FANOUT_MAX_EXCEEDED";
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
    private const string CodeModuleArgsUnknownField = "CFG_MODULE_ARGS_UNKNOWN_FIELD";
    private const string CodePriorityInvalid = "CFG_PRIORITY_INVALID";
    private const string CodeExperimentMappingInvalid = "CFG_EXPERIMENT_MAPPING_INVALID";
    private const string CodeExperimentMappingDuplicate = "CFG_EXPERIMENT_MAPPING_DUPLICATE";
    private const string CodeExperimentPatchInvalid = "CFG_EXPERIMENT_PATCH_INVALID";
    private const string CodeGateRedundant = "CFG_GATE_REDUNDANT";
    private const string GateCodePrefix = "CFG_GATE_";

    private const int MaxAllowedFanoutMax = 8;
    private const int MinModulePriority = -1000;
    private const int MaxModulePriority = 1000;

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
                        out var hasParamsPatch,
                        out var hasExperimentsPatch,
                        out var experimentsPatch);

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

                    ValidateExperiments(
                        flowName,
                        hasExperimentsPatch,
                        experimentsPatch,
                        patchType,
                        blueprintStageNameSet,
                        _moduleCatalog,
                        flowRegistered,
                        ref findings);
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
        out bool hasParamsPatch,
        out bool hasExperimentsPatch,
        out JsonElement experimentsPatch)
    {
        hasStagesPatch = false;
        stagesPatch = default;
        paramsPatch = default;
        hasParamsPatch = false;
        hasExperimentsPatch = false;
        experimentsPatch = default;

        foreach (var flowField in flowPatch.EnumerateObject())
        {
            if (flowField.NameEquals("params"))
            {
                hasParamsPatch = true;
                paramsPatch = flowField.Value;
                continue;
            }

            if (flowField.NameEquals("experiments"))
            {
                hasExperimentsPatch = true;
                experimentsPatch = flowField.Value;
                continue;
            }

            if (flowField.NameEquals("emergency"))
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

    private static void ValidateExperiments(
        string flowName,
        bool hasExperimentsPatch,
        JsonElement experimentsPatch,
        Type? patchType,
        string[] blueprintStageNameSet,
        ModuleCatalog moduleCatalog,
        bool flowRegistered,
        ref FindingBuffer findings)
    {
        if (!hasExperimentsPatch)
        {
            return;
        }

        var experimentsPathPrefix = string.Concat("$.flows.", flowName, ".experiments");

        if (experimentsPatch.ValueKind != JsonValueKind.Array)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeExperimentMappingInvalid,
                    path: experimentsPathPrefix,
                    message: "experiments must be an array."));
            return;
        }

        Dictionary<(string Layer, string Variant), int>? experimentKeyIndexMap = null;

        var index = 0;

        foreach (var experimentMapping in experimentsPatch.EnumerateArray())
        {
            var indexString = index.ToString(System.Globalization.CultureInfo.InvariantCulture);
            var itemPathPrefix = string.Concat(experimentsPathPrefix, "[", indexString, "]");

            if (experimentMapping.ValueKind != JsonValueKind.Object)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeExperimentMappingInvalid,
                        path: itemPathPrefix,
                        message: "experiments must be an array of objects."));
                index++;
                continue;
            }

            string? layer = null;
            var hasLayer = false;

            string? variant = null;
            var hasVariant = false;

            var hasPatch = false;
            JsonElement patch = default;

            foreach (var field in experimentMapping.EnumerateObject())
            {
                if (field.NameEquals("layer"))
                {
                    hasLayer = true;

                    if (field.Value.ValueKind == JsonValueKind.String)
                    {
                        layer = field.Value.GetString();
                    }
                    else
                    {
                        layer = null;
                    }

                    continue;
                }

                if (field.NameEquals("variant"))
                {
                    hasVariant = true;

                    if (field.Value.ValueKind == JsonValueKind.String)
                    {
                        variant = field.Value.GetString();
                    }
                    else
                    {
                        variant = null;
                    }

                    continue;
                }

                if (field.NameEquals("patch"))
                {
                    hasPatch = true;
                    patch = field.Value;
                    continue;
                }

                var fieldName = field.Name;
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeUnknownField,
                        path: string.Concat(itemPathPrefix, ".", fieldName),
                        message: string.Concat("Unknown field: ", fieldName)));
            }

            if (!hasLayer || string.IsNullOrEmpty(layer))
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeExperimentMappingInvalid,
                        path: string.Concat(itemPathPrefix, ".layer"),
                        message: "experiments[].layer is required and must be a non-empty string."));
            }

            if (!hasVariant || string.IsNullOrEmpty(variant))
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeExperimentMappingInvalid,
                        path: string.Concat(itemPathPrefix, ".variant"),
                        message: "experiments[].variant is required and must be a non-empty string."));
            }

            if (hasLayer
                && hasVariant
                && !string.IsNullOrEmpty(layer)
                && !string.IsNullOrEmpty(variant))
            {
                experimentKeyIndexMap ??= new Dictionary<(string Layer, string Variant), int>();

                var key = (layer: layer!, variant: variant!);

                if (experimentKeyIndexMap.TryGetValue(key, out var firstIndex))
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
                                code: CodeExperimentMappingDuplicate,
                                path: string.Concat(
                                    experimentsPathPrefix,
                                    "[",
                                    normalizedFirstIndex.ToString(System.Globalization.CultureInfo.InvariantCulture),
                                    "]"),
                                message: string.Concat("Duplicate experiment mapping: ", layer, " / ", variant)));

                        experimentKeyIndexMap[key] = -normalizedFirstIndex - 1;
                    }

                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Error,
                            code: CodeExperimentMappingDuplicate,
                            path: itemPathPrefix,
                            message: string.Concat("Duplicate experiment mapping: ", layer, " / ", variant)));
                }
                else
                {
                    experimentKeyIndexMap.Add(key, index);
                }
            }

            var patchPath = string.Concat(itemPathPrefix, ".patch");

            if (!hasPatch)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeExperimentPatchInvalid,
                        path: patchPath,
                        message: "experiments[].patch is required."));
                index++;
                continue;
            }

            if (patch.ValueKind != JsonValueKind.Object)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeExperimentPatchInvalid,
                        path: patchPath,
                        message: "experiments[].patch must be a non-null object."));
                index++;
                continue;
            }

            if (flowRegistered)
            {
                var patchFlowName = string.Concat(flowName, ".experiments[", indexString, "].patch");
                ValidateExperimentPatch(patchFlowName, patchType, blueprintStageNameSet, moduleCatalog, patch, ref findings);
            }

            index++;
        }
    }

    private static void ValidateExperimentPatch(
        string patchFlowName,
        Type? patchType,
        string[] blueprintStageNameSet,
        ModuleCatalog moduleCatalog,
        JsonElement patch,
        ref FindingBuffer findings)
    {
        var patchFindings = new FindingBuffer();

        ValidateFlowPatchTopLevelFields(
            patchFlowName,
            patch,
            ref patchFindings,
            out var hasStagesPatch,
            out var stagesPatch,
            out var paramsPatch,
            out var hasParamsPatch,
            out var hasExperimentsPatch,
            out var experimentsPatch);

        ValidateFlowParamsPatch(patchFlowName, hasParamsPatch, paramsPatch, patchType, ref patchFindings);

        if (hasStagesPatch && stagesPatch.ValueKind != JsonValueKind.Object)
        {
            patchFindings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeStagesNotObject,
                    path: string.Concat("$.flows.", patchFlowName, ".stages"),
                    message: "stages must be an object."));
        }
        else if (stagesPatch.ValueKind == JsonValueKind.Object)
        {
            ValidateStagePatches(patchFlowName, stagesPatch, blueprintStageNameSet, moduleCatalog, ref patchFindings);
        }

        ValidateExperiments(
            patchFlowName,
            hasExperimentsPatch,
            experimentsPatch,
            patchType,
            blueprintStageNameSet,
            moduleCatalog,
            flowRegistered: true,
            ref patchFindings);

        if (patchFindings.IsEmpty)
        {
            return;
        }

        var items = patchFindings.ToArray();

        for (var i = 0; i < items.Length; i++)
        {
            var item = items[i];

            if (item.Severity == ValidationSeverity.Error)
            {
                if (item.Code.StartsWith(GateCodePrefix, StringComparison.Ordinal)
                    || string.Equals(item.Code, CodeFanoutMaxInvalid, StringComparison.Ordinal)
                    || string.Equals(item.Code, CodeFanoutMaxExceeded, StringComparison.Ordinal))
                {
                    findings.Add(item);
                    continue;
                }

                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeExperimentPatchInvalid,
                        path: item.Path,
                        message: item.Message));
            }
            else
            {
                findings.Add(item);
            }
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
        Dictionary<string, ModuleIdFirstOccurrence>? moduleIdFirstOccurrenceMap = null;

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

            ValidateStagePatch(flowName, stageName, stageProperty.Value, moduleCatalog, ref findings, ref moduleIdFirstOccurrenceMap);
        }
    }

    private static void ValidateStagePatch(
        string flowName,
        string stageName,
        JsonElement stagePatch,
        ModuleCatalog moduleCatalog,
        ref FindingBuffer findings,
        ref Dictionary<string, ModuleIdFirstOccurrence>? moduleIdFirstOccurrenceMap)
    {
        var hasModules = false;
        JsonElement modulesPatch = default;
        var hasFanoutMax = false;
        JsonElement fanoutMax = default;

        foreach (var stageField in stagePatch.EnumerateObject())
        {
            if (stageField.NameEquals("modules"))
            {
                hasModules = true;
                modulesPatch = stageField.Value;
                continue;
            }

            if (stageField.NameEquals("fanoutMax"))
            {
                hasFanoutMax = true;
                fanoutMax = stageField.Value;
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

        if (hasFanoutMax)
        {
            ValidateFanoutMax(flowName, stageName, fanoutMax, ref findings);
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

        ValidateModulesPatch(flowName, stageName, modulesPathPrefix, modulesPatch, moduleCatalog, ref findings, ref moduleIdFirstOccurrenceMap);
    }

    private static void ValidateFanoutMax(
        string flowName,
        string stageName,
        JsonElement fanoutMax,
        ref FindingBuffer findings)
    {
        var path = string.Concat("$.flows.", flowName, ".stages.", stageName, ".fanoutMax");

        if (fanoutMax.ValueKind != JsonValueKind.Number)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeFanoutMaxInvalid,
                    path: path,
                    message: "fanoutMax must be a non-negative integer."));
            return;
        }

        if (fanoutMax.TryGetInt32(out var value32))
        {
            if (value32 < 0)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeFanoutMaxInvalid,
                        path: path,
                        message: "fanoutMax must be a non-negative integer."));
                return;
            }

            if (value32 > MaxAllowedFanoutMax)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeFanoutMaxExceeded,
                        path: path,
                        message: string.Concat(
                            "fanoutMax=",
                            value32.ToString(System.Globalization.CultureInfo.InvariantCulture),
                            " exceeds maxAllowed=",
                            MaxAllowedFanoutMax.ToString(System.Globalization.CultureInfo.InvariantCulture),
                            ".")));
                return;
            }

            return;
        }

        if (fanoutMax.TryGetInt64(out var value64))
        {
            if (value64 < 0)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeFanoutMaxInvalid,
                        path: path,
                        message: "fanoutMax must be a non-negative integer."));
                return;
            }

            if (value64 > MaxAllowedFanoutMax)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeFanoutMaxExceeded,
                        path: path,
                        message: string.Concat(
                            "fanoutMax=",
                            value64.ToString(System.Globalization.CultureInfo.InvariantCulture),
                            " exceeds maxAllowed=",
                            MaxAllowedFanoutMax.ToString(System.Globalization.CultureInfo.InvariantCulture),
                            ".")));
                return;
            }

            return;
        }

        findings.Add(
            new ValidationFinding(
                ValidationSeverity.Error,
                code: CodeFanoutMaxInvalid,
                path: path,
                message: "fanoutMax must be a non-negative integer."));
    }

    private static void ValidateModulesPatch(
        string flowName,
        string stageName,
        string modulesPathPrefix,
        JsonElement modulesPatch,
        ModuleCatalog moduleCatalog,
        ref FindingBuffer findings,
        ref Dictionary<string, ModuleIdFirstOccurrence>? moduleIdFirstOccurrenceMap)
    {
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
            var hasModuleGate = false;
            JsonElement moduleGate = default;
            var hasModuleEnabled = false;
            var moduleEnabled = true;
            var hasModulePriority = false;
            JsonElement modulePriority = default;

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

                if (moduleField.NameEquals("enabled"))
                {
                    hasModuleEnabled = true;

                    if (moduleField.Value.ValueKind == JsonValueKind.True || moduleField.Value.ValueKind == JsonValueKind.False)
                    {
                        moduleEnabled = moduleField.Value.GetBoolean();
                    }

                    continue;
                }

                if (moduleField.NameEquals("priority"))
                {
                    hasModulePriority = true;
                    modulePriority = moduleField.Value;
                    continue;
                }

                if (moduleField.NameEquals("gate"))
                {
                    hasModuleGate = true;
                    moduleGate = moduleField.Value;
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

            if (hasModulePriority)
            {
                if (modulePriority.ValueKind != JsonValueKind.Number
                    || !modulePriority.TryGetInt32(out var priority)
                    || priority < MinModulePriority
                    || priority > MaxModulePriority)
                {
                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Warn,
                            code: CodePriorityInvalid,
                            path: string.Concat(modulesPathPrefix, "[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "].priority"),
                            message: string.Concat(
                                "modules[].priority must be an integer in range ",
                                MinModulePriority.ToString(System.Globalization.CultureInfo.InvariantCulture),
                                "..",
                                MaxModulePriority.ToString(System.Globalization.CultureInfo.InvariantCulture),
                                ".")));
                }
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

                moduleIdFirstOccurrenceMap ??= new Dictionary<string, ModuleIdFirstOccurrence>();

                if (moduleIdFirstOccurrenceMap.TryGetValue(moduleId, out var firstOccurrence))
                {
                    if (!string.Equals(firstOccurrence.StageName, stageName, StringComparison.Ordinal))
                    {
                        var normalizedFirstIndex = firstOccurrence.Index;
                        if (normalizedFirstIndex < 0)
                        {
                            normalizedFirstIndex = -normalizedFirstIndex - 1;
                        }

                        if (firstOccurrence.Index >= 0)
                        {
                            findings.Add(
                                new ValidationFinding(
                                    ValidationSeverity.Error,
                                    code: CodeModuleIdDuplicate,
                                    path: string.Concat(
                                        "$.flows.",
                                        flowName,
                                        ".stages.",
                                        firstOccurrence.StageName,
                                        ".modules[",
                                        normalizedFirstIndex.ToString(System.Globalization.CultureInfo.InvariantCulture),
                                        "].id"),
                                    message: string.Concat("Duplicate module id: ", moduleId)));

                            moduleIdFirstOccurrenceMap[moduleId] = new ModuleIdFirstOccurrence(firstOccurrence.StageName, -normalizedFirstIndex - 1);
                        }

                        findings.Add(
                            new ValidationFinding(
                                ValidationSeverity.Error,
                                code: CodeModuleIdDuplicate,
                                path: string.Concat(modulesPathPrefix, "[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "].id"),
                                message: string.Concat("Duplicate module id: ", moduleId)));
                    }
                }
                else
                {
                    moduleIdFirstOccurrenceMap.Add(moduleId, new ModuleIdFirstOccurrence(stageName, index));
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

                        if (moduleIdFirstOccurrenceMap.TryGetValue(moduleId, out var withinStageFirstOccurrence)
                            && string.Equals(withinStageFirstOccurrence.StageName, stageName, StringComparison.Ordinal)
                            && withinStageFirstOccurrence.Index >= 0)
                        {
                            moduleIdFirstOccurrenceMap[moduleId] = new ModuleIdFirstOccurrence(withinStageFirstOccurrence.StageName, -withinStageFirstOccurrence.Index - 1);
                        }
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
                if (moduleWith.ValueKind == JsonValueKind.Object && ShouldValidateUnknownFields(moduleArgsType))
                {
                    var knownFieldNameSet = BuildJsonPropertyNameSet(moduleArgsType);

                    foreach (var property in moduleWith.EnumerateObject())
                    {
                        var name = property.Name;

                        if (NameSetContains(knownFieldNameSet, name))
                        {
                            continue;
                        }

                        findings.Add(
                            new ValidationFinding(
                                ValidationSeverity.Error,
                                code: CodeModuleArgsUnknownField,
                                path: string.Concat(moduleWithPath, ".", name),
                                message: string.Concat("Unknown module args field: ", name)));
                    }
                }

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

            if (hasModuleGate)
            {
                var gatePath = string.Concat(modulesPathPrefix, "[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "].gate");

                if (hasModuleEnabled
                    && !moduleEnabled
                    && moduleGate.ValueKind != JsonValueKind.Null
                    && moduleGate.ValueKind != JsonValueKind.Undefined)
                {
                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Info,
                            code: CodeGateRedundant,
                            path: gatePath,
                            message: "gate is redundant when enabled=false."));
                }

                if (!GateJsonV1.TryParseOptional(moduleGate, gatePath, out _, out var gateFinding))
                {
                    findings.Add(gateFinding);
                }
            }

            index++;
        }
    }

    private static bool ShouldValidateUnknownFields(Type type)
    {
        if (type == typeof(JsonElement) || type == typeof(JsonDocument))
        {
            return false;
        }

        if (type == typeof(object))
        {
            return false;
        }

        if (typeof(System.Text.Json.Nodes.JsonNode).IsAssignableFrom(type))
        {
            return false;
        }

        if (typeof(System.Collections.IDictionary).IsAssignableFrom(type))
        {
            return false;
        }

        if (IsAssignableToGenericDictionary(type, typeof(System.Collections.Generic.IDictionary<,>)))
        {
            return false;
        }

        if (IsAssignableToGenericDictionary(type, typeof(System.Collections.Generic.IReadOnlyDictionary<,>)))
        {
            return false;
        }

        return true;
    }

    private static bool IsAssignableToGenericDictionary(Type type, Type openGenericType)
    {
        if (type.IsGenericType && type.GetGenericTypeDefinition() == openGenericType)
        {
            return true;
        }

        var interfaces = type.GetInterfaces();

        for (var i = 0; i < interfaces.Length; i++)
        {
            var candidate = interfaces[i];

            if (candidate.IsGenericType && candidate.GetGenericTypeDefinition() == openGenericType)
            {
                return true;
            }
        }

        return false;
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

    private readonly struct ModuleIdFirstOccurrence
    {
        public readonly string StageName;
        public readonly int Index;

        public ModuleIdFirstOccurrence(string stageName, int index)
        {
            StageName = stageName;
            Index = index;
        }
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
