using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Reflection;
using Rockestra.Core.Gates;
using Rockestra.Core.Selectors;

namespace Rockestra.Core;

public sealed class ConfigValidator
{
    private const string SupportedSchemaVersion = "v1";

    private const string CodeParseError = "CFG_PARSE_ERROR";
    private const string CodeSchemaVersionUnsupported = "CFG_SCHEMA_VERSION_UNSUPPORTED";
    private const string CodeUnknownField = "CFG_UNKNOWN_FIELD";
    private const string CodeLimitsNotObject = "CFG_LIMITS_NOT_OBJECT";
    private const string CodeLimitModuleConcurrencyNotObject = "CFG_LIMIT_MODULE_CONCURRENCY_NOT_OBJECT";
    private const string CodeLimitMaxInFlightNotObject = "CFG_LIMIT_MAX_IN_FLIGHT_NOT_OBJECT";
    private const string CodeLimitMaxInFlightInvalid = "CFG_LIMIT_MAX_IN_FLIGHT_INVALID";
    private const string CodeLimitKeyInvalid = "CFG_LIMIT_KEY_INVALID";
    private const string CodeMemoKeyInvalid = "CFG_MEMO_KEY_INVALID";
    private const string CodeQosNotObject = "CFG_QOS_NOT_OBJECT";
    private const string CodeQosTiersNotObject = "CFG_QOS_TIERS_NOT_OBJECT";
    private const string CodeQosTierUnknown = "CFG_QOS_TIER_UNKNOWN";
    private const string CodeQosTierNotObject = "CFG_QOS_TIER_NOT_OBJECT";
    private const string CodeQosPatchNotObject = "CFG_QOS_PATCH_NOT_OBJECT";
    private const string CodeQosFanoutMaxIncreaseForbidden = "CFG_QOS_FANOUT_MAX_INCREASE_FORBIDDEN";
    private const string CodeQosModuleEnableForbidden = "CFG_QOS_MODULE_ENABLE_FORBIDDEN";
    private const string CodeQosShadowSampleIncreaseForbidden = "CFG_QOS_SHADOW_SAMPLE_INCREASE_FORBIDDEN";
    private const string CodeFanoutMaxInvalid = "CFG_FANOUT_MAX_INVALID";
    private const string CodeFanoutMaxExceeded = "CFG_FANOUT_MAX_EXCEEDED";
    private const string CodeFanoutTrimLikely = "CFG_FANOUT_TRIM_LIKELY";
    private const string CodeFlowsNotObject = "CFG_FLOWS_NOT_OBJECT";
    private const string CodeFlowPatchNotObject = "CFG_FLOW_PATCH_NOT_OBJECT";
    private const string CodeFlowNotRegistered = "CFG_FLOW_NOT_REGISTERED";
    private const string CodeStagesNotObject = "CFG_STAGES_NOT_OBJECT";
    private const string CodeStagePatchNotObject = "CFG_STAGE_PATCH_NOT_OBJECT";
    private const string CodeStageNotInBlueprint = "CFG_STAGE_NOT_IN_BLUEPRINT";
    private const string CodeStageDynamicModulesForbidden = "CFG_STAGE_DYNAMIC_MODULES_FORBIDDEN";
    private const string CodeStageModuleTypeForbidden = "CFG_STAGE_MODULE_TYPE_FORBIDDEN";
    private const string CodeStageModuleCountWarnExceeded = "CFG_STAGE_MODULE_COUNT_WARN_EXCEEDED";
    private const string CodeStageModuleCountHardExceeded = "CFG_STAGE_MODULE_COUNT_HARD_EXCEEDED";
    private const string CodeStageShadowForbidden = "CFG_STAGE_SHADOW_FORBIDDEN";
    private const string CodeStageShadowModuleCountHardExceeded = "CFG_STAGE_SHADOW_MODULE_COUNT_HARD_EXCEEDED";
    private const string CodeStageShadowSampleBpsExceeded = "CFG_STAGE_SHADOW_SAMPLE_BPS_EXCEEDED";
    private const string CodeStageFanoutMaxOutOfRange = "CFG_STAGE_FANOUT_MAX_OUT_OF_RANGE";
    private const string CodeParamsBindFailed = "CFG_PARAMS_BIND_FAILED";
    private const string CodeParamsUnknownField = "CFG_PARAMS_UNKNOWN_FIELD";
    private const string CodeModulesNotArray = "CFG_MODULES_NOT_ARRAY";
    private const string CodeModuleIdMissing = "CFG_MODULE_ID_MISSING";
    private const string CodeModuleIdDuplicate = "CFG_MODULE_ID_DUPLICATE";
    private const string CodeModuleIdInvalidFormat = "CFG_MODULE_ID_INVALID_FORMAT";
    private const string CodeModuleIdConflictsWithBlueprintNodeName = "CFG_MODULE_ID_CONFLICTS_WITH_BLUEPRINT_NODE_NAME";
    private const string CodeModuleTypeMissing = "CFG_MODULE_TYPE_MISSING";
    private const string CodeModuleTypeNotRegistered = "CFG_MODULE_TYPE_NOT_REGISTERED";
    private const string CodeModuleArgsMissing = "CFG_MODULE_ARGS_MISSING";
    private const string CodeModuleArgsBindFailed = "CFG_MODULE_ARGS_BIND_FAILED";
    private const string CodeModuleArgsUnknownField = "CFG_MODULE_ARGS_UNKNOWN_FIELD";
    private const string CodeModuleArgsInvalid = "CFG_MODULE_ARGS_INVALID";
    private const string CodeModuleEnabledInvalid = "CFG_MODULE_ENABLED_INVALID";
    private const string CodePriorityInvalid = "CFG_PRIORITY_INVALID";
    private const string CodeExperimentMappingInvalid = "CFG_EXPERIMENT_MAPPING_INVALID";
    private const string CodeExperimentMappingDuplicate = "CFG_EXPERIMENT_MAPPING_DUPLICATE";
    private const string CodeExperimentPatchInvalid = "CFG_EXPERIMENT_PATCH_INVALID";
    private const string CodeExperimentOverrideForbidden = "CFG_EXPERIMENT_OVERRIDE_FORBIDDEN";
    private const string CodeEmergencyAuditMissing = "CFG_EMERGENCY_AUDIT_MISSING";
    private const string CodeEmergencyOverrideForbidden = "CFG_EMERGENCY_OVERRIDE_FORBIDDEN";
    private const string CodeLayerConflict = "CFG_LAYER_CONFLICT";
    private const string CodeLayerParamLeak = "CFG_LAYER_PARAM_LEAK";
    private const string CodeGateRedundant = "CFG_GATE_REDUNDANT";
    private const string CodeShadowSampleInvalid = "CFG_SHADOW_SAMPLE_INVALID";
    private const string GateCodePrefix = "CFG_GATE_";

    private const int MinModulePriority = -1000;
    private const int MaxModulePriority = 1000;

    private readonly FlowRegistry _flowRegistry;
    private readonly ModuleCatalog _moduleCatalog;
    private readonly SelectorRegistry _selectorRegistry;

    public ConfigValidator(FlowRegistry flowRegistry, ModuleCatalog moduleCatalog)
        : this(flowRegistry, moduleCatalog, SelectorRegistry.Empty)
    {
    }

    public ConfigValidator(FlowRegistry flowRegistry, ModuleCatalog moduleCatalog, SelectorRegistry selectorRegistry)
    {
        _flowRegistry = flowRegistry ?? throw new ArgumentNullException(nameof(flowRegistry));
        _moduleCatalog = moduleCatalog ?? throw new ArgumentNullException(nameof(moduleCatalog));
        _selectorRegistry = selectorRegistry ?? throw new ArgumentNullException(nameof(selectorRegistry));
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

            var hasLimits = false;
            JsonElement limitsElement = default;

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

                if (property.NameEquals("limits"))
                {
                    hasLimits = true;
                    limitsElement = property.Value;
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

            ValidateLimits(hasLimits, limitsElement, ref findings);

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
                        string[] blueprintNodeNameSet = Array.Empty<string>();
                        StageContractEntry[] blueprintStageContracts = Array.Empty<StageContractEntry>();
                        var flowRegistered = false;
                        Type? patchType = null;
                        ExperimentLayerOwnershipContract? experimentLayerOwnershipContract = null;

                         if (flowName.Length != 0)
                         {
                             flowRegistered = _flowRegistry.TryGetStageNameSetAndPatchType(
                                 flowName,
                                 out blueprintStageNameSet,
                                 out blueprintNodeNameSet,
                                 out blueprintStageContracts,
                                 out patchType,
                                 out experimentLayerOwnershipContract);
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
                        out var experimentsPatch,
                        out var hasQosPatch,
                        out var qosPatch,
                        out var hasEmergencyPatch,
                        out var emergencyPatch);

                    if (flowRegistered && hasParamsPatch)
                    {
                        ValidateParamsPatchAtPath(string.Concat("$.flows.", flowName), paramsPatch, patchType, ref findings);
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
                        ValidateStagePatches(
                            flowName,
                            stagesPatch,
                            blueprintStageNameSet,
                            blueprintStageContracts,
                            blueprintNodeNameSet,
                            _moduleCatalog,
                            _selectorRegistry,
                            ref findings);
                    }

                    ValidateEmergency(
                        flowName,
                        hasEmergencyPatch,
                        emergencyPatch,
                        hasStagesPatch,
                        stagesPatch,
                        patchType,
                        blueprintStageNameSet,
                        blueprintNodeNameSet,
                        flowRegistered,
                        ref findings);

                    ValidateQos(
                        flowName,
                        hasQosPatch,
                        qosPatch,
                        hasStagesPatch,
                        stagesPatch,
                        patchType,
                        blueprintStageNameSet,
                        flowRegistered,
                        ref findings);

                    ValidateExperiments(
                        flowName,
                        hasExperimentsPatch,
                        experimentsPatch,
                        patchType,
                        blueprintStageNameSet,
                        blueprintStageContracts,
                        blueprintNodeNameSet,
                        _moduleCatalog,
                        _selectorRegistry,
                        experimentLayerOwnershipContract,
                        flowRegistered,
                        ref findings);
                }
            }

            return findings.IsEmpty ? ValidationReport.Empty : new ValidationReport(findings.ToArray());
        }
    }

    private static void ValidateLimits(bool hasLimits, JsonElement limitsPatch, ref FindingBuffer findings)
    {
        if (!hasLimits)
        {
            return;
        }

        const string limitsPath = "$.limits";

        if (limitsPatch.ValueKind != JsonValueKind.Object)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeLimitsNotObject,
                    path: limitsPath,
                    message: "limits must be an object."));
            return;
        }

        foreach (var limitsField in limitsPatch.EnumerateObject())
        {
            if (limitsField.NameEquals("moduleConcurrency"))
            {
                ValidateModuleConcurrencyLimits(limitsField.Value, limitsPath, ref findings);
                continue;
            }

            var fieldName = limitsField.Name;
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeUnknownField,
                    path: string.Concat(limitsPath, ".", fieldName),
                    message: string.Concat("Unknown field: ", fieldName)));
        }
    }

    private static void ValidateModuleConcurrencyLimits(JsonElement moduleConcurrencyPatch, string limitsPath, ref FindingBuffer findings)
    {
        var moduleConcurrencyPath = string.Concat(limitsPath, ".moduleConcurrency");

        if (moduleConcurrencyPatch.ValueKind != JsonValueKind.Object)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeLimitModuleConcurrencyNotObject,
                    path: moduleConcurrencyPath,
                    message: "limits.moduleConcurrency must be an object."));
            return;
        }

        var hasMaxInFlight = false;
        JsonElement maxInFlightPatch = default;

        foreach (var field in moduleConcurrencyPatch.EnumerateObject())
        {
            if (field.NameEquals("maxInFlight"))
            {
                hasMaxInFlight = true;
                maxInFlightPatch = field.Value;
                continue;
            }

            var fieldName = field.Name;
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeUnknownField,
                    path: string.Concat(moduleConcurrencyPath, ".", fieldName),
                    message: string.Concat("Unknown field: ", fieldName)));
        }

        if (!hasMaxInFlight)
        {
            return;
        }

        var maxInFlightPath = string.Concat(moduleConcurrencyPath, ".maxInFlight");

        if (maxInFlightPatch.ValueKind != JsonValueKind.Object)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeLimitMaxInFlightNotObject,
                    path: maxInFlightPath,
                    message: "limits.moduleConcurrency.maxInFlight must be an object."));
            return;
        }

        foreach (var entry in maxInFlightPatch.EnumerateObject())
        {
            if (!IsValidLimitKey(entry.Name))
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeLimitKeyInvalid,
                        path: string.Concat(maxInFlightPath, ".", entry.Name),
                        message: "limits.moduleConcurrency.maxInFlight has an invalid key."));
            }

            if (entry.Value.ValueKind != JsonValueKind.Number
                || !entry.Value.TryGetInt32(out var max)
                || max <= 0)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeLimitMaxInFlightInvalid,
                        path: string.Concat(maxInFlightPath, ".", entry.Name),
                        message: "limits.moduleConcurrency.maxInFlight must be a positive int32."));
            }
        }
    }

    private static bool IsValidLimitKey(string? key)
    {
        if (string.IsNullOrEmpty(key))
        {
            return false;
        }

        if (key.Length > 128)
        {
            return false;
        }

        for (var i = 0; i < key.Length; i++)
        {
            var ch = key[i];
            if (char.IsWhiteSpace(ch) || char.IsControl(ch))
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsValidMemoKey(string? key)
    {
        return IsValidLimitKey(key);
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
        out JsonElement experimentsPatch,
        out bool hasQosPatch,
        out JsonElement qosPatch,
        out bool hasEmergencyPatch,
        out JsonElement emergencyPatch)
    {
        hasStagesPatch = false;
        stagesPatch = default;
        paramsPatch = default;
        hasParamsPatch = false;
        hasExperimentsPatch = false;
        experimentsPatch = default;
        hasQosPatch = false;
        qosPatch = default;
        hasEmergencyPatch = false;
        emergencyPatch = default;

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
                hasEmergencyPatch = true;
                emergencyPatch = flowField.Value;
                continue;
            }

            if (flowField.NameEquals("qos"))
            {
                hasQosPatch = true;
                qosPatch = flowField.Value;
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
        StageContractEntry[] blueprintStageContracts,
        string[] blueprintNodeNameSet,
        ModuleCatalog moduleCatalog,
        SelectorRegistry selectorRegistry,
        ExperimentLayerOwnershipContract? experimentLayerOwnershipContract,
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
        Dictionary<string, LayerConflictFirstOccurrence>? paramsConflictMap = null;
        Dictionary<string, LayerConflictFirstOccurrence>? fanoutConflictMap = null;
        Dictionary<string, LayerConflictFirstOccurrence>? moduleIdConflictMap = null;

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

            if (patch.TryGetProperty("experiments", out _))
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeExperimentOverrideForbidden,
                        path: string.Concat(patchPath, ".experiments"),
                        message: "experiments[].patch must not override structural field: experiments."));
                index++;
                continue;
            }

            if (patch.TryGetProperty("emergency", out _))
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeExperimentOverrideForbidden,
                        path: string.Concat(patchPath, ".emergency"),
                        message: "experiments[].patch must not override structural field: emergency."));
                index++;
                continue;
            }

            if (patch.TryGetProperty("qos", out _))
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeExperimentOverrideForbidden,
                        path: string.Concat(patchPath, ".qos"),
                        message: "experiments[].patch must not override structural field: qos."));
                index++;
                continue;
            }

            var patchFlowName = string.Concat(flowName, ".experiments[", indexString, "].patch");

            if (hasLayer
                && !string.IsNullOrEmpty(layer)
                && experimentLayerOwnershipContract is not null)
            {
                ValidateExperimentLayerOwnershipContract(
                    layer: layer!,
                    patchFlowName,
                    patch,
                    experimentLayerOwnershipContract,
                    ref findings);
            }

            if (hasLayer && !string.IsNullOrEmpty(layer))
            {
                RecordExperimentPatchLayerConflicts(
                    layer: layer!,
                    patchFlowName,
                    patch,
                    ref paramsConflictMap,
                    ref fanoutConflictMap,
                    ref moduleIdConflictMap,
                    ref findings);
            }

            if (flowRegistered)
            {
                ValidateExperimentPatch(
                    patchFlowName,
                    patchType,
                    blueprintStageNameSet,
                    blueprintStageContracts,
                    blueprintNodeNameSet,
                    moduleCatalog,
                    selectorRegistry,
                    patch,
                    ref findings);
            }

            index++;
        }
    }

    private static void ValidateExperimentLayerOwnershipContract(
        string layer,
        string patchFlowName,
        JsonElement patch,
        ExperimentLayerOwnershipContract experimentLayerOwnershipContract,
        ref FindingBuffer findings)
    {
        if (string.IsNullOrEmpty(layer))
        {
            throw new ArgumentException("Layer must be non-empty.", nameof(layer));
        }

        if (string.IsNullOrEmpty(patchFlowName))
        {
            throw new ArgumentException("PatchFlowName must be non-empty.", nameof(patchFlowName));
        }

        if (experimentLayerOwnershipContract is null)
        {
            throw new ArgumentNullException(nameof(experimentLayerOwnershipContract));
        }

        if (!experimentLayerOwnershipContract.TryGetLayerOwnership(layer, out var ownership))
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeLayerParamLeak,
                    path: string.Concat("$.flows.", patchFlowName),
                    message: string.Concat("Experiment layer is not declared in ownership contract: ", layer)));
            return;
        }

        if (patch.TryGetProperty("params", out var paramsPatch) && paramsPatch.ValueKind == JsonValueKind.Object)
        {
            ValidateExperimentLayerParamOwnershipRecursive(
                patchFlowName,
                paramsPatch,
                parentPath: null,
                ownership,
                ref findings);
        }

        if (!patch.TryGetProperty("stages", out var stagesPatch) || stagesPatch.ValueKind != JsonValueKind.Object)
        {
            return;
        }

        foreach (var stageProperty in stagesPatch.EnumerateObject())
        {
            var stageName = stageProperty.Name;

            if (stageProperty.Value.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            var stagePatch = stageProperty.Value;

            if (!stagePatch.TryGetProperty("modules", out var modulesPatch) || modulesPatch.ValueKind != JsonValueKind.Array)
            {
                continue;
            }

            var moduleIndex = 0;

            foreach (var moduleElement in modulesPatch.EnumerateArray())
            {
                if (moduleElement.ValueKind != JsonValueKind.Object)
                {
                    moduleIndex++;
                    continue;
                }

                if (!moduleElement.TryGetProperty("id", out var moduleIdElement) || moduleIdElement.ValueKind != JsonValueKind.String)
                {
                    moduleIndex++;
                    continue;
                }

                var moduleId = moduleIdElement.GetString();
                if (string.IsNullOrEmpty(moduleId))
                {
                    moduleIndex++;
                    continue;
                }

                if (!ownership.OwnsModuleId(moduleId))
                {
                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Error,
                            code: CodeLayerParamLeak,
                            path: string.Concat(
                                "$.flows.",
                                patchFlowName,
                                ".stages.",
                                stageName,
                                ".modules[",
                                moduleIndex.ToString(System.Globalization.CultureInfo.InvariantCulture),
                                "].id"),
                            message: string.Concat("Experiment layer must not modify unowned module id: ", moduleId)));
                }

                moduleIndex++;
            }
        }
    }

    private static void ValidateExperimentLayerParamOwnershipRecursive(
        string patchFlowName,
        JsonElement paramsPatch,
        string? parentPath,
        ExperimentLayerOwnershipContract.LayerOwnership ownership,
        ref FindingBuffer findings)
    {
        foreach (var property in paramsPatch.EnumerateObject())
        {
            var name = property.Name;
            var currentPath = string.IsNullOrEmpty(parentPath) ? name : string.Concat(parentPath, ".", name);

            if (property.Value.ValueKind == JsonValueKind.Object)
            {
                ValidateExperimentLayerParamOwnershipRecursive(patchFlowName, property.Value, currentPath, ownership, ref findings);
                continue;
            }

            if (ownership.OwnsParamPath(currentPath))
            {
                continue;
            }

            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeLayerParamLeak,
                    path: string.Concat("$.flows.", patchFlowName, ".params.", currentPath),
                    message: string.Concat("Experiment layer must not modify unowned params path: ", currentPath)));
        }
    }

    private static void ValidateEmergency(
        string flowName,
        bool hasEmergencyPatch,
        JsonElement emergencyPatch,
        bool hasBaseStagesPatch,
        JsonElement baseStagesPatch,
        Type? patchType,
        string[] blueprintStageNameSet,
        string[] blueprintNodeNameSet,
        bool flowRegistered,
        ref FindingBuffer findings)
    {
        if (!hasEmergencyPatch)
        {
            return;
        }

        var emergencyPathPrefix = string.Concat("$.flows.", flowName, ".emergency");

        if (emergencyPatch.ValueKind != JsonValueKind.Object)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeEmergencyOverrideForbidden,
                    path: emergencyPathPrefix,
                    message: "emergency must be an object."));
            return;
        }

        string? reason = null;
        string? operatorName = null;
        var ttlMinutes = 0;
        var hasPatch = false;
        JsonElement patch = default;

        var hasReason = false;
        var hasOperator = false;
        var hasTtlMinutes = false;

        foreach (var field in emergencyPatch.EnumerateObject())
        {
            if (field.NameEquals("reason"))
            {
                hasReason = true;

                if (field.Value.ValueKind == JsonValueKind.String)
                {
                    reason = field.Value.GetString();
                }
                else
                {
                    reason = null;
                }

                continue;
            }

            if (field.NameEquals("operator"))
            {
                hasOperator = true;

                if (field.Value.ValueKind == JsonValueKind.String)
                {
                    operatorName = field.Value.GetString();
                }
                else
                {
                    operatorName = null;
                }

                continue;
            }

            if (field.NameEquals("ttl_minutes"))
            {
                hasTtlMinutes = true;

                if (field.Value.ValueKind == JsonValueKind.Number && field.Value.TryGetInt32(out var ttl32))
                {
                    ttlMinutes = ttl32;
                }
                else
                {
                    ttlMinutes = 0;
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
                    path: string.Concat(emergencyPathPrefix, ".", fieldName),
                    message: string.Concat("Unknown field: ", fieldName)));
        }

        if (!hasReason
            || string.IsNullOrEmpty(reason)
            || !hasOperator
            || string.IsNullOrEmpty(operatorName)
            || !hasTtlMinutes
            || ttlMinutes <= 0)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeEmergencyAuditMissing,
                    path: emergencyPathPrefix,
                    message: "emergency requires reason/operator/ttl_minutes."));
        }

        if (!hasPatch || patch.ValueKind != JsonValueKind.Object)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeEmergencyOverrideForbidden,
                    path: string.Concat(emergencyPathPrefix, ".patch"),
                    message: "emergency.patch is required and must be a non-null object."));
            return;
        }

        ValidateEmergencyPatch(
            flowName,
            emergencyPathPrefix: string.Concat(emergencyPathPrefix, ".patch"),
            patch,
            hasBaseStagesPatch,
            baseStagesPatch,
            patchType,
            blueprintStageNameSet,
            blueprintNodeNameSet,
            flowRegistered,
            ref findings);
    }

    private static void ValidateQos(
        string flowName,
        bool hasQosPatch,
        JsonElement qosPatch,
        bool hasBaseStagesPatch,
        JsonElement baseStagesPatch,
        Type? patchType,
        string[] blueprintStageNameSet,
        bool flowRegistered,
        ref FindingBuffer findings)
    {
        if (!hasQosPatch)
        {
            return;
        }

        var qosPathPrefix = string.Concat("$.flows.", flowName, ".qos");

        if (qosPatch.ValueKind != JsonValueKind.Object)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeQosNotObject,
                    path: qosPathPrefix,
                    message: "qos must be an object."));
            return;
        }

        var hasTiers = false;
        JsonElement tiers = default;

        foreach (var field in qosPatch.EnumerateObject())
        {
            if (field.NameEquals("tiers"))
            {
                hasTiers = true;
                tiers = field.Value;
                continue;
            }

            var fieldName = field.Name;
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeUnknownField,
                    path: string.Concat(qosPathPrefix, ".", fieldName),
                    message: string.Concat("Unknown field: ", fieldName)));
        }

        if (!hasTiers)
        {
            return;
        }

        var tiersPathPrefix = string.Concat(qosPathPrefix, ".tiers");

        if (tiers.ValueKind != JsonValueKind.Object)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeQosTiersNotObject,
                    path: tiersPathPrefix,
                    message: "qos.tiers must be an object."));
            return;
        }

        var hasValidBaseStages = hasBaseStagesPatch && baseStagesPatch.ValueKind == JsonValueKind.Object;

        foreach (var tierProperty in tiers.EnumerateObject())
        {
            var tierName = tierProperty.Name;
            var tierPathPrefix = string.Concat(tiersPathPrefix, ".", tierName);

            if (!IsKnownQosTierName(tierName))
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeQosTierUnknown,
                        path: tierPathPrefix,
                        message: string.Concat("Unknown QoS tier: ", tierName)));
                continue;
            }

            if (tierProperty.Value.ValueKind != JsonValueKind.Object)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeQosTierNotObject,
                        path: tierPathPrefix,
                        message: "qos.tiers.<tier> must be an object."));
                continue;
            }

            var tierElement = tierProperty.Value;

            var hasPatch = false;
            JsonElement patch = default;

            foreach (var field in tierElement.EnumerateObject())
            {
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
                        path: string.Concat(tierPathPrefix, ".", fieldName),
                        message: string.Concat("Unknown field: ", fieldName)));
            }

            if (!hasPatch)
            {
                continue;
            }

            var patchPathPrefix = string.Concat(tierPathPrefix, ".patch");

            if (patch.ValueKind != JsonValueKind.Object)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeQosPatchNotObject,
                        path: patchPathPrefix,
                        message: "qos.tiers.<tier>.patch must be an object."));
                continue;
            }

            var hasStagesPatch = false;
            JsonElement stagesPatch = default;

            var hasParamsPatch = false;
            JsonElement paramsPatch = default;

            foreach (var field in patch.EnumerateObject())
            {
                if (field.NameEquals("stages"))
                {
                    hasStagesPatch = true;
                    stagesPatch = field.Value;
                    continue;
                }

                if (field.NameEquals("params"))
                {
                    hasParamsPatch = true;
                    paramsPatch = field.Value;
                    continue;
                }

                var fieldName = field.Name;
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeUnknownField,
                        path: string.Concat(patchPathPrefix, ".", fieldName),
                        message: string.Concat("Unknown field: ", fieldName)));
            }

            if (hasParamsPatch)
            {
                ValidateParamsPatchAtPath(patchPathPrefix, paramsPatch, patchType, ref findings);
            }

            if (hasStagesPatch)
            {
                ValidateQosStagesPatch(
                    patchPathPrefix,
                    stagesPatch,
                    hasValidBaseStages,
                    baseStagesPatch,
                    blueprintStageNameSet,
                    flowRegistered,
                    ref findings);
            }
        }
    }

    private static bool IsKnownQosTierName(string tierName)
    {
        return string.Equals(tierName, "full", StringComparison.Ordinal)
            || string.Equals(tierName, "conserve", StringComparison.Ordinal)
            || string.Equals(tierName, "emergency", StringComparison.Ordinal)
            || string.Equals(tierName, "fallback", StringComparison.Ordinal);
    }

    private static void ValidateParamsPatchAtPath(
        string patchPathPrefix,
        JsonElement paramsPatch,
        Type? patchType,
        ref FindingBuffer findings)
    {
        var paramsPath = string.Concat(patchPathPrefix, ".params");

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

        ValidateUnknownFieldsRecursive(
            paramsPath,
            paramsPatch,
            patchType,
            code: CodeParamsUnknownField,
            messagePrefix: "Unknown params field: ",
            ref findings);

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

    private static void ValidateQosStagesPatch(
        string patchPathPrefix,
        JsonElement qosStagesPatch,
        bool hasValidBaseStages,
        JsonElement baseStagesPatch,
        string[] blueprintStageNameSet,
        bool flowRegistered,
        ref FindingBuffer findings)
    {
        var qosStagesPathPrefix = string.Concat(patchPathPrefix, ".stages");

        if (qosStagesPatch.ValueKind != JsonValueKind.Object)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeStagesNotObject,
                    path: qosStagesPathPrefix,
                    message: "qos.tiers.<tier>.patch.stages must be an object."));
            return;
        }

        foreach (var stageProperty in qosStagesPatch.EnumerateObject())
        {
            var stageName = stageProperty.Name;
            var stagePathPrefix = string.Concat(qosStagesPathPrefix, ".", stageName);

            var stageNameInBlueprint = flowRegistered && StageNameSetContains(blueprintStageNameSet, stageName);

            if (flowRegistered && !stageNameInBlueprint)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeStageNotInBlueprint,
                        path: stagePathPrefix,
                        message: string.Concat("Stage is not in blueprint: ", stageName)));
            }

            if (stageProperty.Value.ValueKind != JsonValueKind.Object)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeStagePatchNotObject,
                        path: stagePathPrefix,
                        message: "Stage patch must be an object."));
                continue;
            }

            JsonElement baseStagePatch = default;
            var hasValidBaseStagePatch = hasValidBaseStages
                && baseStagesPatch.TryGetProperty(stageName, out baseStagePatch)
                && baseStagePatch.ValueKind == JsonValueKind.Object;

            ValidateQosStagePatch(stagePathPrefix, stageProperty.Value, hasValidBaseStagePatch, baseStagePatch, ref findings);
        }
    }

    private static void ValidateQosStagePatch(
        string stagePathPrefix,
        JsonElement qosStagePatch,
        bool hasValidBaseStagePatch,
        JsonElement baseStagePatch,
        ref FindingBuffer findings)
    {
        var hasFanoutMax = false;
        JsonElement fanoutMax = default;

        var hasModules = false;
        JsonElement modules = default;

        foreach (var stageField in qosStagePatch.EnumerateObject())
        {
            if (stageField.NameEquals("fanoutMax"))
            {
                hasFanoutMax = true;
                fanoutMax = stageField.Value;
                continue;
            }

            if (stageField.NameEquals("modules"))
            {
                hasModules = true;
                modules = stageField.Value;
                continue;
            }

            var fieldName = stageField.Name;
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeUnknownField,
                    path: string.Concat(stagePathPrefix, ".", fieldName),
                    message: string.Concat("Unknown field: ", fieldName)));
        }

        if (hasFanoutMax)
        {
            ValidateFanoutMaxAtPath(string.Concat(stagePathPrefix, ".fanoutMax"), fanoutMax, ref findings);

            if (hasValidBaseStagePatch
                && TryGetStageFanoutMax(baseStagePatch, out var baseFanoutMax)
                && TryGetValidFanoutMaxValue(fanoutMax, out var qosFanoutMax)
                && qosFanoutMax > baseFanoutMax)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeQosFanoutMaxIncreaseForbidden,
                        path: string.Concat(stagePathPrefix, ".fanoutMax"),
                        message: "QoS must not increase fanoutMax."));
            }
        }

        if (!hasModules)
        {
            return;
        }

        var modulesPathPrefix = string.Concat(stagePathPrefix, ".modules");

        if (modules.ValueKind != JsonValueKind.Array)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeModulesNotArray,
                    path: modulesPathPrefix,
                    message: "modules must be an array."));
            return;
        }

        var index = 0;
        foreach (var modulePatch in modules.EnumerateArray())
        {
            var modulePathPrefix = string.Concat(modulesPathPrefix, "[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "]");

            if (modulePatch.ValueKind != JsonValueKind.Object)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeModulesNotArray,
                        path: modulePathPrefix,
                        message: "modules must be an array of objects."));
                index++;
                continue;
            }

            string? moduleId = null;
            var hasEnabled = false;
            var enabled = true;
            var hasShadow = false;
            JsonElement shadow = default;

            foreach (var field in modulePatch.EnumerateObject())
            {
                if (field.NameEquals("id"))
                {
                    if (field.Value.ValueKind == JsonValueKind.String)
                    {
                        moduleId = field.Value.GetString();
                    }

                    continue;
                }

                if (field.NameEquals("enabled"))
                {
                    hasEnabled = true;

                    if (field.Value.ValueKind == JsonValueKind.True || field.Value.ValueKind == JsonValueKind.False)
                    {
                        enabled = field.Value.GetBoolean();
                    }
                    else
                    {
                        findings.Add(
                            new ValidationFinding(
                                ValidationSeverity.Error,
                                code: CodeModuleEnabledInvalid,
                                path: string.Concat(modulePathPrefix, ".enabled"),
                                message: "modules[].enabled must be a boolean."));
                    }

                    continue;
                }

                if (field.NameEquals("shadow"))
                {
                    hasShadow = true;
                    shadow = field.Value;
                    continue;
                }

                var fieldName = field.Name;
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeUnknownField,
                        path: string.Concat(modulePathPrefix, ".", fieldName),
                        message: string.Concat("Unknown field: ", fieldName)));
            }

            if (string.IsNullOrEmpty(moduleId))
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeModuleIdMissing,
                        path: string.Concat(modulePathPrefix, ".id"),
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
                            path: string.Concat(modulePathPrefix, ".id"),
                            message: "modules[].id must match [a-z0-9_]+ and length <= 64."));
                }

                if (hasEnabled
                    && enabled
                    && hasValidBaseStagePatch
                    && TryGetBaseModuleEnabled(baseStagePatch, moduleId, out var baseEnabled)
                    && !baseEnabled)
                {
                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Error,
                            code: CodeQosModuleEnableForbidden,
                            path: string.Concat(modulePathPrefix, ".enabled"),
                            message: "QoS must not enable modules that are disabled in the base patch."));
                }

                if (hasShadow && hasValidBaseStagePatch)
                {
                    var baseShadowSample = 0.0;
                    _ = TryGetBaseModuleShadowSample(baseStagePatch, moduleId, out baseShadowSample);

                    if (TryGetShadowSample(shadow, out var qosShadowSample) && qosShadowSample > baseShadowSample)
                    {
                        findings.Add(
                            new ValidationFinding(
                                ValidationSeverity.Error,
                                code: CodeQosShadowSampleIncreaseForbidden,
                                path: string.Concat(modulePathPrefix, ".shadow.sample"),
                                message: "QoS must not increase shadow sampling rate."));
                    }
                }
            }

            if (hasShadow)
            {
                ValidateShadowPatchAtPath(string.Concat(modulePathPrefix, ".shadow"), shadow, ref findings);
            }

            index++;
        }
    }

    private static void ValidateFanoutMaxAtPath(string path, JsonElement fanoutMax, ref FindingBuffer findings)
    {
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

            if (value32 > StageContract.MaxAllowedFanoutMax)
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
                            StageContract.MaxAllowedFanoutMax.ToString(System.Globalization.CultureInfo.InvariantCulture),
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

            if (value64 > StageContract.MaxAllowedFanoutMax)
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
                            StageContract.MaxAllowedFanoutMax.ToString(System.Globalization.CultureInfo.InvariantCulture),
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

    private static void ValidateShadowPatchAtPath(string shadowPath, JsonElement shadow, ref FindingBuffer findings)
    {
        if (shadow.ValueKind != JsonValueKind.Object)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeShadowSampleInvalid,
                    path: shadowPath,
                    message: "modules[].shadow must be an object."));
            return;
        }

        var hasSample = false;
        JsonElement sampleElement = default;

        foreach (var shadowField in shadow.EnumerateObject())
        {
            if (shadowField.NameEquals("sample"))
            {
                hasSample = true;
                sampleElement = shadowField.Value;
                continue;
            }

            var fieldName = shadowField.Name;
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeUnknownField,
                    path: string.Concat(shadowPath, ".", fieldName),
                    message: string.Concat("Unknown field: ", fieldName)));
        }

        if (!hasSample)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeShadowSampleInvalid,
                    path: string.Concat(shadowPath, ".sample"),
                    message: "modules[].shadow.sample is required."));
            return;
        }

        if (sampleElement.ValueKind != JsonValueKind.Number
            || !sampleElement.TryGetDouble(out var sampleRate)
            || sampleRate < 0
            || sampleRate > 1)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeShadowSampleInvalid,
                    path: string.Concat(shadowPath, ".sample"),
                    message: "modules[].shadow.sample must be a number in range 0..1."));
        }
    }

    private static bool TryGetShadowSample(JsonElement shadow, out double sample)
    {
        sample = 0;

        if (shadow.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        foreach (var field in shadow.EnumerateObject())
        {
            if (!field.NameEquals("sample"))
            {
                continue;
            }

            if (field.Value.ValueKind != JsonValueKind.Number)
            {
                return false;
            }

            if (!field.Value.TryGetDouble(out sample))
            {
                return false;
            }

            if (sample < 0 || sample > 1)
            {
                return false;
            }

            return true;
        }

        return false;
    }

    private static bool TryGetStageFanoutMax(JsonElement stagePatch, out int fanoutMax)
    {
        if (!stagePatch.TryGetProperty("fanoutMax", out var fanoutElement))
        {
            fanoutMax = 0;
            return false;
        }

        if (!TryGetValidFanoutMaxValue(fanoutElement, out fanoutMax))
        {
            fanoutMax = 0;
            return false;
        }

        return true;
    }

    private static bool TryGetBaseModuleEnabled(JsonElement baseStagePatch, string moduleId, out bool enabled)
    {
        enabled = true;

        if (!baseStagePatch.TryGetProperty("modules", out var modules) || modules.ValueKind != JsonValueKind.Array)
        {
            return false;
        }

        foreach (var module in modules.EnumerateArray())
        {
            if (module.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            if (!module.TryGetProperty("id", out var idElement) || idElement.ValueKind != JsonValueKind.String)
            {
                continue;
            }

            var id = idElement.GetString();

            if (!string.Equals(id, moduleId, StringComparison.Ordinal))
            {
                continue;
            }

            if (module.TryGetProperty("enabled", out var enabledElement)
                && (enabledElement.ValueKind == JsonValueKind.True || enabledElement.ValueKind == JsonValueKind.False))
            {
                enabled = enabledElement.GetBoolean();
                return true;
            }

            enabled = true;
            return true;
        }

        return false;
    }

    private static bool TryGetBaseModuleShadowSample(JsonElement baseStagePatch, string moduleId, out double sample)
    {
        sample = 0;

        if (!baseStagePatch.TryGetProperty("modules", out var modules) || modules.ValueKind != JsonValueKind.Array)
        {
            return false;
        }

        foreach (var module in modules.EnumerateArray())
        {
            if (module.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            if (!module.TryGetProperty("id", out var idElement) || idElement.ValueKind != JsonValueKind.String)
            {
                continue;
            }

            var id = idElement.GetString();

            if (!string.Equals(id, moduleId, StringComparison.Ordinal))
            {
                continue;
            }

            if (!module.TryGetProperty("shadow", out var shadow))
            {
                sample = 0;
                return true;
            }

            if (TryGetShadowSample(shadow, out sample))
            {
                return true;
            }

            sample = 0;
            return true;
        }

        return false;
    }

    private static void ValidateEmergencyPatch(
        string flowName,
        string emergencyPathPrefix,
        JsonElement patch,
        bool hasBaseStagesPatch,
        JsonElement baseStagesPatch,
        Type? patchType,
        string[] blueprintStageNameSet,
        string[] blueprintNodeNameSet,
        bool flowRegistered,
        ref FindingBuffer findings)
    {
        var hasStagesPatch = false;
        JsonElement stagesPatch = default;

        var hasParamsPatch = false;
        JsonElement paramsPatch = default;

        foreach (var field in patch.EnumerateObject())
        {
            if (field.NameEquals("stages"))
            {
                hasStagesPatch = true;
                stagesPatch = field.Value;
                continue;
            }

            if (field.NameEquals("params"))
            {
                hasParamsPatch = true;
                paramsPatch = field.Value;
                continue;
            }

            var fieldName = field.Name;
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeEmergencyOverrideForbidden,
                    path: string.Concat(emergencyPathPrefix, ".", fieldName),
                    message: string.Concat("emergency.patch must not override forbidden field: ", fieldName)));
        }

        if (hasParamsPatch && flowRegistered)
        {
            ValidateParamsPatchAtPath(emergencyPathPrefix, paramsPatch, patchType, ref findings);
        }

        if (!hasStagesPatch)
        {
            return;
        }

        if (stagesPatch.ValueKind != JsonValueKind.Object)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeEmergencyOverrideForbidden,
                    path: string.Concat(emergencyPathPrefix, ".stages"),
                    message: "emergency.patch.stages must be an object."));
            return;
        }

        foreach (var stageProperty in stagesPatch.EnumerateObject())
        {
            var stageName = stageProperty.Name;
            var stagePathPrefix = string.Concat(emergencyPathPrefix, ".stages.", stageName);

            var stageNameInBlueprint = flowRegistered && StageNameSetContains(blueprintStageNameSet, stageName);

            if (flowRegistered && !stageNameInBlueprint)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeStageNotInBlueprint,
                        path: stagePathPrefix,
                        message: string.Concat("Stage is not in blueprint: ", stageName)));
            }

            if (stageProperty.Value.ValueKind != JsonValueKind.Object)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeStagePatchNotObject,
                        path: stagePathPrefix,
                        message: "Stage patch must be an object."));
                continue;
            }

            if (flowRegistered && !stageNameInBlueprint)
            {
                continue;
            }

            var stagePatch = stageProperty.Value;

            var hasFanoutMax = false;
            JsonElement fanoutMax = default;
            var hasModules = false;
            JsonElement modulesPatch = default;

            foreach (var stageField in stagePatch.EnumerateObject())
            {
                if (stageField.NameEquals("fanoutMax"))
                {
                    hasFanoutMax = true;
                    fanoutMax = stageField.Value;
                    continue;
                }

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
                        code: CodeEmergencyOverrideForbidden,
                        path: string.Concat(stagePathPrefix, ".", fieldName),
                        message: string.Concat("emergency.patch must not override forbidden field: stages.*.", fieldName)));
            }

            if (hasFanoutMax)
            {
                ValidateEmergencyFanoutMax(stagePathPrefix, fanoutMax, ref findings);
            }

            if (hasFanoutMax
                && hasBaseStagesPatch
                && baseStagesPatch.ValueKind == JsonValueKind.Object
                && baseStagesPatch.TryGetProperty(stageName, out var baseStagePatch)
                && baseStagePatch.ValueKind == JsonValueKind.Object
                && TryGetValidFanoutMaxValue(fanoutMax, out var emergencyFanoutMax)
                && baseStagePatch.TryGetProperty("modules", out var baseModulesPatch)
                && baseModulesPatch.ValueKind == JsonValueKind.Array)
            {
                HashSet<string>? disabledModuleIdSet = null;

                if (hasModules && modulesPatch.ValueKind == JsonValueKind.Array)
                {
                    foreach (var moduleElement in modulesPatch.EnumerateArray())
                    {
                        if (moduleElement.ValueKind != JsonValueKind.Object)
                        {
                            continue;
                        }

                        if (!moduleElement.TryGetProperty("enabled", out var enabledElement)
                            || enabledElement.ValueKind != JsonValueKind.False)
                        {
                            continue;
                        }

                        if (!moduleElement.TryGetProperty("id", out var idElement)
                            || idElement.ValueKind != JsonValueKind.String)
                        {
                            continue;
                        }

                        var id = idElement.GetString();
                        if (string.IsNullOrEmpty(id))
                        {
                            continue;
                        }

                        disabledModuleIdSet ??= new HashSet<string>();
                        disabledModuleIdSet.Add(id);
                    }
                }

                var enabledModuleCount = CountEnabledModules(baseModulesPatch, disabledModuleIdSet);

                MaybeReportFanoutTrimLikely(stagePathPrefix, enabledModuleCount, emergencyFanoutMax, ref findings);
            }

            if (!hasModules)
            {
                continue;
            }

            var modulesPathPrefix = string.Concat(stagePathPrefix, ".modules");

            if (modulesPatch.ValueKind != JsonValueKind.Array)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeEmergencyOverrideForbidden,
                        path: modulesPathPrefix,
                        message: "emergency.patch.stages.*.modules must be an array."));
                continue;
            }

            var moduleIndex = 0;

            foreach (var moduleElement in modulesPatch.EnumerateArray())
            {
                var modulePathPrefix = string.Concat(
                    modulesPathPrefix,
                    "[",
                    moduleIndex.ToString(System.Globalization.CultureInfo.InvariantCulture),
                    "]");

                if (moduleElement.ValueKind != JsonValueKind.Object)
                {
                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Error,
                            code: CodeEmergencyOverrideForbidden,
                            path: modulePathPrefix,
                            message: "emergency.patch.stages.*.modules[] must be an object."));
                    moduleIndex++;
                    continue;
                }

                string? moduleId = null;
                var hasModuleId = false;
                var hasEnabled = false;
                var enabled = true;

                foreach (var moduleField in moduleElement.EnumerateObject())
                {
                    if (moduleField.NameEquals("id"))
                    {
                        hasModuleId = true;

                        if (moduleField.Value.ValueKind == JsonValueKind.String)
                        {
                            moduleId = moduleField.Value.GetString();
                        }
                        else
                        {
                            moduleId = null;
                        }

                        continue;
                    }

                    if (moduleField.NameEquals("enabled"))
                    {
                        hasEnabled = true;

                        if (moduleField.Value.ValueKind == JsonValueKind.True || moduleField.Value.ValueKind == JsonValueKind.False)
                        {
                            enabled = moduleField.Value.GetBoolean();
                        }
                        else
                        {
                            enabled = true;
                        }

                        continue;
                    }

                    var fieldName = moduleField.Name;
                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Error,
                            code: CodeEmergencyOverrideForbidden,
                            path: string.Concat(modulePathPrefix, ".", fieldName),
                            message: string.Concat("emergency.patch.modules[] must not override forbidden field: ", fieldName)));
                }

                if (!hasModuleId || string.IsNullOrEmpty(moduleId))
                {
                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Error,
                            code: CodeEmergencyOverrideForbidden,
                            path: string.Concat(modulePathPrefix, ".id"),
                            message: "emergency.patch.modules[].id is required and must be a non-empty string."));
                    moduleIndex++;
                    continue;
                }

                if (!IsValidModuleIdFormat(moduleId))
                {
                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Warn,
                            code: CodeModuleIdInvalidFormat,
                            path: string.Concat(modulePathPrefix, ".id"),
                            message: "emergency.patch.modules[].id must match [a-z0-9_]+ and length <= 64."));
                }

                if (NameSetContains(blueprintNodeNameSet, moduleId))
                {
                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Error,
                            code: CodeModuleIdConflictsWithBlueprintNodeName,
                            path: string.Concat(modulePathPrefix, ".id"),
                            message: string.Concat("modules[].id conflicts with blueprint node name: ", moduleId)));
                }

                if (!hasEnabled || enabled)
                {
                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Error,
                            code: CodeEmergencyOverrideForbidden,
                            path: string.Concat(modulePathPrefix, ".enabled"),
                            message: "emergency.patch.modules[].enabled is required and must be false."));
                }

                moduleIndex++;
            }
        }
    }

    private static void ValidateEmergencyFanoutMax(string stagePathPrefix, JsonElement fanoutMax, ref FindingBuffer findings)
    {
        var path = string.Concat(stagePathPrefix, ".fanoutMax");

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

            if (value32 > StageContract.MaxAllowedFanoutMax)
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
                            StageContract.MaxAllowedFanoutMax.ToString(System.Globalization.CultureInfo.InvariantCulture),
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

            if (value64 > StageContract.MaxAllowedFanoutMax)
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
                            StageContract.MaxAllowedFanoutMax.ToString(System.Globalization.CultureInfo.InvariantCulture),
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

    private static void RecordExperimentPatchLayerConflicts(
        string layer,
        string patchFlowName,
        JsonElement patch,
        ref Dictionary<string, LayerConflictFirstOccurrence>? paramsConflictMap,
        ref Dictionary<string, LayerConflictFirstOccurrence>? fanoutConflictMap,
        ref Dictionary<string, LayerConflictFirstOccurrence>? moduleIdConflictMap,
        ref FindingBuffer findings)
    {
        if (string.IsNullOrEmpty(layer))
        {
            throw new ArgumentException("Layer must be non-empty.", nameof(layer));
        }

        var patchValue = patch;
        if (patchValue.ValueKind != JsonValueKind.Object)
        {
            return;
        }

        if (patchValue.TryGetProperty("params", out var paramsPatch))
        {
            if (paramsPatch.ValueKind == JsonValueKind.Object)
            {
                RecordParamLayerConflicts(
                    layer,
                    patchFlowName,
                    paramsPatch,
                    parentPath: null,
                    ref paramsConflictMap,
                    ref findings);
            }
            else if (paramsPatch.ValueKind != JsonValueKind.Undefined)
            {
                var path = string.Concat("$.flows.", patchFlowName, ".params");
                RecordLayerConflict(
                    kind: "params",
                    key: string.Empty,
                    layer,
                    path,
                    ref paramsConflictMap,
                    ref findings);
            }
        }

        if (!patchValue.TryGetProperty("stages", out var stagesPatch) || stagesPatch.ValueKind != JsonValueKind.Object)
        {
            return;
        }

        foreach (var stageProperty in stagesPatch.EnumerateObject())
        {
            var stageName = stageProperty.Name;
            if (stageProperty.Value.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            var stagePatch = stageProperty.Value;

            if (stagePatch.TryGetProperty("fanoutMax", out _))
            {
                var path = string.Concat("$.flows.", patchFlowName, ".stages.", stageName, ".fanoutMax");
                RecordLayerConflict(
                    kind: "fanoutMax",
                    key: stageName,
                    layer,
                    path,
                    ref fanoutConflictMap,
                    ref findings);
            }

            if (!stagePatch.TryGetProperty("modules", out var modulesPatch) || modulesPatch.ValueKind != JsonValueKind.Array)
            {
                continue;
            }

            var moduleIndex = 0;

            foreach (var moduleElement in modulesPatch.EnumerateArray())
            {
                if (moduleElement.ValueKind != JsonValueKind.Object)
                {
                    moduleIndex++;
                    continue;
                }

                if (!moduleElement.TryGetProperty("id", out var moduleIdElement) || moduleIdElement.ValueKind != JsonValueKind.String)
                {
                    moduleIndex++;
                    continue;
                }

                var moduleId = moduleIdElement.GetString();
                if (string.IsNullOrEmpty(moduleId))
                {
                    moduleIndex++;
                    continue;
                }

                var path = string.Concat(
                    "$.flows.",
                    patchFlowName,
                    ".stages.",
                    stageName,
                    ".modules[",
                    moduleIndex.ToString(System.Globalization.CultureInfo.InvariantCulture),
                    "].id");

                RecordLayerConflict(
                    kind: "moduleId",
                    key: moduleId!,
                    layer,
                    path,
                    ref moduleIdConflictMap,
                    ref findings);

                moduleIndex++;
            }
        }
    }

    private static void RecordParamLayerConflicts(
        string layer,
        string patchFlowName,
        JsonElement paramsPatch,
        string? parentPath,
        ref Dictionary<string, LayerConflictFirstOccurrence>? conflictMap,
        ref FindingBuffer findings)
    {
        foreach (var property in paramsPatch.EnumerateObject())
        {
            var name = property.Name;

            var currentPath = string.IsNullOrEmpty(parentPath) ? name : string.Concat(parentPath, ".", name);

            if (property.Value.ValueKind == JsonValueKind.Object)
            {
                RecordParamLayerConflicts(layer, patchFlowName, property.Value, currentPath, ref conflictMap, ref findings);
                continue;
            }

            var path = string.Concat("$.flows.", patchFlowName, ".params.", currentPath);
            RecordLayerConflict(
                kind: "params",
                key: currentPath,
                layer,
                path,
                ref conflictMap,
                ref findings);
        }
    }

    private static void RecordLayerConflict(
        string kind,
        string key,
        string layer,
        string path,
        ref Dictionary<string, LayerConflictFirstOccurrence>? conflictMap,
        ref FindingBuffer findings)
    {
        conflictMap ??= new Dictionary<string, LayerConflictFirstOccurrence>();

        if (conflictMap.TryGetValue(key, out var first))
        {
            if (string.Equals(first.Layer, layer, StringComparison.Ordinal))
            {
                return;
            }

            if (!first.Reported)
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeLayerConflict,
                        path: first.Path,
                        message: string.Concat(
                            "Experiment layer conflict: ",
                            kind,
                            " is modified by multiple layers (",
                            first.Layer,
                            ", ",
                            layer,
                            ").")));

                first.Reported = true;
                conflictMap[key] = first;
            }

            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeLayerConflict,
                    path: path,
                    message: string.Concat(
                        "Experiment layer conflict: ",
                        kind,
                        " is modified by multiple layers (",
                        first.Layer,
                        ", ",
                        layer,
                        ").")));

            return;
        }

        conflictMap.Add(key, new LayerConflictFirstOccurrence(layer, path));
    }

    private struct LayerConflictFirstOccurrence
    {
        public string Layer { get; }

        public string Path { get; }

        public bool Reported { get; set; }

        public LayerConflictFirstOccurrence(string layer, string path)
        {
            Layer = layer;
            Path = path;
            Reported = false;
        }
    }

    private static void ValidateExperimentPatch(
        string patchFlowName,
        Type? patchType,
        string[] blueprintStageNameSet,
        StageContractEntry[] blueprintStageContracts,
        string[] blueprintNodeNameSet,
        ModuleCatalog moduleCatalog,
        SelectorRegistry selectorRegistry,
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
            out var experimentsPatch,
            out _,
            out _,
            out _,
            out _);

        if (hasParamsPatch)
        {
            ValidateParamsPatchAtPath(string.Concat("$.flows.", patchFlowName), paramsPatch, patchType, ref patchFindings);
        }

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
            ValidateStagePatches(patchFlowName, stagesPatch, blueprintStageNameSet, blueprintStageContracts, blueprintNodeNameSet, moduleCatalog, selectorRegistry, ref patchFindings);
        }

        ValidateExperiments(
            patchFlowName,
            hasExperimentsPatch,
            experimentsPatch,
            patchType,
            blueprintStageNameSet,
            blueprintStageContracts,
            blueprintNodeNameSet,
            moduleCatalog,
            selectorRegistry,
            experimentLayerOwnershipContract: null,
            flowRegistered: true,
            ref patchFindings);

        if (patchFindings.IsEmpty)
        {
            return;
        }

        var items = patchFindings.ToArray();

        for (var i = 0; i < items.Length; i++)
        {
            findings.Add(items[i]);
        }
    }

    private static void ValidateStagePatches(
        string flowName,
        JsonElement stagesPatch,
        string[] blueprintStageNameSet,
        StageContractEntry[] blueprintStageContracts,
        string[] blueprintNodeNameSet,
        ModuleCatalog moduleCatalog,
        SelectorRegistry selectorRegistry,
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

            var stageContract = FindStageContract(blueprintStageContracts, stageName);
            ValidateStagePatch(
                flowName,
                stageName,
                stageProperty.Value,
                stageContract,
                moduleCatalog,
                selectorRegistry,
                blueprintNodeNameSet,
                ref findings,
                ref moduleIdFirstOccurrenceMap);
        }
    }

    private static void ValidateStagePatch(
        string flowName,
        string stageName,
        JsonElement stagePatch,
        StageContract stageContract,
        ModuleCatalog moduleCatalog,
        SelectorRegistry selectorRegistry,
        string[] blueprintNodeNameSet,
        ref FindingBuffer findings,
        ref Dictionary<string, ModuleIdFirstOccurrence>? moduleIdFirstOccurrenceMap)
    {
        var stagePathPrefix = string.Concat("$.flows.", flowName, ".stages.", stageName);

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
                    path: string.Concat(stagePathPrefix, ".", fieldName),
                    message: string.Concat("Unknown field: ", fieldName)));
        }

        var validFanoutMaxValue = 0;
        var hasValidFanoutMaxValue = hasFanoutMax && TryGetValidFanoutMaxValue(fanoutMax, out validFanoutMaxValue);

        if (hasFanoutMax)
        {
            ValidateFanoutMax(flowName, stageName, fanoutMax, ref findings);

            if (hasValidFanoutMaxValue
                && (validFanoutMaxValue < stageContract.MinFanoutMax || validFanoutMaxValue > stageContract.MaxFanoutMax))
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeStageFanoutMaxOutOfRange,
                        path: string.Concat(stagePathPrefix, ".fanoutMax"),
                        message: string.Concat(
                            "fanoutMax=",
                            validFanoutMaxValue.ToString(System.Globalization.CultureInfo.InvariantCulture),
                            " is outside stage contract range ",
                            stageContract.MinFanoutMax.ToString(System.Globalization.CultureInfo.InvariantCulture),
                            "..",
                            stageContract.MaxFanoutMax.ToString(System.Globalization.CultureInfo.InvariantCulture),
                            ".")));
            }
        }

        if (!hasModules)
        {
            return;
        }

        var modulesPathPrefix = string.Concat(stagePathPrefix, ".modules");

        if (!stageContract.AllowsDynamicModules)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeStageDynamicModulesForbidden,
                    path: modulesPathPrefix,
                    message: string.Concat("Dynamic modules are not allowed for stage: ", stageName)));
            return;
        }

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

        var enabledModuleCount = 0;

        ValidateModulesPatch(
            flowName,
            stageName,
            modulesPathPrefix,
            modulesPatch,
            stageContract,
            moduleCatalog,
            selectorRegistry,
            blueprintNodeNameSet,
            ref findings,
            ref enabledModuleCount,
            ref moduleIdFirstOccurrenceMap);

        var maxModulesHard = stageContract.MaxModulesHard;
        if (maxModulesHard != 0 && enabledModuleCount > maxModulesHard)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeStageModuleCountHardExceeded,
                    path: modulesPathPrefix,
                    message: string.Concat(
                        "enabledModules=",
                        enabledModuleCount.ToString(System.Globalization.CultureInfo.InvariantCulture),
                        " exceeds stage contract hard limit=",
                        maxModulesHard.ToString(System.Globalization.CultureInfo.InvariantCulture),
                        ".")));
            return;
        }

        var maxModulesWarn = stageContract.MaxModulesWarn;
        if (maxModulesWarn != 0 && enabledModuleCount > maxModulesWarn)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Warn,
                    code: CodeStageModuleCountWarnExceeded,
                    path: modulesPathPrefix,
                    message: string.Concat(
                        "enabledModules=",
                        enabledModuleCount.ToString(System.Globalization.CultureInfo.InvariantCulture),
                        " exceeds stage contract warn limit=",
                        maxModulesWarn.ToString(System.Globalization.CultureInfo.InvariantCulture),
                        ".")));
        }

        if (hasValidFanoutMaxValue)
        {
            MaybeReportFanoutTrimLikely(stagePathPrefix, enabledModuleCount, validFanoutMaxValue, ref findings);
        }
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

            if (value32 > StageContract.MaxAllowedFanoutMax)
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
                            StageContract.MaxAllowedFanoutMax.ToString(System.Globalization.CultureInfo.InvariantCulture),
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

            if (value64 > StageContract.MaxAllowedFanoutMax)
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
                            StageContract.MaxAllowedFanoutMax.ToString(System.Globalization.CultureInfo.InvariantCulture),
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
        StageContract stageContract,
        ModuleCatalog moduleCatalog,
        SelectorRegistry selectorRegistry,
        string[] blueprintNodeNameSet,
        ref FindingBuffer findings,
        ref int enabledModuleCount,
        ref Dictionary<string, ModuleIdFirstOccurrence>? moduleIdFirstOccurrenceMap)
    {
        Dictionary<string, int>? moduleIdIndexMap = null;
        var shadowModuleCount = 0;

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
            var hasModuleShadow = false;
            JsonElement moduleShadow = default;
            var hasModuleLimitKey = false;
            string? moduleLimitKey = null;
            var hasModuleMemoKey = false;
            string? moduleMemoKey = null;

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
                    else
                    {
                        findings.Add(
                            new ValidationFinding(
                                ValidationSeverity.Error,
                                code: CodeModuleEnabledInvalid,
                                path: string.Concat(
                                    modulesPathPrefix,
                                    "[",
                                    index.ToString(System.Globalization.CultureInfo.InvariantCulture),
                                    "].enabled"),
                                message: "modules[].enabled must be a boolean."));
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

                if (moduleField.NameEquals("shadow"))
                {
                    hasModuleShadow = true;
                    moduleShadow = moduleField.Value;
                    continue;
                }

                if (moduleField.NameEquals("limitKey"))
                {
                    hasModuleLimitKey = true;
                    moduleLimitKey = moduleField.Value.ValueKind == JsonValueKind.String ? moduleField.Value.GetString() : null;
                    continue;
                }

                if (moduleField.NameEquals("memoKey"))
                {
                    hasModuleMemoKey = true;
                    moduleMemoKey = moduleField.Value.ValueKind == JsonValueKind.String ? moduleField.Value.GetString() : null;
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

            if (hasModuleLimitKey && !IsValidLimitKey(moduleLimitKey))
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeLimitKeyInvalid,
                        path: string.Concat(modulesPathPrefix, "[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "].limitKey"),
                        message: "modules[].limitKey must be a non-empty string with no whitespace."));
            }

            if (hasModuleMemoKey && !IsValidMemoKey(moduleMemoKey))
            {
                findings.Add(
                    new ValidationFinding(
                        ValidationSeverity.Error,
                        code: CodeMemoKeyInvalid,
                        path: string.Concat(modulesPathPrefix, "[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "].memoKey"),
                        message: "modules[].memoKey must be a non-empty string with no whitespace."));
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

                if (NameSetContains(blueprintNodeNameSet, moduleId))
                {
                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Error,
                            code: CodeModuleIdConflictsWithBlueprintNodeName,
                            path: string.Concat(modulesPathPrefix, "[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "].id"),
                            message: string.Concat("modules[].id conflicts with blueprint node name: ", moduleId)));
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
            IModuleArgsValidatorInvoker? moduleArgsValidator = null;

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
                if (!stageContract.IsModuleTypeAllowed(moduleUse))
                {
                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Error,
                            code: CodeStageModuleTypeForbidden,
                            path: string.Concat(modulesPathPrefix, "[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "].use"),
                            message: string.Concat("Module type is not allowed by stage contract: ", moduleUse)));
                }

                if (!moduleCatalog.TryGetSignature(moduleUse, out var argsType, out _, out var argsValidator))
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
                    moduleArgsValidator = argsValidator;
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
                ValidateUnknownFieldsRecursive(
                    moduleWithPath,
                    moduleWith,
                    moduleArgsType,
                    code: CodeModuleArgsUnknownField,
                    messagePrefix: "Unknown module args field: ",
                    ref findings);

                object? moduleArgs = null;

                try
                {
                    moduleArgs = moduleWith.Deserialize(moduleArgsType);
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

                if (moduleArgs is not null && moduleArgsValidator is not null)
                {
                    try
                    {
                        if (!moduleArgsValidator.TryValidate(moduleArgs, out var relativePath, out var message))
                        {
                            if (string.IsNullOrEmpty(message))
                            {
                                message = "module args validation failed.";
                            }

                            var path = moduleWithPath;

                            if (!string.IsNullOrEmpty(relativePath))
                            {
                                if (relativePath[0] == '[' || relativePath[0] == '.')
                                {
                                    path = string.Concat(moduleWithPath, relativePath);
                                }
                                else
                                {
                                    path = string.Concat(moduleWithPath, ".", relativePath);
                                }
                            }

                            findings.Add(
                                new ValidationFinding(
                                    ValidationSeverity.Error,
                                    code: CodeModuleArgsInvalid,
                                    path: path,
                                    message: message));
                        }
                    }
                    catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
                    {
                        var message = ex.Message;
                        if (string.IsNullOrEmpty(message))
                        {
                            message = "module args validation failed.";
                        }

                        findings.Add(
                            new ValidationFinding(
                                ValidationSeverity.Error,
                                code: CodeModuleArgsInvalid,
                                path: moduleWithPath,
                                message: message));
                    }
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

                if (!GateJsonV1.TryParseOptional(moduleGate, gatePath, selectorRegistry, out _, out var gateFinding))
                {
                    findings.Add(gateFinding);
                }
            }

            if (hasModuleShadow)
            {
                shadowModuleCount++;

                var shadowPath = string.Concat(modulesPathPrefix, "[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "].shadow");

                if (!stageContract.AllowsShadowModules)
                {
                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Error,
                            code: CodeStageShadowForbidden,
                            path: shadowPath,
                            message: string.Concat("Shadow modules are not allowed for stage: ", stageName)));
                }

                if (moduleShadow.ValueKind != JsonValueKind.Object)
                {
                    findings.Add(
                        new ValidationFinding(
                            ValidationSeverity.Error,
                            code: CodeShadowSampleInvalid,
                            path: shadowPath,
                            message: "modules[].shadow must be an object."));
                }
                else
                {
                    var hasSample = false;
                    JsonElement sampleElement = default;

                    foreach (var shadowField in moduleShadow.EnumerateObject())
                    {
                        if (shadowField.NameEquals("sample"))
                        {
                            hasSample = true;
                            sampleElement = shadowField.Value;
                            continue;
                        }

                        var fieldName = shadowField.Name;
                        findings.Add(
                            new ValidationFinding(
                                ValidationSeverity.Error,
                                code: CodeUnknownField,
                                path: string.Concat(shadowPath, ".", fieldName),
                                message: string.Concat("Unknown field: ", fieldName)));
                    }

                    if (!hasSample)
                    {
                        findings.Add(
                            new ValidationFinding(
                                ValidationSeverity.Error,
                                code: CodeShadowSampleInvalid,
                                path: string.Concat(shadowPath, ".sample"),
                                message: "modules[].shadow.sample is required."));
                    }
                    else
                    {
                        if (sampleElement.ValueKind != JsonValueKind.Number
                            || !sampleElement.TryGetDouble(out var sampleRate)
                            || sampleRate < 0
                            || sampleRate > 1)
                        {
                            findings.Add(
                                new ValidationFinding(
                                    ValidationSeverity.Error,
                                    code: CodeShadowSampleInvalid,
                                    path: string.Concat(shadowPath, ".sample"),
                                    message: "modules[].shadow.sample must be a number in range 0..1."));
                        }
                        else if (stageContract.MaxShadowSampleBps < 10000)
                        {
                            var sampleBps = ConvertShadowSampleToBps(sampleRate);
                            var maxShadowSampleBps = stageContract.MaxShadowSampleBps;

                            if (sampleBps > maxShadowSampleBps)
                            {
                                findings.Add(
                                    new ValidationFinding(
                                        ValidationSeverity.Error,
                                        code: CodeStageShadowSampleBpsExceeded,
                                        path: string.Concat(shadowPath, ".sample"),
                                        message: string.Concat(
                                            "shadowSampleBps=",
                                            sampleBps.ToString(System.Globalization.CultureInfo.InvariantCulture),
                                            " exceeds stage contract maxShadowSampleBps=",
                                            maxShadowSampleBps.ToString(System.Globalization.CultureInfo.InvariantCulture),
                                            ".")));
                            }
                        }
                    }
                }
            }

            if ((!hasModuleEnabled || moduleEnabled) && !hasModuleShadow)
            {
                enabledModuleCount++;
            }

            index++;
        }

        var maxShadowModulesHard = stageContract.MaxShadowModulesHard;
        if (maxShadowModulesHard != 0 && shadowModuleCount > maxShadowModulesHard)
        {
            findings.Add(
                new ValidationFinding(
                    ValidationSeverity.Error,
                    code: CodeStageShadowModuleCountHardExceeded,
                    path: modulesPathPrefix,
                    message: string.Concat(
                        "shadowModules=",
                        shadowModuleCount.ToString(System.Globalization.CultureInfo.InvariantCulture),
                        " exceeds stage contract hard limit=",
                        maxShadowModulesHard.ToString(System.Globalization.CultureInfo.InvariantCulture),
                        ".")));
        }
    }

    private static int ConvertShadowSampleToBps(double sampleRate)
    {
        if (sampleRate <= 0)
        {
            return 0;
        }

        if (sampleRate >= 1)
        {
            return 10000;
        }

        var bps = (int)Math.Round(sampleRate * 10000.0, MidpointRounding.AwayFromZero);

        if (bps < 0)
        {
            return 0;
        }

        if (bps > 10000)
        {
            return 10000;
        }

        return bps;
    }

    private static bool TryGetValidFanoutMaxValue(JsonElement fanoutMax, out int value)
    {
        if (fanoutMax.ValueKind != JsonValueKind.Number)
        {
            value = 0;
            return false;
        }

        if (fanoutMax.TryGetInt32(out var value32))
        {
            if (value32 < 0 || value32 > StageContract.MaxAllowedFanoutMax)
            {
                value = 0;
                return false;
            }

            value = value32;
            return true;
        }

        if (fanoutMax.TryGetInt64(out var value64))
        {
            if (value64 < 0 || value64 > StageContract.MaxAllowedFanoutMax)
            {
                value = 0;
                return false;
            }

            value = (int)value64;
            return true;
        }

        value = 0;
        return false;
    }

    private static int CountEnabledModules(JsonElement modulesPatch, HashSet<string>? disabledModuleIdSet)
    {
        var count = 0;

        foreach (var modulePatch in modulesPatch.EnumerateArray())
        {
            if (modulePatch.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            if (modulePatch.TryGetProperty("shadow", out _))
            {
                continue;
            }

            var enabled = true;

            if (modulePatch.TryGetProperty("enabled", out var enabledElement)
                && (enabledElement.ValueKind == JsonValueKind.True || enabledElement.ValueKind == JsonValueKind.False))
            {
                enabled = enabledElement.GetBoolean();
            }

            if (!enabled)
            {
                continue;
            }

            if (disabledModuleIdSet is not null
                && modulePatch.TryGetProperty("id", out var idElement)
                && idElement.ValueKind == JsonValueKind.String)
            {
                var id = idElement.GetString();

                if (!string.IsNullOrEmpty(id) && disabledModuleIdSet.Contains(id))
                {
                    continue;
                }
            }

            count++;
        }

        return count;
    }

    private static void MaybeReportFanoutTrimLikely(string stagePath, int enabledModuleCount, int fanoutMax, ref FindingBuffer findings)
    {
        if (enabledModuleCount <= fanoutMax)
        {
            return;
        }

        findings.Add(
            new ValidationFinding(
                ValidationSeverity.Warn,
                code: CodeFanoutTrimLikely,
                path: stagePath,
                message: string.Concat(
                    "enabledModules=",
                    enabledModuleCount.ToString(System.Globalization.CultureInfo.InvariantCulture),
                    " exceeds fanoutMax=",
                    fanoutMax.ToString(System.Globalization.CultureInfo.InvariantCulture),
                    ".")));
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

    private static StageContract FindStageContract(StageContractEntry[] stageContracts, string stageName)
    {
        if (stageContracts is null)
        {
            throw new ArgumentNullException(nameof(stageContracts));
        }

        if (string.IsNullOrEmpty(stageName))
        {
            throw new ArgumentException("StageName must be non-empty.", nameof(stageName));
        }

        for (var i = 0; i < stageContracts.Length; i++)
        {
            if (string.Equals(stageContracts[i].StageName, stageName, StringComparison.Ordinal))
            {
                return stageContracts[i].Contract;
            }
        }

        return StageContract.Default;
    }

    private static void ValidateUnknownFieldsRecursive(
        string pathPrefix,
        JsonElement patch,
        Type targetType,
        string code,
        string messagePrefix,
        ref FindingBuffer findings)
    {
        if (string.IsNullOrEmpty(pathPrefix))
        {
            throw new ArgumentException("PathPrefix must be non-empty.", nameof(pathPrefix));
        }

        if (string.IsNullOrEmpty(code))
        {
            throw new ArgumentException("Code must be non-empty.", nameof(code));
        }

        if (targetType is null)
        {
            throw new ArgumentNullException(nameof(targetType));
        }

        if (patch.ValueKind != JsonValueKind.Object && patch.ValueKind != JsonValueKind.Array)
        {
            return;
        }

        var rootType = UnwrapNullableType(targetType);
        if (IsOpaqueType(rootType))
        {
            return;
        }

        var cache = new Dictionary<Type, TypeMetadata>();

        ValidateUnknownFieldsRecursiveCore(
            pathPrefix,
            patch,
            targetType,
            code,
            messagePrefix,
            ref findings,
            cache);
    }

    private static void ValidateUnknownFieldsRecursiveCore(
        string currentPath,
        JsonElement element,
        Type targetType,
        string code,
        string messagePrefix,
        ref FindingBuffer findings,
        Dictionary<Type, TypeMetadata> typeMetadataCache)
    {
        var type = UnwrapNullableType(targetType);

        if (IsOpaqueType(type) || IsLeafType(type))
        {
            return;
        }

        if (element.ValueKind == JsonValueKind.Object)
        {
            var metadata = GetTypeMetadata(type, typeMetadataCache);

            foreach (var property in element.EnumerateObject())
            {
                var name = property.Name;

                if (!TryGetPropertyType(metadata, name, out var propertyType))
                {
                    if (!metadata.AllowsUnmappedMembers)
                    {
                        findings.Add(
                            new ValidationFinding(
                                ValidationSeverity.Error,
                                code: code,
                                path: string.Concat(currentPath, ".", name),
                                message: string.Concat(messagePrefix, name)));
                    }

                    continue;
                }

                ValidateUnknownFieldsRecursiveCore(
                    string.Concat(currentPath, ".", name),
                    property.Value,
                    propertyType,
                    code,
                    messagePrefix,
                    ref findings,
                    typeMetadataCache);
            }

            return;
        }

        if (element.ValueKind == JsonValueKind.Array)
        {
            if (!TryGetCollectionElementType(type, out var elementType))
            {
                return;
            }

            var index = 0;

            foreach (var item in element.EnumerateArray())
            {
                var indexString = index.ToString(System.Globalization.CultureInfo.InvariantCulture);
                ValidateUnknownFieldsRecursiveCore(
                    string.Concat(currentPath, "[", indexString, "]"),
                    item,
                    elementType,
                    code,
                    messagePrefix,
                    ref findings,
                    typeMetadataCache);
                index++;
            }
        }
    }

    private static Type UnwrapNullableType(Type type)
    {
        if (type is null)
        {
            throw new ArgumentNullException(nameof(type));
        }

        return Nullable.GetUnderlyingType(type) ?? type;
    }

    private static bool IsOpaqueType(Type type)
    {
        if (type == typeof(object) || type == typeof(JsonElement) || type == typeof(JsonDocument))
        {
            return true;
        }

        if (typeof(JsonNode).IsAssignableFrom(type))
        {
            return true;
        }

        if (IsDictionaryLikeType(type))
        {
            return true;
        }

        return false;
    }

    private static bool IsDictionaryLikeType(Type type)
    {
        if (typeof(System.Collections.IDictionary).IsAssignableFrom(type))
        {
            return true;
        }

        if (type.IsGenericType)
        {
            var def = type.GetGenericTypeDefinition();
            if (def == typeof(IDictionary<,>) || def == typeof(IReadOnlyDictionary<,>))
            {
                return true;
            }
        }

        var interfaces = type.GetInterfaces();

        for (var i = 0; i < interfaces.Length; i++)
        {
            var iface = interfaces[i];

            if (!iface.IsGenericType)
            {
                continue;
            }

            var def = iface.GetGenericTypeDefinition();
            if (def == typeof(IDictionary<,>) || def == typeof(IReadOnlyDictionary<,>))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsLeafType(Type type)
    {
        if (type.IsPrimitive || type.IsEnum)
        {
            return true;
        }

        if (type == typeof(string)
            || type == typeof(decimal)
            || type == typeof(DateTime)
            || type == typeof(DateTimeOffset)
            || type == typeof(Guid)
            || type == typeof(TimeSpan))
        {
            return true;
        }

        return false;
    }

    private static bool TryGetCollectionElementType(Type type, out Type elementType)
    {
        if (type.IsArray)
        {
            elementType = type.GetElementType()!;
            return true;
        }

        if (typeof(System.Collections.IEnumerable).IsAssignableFrom(type) && type != typeof(string))
        {
            if (type.IsGenericType)
            {
                var def = type.GetGenericTypeDefinition();
                if (def == typeof(IEnumerable<>)
                    || def == typeof(IReadOnlyList<>)
                    || def == typeof(IList<>)
                    || def == typeof(IReadOnlyCollection<>)
                    || def == typeof(ICollection<>)
                    || def == typeof(List<>))
                {
                    elementType = type.GetGenericArguments()[0];
                    return true;
                }
            }

            var interfaces = type.GetInterfaces();

            for (var i = 0; i < interfaces.Length; i++)
            {
                var iface = interfaces[i];

                if (!iface.IsGenericType)
                {
                    continue;
                }

                if (iface.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                {
                    elementType = iface.GetGenericArguments()[0];
                    return true;
                }
            }
        }

        elementType = null!;
        return false;
    }

    private static TypeMetadata GetTypeMetadata(Type type, Dictionary<Type, TypeMetadata> cache)
    {
        if (cache.TryGetValue(type, out var cached))
        {
            return cached;
        }

        var metadata = TypeMetadata.Create(type);
        cache.Add(type, metadata);
        return metadata;
    }

    private static bool TryGetPropertyType(TypeMetadata metadata, string propertyName, out Type propertyType)
    {
        var names = metadata.PropertyNames;

        for (var i = 0; i < names.Length; i++)
        {
            if (string.Equals(names[i], propertyName, StringComparison.Ordinal))
            {
                propertyType = metadata.PropertyTypes[i];
                return true;
            }
        }

        propertyType = null!;
        return false;
    }

    private readonly struct TypeMetadata
    {
        public string[] PropertyNames { get; }

        public Type[] PropertyTypes { get; }

        public bool AllowsUnmappedMembers { get; }

        private TypeMetadata(string[] propertyNames, Type[] propertyTypes, bool allowsUnmappedMembers)
        {
            PropertyNames = propertyNames;
            PropertyTypes = propertyTypes;
            AllowsUnmappedMembers = allowsUnmappedMembers;
        }

        public static TypeMetadata Create(Type type)
        {
            var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);

            if (properties.Length == 0)
            {
                return new TypeMetadata(Array.Empty<string>(), Array.Empty<Type>(), allowsUnmappedMembers: false);
            }

            var nameBuffer = new string[properties.Length];
            var typeBuffer = new Type[properties.Length];
            var count = 0;
            var allowsUnmappedMembers = false;

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

                if (property.GetCustomAttribute<JsonExtensionDataAttribute>(inherit: true) is not null)
                {
                    allowsUnmappedMembers = true;
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
                        if (string.Equals(nameBuffer[j], name, StringComparison.Ordinal))
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

                nameBuffer[count] = name;
                typeBuffer[count] = property.PropertyType;
                count++;
            }

            if (count == 0)
            {
                return new TypeMetadata(Array.Empty<string>(), Array.Empty<Type>(), allowsUnmappedMembers);
            }

            if (count == nameBuffer.Length)
            {
                return new TypeMetadata(nameBuffer, typeBuffer, allowsUnmappedMembers);
            }

            var finalNames = new string[count];
            var finalTypes = new Type[count];
            Array.Copy(nameBuffer, 0, finalNames, 0, count);
            Array.Copy(typeBuffer, 0, finalTypes, 0, count);
            return new TypeMetadata(finalNames, finalTypes, allowsUnmappedMembers);
        }
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

