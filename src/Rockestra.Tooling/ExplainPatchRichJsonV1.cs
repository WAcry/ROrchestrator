using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using Rockestra.Core;
using Rockestra.Core.Gates;
using Rockestra.Core.Selectors;

namespace Rockestra.Tooling;

public static class ExplainPatchRichJsonV1
{
    private const string ToolingJsonVersion = "v1";
    private const string SelectedDecisionCode = "SELECTED";

    private static readonly IReadOnlyDictionary<string, string> EmptyVariantDictionary =
        new System.Collections.ObjectModel.ReadOnlyDictionary<string, string>(new Dictionary<string, string>(0));

    private static readonly DateTimeOffset SelectorFlowContextDeadline =
        new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static string EncodeRedactedJson(ReadOnlyMemory<byte> jsonUtf8)
    {
        var redacted = ExplainRedactor.Redact(jsonUtf8, ExplainRedactionPolicy.Default);
        return Encoding.UTF8.GetString(redacted);
    }

    public static ToolingCommandResult ExplainPatchJson(
        string flowName,
        string patchJson,
        FlowRegistry registry,
        ModuleCatalog catalog,
        SelectorRegistry selectorRegistry,
        FlowRequestOptions requestOptions = default,
        QosTier qosTier = QosTier.Full,
        DateTimeOffset? configTimestampUtc = null)
    {
        if (flowName is null)
        {
            throw new ArgumentNullException(nameof(flowName));
        }

        if (patchJson is null)
        {
            throw new ArgumentNullException(nameof(patchJson));
        }

        if (registry is null)
        {
            throw new ArgumentNullException(nameof(registry));
        }

        if (catalog is null)
        {
            throw new ArgumentNullException(nameof(catalog));
        }

        if (selectorRegistry is null)
        {
            throw new ArgumentNullException(nameof(selectorRegistry));
        }

        try
        {
            var validator = new ConfigValidator(registry, catalog, selectorRegistry);
            var report = validator.ValidatePatchJson(patchJson);

            using var evaluation = PatchEvaluatorV1.Evaluate(flowName, patchJson, requestOptions, qosTier: qosTier, configTimestampUtc: configTimestampUtc);
            var planExplain = registry.Explain(flowName, catalog);

            var json = BuildExplainPatchRichJson(registry, flowName, evaluation, planExplain, report, requestOptions, qosTier, configTimestampUtc, catalog, selectorRegistry);
            var exitCode = report.IsValid ? 0 : 2;
            return new ToolingCommandResult(exitCode, json);
        }
        catch (Exception ex) when (IsExplainPatchRichInputException(ex))
        {
            var json = BuildExplainPatchRichErrorJson(
                code: "EXPLAIN_PATCH_INPUT_INVALID",
                message: ex.Message);
            return new ToolingCommandResult(exitCode: 2, json);
        }
        catch (Exception ex) when (ToolingExceptionGuard.ShouldHandle(ex))
        {
            var json = BuildExplainPatchRichErrorJson(
                code: "EXPLAIN_PATCH_INTERNAL_ERROR",
                message: string.IsNullOrEmpty(ex.Message) ? "Internal error." : ex.Message);
            return new ToolingCommandResult(exitCode: 1, json);
        }
    }

    private static bool IsExplainPatchRichInputException(Exception exception)
    {
        return exception is ArgumentException
            || exception is FormatException
            || exception is InvalidOperationException
            || exception is NotSupportedException;
    }

    private static string BuildExplainPatchRichErrorJson(string code, string message)
    {
        var output = new ArrayBufferWriter<byte>(256);
        using var writer = new Utf8JsonWriter(
            output,
            new JsonWriterOptions
            {
                Indented = false,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            });

        writer.WriteStartObject();
        writer.WriteString("kind", "explain_patch_rich");
        writer.WriteString("tooling_json_version", ToolingJsonVersion);
        writer.WritePropertyName("error");
        writer.WriteStartObject();
        writer.WriteString("code", code);
        writer.WriteString("message", message);
        writer.WriteEndObject();
        writer.WriteEndObject();
        writer.Flush();

        return EncodeRedactedJson(output.WrittenMemory);
    }

    private static string BuildExplainPatchRichJson(
        FlowRegistry registry,
        string flowName,
        PatchEvaluatorV1.FlowPatchEvaluationV1 evaluation,
        PlanExplain planExplain,
        ValidationReport validationReport,
        FlowRequestOptions requestOptions,
        QosTier qosTier,
        DateTimeOffset? configTimestampUtc,
        ModuleCatalog catalog,
        SelectorRegistry selectorRegistry)
    {
        var output = new ArrayBufferWriter<byte>(512);
        using var writer = new Utf8JsonWriter(
            output,
            new JsonWriterOptions
            {
                Indented = false,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            });

        writer.WriteStartObject();
        writer.WriteString("kind", "explain_patch_rich");
        writer.WriteString("tooling_json_version", ToolingJsonVersion);
        writer.WriteString("flow_name", flowName);
        writer.WriteString("plan_template_hash", planExplain.PlanTemplateHash.ToString("X16"));

        writer.WritePropertyName("qos");
        writer.WriteStartObject();
        writer.WriteString("selected_tier", GetQosTierString(qosTier));
        writer.WriteNull("reason_code");
        writer.WriteNull("signals");
        writer.WriteEndObject();

        WriteSortedVariants(writer, requestOptions.Variants);

        if (string.IsNullOrEmpty(evaluation.EmergencyOverlayIgnoredReasonCode))
        {
            writer.WriteNull("emergency_ignored_reason_code");
        }
        else
        {
            writer.WriteString("emergency_ignored_reason_code", evaluation.EmergencyOverlayIgnoredReasonCode);
        }

        writer.WritePropertyName("overlays_applied");
        writer.WriteStartArray();

        var overlays = evaluation.OverlaysApplied;
        for (var i = 0; i < overlays.Count; i++)
        {
            WriteOverlayApplied(writer, overlays[i]);
        }

        writer.WriteEndArray();

        writer.WritePropertyName("validation");
        WriteValidationReport(writer, validationReport);

        writer.WritePropertyName("stage_contracts");
        WriteStageContracts(writer, planExplain.StageContracts);

        writer.WritePropertyName("module_types");
        WriteModuleTypes(writer, evaluation, catalog);

        writer.WritePropertyName("params");
        WriteParamsExplain(writer, evaluation, registry, flowName, requestOptions, qosTier, configTimestampUtc);

        writer.WritePropertyName("stage_snapshots");
        WriteStageSnapshots(writer, evaluation, planExplain.StageContracts, requestOptions, catalog, selectorRegistry);

        writer.WriteEndObject();
        writer.Flush();

        return EncodeRedactedJson(output.WrittenMemory);
    }

    private static string GetQosTierString(QosTier tier)
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

    private static void WriteSortedVariants(Utf8JsonWriter writer, IReadOnlyDictionary<string, string>? variants)
    {
        if (variants is null)
        {
            writer.WriteNull("variants");
            return;
        }

        writer.WritePropertyName("variants");
        writer.WriteStartObject();

        if (variants.Count != 0)
        {
            var rented = ArrayPool<KeyValuePair<string, string>>.Shared.Rent(variants.Count);
            var filledCount = 0;

            try
            {
                foreach (var pair in variants)
                {
                    rented[filledCount] = pair;
                    filledCount++;
                }

                Array.Sort(rented, 0, filledCount, KeyValuePairByKeyComparer.Instance);

                for (var i = 0; i < filledCount; i++)
                {
                    var pair = rented[i];
                    writer.WriteString(pair.Key, pair.Value);
                }
            }
            finally
            {
                Array.Clear(rented, 0, filledCount);
                ArrayPool<KeyValuePair<string, string>>.Shared.Return(rented);
            }
        }

        writer.WriteEndObject();
    }

    private sealed class KeyValuePairByKeyComparer : IComparer<KeyValuePair<string, string>>
    {
        public static readonly KeyValuePairByKeyComparer Instance = new();

        public int Compare(KeyValuePair<string, string> x, KeyValuePair<string, string> y)
        {
            return string.Compare(x.Key, y.Key, StringComparison.Ordinal);
        }
    }

    private static void WriteOverlayApplied(Utf8JsonWriter writer, PatchEvaluatorV1.PatchOverlayAppliedV1 overlay)
    {
        writer.WriteStartObject();
        writer.WriteString("layer", overlay.Layer);

        if (overlay.ExperimentLayer is null)
        {
            writer.WriteNull("experiment_layer");
        }
        else
        {
            writer.WriteString("experiment_layer", overlay.ExperimentLayer);
        }

        if (overlay.ExperimentVariant is null)
        {
            writer.WriteNull("experiment_variant");
        }
        else
        {
            writer.WriteString("experiment_variant", overlay.ExperimentVariant);
        }

        writer.WriteEndObject();
    }

    private static void WriteValidationReport(Utf8JsonWriter writer, ValidationReport report)
    {
        writer.WriteStartObject();
        writer.WriteBoolean("is_valid", report.IsValid);

        writer.WritePropertyName("findings");
        writer.WriteStartArray();

        var findings = report.Findings;

        if (findings.Count == 1)
        {
            WriteFinding(writer, findings[0]);
        }
        else if (findings.Count > 1)
        {
            var sorted = new ValidationFinding[findings.Count];

            for (var i = 0; i < sorted.Length; i++)
            {
                sorted[i] = findings[i];
            }

            Array.Sort(sorted, ValidationFindingComparer.Instance);

            for (var i = 0; i < sorted.Length; i++)
            {
                WriteFinding(writer, sorted[i]);
            }
        }

        writer.WriteEndArray();
        writer.WriteEndObject();
    }

    private static void WriteFinding(Utf8JsonWriter writer, ValidationFinding finding)
    {
        writer.WriteStartObject();
        writer.WriteString("severity", GetSeverityString(finding.Severity));
        writer.WriteString("code", finding.Code);
        writer.WriteString("path", finding.Path);
        writer.WriteString("message", finding.Message);
        writer.WriteEndObject();
    }

    private static string GetSeverityString(ValidationSeverity severity)
    {
        return severity switch
        {
            ValidationSeverity.Error => "error",
            ValidationSeverity.Warn => "warn",
            _ => "error",
        };
    }

    private sealed class ValidationFindingComparer : IComparer<ValidationFinding>
    {
        public static readonly ValidationFindingComparer Instance = new();

        public int Compare(ValidationFinding x, ValidationFinding y)
        {
            var c = x.Severity.CompareTo(y.Severity);
            if (c != 0)
            {
                return c;
            }

            c = string.CompareOrdinal(x.Code, y.Code);
            if (c != 0)
            {
                return c;
            }

            c = string.CompareOrdinal(x.Path, y.Path);
            if (c != 0)
            {
                return c;
            }

            return string.CompareOrdinal(x.Message, y.Message);
        }
    }

    private static void WriteStageContracts(Utf8JsonWriter writer, IReadOnlyList<PlanExplainStageContract> stageContracts)
    {
        writer.WriteStartArray();

        if (stageContracts.Count != 0)
        {
            var sorted = new PlanExplainStageContract[stageContracts.Count];

            for (var i = 0; i < sorted.Length; i++)
            {
                sorted[i] = stageContracts[i];
            }

            if (sorted.Length > 1)
            {
                Array.Sort(sorted, PlanExplainStageContractByNameComparer.Instance);
            }

            for (var i = 0; i < sorted.Length; i++)
            {
                WriteStageContract(writer, sorted[i]);
            }
        }

        writer.WriteEndArray();
    }

    private static void WriteStageContract(Utf8JsonWriter writer, PlanExplainStageContract stageContract)
    {
        writer.WriteStartObject();
        writer.WriteString("stage_name", stageContract.StageName);

        var contract = stageContract.Contract;

        writer.WritePropertyName("contract");
        writer.WriteStartObject();
        writer.WriteBoolean("allows_dynamic_modules", contract.AllowsDynamicModules);
        writer.WriteBoolean("allows_shadow_modules", contract.AllowsShadowModules);

        var allowed = contract.AllowedModuleTypes;

        if (!contract.AllowsDynamicModules || allowed.Length == 0)
        {
            writer.WriteNull("allowed_module_types");
        }
        else
        {
            var allowedTypes = new string[allowed.Length];

            for (var i = 0; i < allowedTypes.Length; i++)
            {
                allowedTypes[i] = allowed[i];
            }

            if (allowedTypes.Length > 1)
            {
                Array.Sort(allowedTypes, StringComparer.Ordinal);
            }

            writer.WritePropertyName("allowed_module_types");
            writer.WriteStartArray();

            for (var i = 0; i < allowedTypes.Length; i++)
            {
                writer.WriteStringValue(allowedTypes[i]);
            }

            writer.WriteEndArray();
        }

        writer.WriteNumber("max_modules_warn", contract.MaxModulesWarn);
        writer.WriteNumber("max_modules_hard", contract.MaxModulesHard);
        writer.WriteNumber("max_shadow_modules_hard", contract.MaxShadowModulesHard);
        writer.WriteNumber("max_shadow_sample_bps", contract.MaxShadowSampleBps);
        writer.WriteNumber("min_fanout_max", contract.MinFanoutMax);
        writer.WriteNumber("max_fanout_max", contract.MaxFanoutMax);
        writer.WriteEndObject();

        writer.WriteEndObject();
    }

    private sealed class PlanExplainStageContractByNameComparer : IComparer<PlanExplainStageContract>
    {
        public static readonly PlanExplainStageContractByNameComparer Instance = new();

        public int Compare(PlanExplainStageContract x, PlanExplainStageContract y)
        {
            return string.CompareOrdinal(x.StageName, y.StageName);
        }
    }

    private static void WriteModuleTypes(Utf8JsonWriter writer, PatchEvaluatorV1.FlowPatchEvaluationV1 evaluation, ModuleCatalog catalog)
    {
        var moduleTypes = new List<string>(capacity: 8);

        var stages = evaluation.Stages;
        for (var stageIndex = 0; stageIndex < stages.Count; stageIndex++)
        {
            var stage = stages[stageIndex];

            var modules = stage.Modules;
            for (var i = 0; i < modules.Count; i++)
            {
                AddUnique(moduleTypes, modules[i].ModuleType);
            }

            var shadowModules = stage.ShadowModules;
            for (var i = 0; i < shadowModules.Count; i++)
            {
                AddUnique(moduleTypes, shadowModules[i].ModuleType);
            }
        }

        if (moduleTypes.Count > 1)
        {
            moduleTypes.Sort(StringComparer.Ordinal);
        }

        writer.WriteStartArray();

        for (var i = 0; i < moduleTypes.Count; i++)
        {
            var moduleType = moduleTypes[i];
            writer.WriteStartObject();
            writer.WriteString("module_type", moduleType);
            writer.WriteBoolean("is_registered", catalog.IsRegistered(moduleType));
            writer.WriteEndObject();
        }

        writer.WriteEndArray();
    }

    private static void AddUnique(List<string> list, string value)
    {
        for (var i = 0; i < list.Count; i++)
        {
            if (string.Equals(list[i], value, StringComparison.Ordinal))
            {
                return;
            }
        }

        list.Add(value);
    }

    private static void WriteParamsExplain(
        Utf8JsonWriter writer,
        PatchEvaluatorV1.FlowPatchEvaluationV1 evaluation,
        FlowRegistry registry,
        string flowName,
        FlowRequestOptions requestOptions,
        QosTier qosTier,
        DateTimeOffset? configTimestampUtc)
    {
        writer.WriteStartObject();

        if (!evaluation.TryGetFlowPatch(out var flowPatch) || flowPatch.ValueKind != JsonValueKind.Object)
        {
            writer.WriteNull("hash");
            writer.WriteNull("effective");
            writer.WriteNull("sources");
            writer.WriteEndObject();
            return;
        }

        if (!registry.TryGetParamsBinding(flowName, out var paramsType, out _, out var defaultParams))
        {
            writer.WriteNull("hash");
            writer.WriteNull("effective");
            writer.WriteNull("sources");
            writer.WriteEndObject();
            return;
        }

        JsonElement defaultParamsElement;

        try
        {
            defaultParamsElement = JsonSerializer.SerializeToElement(defaultParams, paramsType);
        }
        catch
        {
            writer.WriteNull("hash");
            writer.WriteNull("effective");
            writer.WriteNull("sources");
            writer.WriteEndObject();
            return;
        }

        try
        {
            if (!FlowParamsResolver.TryComputeExplainFull(
                    defaultParamsElement,
                    flowPatch,
                    requestOptions.Variants,
                    qosTier,
                    out var effectiveJsonUtf8,
                    out var sources,
                    out var hash,
                    configTimestampUtc))
            {
                writer.WriteNull("hash");
                writer.WriteNull("effective");
                writer.WriteNull("sources");
                writer.WriteEndObject();
                return;
            }

            writer.WriteString("hash", hash.ToString("X16"));

            using var effectiveDoc = JsonDocument.Parse(effectiveJsonUtf8);
            writer.WritePropertyName("effective");
            effectiveDoc.RootElement.WriteTo(writer);

            writer.WritePropertyName("sources");
            writer.WriteStartArray();

            for (var i = 0; i < sources.Length; i++)
            {
                WriteParamsSourceEntry(writer, sources[i]);
            }

            writer.WriteEndArray();
            writer.WriteEndObject();
        }
        catch
        {
            writer.WriteNull("hash");
            writer.WriteNull("effective");
            writer.WriteNull("sources");
            writer.WriteEndObject();
        }
    }

    private static void WriteParamsSourceEntry(Utf8JsonWriter writer, ParamsSourceEntry entry)
    {
        writer.WriteStartObject();
        writer.WriteString("path", entry.Path);
        writer.WriteString("layer", entry.Layer);

        if (entry.ExperimentLayer is null)
        {
            writer.WriteNull("experiment_layer");
        }
        else
        {
            writer.WriteString("experiment_layer", entry.ExperimentLayer);
        }

        if (entry.ExperimentVariant is null)
        {
            writer.WriteNull("experiment_variant");
        }
        else
        {
            writer.WriteString("experiment_variant", entry.ExperimentVariant);
        }

        if (entry.QosTier is null)
        {
            writer.WriteNull("qos_tier");
        }
        else
        {
            writer.WriteString("qos_tier", entry.QosTier);
        }

        writer.WriteEndObject();
    }

    private static void WriteStageSnapshots(
        Utf8JsonWriter writer,
        PatchEvaluatorV1.FlowPatchEvaluationV1 evaluation,
        IReadOnlyList<PlanExplainStageContract> stageContracts,
        FlowRequestOptions requestOptions,
        ModuleCatalog catalog,
        SelectorRegistry selectorRegistry)
    {
        var stageContractMap = BuildStageContractMap(stageContracts);

        var stages = evaluation.Stages;
        var sortedStages = stages.Count == 0 ? Array.Empty<PatchEvaluatorV1.StagePatchV1>() : new PatchEvaluatorV1.StagePatchV1[stages.Count];

        for (var i = 0; i < sortedStages.Length; i++)
        {
            sortedStages[i] = stages[i];
        }

        if (sortedStages.Length > 1)
        {
            Array.Sort(sortedStages, StagePatchByNameComparer.Instance);
        }

        writer.WriteStartArray();

        FlowContext? selectorFlowContext = null;

        for (var i = 0; i < sortedStages.Length; i++)
        {
            WriteStageSnapshot(writer, sortedStages[i], stageContractMap, requestOptions, catalog, selectorRegistry, ref selectorFlowContext);
        }

        writer.WriteEndArray();
    }

    private static Dictionary<string, StageContract> BuildStageContractMap(IReadOnlyList<PlanExplainStageContract> stageContracts)
    {
        if (stageContracts.Count == 0)
        {
            return new Dictionary<string, StageContract>(capacity: 0);
        }

        var map = new Dictionary<string, StageContract>(capacity: stageContracts.Count);

        for (var i = 0; i < stageContracts.Count; i++)
        {
            var contract = stageContracts[i];
            map[contract.StageName] = contract.Contract;
        }

        return map;
    }

    private sealed class StagePatchByNameComparer : IComparer<PatchEvaluatorV1.StagePatchV1>
    {
        public static readonly StagePatchByNameComparer Instance = new();

        public int Compare(PatchEvaluatorV1.StagePatchV1 x, PatchEvaluatorV1.StagePatchV1 y)
        {
            return string.CompareOrdinal(x.StageName, y.StageName);
        }
    }

    private static void WriteStageSnapshot(
        Utf8JsonWriter writer,
        PatchEvaluatorV1.StagePatchV1 stage,
        Dictionary<string, StageContract> stageContractMap,
        FlowRequestOptions requestOptions,
        ModuleCatalog catalog,
        SelectorRegistry selectorRegistry,
        ref FlowContext? selectorFlowContext)
    {
        var stageName = stage.StageName;

        var hasContract = stageContractMap.TryGetValue(stageName, out var stageContract);
        var allowsDynamicModules = hasContract && stageContract.AllowsDynamicModules;

        var fanoutMaxRequested = stage.HasFanoutMax ? stage.FanoutMax : (int?)null;
        var fanoutMaxEffective = ComputeEffectiveFanoutMax(stage, hasContract, stageContract);

        writer.WriteStartObject();
        writer.WriteString("stage_name", stageName);

        if (fanoutMaxRequested.HasValue)
        {
            writer.WriteNumber("fanout_max", fanoutMaxRequested.Value);
        }
        else
        {
            writer.WriteNull("fanout_max");
        }

        if (fanoutMaxEffective.HasValue)
        {
            writer.WriteNumber("fanout_max_effective", fanoutMaxEffective.Value);
        }
        else
        {
            writer.WriteNull("fanout_max_effective");
        }

        writer.WritePropertyName("modules");
        WriteStageModules(
            writer,
            stage,
            allowsDynamicModules,
            stageContract,
            fanoutMaxEffective,
            requestOptions,
            catalog,
            selectorRegistry,
            ref selectorFlowContext);

        writer.WritePropertyName("shadow_modules");
        WriteShadowModules(
            writer,
            stage,
            allowsDynamicModules,
            hasContract,
            stageContract,
            requestOptions,
            catalog,
            selectorRegistry,
            ref selectorFlowContext);

        writer.WriteEndObject();
    }

    private static int? ComputeEffectiveFanoutMax(PatchEvaluatorV1.StagePatchV1 stage, bool hasContract, StageContract contract)
    {
        if (!hasContract)
        {
            return null;
        }

        var fanoutMax = stage.HasFanoutMax ? stage.FanoutMax : int.MaxValue;

        if (fanoutMax < 0)
        {
            fanoutMax = 0;
        }

        var contractMaxFanoutMax = contract.MaxFanoutMax;
        if (fanoutMax > contractMaxFanoutMax)
        {
            fanoutMax = contractMaxFanoutMax;
        }

        return fanoutMax;
    }

    private static void WriteStageModules(
        Utf8JsonWriter writer,
        PatchEvaluatorV1.StagePatchV1 stage,
        bool allowsDynamicModules,
        StageContract stageContract,
        int? fanoutMaxEffective,
        FlowRequestOptions requestOptions,
        ModuleCatalog catalog,
        SelectorRegistry selectorRegistry,
        ref FlowContext? selectorFlowContext)
    {
        var modules = stage.Modules;
        var moduleCount = modules.Count;

        var decisionKinds = moduleCount == 0 ? Array.Empty<byte>() : new byte[moduleCount];
        var decisionCodes = moduleCount == 0 ? Array.Empty<string>() : new string[moduleCount];
        var gateDecisionCodes = moduleCount == 0 ? Array.Empty<string?>() : new string?[moduleCount];
        var gateReasonCodes = moduleCount == 0 ? Array.Empty<string?>() : new string?[moduleCount];
        var gateSelectorNames = moduleCount == 0 ? Array.Empty<string?>() : new string?[moduleCount];

        if (!allowsDynamicModules)
        {
            for (var i = 0; i < moduleCount; i++)
            {
                decisionKinds[i] = 0;
                decisionCodes[i] = ExecutionEngine.StageContractDynamicModulesForbiddenCode;
            }

            writer.WriteStartArray();

            for (var i = 0; i < moduleCount; i++)
            {
                WriteModuleDecision(
                    writer,
                    modules[i],
                    catalog,
                    gateDecisionCodes[i],
                    gateReasonCodes[i],
                    gateSelectorNames[i],
                    isSelected: decisionKinds[i] == 1,
                    decisionCode: decisionCodes[i]);
            }

            writer.WriteEndArray();
            return;
        }

        var candidates = moduleCount == 0 ? Array.Empty<StageModuleCandidate>() : new StageModuleCandidate[moduleCount];
        var candidateCount = 0;

        var allowedModuleTypes = stageContract.AllowedModuleTypes;

        for (var i = 0; i < moduleCount; i++)
        {
            var module = modules[i];

            if (!module.Enabled)
            {
                decisionKinds[i] = 0;
                decisionCodes[i] = ExecutionEngine.DisabledCode;
                continue;
            }

            if (!IsModuleTypeAllowed(allowedModuleTypes, module.ModuleType))
            {
                decisionKinds[i] = 0;
                decisionCodes[i] = ExecutionEngine.StageContractModuleTypeForbiddenCode;
                continue;
            }

            if (module.HasGate)
            {
                var gateDecision = EvaluateGateDecision(module.Gate, requestOptions, selectorRegistry, ref selectorFlowContext, out var selectorName);
                gateDecisionCodes[i] = gateDecision.Code;
                gateReasonCodes[i] = gateDecision.ReasonCode;
                gateSelectorNames[i] = selectorName;

                if (!gateDecision.Allowed)
                {
                    decisionKinds[i] = 0;
                    decisionCodes[i] = ExecutionEngine.GateFalseCode;
                    continue;
                }
            }

            candidates[candidateCount] = new StageModuleCandidate(i, module.Priority);
            candidateCount++;
        }

        SortCandidates(candidates, candidateCount);

        var maxModulesHard = stageContract.MaxModulesHard;
        if (maxModulesHard != 0 && maxModulesHard < candidateCount)
        {
            for (var rank = maxModulesHard; rank < candidateCount; rank++)
            {
                var moduleIndex = candidates[rank].ModuleIndex;
                decisionKinds[moduleIndex] = 0;
                decisionCodes[moduleIndex] = ExecutionEngine.StageContractMaxModulesHardExceededCode;
            }

            candidateCount = maxModulesHard;
        }

        var executeCount = candidateCount;
        var fanoutMax = fanoutMaxEffective ?? int.MaxValue;

        if (fanoutMax < executeCount)
        {
            executeCount = fanoutMax;
        }

        if (executeCount < 0)
        {
            executeCount = 0;
        }

        for (var rank = 0; rank < candidateCount; rank++)
        {
            var moduleIndex = candidates[rank].ModuleIndex;

            if (rank < executeCount)
            {
                decisionKinds[moduleIndex] = 1;
                decisionCodes[moduleIndex] = SelectedDecisionCode;
            }
            else
            {
                decisionKinds[moduleIndex] = 0;
                decisionCodes[moduleIndex] = ExecutionEngine.FanoutTrimCode;
            }
        }

        writer.WriteStartArray();

        for (var i = 0; i < moduleCount; i++)
        {
            WriteModuleDecision(
                writer,
                modules[i],
                catalog,
                gateDecisionCodes[i],
                gateReasonCodes[i],
                gateSelectorNames[i],
                isSelected: decisionKinds[i] == 1,
                decisionCode: decisionCodes[i]);
        }

        writer.WriteEndArray();
    }

    private static void WriteModuleDecision(
        Utf8JsonWriter writer,
        PatchEvaluatorV1.StageModulePatchV1 module,
        ModuleCatalog catalog,
        string? gateDecisionCode,
        string? gateReasonCode,
        string? gateSelectorName,
        bool isSelected,
        string decisionCode)
    {
        writer.WriteStartObject();
        writer.WriteString("module_id", module.ModuleId);
        writer.WriteString("module_type", module.ModuleType);
        writer.WriteBoolean("module_type_registered", catalog.IsRegistered(module.ModuleType));
        writer.WriteBoolean("enabled", module.Enabled);
        writer.WriteBoolean("disabled_by_emergency", module.DisabledByEmergency);
        writer.WriteNumber("priority", module.Priority);

        if (gateDecisionCode is null)
        {
            writer.WriteNull("gate_decision_code");
        }
        else
        {
            writer.WriteString("gate_decision_code", gateDecisionCode);
        }

        if (string.IsNullOrEmpty(gateReasonCode))
        {
            writer.WriteNull("gate_reason_code");
        }
        else
        {
            writer.WriteString("gate_reason_code", gateReasonCode);
        }

        if (gateSelectorName is null)
        {
            writer.WriteNull("gate_selector_name");
        }
        else
        {
            writer.WriteString("gate_selector_name", gateSelectorName);
        }

        writer.WriteString("decision_kind", isSelected ? "execute" : "skip");
        writer.WriteString("decision_code", decisionCode);
        writer.WriteEndObject();
    }

    private static bool IsModuleTypeAllowed(ReadOnlySpan<string> allowedModuleTypes, string moduleType)
    {
        if (allowedModuleTypes.Length == 0)
        {
            return true;
        }

        for (var i = 0; i < allowedModuleTypes.Length; i++)
        {
            if (string.Equals(allowedModuleTypes[i], moduleType, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static void SortCandidates(StageModuleCandidate[] candidates, int count)
    {
        for (var i = 1; i < count; i++)
        {
            var candidate = candidates[i];
            var j = i - 1;

            while (j >= 0
                   && (candidates[j].Priority < candidate.Priority
                       || (candidates[j].Priority == candidate.Priority && candidates[j].ModuleIndex > candidate.ModuleIndex)))
            {
                candidates[j + 1] = candidates[j];
                j--;
            }

            candidates[j + 1] = candidate;
        }
    }

    private readonly struct StageModuleCandidate
    {
        public int ModuleIndex { get; }

        public int Priority { get; }

        public StageModuleCandidate(int moduleIndex, int priority)
        {
            ModuleIndex = moduleIndex;
            Priority = priority;
        }
    }

    private static GateDecision EvaluateGateDecision(
        JsonElement gateElement,
        FlowRequestOptions requestOptions,
        SelectorRegistry selectorRegistry,
        ref FlowContext? selectorFlowContext,
        out string? gateSelectorName)
    {
        if (!GateJsonV1.TryParseOptional(gateElement, "$.gate", selectorRegistry, out var gate, out var finding))
        {
            throw new FormatException(finding.Message);
        }

        if (gate is null)
        {
            gateSelectorName = null;
            return GateDecision.AllowedDecision;
        }

        gateSelectorName = gate is SelectorGate selectorGate ? selectorGate.SelectorName : null;

        if (GateContainsSelectorGate(gate))
        {
            selectorFlowContext ??= new FlowContext(
                services: EmptyServiceProvider.Instance,
                cancellationToken: System.Threading.CancellationToken.None,
                deadline: SelectorFlowContextDeadline,
                requestOptions: requestOptions);

            return GateEvaluator.Evaluate(gate, selectorFlowContext, selectorRegistry);
        }

        var variantsDictionary = requestOptions.Variants ?? EmptyVariantDictionary;
        var evalContext = new GateEvaluationContext(
            variants: new VariantSet(variantsDictionary),
            userId: requestOptions.UserId,
            requestAttributes: requestOptions.RequestAttributes,
            selectorRegistry: null,
            flowContext: null);

        return GateEvaluator.Evaluate(gate, in evalContext);
    }

    private static bool GateContainsSelectorGate(Gate gate)
    {
        if (gate is SelectorGate)
        {
            return true;
        }

        if (gate is AllGate all)
        {
            var children = all.Children.Span;
            for (var i = 0; i < children.Length; i++)
            {
                if (GateContainsSelectorGate(children[i]))
                {
                    return true;
                }
            }

            return false;
        }

        if (gate is AnyGate any)
        {
            var children = any.Children.Span;
            for (var i = 0; i < children.Length; i++)
            {
                if (GateContainsSelectorGate(children[i]))
                {
                    return true;
                }
            }

            return false;
        }

        if (gate is NotGate not)
        {
            return GateContainsSelectorGate(not.Child);
        }

        return false;
    }

    private sealed class EmptyServiceProvider : IServiceProvider
    {
        public static readonly EmptyServiceProvider Instance = new();

        private EmptyServiceProvider()
        {
        }

        public object? GetService(Type serviceType)
        {
            return null;
        }
    }

    private static void WriteShadowModules(
        Utf8JsonWriter writer,
        PatchEvaluatorV1.StagePatchV1 stage,
        bool allowsDynamicModules,
        bool hasContract,
        StageContract stageContract,
        FlowRequestOptions requestOptions,
        ModuleCatalog catalog,
        SelectorRegistry selectorRegistry,
        ref FlowContext? selectorFlowContext)
    {
        var shadowModules = stage.ShadowModules;
        var moduleCount = shadowModules.Count;

        writer.WriteStartArray();

        if (moduleCount == 0)
        {
            writer.WriteEndArray();
            return;
        }

        if (!allowsDynamicModules)
        {
            for (var i = 0; i < moduleCount; i++)
            {
                WriteShadowModuleDecision(
                    writer,
                    shadowModules[i],
                    catalog,
                    gateDecisionCode: null,
                    gateReasonCode: null,
                    gateSelectorName: null,
                    isSelected: false,
                    decisionCode: ExecutionEngine.StageContractDynamicModulesForbiddenCode,
                    effectiveShadowSampleBps: shadowModules[i].ShadowSampleBps);
            }

            writer.WriteEndArray();
            return;
        }

        if (!hasContract || !stageContract.AllowsShadowModules)
        {
            for (var i = 0; i < moduleCount; i++)
            {
                WriteShadowModuleDecision(
                    writer,
                    shadowModules[i],
                    catalog,
                    gateDecisionCode: null,
                    gateReasonCode: null,
                    gateSelectorName: null,
                    isSelected: false,
                    decisionCode: ExecutionEngine.StageContractShadowModulesForbiddenCode,
                    effectiveShadowSampleBps: shadowModules[i].ShadowSampleBps);
            }

            writer.WriteEndArray();
            return;
        }

        var decisionKinds = new byte[moduleCount];
        var decisionCodes = new string[moduleCount];
        var gateDecisionCodes = new string?[moduleCount];
        var gateReasonCodes = new string?[moduleCount];
        var gateSelectorNames = new string?[moduleCount];
        var effectiveSampleBps = new ushort[moduleCount];

        var candidates = new StageModuleCandidate[moduleCount];
        var candidateCount = 0;
        var maxShadowSampleBps = stageContract.MaxShadowSampleBps;
        var userId = requestOptions.UserId;

        var allowedModuleTypes = stageContract.AllowedModuleTypes;

        for (var i = 0; i < moduleCount; i++)
        {
            var module = shadowModules[i];
            var effectiveShadowSampleBps = module.ShadowSampleBps;
            if (effectiveShadowSampleBps > maxShadowSampleBps)
            {
                effectiveShadowSampleBps = (ushort)maxShadowSampleBps;
            }

            effectiveSampleBps[i] = effectiveShadowSampleBps;

            if (!module.Enabled)
            {
                decisionKinds[i] = 0;
                decisionCodes[i] = ExecutionEngine.DisabledCode;
                continue;
            }

            if (!IsModuleTypeAllowed(allowedModuleTypes, module.ModuleType))
            {
                decisionKinds[i] = 0;
                decisionCodes[i] = ExecutionEngine.StageContractModuleTypeForbiddenCode;
                continue;
            }

            if (module.HasGate)
            {
                var gateDecision = EvaluateGateDecision(module.Gate, requestOptions, selectorRegistry, ref selectorFlowContext, out var selectorName);
                gateDecisionCodes[i] = gateDecision.Code;
                gateReasonCodes[i] = gateDecision.ReasonCode;
                gateSelectorNames[i] = selectorName;

                if (!gateDecision.Allowed)
                {
                    decisionKinds[i] = 0;
                    decisionCodes[i] = ExecutionEngine.GateFalseCode;
                    continue;
                }
            }

            if (!ShouldExecuteShadow(effectiveShadowSampleBps, userId, module.ModuleId))
            {
                decisionKinds[i] = 0;
                decisionCodes[i] = ExecutionEngine.ShadowNotSampledCode;
                continue;
            }

            candidates[candidateCount] = new StageModuleCandidate(i, module.Priority);
            candidateCount++;
        }

        SortCandidates(candidates, candidateCount);

        var maxShadowModulesHard = stageContract.MaxShadowModulesHard;
        if (maxShadowModulesHard != 0 && maxShadowModulesHard < candidateCount)
        {
            for (var rank = maxShadowModulesHard; rank < candidateCount; rank++)
            {
                var moduleIndex = candidates[rank].ModuleIndex;
                decisionKinds[moduleIndex] = 0;
                decisionCodes[moduleIndex] = ExecutionEngine.StageContractMaxShadowModulesHardExceededCode;
            }

            candidateCount = maxShadowModulesHard;
        }

        for (var rank = 0; rank < candidateCount; rank++)
        {
            var moduleIndex = candidates[rank].ModuleIndex;
            decisionKinds[moduleIndex] = 1;
            decisionCodes[moduleIndex] = SelectedDecisionCode;
        }

        for (var i = 0; i < moduleCount; i++)
        {
            WriteShadowModuleDecision(
                writer,
                shadowModules[i],
                catalog,
                gateDecisionCodes[i],
                gateReasonCodes[i],
                gateSelectorNames[i],
                isSelected: decisionKinds[i] == 1,
                decisionCode: decisionCodes[i],
                effectiveShadowSampleBps: effectiveSampleBps[i]);
        }

        writer.WriteEndArray();
    }

    private static void WriteShadowModuleDecision(
        Utf8JsonWriter writer,
        PatchEvaluatorV1.StageModulePatchV1 module,
        ModuleCatalog catalog,
        string? gateDecisionCode,
        string? gateReasonCode,
        string? gateSelectorName,
        bool isSelected,
        string decisionCode,
        ushort effectiveShadowSampleBps)
    {
        writer.WriteStartObject();
        writer.WriteString("module_id", module.ModuleId);
        writer.WriteString("module_type", module.ModuleType);
        writer.WriteBoolean("module_type_registered", catalog.IsRegistered(module.ModuleType));
        writer.WriteBoolean("enabled", module.Enabled);
        writer.WriteBoolean("disabled_by_emergency", module.DisabledByEmergency);
        writer.WriteNumber("priority", module.Priority);
        writer.WriteNumber("shadow_sample_bps", module.ShadowSampleBps);
        writer.WriteNumber("shadow_sample_bps_effective", effectiveShadowSampleBps);

        if (gateDecisionCode is null)
        {
            writer.WriteNull("gate_decision_code");
        }
        else
        {
            writer.WriteString("gate_decision_code", gateDecisionCode);
        }

        if (string.IsNullOrEmpty(gateReasonCode))
        {
            writer.WriteNull("gate_reason_code");
        }
        else
        {
            writer.WriteString("gate_reason_code", gateReasonCode);
        }

        if (gateSelectorName is null)
        {
            writer.WriteNull("gate_selector_name");
        }
        else
        {
            writer.WriteString("gate_selector_name", gateSelectorName);
        }

        writer.WriteString("decision_kind", isSelected ? "execute" : "skip");
        writer.WriteString("decision_code", decisionCode);
        writer.WriteEndObject();
    }

    private static bool ShouldExecuteShadow(ushort sampleBps, string? userId, string moduleId)
    {
        if (sampleBps == 0)
        {
            return false;
        }

        if (sampleBps >= 10000)
        {
            return true;
        }

        if (string.IsNullOrEmpty(userId))
        {
            return false;
        }

        var bucket = ComputeShadowBucket(userId!, moduleId);
        return bucket < sampleBps;
    }

    private static uint ComputeShadowBucket(string userId, string moduleId)
    {
        const ulong offsetBasis = 14695981039346656037;
        const ulong prime = 1099511628211;

        var hash = offsetBasis;
        hash = HashChars(hash, userId);
        hash = HashChar(hash, '\0');
        hash = HashChars(hash, moduleId);

        return (uint)(hash % 10000);

        static ulong HashChars(ulong hash, string value)
        {
            for (var i = 0; i < value.Length; i++)
            {
                hash = HashChar(hash, value[i]);
            }

            return hash;
        }

        static ulong HashChar(ulong hash, char c)
        {
            var u = (ushort)c;

            hash ^= (byte)u;
            hash *= prime;
            hash ^= (byte)(u >> 8);
            hash *= prime;

            return hash;
        }
    }
}
