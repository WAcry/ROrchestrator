using System.Buffers;
using System.Text;
using System.Text.Json;
using ROrchestrator.Core;
using ROrchestrator.Core.Gates;
using ROrchestrator.Core.Selectors;

namespace ROrchestrator.Tooling;

public static class ToolingJsonV1
{
    public static ToolingCommandResult ValidatePatchJson(string patchJson, FlowRegistry registry, ModuleCatalog catalog)
    {
        return ValidatePatchJson(patchJson, registry, catalog, SelectorRegistry.Empty);
    }

    public static ToolingCommandResult ValidatePatchJson(
        string patchJson,
        FlowRegistry registry,
        ModuleCatalog catalog,
        SelectorRegistry selectorRegistry)
    {
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

        var validator = new ConfigValidator(registry, catalog, selectorRegistry);
        var report = validator.ValidatePatchJson(patchJson);

        var exitCode = report.IsValid ? 0 : 2;
        var json = BuildValidateJson(report);

        return new ToolingCommandResult(exitCode, json);
    }

    public static ToolingCommandResult ExplainFlowJson(
        string flowName,
        FlowRegistry registry,
        ModuleCatalog catalog,
        bool includeMermaid = false)
    {
        if (flowName is null)
        {
            throw new ArgumentNullException(nameof(flowName));
        }

        if (registry is null)
        {
            throw new ArgumentNullException(nameof(registry));
        }

        if (catalog is null)
        {
            throw new ArgumentNullException(nameof(catalog));
        }

        try
        {
            var explain = registry.Explain(flowName, catalog);
            var json = BuildExplainJson(explain, includeMermaid);
            return new ToolingCommandResult(exitCode: 0, json);
        }
        catch (Exception ex) when (IsExplainInputException(ex))
        {
            var json = BuildExplainErrorJson(
                code: "EXPLAIN_INPUT_INVALID",
                message: ex.Message);
            return new ToolingCommandResult(exitCode: 2, json);
        }
        catch (Exception ex) when (ToolingExceptionGuard.ShouldHandle(ex))
        {
            var json = BuildExplainErrorJson(
                code: "EXPLAIN_INTERNAL_ERROR",
                message: string.IsNullOrEmpty(ex.Message) ? "Internal error." : ex.Message);
            return new ToolingCommandResult(exitCode: 1, json);
        }
    }

    public static ToolingCommandResult ExplainPatchJson(
        string flowName,
        string patchJson,
        FlowRequestOptions requestOptions = default,
        bool includeMermaid = false)
    {
        if (flowName is null)
        {
            throw new ArgumentNullException(nameof(flowName));
        }

        if (patchJson is null)
        {
            throw new ArgumentNullException(nameof(patchJson));
        }

        try
        {
            using var evaluation = PatchEvaluatorV1.Evaluate(flowName, patchJson, requestOptions);
            var json = BuildExplainPatchJson(flowName, evaluation, requestOptions, includeMermaid);
            return new ToolingCommandResult(exitCode: 0, json);
        }
        catch (Exception ex) when (IsExplainPatchInputException(ex))
        {
            var json = BuildExplainPatchErrorJson(
                code: "EXPLAIN_PATCH_INPUT_INVALID",
                message: ex.Message);
            return new ToolingCommandResult(exitCode: 2, json);
        }
        catch (Exception ex) when (ToolingExceptionGuard.ShouldHandle(ex))
        {
            var json = BuildExplainPatchErrorJson(
                code: "EXPLAIN_PATCH_INTERNAL_ERROR",
                message: string.IsNullOrEmpty(ex.Message) ? "Internal error." : ex.Message);
            return new ToolingCommandResult(exitCode: 1, json);
        }
    }

    public static ToolingCommandResult DiffPatchJson(string oldPatchJson, string newPatchJson)
    {
        if (oldPatchJson is null)
        {
            throw new ArgumentNullException(nameof(oldPatchJson));
        }

        if (newPatchJson is null)
        {
            throw new ArgumentNullException(nameof(newPatchJson));
        }

        try
        {
            var moduleReport = PatchDiffV1.DiffModules(oldPatchJson, newPatchJson);
            var paramReport = PatchDiffV1.DiffParams(oldPatchJson, newPatchJson);
            var fanoutReport = PatchDiffV1.DiffFanoutMax(oldPatchJson, newPatchJson);
            var emergencyReport = PatchDiffV1.DiffEmergency(oldPatchJson, newPatchJson);

            var json = BuildDiffJson(moduleReport, paramReport, fanoutReport, emergencyReport);
            return new ToolingCommandResult(exitCode: 0, json);
        }
        catch (Exception ex) when (IsDiffInputException(ex))
        {
            var json = BuildDiffErrorJson(
                code: "DIFF_INPUT_INVALID",
                message: ex.Message);
            return new ToolingCommandResult(exitCode: 2, json);
        }
        catch (Exception ex) when (ToolingExceptionGuard.ShouldHandle(ex))
        {
            var json = BuildDiffErrorJson(
                code: "DIFF_INTERNAL_ERROR",
                message: string.IsNullOrEmpty(ex.Message) ? "Internal error." : ex.Message);
            return new ToolingCommandResult(exitCode: 1, json);
        }
    }

    private const string SelectedDecisionCode = "SELECTED";

    private static string BuildExplainPatchJson(
        string flowName,
        PatchEvaluatorV1.FlowPatchEvaluationV1 evaluation,
        FlowRequestOptions requestOptions,
        bool includeMermaid)
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
        writer.WriteString("kind", "explain_patch");
        writer.WriteString("flow_name", flowName);

        if (requestOptions.Variants is null)
        {
            writer.WriteNull("variants");
        }
        else
        {
            writer.WritePropertyName("variants");
            writer.WriteStartObject();

            foreach (var pair in requestOptions.Variants)
            {
                writer.WriteString(pair.Key, pair.Value);
            }

            writer.WriteEndObject();
        }

        writer.WritePropertyName("overlays_applied");
        writer.WriteStartArray();

        var overlays = evaluation.OverlaysApplied;
        for (var i = 0; i < overlays.Count; i++)
        {
            WriteExplainPatchOverlay(writer, overlays[i]);
        }

        writer.WriteEndArray();

        writer.WritePropertyName("stages");
        writer.WriteStartArray();

        var stages = evaluation.Stages;

        for (var i = 0; i < stages.Count; i++)
        {
            WriteExplainPatchStage(writer, stages[i], requestOptions);
        }

        writer.WriteEndArray();

        if (includeMermaid)
        {
            writer.WriteString("mermaid", BuildExplainPatchMermaid(evaluation, requestOptions));
        }

        writer.WriteEndObject();
        writer.Flush();

        return Encoding.UTF8.GetString(output.WrittenSpan);
    }

    private static void WriteExplainPatchOverlay(Utf8JsonWriter writer, PatchEvaluatorV1.PatchOverlayAppliedV1 overlay)
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

    private static void WriteExplainPatchStage(Utf8JsonWriter writer, PatchEvaluatorV1.StagePatchV1 stage, FlowRequestOptions requestOptions)
    {
        var modules = stage.Modules;
        var moduleCount = modules.Count;

        var decisionKinds = moduleCount == 0 ? Array.Empty<byte>() : new byte[moduleCount];
        var decisionCodes = moduleCount == 0 ? Array.Empty<string>() : new string[moduleCount];

        var candidates = moduleCount == 0 ? Array.Empty<StageModuleCandidate>() : new StageModuleCandidate[moduleCount];
        var candidateCount = 0;

        var variants = requestOptions.Variants;
        var requestAttributes = requestOptions.RequestAttributes;
        var userId = requestOptions.UserId;

        for (var i = 0; i < moduleCount; i++)
        {
            var module = modules[i];

            if (!module.Enabled)
            {
                decisionKinds[i] = 0;
                decisionCodes[i] = ExecutionEngine.DisabledCode;
                continue;
            }

            if (module.HasGate && !EvaluateGateAllowed(module.Gate, variants, userId, requestAttributes))
            {
                decisionKinds[i] = 0;
                decisionCodes[i] = ExecutionEngine.GateFalseCode;
                continue;
            }

            candidates[candidateCount] = new StageModuleCandidate(i, module.Priority);
            candidateCount++;
        }

        SortCandidates(candidates, candidateCount);

        var fanoutMax = stage.HasFanoutMax ? stage.FanoutMax : int.MaxValue;

        if (fanoutMax < 0)
        {
            fanoutMax = 0;
        }

        var executeCount = candidateCount;

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

        writer.WriteStartObject();
        writer.WriteString("stage_name", stage.StageName);

        if (stage.HasFanoutMax)
        {
            writer.WriteNumber("fanout_max", stage.FanoutMax);
        }
        else
        {
            writer.WriteNull("fanout_max");
        }

        writer.WritePropertyName("modules");
        writer.WriteStartArray();

        for (var i = 0; i < moduleCount; i++)
        {
            var module = modules[i];
            var kind = decisionKinds[i] == 1 ? "execute" : "skip";
            var code = decisionCodes[i];

            writer.WriteStartObject();
            writer.WriteString("module_id", module.ModuleId);
            writer.WriteString("module_type", module.ModuleType);
            writer.WriteBoolean("enabled", module.Enabled);
            writer.WriteBoolean("disabled_by_emergency", module.DisabledByEmergency);
            writer.WriteNumber("priority", module.Priority);
            writer.WriteString("decision_kind", kind);
            writer.WriteString("decision_code", code);
            writer.WriteEndObject();
        }

        writer.WriteEndArray();
        writer.WriteEndObject();
    }

    private static bool EvaluateGateAllowed(
        JsonElement gateElement,
        IReadOnlyDictionary<string, string>? variants,
        string? userId,
        IReadOnlyDictionary<string, string>? requestAttributes)
    {
        if (!GateJsonV1.TryParseOptional(gateElement, "$.gate", out var gate, out var finding))
        {
            throw new FormatException(finding.Message);
        }

        if (gate is null)
        {
            return true;
        }

        var variantsDictionary = variants ?? EmptyVariantDictionary;
        var evalContext = new GateEvaluationContext(
            variants: new VariantSet(variantsDictionary),
            userId: userId,
            requestAttributes: requestAttributes,
            selectorRegistry: null,
            flowContext: null);

        return GateEvaluator.Evaluate(gate, in evalContext).Allowed;
    }

    private static string BuildExplainPatchMermaid(PatchEvaluatorV1.FlowPatchEvaluationV1 evaluation, FlowRequestOptions requestOptions)
    {
        var stages = evaluation.Stages;
        var builder = new StringBuilder(256);
        builder.Append("flowchart TD\n");

        var moduleIndex = 0;

        for (var stageIndex = 0; stageIndex < stages.Count; stageIndex++)
        {
            var stage = stages[stageIndex];

            builder.Append("  s");
            builder.Append(stageIndex);
            builder.Append("[\"");
            builder.Append(stage.StageName);
            builder.Append("\\nfanout_max=");
            builder.Append(stage.HasFanoutMax ? stage.FanoutMax.ToString(System.Globalization.CultureInfo.InvariantCulture) : "null");
            builder.Append("\"]\n");

            var modules = stage.Modules;
            var moduleCount = modules.Count;

            if (moduleCount == 0)
            {
                continue;
            }

            var decisionKinds = new byte[moduleCount];
            var decisionCodes = new string[moduleCount];
            var candidates = new StageModuleCandidate[moduleCount];
            var candidateCount = 0;

            var variants = requestOptions.Variants;
            var requestAttributes = requestOptions.RequestAttributes;
            var userId = requestOptions.UserId;

            for (var i = 0; i < moduleCount; i++)
            {
                var module = modules[i];

                if (!module.Enabled)
                {
                    decisionKinds[i] = 0;
                    decisionCodes[i] = ExecutionEngine.DisabledCode;
                    continue;
                }

                if (module.HasGate && !EvaluateGateAllowed(module.Gate, variants, userId, requestAttributes))
                {
                    decisionKinds[i] = 0;
                    decisionCodes[i] = ExecutionEngine.GateFalseCode;
                    continue;
                }

                candidates[candidateCount] = new StageModuleCandidate(i, module.Priority);
                candidateCount++;
            }

            SortCandidates(candidates, candidateCount);

            var fanoutMax = stage.HasFanoutMax ? stage.FanoutMax : int.MaxValue;

            if (fanoutMax < 0)
            {
                fanoutMax = 0;
            }

            var executeCount = candidateCount;

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
                var idx = candidates[rank].ModuleIndex;

                if (rank < executeCount)
                {
                    decisionKinds[idx] = 1;
                    decisionCodes[idx] = SelectedDecisionCode;
                }
                else
                {
                    decisionKinds[idx] = 0;
                    decisionCodes[idx] = ExecutionEngine.FanoutTrimCode;
                }
            }

            for (var i = 0; i < moduleCount; i++)
            {
                var module = modules[i];
                var kind = decisionKinds[i] == 1 ? "execute" : "skip";
                var code = decisionCodes[i];

                builder.Append("  s");
                builder.Append(stageIndex);
                builder.Append(" --> m");
                builder.Append(moduleIndex);
                builder.Append("[\"");
                builder.Append(module.ModuleId);
                builder.Append("\\n");
                builder.Append(kind);
                builder.Append("\\n");
                builder.Append(code);
                builder.Append("\"]\n");

                moduleIndex++;
            }
        }

        return builder.ToString();
    }

    private static string BuildExplainPatchErrorJson(string code, string message)
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
        writer.WriteString("kind", "explain_patch");
        writer.WritePropertyName("error");
        writer.WriteStartObject();
        writer.WriteString("code", code);
        writer.WriteString("message", message);
        writer.WriteEndObject();
        writer.WriteEndObject();
        writer.Flush();

        return Encoding.UTF8.GetString(output.WrittenSpan);
    }

    private static readonly IReadOnlyDictionary<string, string> EmptyVariantDictionary =
        new System.Collections.ObjectModel.ReadOnlyDictionary<string, string>(new Dictionary<string, string>(0));

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

    private static string BuildExplainJson(PlanExplain explain, bool includeMermaid)
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
        writer.WriteString("kind", "explain");
        writer.WriteString("flow_name", explain.FlowName);
        writer.WriteString("plan_template_hash", explain.PlanTemplateHash.ToString("X16"));

        writer.WritePropertyName("nodes");
        writer.WriteStartArray();

        var nodes = explain.Nodes;
        for (var i = 0; i < nodes.Count; i++)
        {
            WriteExplainNode(writer, nodes[i]);
        }

        writer.WriteEndArray();

        if (includeMermaid)
        {
            writer.WriteString("mermaid", BuildExplainMermaid(explain));
        }

        writer.WriteEndObject();
        writer.Flush();

        return Encoding.UTF8.GetString(output.WrittenSpan);
    }

    private static void WriteExplainNode(Utf8JsonWriter writer, PlanExplainNode node)
    {
        writer.WriteStartObject();
        writer.WriteString("kind", GetNodeKindString(node.Kind));
        writer.WriteString("name", node.Name);

        if (node.StageName is null)
        {
            writer.WriteNull("stage_name");
        }
        else
        {
            writer.WriteString("stage_name", node.StageName);
        }

        if (node.ModuleType is null)
        {
            writer.WriteNull("module_type");
        }
        else
        {
            writer.WriteString("module_type", node.ModuleType);
        }

        writer.WriteString("output_type", FormatTypeName(node.OutputType));
        writer.WriteEndObject();
    }

    private static string BuildExplainMermaid(PlanExplain explain)
    {
        var nodes = explain.Nodes;
        var nodeCount = nodes.Count;

        var builder = new StringBuilder(256);
        builder.Append("flowchart TD\n");

        if (nodeCount > 1)
        {
            for (var i = 0; i < nodeCount - 1; i++)
            {
                builder.Append("  n");
                builder.Append(i);
                builder.Append("[\"");
                AppendMermaidNodeLabel(builder, nodes[i]);
                builder.Append("\"] --> n");
                builder.Append(i + 1);
                builder.Append("[\"");
                AppendMermaidNodeLabel(builder, nodes[i + 1]);
                builder.Append("\"]\n");
            }
        }
        else if (nodeCount == 1)
        {
            builder.Append("  n0[\"");
            AppendMermaidNodeLabel(builder, nodes[0]);
            builder.Append("\"]\n");
        }

        return builder.ToString();
    }

    private static void AppendMermaidNodeLabel(StringBuilder builder, PlanExplainNode node)
    {
        AppendMermaidEscaped(builder, node.Name);
        builder.Append("\\n");
        builder.Append(GetNodeKindString(node.Kind));

        if (node.Kind == ROrchestrator.Core.Blueprint.BlueprintNodeKind.Step)
        {
            builder.Append("\\n(");
            AppendMermaidEscaped(builder, node.ModuleType!);
            builder.Append(')');
        }

        builder.Append("\\n");
        AppendMermaidEscaped(builder, FormatTypeName(node.OutputType));
    }

    private static void AppendMermaidEscaped(StringBuilder builder, string value)
    {
        for (var i = 0; i < value.Length; i++)
        {
            var c = value[i];

            if (c == '\\' || c == '"')
            {
                builder.Append('\\');
            }

            builder.Append(c);
        }
    }

    private static string BuildExplainErrorJson(string code, string message)
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
        writer.WriteString("kind", "explain");

        writer.WritePropertyName("error");
        writer.WriteStartObject();
        writer.WriteString("code", code);
        writer.WriteString("message", message);
        writer.WriteEndObject();

        writer.WriteEndObject();
        writer.Flush();

        return Encoding.UTF8.GetString(output.WrittenSpan);
    }

    private static string BuildValidateJson(ValidationReport report)
    {
        var output = new ArrayBufferWriter<byte>(256);
        using var writer = new Utf8JsonWriter(output, new JsonWriterOptions { Indented = false });

        writer.WriteStartObject();
        writer.WriteString("kind", "validate");
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
        writer.Flush();

        return Encoding.UTF8.GetString(output.WrittenSpan);
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

    private static string BuildDiffJson(
        PatchModuleDiffReport moduleReport,
        PatchParamDiffReport paramReport,
        PatchFanoutMaxDiffReport fanoutReport,
        PatchEmergencyDiffReport emergencyReport)
    {
        var output = new ArrayBufferWriter<byte>(512);
        using var writer = new Utf8JsonWriter(output, new JsonWriterOptions { Indented = false });

        writer.WriteStartObject();
        writer.WriteString("kind", "diff");

        WriteModuleDiffs(writer, moduleReport.Diffs);
        WriteParamDiffs(writer, paramReport.Diffs);
        WriteFanoutMaxDiffs(writer, fanoutReport.Diffs);
        WriteEmergencyDiffs(writer, emergencyReport.Diffs);

        writer.WriteEndObject();
        writer.Flush();

        return Encoding.UTF8.GetString(output.WrittenSpan);
    }

    private static string BuildDiffErrorJson(string code, string message)
    {
        var output = new ArrayBufferWriter<byte>(256);
        using var writer = new Utf8JsonWriter(output, new JsonWriterOptions { Indented = false });

        writer.WriteStartObject();
        writer.WriteString("kind", "diff");
        writer.WritePropertyName("error");
        writer.WriteStartObject();
        writer.WriteString("code", code);
        writer.WriteString("message", message);
        writer.WriteEndObject();
        writer.WriteEndObject();
        writer.Flush();

        return Encoding.UTF8.GetString(output.WrittenSpan);
    }

    private static void WriteModuleDiffs(Utf8JsonWriter writer, IReadOnlyList<PatchModuleDiff> diffs)
    {
        writer.WritePropertyName("module_diffs");
        writer.WriteStartArray();

        if (diffs.Count == 1)
        {
            WriteModuleDiff(writer, diffs[0]);
        }
        else if (diffs.Count > 1)
        {
            var sorted = new PatchModuleDiff[diffs.Count];

            for (var i = 0; i < sorted.Length; i++)
            {
                sorted[i] = diffs[i];
            }

            Array.Sort(sorted, PatchModuleDiffComparer.Instance);

            for (var i = 0; i < sorted.Length; i++)
            {
                WriteModuleDiff(writer, sorted[i]);
            }
        }

        writer.WriteEndArray();
    }

    private static void WriteModuleDiff(Utf8JsonWriter writer, PatchModuleDiff diff)
    {
        writer.WriteStartObject();
        writer.WriteString("kind", GetModuleDiffKindString(diff.Kind));
        writer.WriteString("flow_name", diff.FlowName);
        writer.WriteString("stage_name", diff.StageName);
        writer.WriteString("module_id", diff.ModuleId);
        writer.WriteString("path", diff.Path);
        writer.WriteString("experiment_layer", diff.ExperimentLayer);
        writer.WriteString("experiment_variant", diff.ExperimentVariant);
        writer.WriteEndObject();
    }

    private static void WriteParamDiffs(Utf8JsonWriter writer, IReadOnlyList<PatchParamDiff> diffs)
    {
        writer.WritePropertyName("param_diffs");
        writer.WriteStartArray();

        if (diffs.Count == 1)
        {
            WriteParamDiff(writer, diffs[0]);
        }
        else if (diffs.Count > 1)
        {
            var sorted = new PatchParamDiff[diffs.Count];

            for (var i = 0; i < sorted.Length; i++)
            {
                sorted[i] = diffs[i];
            }

            Array.Sort(sorted, PatchParamDiffComparer.Instance);

            for (var i = 0; i < sorted.Length; i++)
            {
                WriteParamDiff(writer, sorted[i]);
            }
        }

        writer.WriteEndArray();
    }

    private static void WriteParamDiff(Utf8JsonWriter writer, PatchParamDiff diff)
    {
        writer.WriteStartObject();
        writer.WriteString("kind", GetPatchDiffKindString(diff.Kind));
        writer.WriteString("flow_name", diff.FlowName);
        writer.WriteString("path", diff.Path);
        writer.WriteString("experiment_layer", diff.ExperimentLayer);
        writer.WriteString("experiment_variant", diff.ExperimentVariant);
        writer.WriteEndObject();
    }

    private static void WriteFanoutMaxDiffs(Utf8JsonWriter writer, IReadOnlyList<PatchFanoutMaxDiff> diffs)
    {
        writer.WritePropertyName("fanout_max_diffs");
        writer.WriteStartArray();

        if (diffs.Count == 1)
        {
            WriteFanoutMaxDiff(writer, diffs[0]);
        }
        else if (diffs.Count > 1)
        {
            var sorted = new PatchFanoutMaxDiff[diffs.Count];

            for (var i = 0; i < sorted.Length; i++)
            {
                sorted[i] = diffs[i];
            }

            Array.Sort(sorted, PatchFanoutMaxDiffComparer.Instance);

            for (var i = 0; i < sorted.Length; i++)
            {
                WriteFanoutMaxDiff(writer, sorted[i]);
            }
        }

        writer.WriteEndArray();
    }

    private static void WriteFanoutMaxDiff(Utf8JsonWriter writer, PatchFanoutMaxDiff diff)
    {
        writer.WriteStartObject();
        writer.WriteString("kind", GetPatchDiffKindString(diff.Kind));
        writer.WriteString("flow_name", diff.FlowName);
        writer.WriteString("stage_name", diff.StageName);
        writer.WriteString("path", diff.Path);
        writer.WriteString("experiment_layer", diff.ExperimentLayer);
        writer.WriteString("experiment_variant", diff.ExperimentVariant);
        writer.WriteEndObject();
    }

    private static void WriteEmergencyDiffs(Utf8JsonWriter writer, IReadOnlyList<PatchEmergencyDiff> diffs)
    {
        writer.WritePropertyName("emergency_diffs");
        writer.WriteStartArray();

        if (diffs.Count == 1)
        {
            WriteEmergencyDiff(writer, diffs[0]);
        }
        else if (diffs.Count > 1)
        {
            var sorted = new PatchEmergencyDiff[diffs.Count];

            for (var i = 0; i < sorted.Length; i++)
            {
                sorted[i] = diffs[i];
            }

            Array.Sort(sorted, PatchEmergencyDiffComparer.Instance);

            for (var i = 0; i < sorted.Length; i++)
            {
                WriteEmergencyDiff(writer, sorted[i]);
            }
        }

        writer.WriteEndArray();
    }

    private static void WriteEmergencyDiff(Utf8JsonWriter writer, PatchEmergencyDiff diff)
    {
        writer.WriteStartObject();
        writer.WriteString("kind", GetPatchDiffKindString(diff.Kind));
        writer.WriteString("flow_name", diff.FlowName);
        writer.WriteString("path", diff.Path);
        writer.WriteEndObject();
    }

    private static string GetSeverityString(ValidationSeverity severity)
    {
        return severity switch
        {
            ValidationSeverity.Error => "error",
            ValidationSeverity.Warn => "warn",
            ValidationSeverity.Info => "info",
            _ => "unknown",
        };
    }

    private static string GetModuleDiffKindString(PatchModuleDiffKind kind)
    {
        return kind switch
        {
            PatchModuleDiffKind.Added => "added",
            PatchModuleDiffKind.Removed => "removed",
            PatchModuleDiffKind.UseChanged => "use_changed",
            PatchModuleDiffKind.WithChanged => "with_changed",
            PatchModuleDiffKind.WithAdded => "with_added",
            PatchModuleDiffKind.WithRemoved => "with_removed",
            PatchModuleDiffKind.GateAdded => "gate_added",
            PatchModuleDiffKind.GateRemoved => "gate_removed",
            PatchModuleDiffKind.GateChanged => "gate_changed",
            PatchModuleDiffKind.EnabledChanged => "enabled_changed",
            PatchModuleDiffKind.PriorityChanged => "priority_changed",
            _ => "unknown",
        };
    }

    private static string GetPatchDiffKindString(PatchParamDiffKind kind)
    {
        return kind switch
        {
            PatchParamDiffKind.Added => "added",
            PatchParamDiffKind.Removed => "removed",
            PatchParamDiffKind.Changed => "changed",
            _ => "unknown",
        };
    }

    private static string GetPatchDiffKindString(PatchFanoutMaxDiffKind kind)
    {
        return kind switch
        {
            PatchFanoutMaxDiffKind.Added => "added",
            PatchFanoutMaxDiffKind.Removed => "removed",
            PatchFanoutMaxDiffKind.Changed => "changed",
            _ => "unknown",
        };
    }

    private static string GetPatchDiffKindString(PatchEmergencyDiffKind kind)
    {
        return kind switch
        {
            PatchEmergencyDiffKind.Added => "added",
            PatchEmergencyDiffKind.Removed => "removed",
            PatchEmergencyDiffKind.Changed => "changed",
            _ => "unknown",
        };
    }

    private static bool IsDiffInputException(Exception ex)
    {
        return ex is FormatException
            || ex is NotSupportedException
            || ex is InvalidOperationException;
    }

    private static bool IsExplainPatchInputException(Exception ex)
    {
        return ex is ArgumentException
            || ex is FormatException
            || ex is InvalidOperationException
            || ex is NotSupportedException;
    }

    private sealed class ValidationFindingComparer : IComparer<ValidationFinding>
    {
        public static ValidationFindingComparer Instance { get; } = new();

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

            c = x.Kind.CompareTo(y.Kind);
            if (c != 0)
            {
                return c;
            }

            c = string.CompareOrdinal(x.Path, y.Path);
            if (c != 0)
            {
                return c;
            }

            c = CompareNullableString(x.ExperimentLayer, y.ExperimentLayer);
            if (c != 0)
            {
                return c;
            }

            return CompareNullableString(x.ExperimentVariant, y.ExperimentVariant);
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

            c = string.CompareOrdinal(x.Path, y.Path);
            if (c != 0)
            {
                return c;
            }

            c = x.Kind.CompareTo(y.Kind);
            if (c != 0)
            {
                return c;
            }

            c = CompareNullableString(x.ExperimentLayer, y.ExperimentLayer);
            if (c != 0)
            {
                return c;
            }

            return CompareNullableString(x.ExperimentVariant, y.ExperimentVariant);
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

            c = string.CompareOrdinal(x.StageName, y.StageName);
            if (c != 0)
            {
                return c;
            }

            c = x.Kind.CompareTo(y.Kind);
            if (c != 0)
            {
                return c;
            }

            c = string.CompareOrdinal(x.Path, y.Path);
            if (c != 0)
            {
                return c;
            }

            c = CompareNullableString(x.ExperimentLayer, y.ExperimentLayer);
            if (c != 0)
            {
                return c;
            }

            return CompareNullableString(x.ExperimentVariant, y.ExperimentVariant);
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

            c = x.Kind.CompareTo(y.Kind);
            if (c != 0)
            {
                return c;
            }

            return string.CompareOrdinal(x.Path, y.Path);
        }
    }

    private static bool IsExplainInputException(Exception ex)
    {
        return ex is ArgumentException
            || ex is InvalidOperationException
            || ex is NotSupportedException;
    }

    private static string GetNodeKindString(ROrchestrator.Core.Blueprint.BlueprintNodeKind kind)
    {
        return kind switch
        {
            ROrchestrator.Core.Blueprint.BlueprintNodeKind.Step => "step",
            ROrchestrator.Core.Blueprint.BlueprintNodeKind.Join => "join",
            _ => throw new InvalidOperationException($"Unsupported node kind: '{kind}'."),
        };
    }

    private static string FormatTypeName(Type type)
    {
        if (type is null)
        {
            throw new ArgumentNullException(nameof(type));
        }

        if (type.IsArray)
        {
            var element = type.GetElementType()!;
            var rank = type.GetArrayRank();

            if (rank == 1)
            {
                return string.Concat(FormatTypeName(element), "[]");
            }

            var commas = new string(',', rank - 1);
            return string.Concat(FormatTypeName(element), "[", commas, "]");
        }

        if (type.IsGenericType)
        {
            var definition = type.GetGenericTypeDefinition();
            var name = definition.FullName ?? definition.Name;
            name = TrimGenericTypeArity(name);

            var args = type.GetGenericArguments();

            var builder = new StringBuilder(name.Length + 16);
            builder.Append(name);
            builder.Append('<');

            for (var i = 0; i < args.Length; i++)
            {
                if (i != 0)
                {
                    builder.Append(',');
                }

                builder.Append(FormatTypeName(args[i]));
            }

            builder.Append('>');
            return builder.ToString();
        }

        return type.FullName ?? type.Name;
    }

    private static string TrimGenericTypeArity(string typeName)
    {
        var tickIndex = typeName.IndexOf('`', StringComparison.Ordinal);
        if (tickIndex < 0)
        {
            return typeName;
        }

        var builder = new StringBuilder(typeName.Length);

        for (var i = 0; i < typeName.Length; i++)
        {
            var c = typeName[i];
            if (c == '`')
            {
                i++;

                while (i < typeName.Length && (uint)(typeName[i] - '0') <= 9)
                {
                    i++;
                }

                i--;
                continue;
            }

            builder.Append(c);
        }

        return builder.ToString();
    }

    private static int CompareNullableString(string? x, string? y)
    {
        if (x is null)
        {
            return y is null ? 0 : -1;
        }

        if (y is null)
        {
            return 1;
        }

        return string.CompareOrdinal(x, y);
    }
}
