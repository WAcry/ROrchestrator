using System.Buffers;
using System.Text;
using System.Text.Json;
using Rockestra.Core;
using Rockestra.Core.Gates;
using Rockestra.Core.Selectors;

namespace Rockestra.Tooling;

public static class ToolingJsonV1
{
    private const string ToolingJsonVersion = "v1";

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
        bool includeMermaid = false,
        SelectorRegistry? selectorRegistry = null,
        QosTier qosTier = QosTier.Full)
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
            using var evaluation = PatchEvaluatorV1.Evaluate(flowName, patchJson, requestOptions, qosTier: qosTier);
            var maxInFlight = ParseMaxInFlightMap(patchJson);
            var json = BuildExplainPatchJson(flowName, evaluation, requestOptions, includeMermaid, selectorRegistry, qosTier, maxInFlight);
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

    public static ToolingCommandResult PreviewMatrixJson(
        string flowName,
        string patchJson,
        IReadOnlyList<Dictionary<string, string>> variantsMatrix,
        SelectorRegistry? selectorRegistry = null,
        QosTier qosTier = QosTier.Full,
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

        if (variantsMatrix is null)
        {
            throw new ArgumentNullException(nameof(variantsMatrix));
        }

        try
        {
            var json = BuildPreviewMatrixJson(flowName, patchJson, variantsMatrix, requestOptions, includeMermaid, selectorRegistry, qosTier);
            return new ToolingCommandResult(exitCode: 0, json);
        }
        catch (Exception ex) when (IsExplainPatchInputException(ex))
        {
            var json = BuildPreviewMatrixErrorJson(
                code: "PREVIEW_MATRIX_INPUT_INVALID",
                message: ex.Message);
            return new ToolingCommandResult(exitCode: 2, json);
        }
        catch (Exception ex) when (ToolingExceptionGuard.ShouldHandle(ex))
        {
            var json = BuildPreviewMatrixErrorJson(
                code: "PREVIEW_MATRIX_INTERNAL_ERROR",
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
            var qosReport = PatchDiffV1.DiffQosTierPatches(oldPatchJson, newPatchJson);
            var limitReport = PatchDiffV1.DiffLimitsMaxInFlight(oldPatchJson, newPatchJson);
            var paramReport = PatchDiffV1.DiffParams(oldPatchJson, newPatchJson);
            var fanoutReport = PatchDiffV1.DiffFanoutMax(oldPatchJson, newPatchJson);
            var emergencyReport = PatchDiffV1.DiffEmergency(oldPatchJson, newPatchJson);

            var riskReport = AnalyzeRiskReport(oldPatchJson, newPatchJson, moduleReport, qosReport, limitReport, paramReport, fanoutReport, emergencyReport);
            var json = BuildDiffJson(moduleReport, qosReport, limitReport, paramReport, fanoutReport, emergencyReport, riskReport);
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
        bool includeMermaid,
        SelectorRegistry? selectorRegistry,
        QosTier qosTier,
        Dictionary<string, int>? maxInFlight)
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
        writer.WriteString("tooling_json_version", ToolingJsonVersion);
        writer.WriteString("flow_name", flowName);

        writer.WritePropertyName("qos");
        writer.WriteStartObject();
        writer.WriteString("selected_tier", GetQosTierString(qosTier));
        writer.WriteNull("reason_code");
        writer.WriteNull("signals");
        writer.WriteEndObject();

        WriteSortedVariants(writer, requestOptions.Variants);

        writer.WritePropertyName("overlays_applied");
        writer.WriteStartArray();

        var overlays = evaluation.OverlaysApplied;
        for (var i = 0; i < overlays.Count; i++)
        {
            WriteExplainPatchOverlay(writer, overlays[i]);
        }

        writer.WriteEndArray();

        WriteExplainPatchParams(writer, evaluation, requestOptions, qosTier);

        writer.WritePropertyName("stages");
        writer.WriteStartArray();

        var stages = evaluation.Stages;

        FlowContext? selectorFlowContext = null;

        for (var i = 0; i < stages.Count; i++)
        {
            WriteExplainPatchStage(writer, stages[i], requestOptions, selectorRegistry, maxInFlight, ref selectorFlowContext);
        }

        writer.WriteEndArray();

        if (includeMermaid)
        {
            writer.WriteString("mermaid", BuildExplainPatchMermaid(evaluation, requestOptions, selectorRegistry));
        }

        writer.WriteEndObject();
        writer.Flush();

        return Encoding.UTF8.GetString(output.WrittenSpan);
    }

    private static string BuildPreviewMatrixJson(
        string flowName,
        string patchJson,
        IReadOnlyList<Dictionary<string, string>> variantsMatrix,
        FlowRequestOptions requestOptions,
        bool includeMermaid,
        SelectorRegistry? selectorRegistry,
        QosTier qosTier)
    {
        var maxInFlight = ParseMaxInFlightMap(patchJson);

        var output = new ArrayBufferWriter<byte>(512);
        using var writer = new Utf8JsonWriter(
            output,
            new JsonWriterOptions
            {
                Indented = false,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            });

        writer.WriteStartObject();
        writer.WriteString("kind", "preview_matrix");
        writer.WriteString("tooling_json_version", ToolingJsonVersion);
        writer.WriteString("flow_name", flowName);

        writer.WritePropertyName("qos");
        writer.WriteStartObject();
        writer.WriteString("selected_tier", GetQosTierString(qosTier));
        writer.WriteNull("reason_code");
        writer.WriteNull("signals");
        writer.WriteEndObject();

        writer.WritePropertyName("previews");
        writer.WriteStartArray();

        var count = variantsMatrix.Count;
        if (count != 0)
        {
            var entries = new PreviewMatrixEntry[count];

            for (var i = 0; i < count; i++)
            {
                var variants = variantsMatrix[i];
                entries[i] = new PreviewMatrixEntry(i, BuildVariantsSortKey(variants));
            }

            if (entries.Length > 1)
            {
                Array.Sort(entries, PreviewMatrixEntryComparer.Instance);
            }

            FlowContext? selectorFlowContext = null;

            for (var i = 0; i < entries.Length; i++)
            {
                var variants = variantsMatrix[entries[i].Index];
                var mergedRequestOptions = new FlowRequestOptions(
                    variants: variants,
                    userId: requestOptions.UserId,
                    requestAttributes: requestOptions.RequestAttributes);

                using var evaluation = PatchEvaluatorV1.Evaluate(flowName, patchJson, mergedRequestOptions, qosTier: qosTier, configVersion: 1);

                writer.WriteStartObject();

                WriteSortedVariants(writer, mergedRequestOptions.Variants);

                writer.WritePropertyName("overlays_applied");
                writer.WriteStartArray();

                var overlays = evaluation.OverlaysApplied;
                for (var overlayIndex = 0; overlayIndex < overlays.Count; overlayIndex++)
                {
                    WriteExplainPatchOverlay(writer, overlays[overlayIndex]);
                }

                writer.WriteEndArray();

                WriteExplainPatchParams(writer, evaluation, mergedRequestOptions, qosTier);

                writer.WritePropertyName("stages");
                writer.WriteStartArray();

                WritePreviewMatrixStages(writer, evaluation.Stages, mergedRequestOptions, selectorRegistry, maxInFlight, ref selectorFlowContext);

                writer.WriteEndArray();

                if (includeMermaid)
                {
                    writer.WriteString("mermaid", BuildExplainPatchMermaid(evaluation, mergedRequestOptions, selectorRegistry));
                }

                writer.WriteEndObject();
            }
        }

        writer.WriteEndArray();
        writer.WriteEndObject();
        writer.Flush();

        return Encoding.UTF8.GetString(output.WrittenSpan);
    }

    private static string BuildPreviewMatrixErrorJson(string code, string message)
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
        writer.WriteString("kind", "preview_matrix");
        writer.WriteString("tooling_json_version", ToolingJsonVersion);
        writer.WritePropertyName("error");
        writer.WriteStartObject();
        writer.WriteString("code", code);
        writer.WriteString("message", message);
        writer.WriteEndObject();
        writer.WriteEndObject();
        writer.Flush();

        return Encoding.UTF8.GetString(output.WrittenSpan);
    }

    private static Dictionary<string, int>? ParseMaxInFlightMap(string patchJson)
    {
        if (patchJson.Length == 0)
        {
            return null;
        }

        using var document = JsonDocument.Parse(patchJson);

        var root = document.RootElement;

        if (root.ValueKind != JsonValueKind.Object)
        {
            throw new FormatException("patchJson must be a JSON object.");
        }

        if (!root.TryGetProperty("schemaVersion", out var schemaVersion)
            || schemaVersion.ValueKind != JsonValueKind.String
            || !schemaVersion.ValueEquals("v1"))
        {
            throw new FormatException("patchJson schemaVersion is missing or unsupported.");
        }

        if (!root.TryGetProperty("limits", out var limitsElement) || limitsElement.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        if (!limitsElement.TryGetProperty("moduleConcurrency", out var moduleConcurrencyElement)
            || moduleConcurrencyElement.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        if (!moduleConcurrencyElement.TryGetProperty("maxInFlight", out var maxInFlightElement)
            || maxInFlightElement.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        var count = 0;

        foreach (var _ in maxInFlightElement.EnumerateObject())
        {
            count++;
        }

        if (count == 0)
        {
            return null;
        }

        var map = new Dictionary<string, int>(capacity: count);

        foreach (var entry in maxInFlightElement.EnumerateObject())
        {
            var key = entry.Name;

            if (string.IsNullOrEmpty(key))
            {
                continue;
            }

            if (entry.Value.ValueKind != JsonValueKind.Number || !entry.Value.TryGetInt32(out var max) || max <= 0)
            {
                continue;
            }

            map[key] = max;
        }

        return map.Count == 0 ? null : map;
    }

    private static void WritePreviewMatrixStages(
        Utf8JsonWriter writer,
        IReadOnlyList<PatchEvaluatorV1.StagePatchV1> stages,
        FlowRequestOptions requestOptions,
        SelectorRegistry? selectorRegistry,
        Dictionary<string, int>? maxInFlight,
        ref FlowContext? selectorFlowContext)
    {
        if (stages.Count == 0)
        {
            return;
        }

        var sorted = new PatchEvaluatorV1.StagePatchV1[stages.Count];

        for (var i = 0; i < sorted.Length; i++)
        {
            sorted[i] = stages[i];
        }

        if (sorted.Length > 1)
        {
            Array.Sort(sorted, StagePatchByNameComparer.Instance);
        }

        for (var i = 0; i < sorted.Length; i++)
        {
            WritePreviewMatrixStage(writer, sorted[i], requestOptions, selectorRegistry, maxInFlight, ref selectorFlowContext);
        }
    }

    private static void WritePreviewMatrixStage(
        Utf8JsonWriter writer,
        PatchEvaluatorV1.StagePatchV1 stage,
        FlowRequestOptions requestOptions,
        SelectorRegistry? selectorRegistry,
        Dictionary<string, int>? maxInFlight,
        ref FlowContext? selectorFlowContext)
    {
        var modules = stage.Modules;
        var moduleCount = modules.Count;

        var candidates = moduleCount == 0 ? Array.Empty<StageModuleCandidate>() : new StageModuleCandidate[moduleCount];
        var candidateCount = 0;

        for (var i = 0; i < moduleCount; i++)
        {
            var module = modules[i];

            if (!module.Enabled)
            {
                continue;
            }

            if (module.HasGate)
            {
                var gateDecision = EvaluateGateDecision(module.Gate, requestOptions, selectorRegistry, ref selectorFlowContext, out _);
                if (!gateDecision.Allowed)
                {
                    continue;
                }
            }

            candidates[candidateCount] = new StageModuleCandidate(i, module.Priority);
            candidateCount++;
        }

        if (candidateCount > 1)
        {
            SortCandidates(candidates, candidateCount);
        }

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

        var shadowModules = stage.ShadowModules;
        var shadowCount = shadowModules.Count;

        var userId = requestOptions.UserId;

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

        writer.WritePropertyName("selected_modules");
        writer.WriteStartArray();

        for (var rank = 0; rank < executeCount; rank++)
        {
            var moduleIndex = candidates[rank].ModuleIndex;
            var module = modules[moduleIndex];

            writer.WriteStartObject();
            writer.WriteString("module_id", module.ModuleId);

            var limitKey = module.LimitKey ?? module.ModuleType;
            writer.WriteString("limit_key", limitKey);

            if (maxInFlight is not null && maxInFlight.TryGetValue(limitKey, out var max))
            {
                writer.WriteNumber("max_in_flight", max);
            }
            else
            {
                writer.WriteNull("max_in_flight");
            }

            writer.WriteEndObject();
        }

        writer.WriteEndArray();

        writer.WritePropertyName("selected_shadow_modules");
        writer.WriteStartArray();

        for (var i = 0; i < shadowCount; i++)
        {
            var module = shadowModules[i];

            if (!module.Enabled)
            {
                continue;
            }

            if (module.HasGate)
            {
                var gateDecision = EvaluateGateDecision(module.Gate, requestOptions, selectorRegistry, ref selectorFlowContext, out _);
                if (!gateDecision.Allowed)
                {
                    continue;
                }
            }

            if (!ShouldExecuteShadow(module.ShadowSampleBps, userId, module.ModuleId))
            {
                continue;
            }

            writer.WriteStartObject();
            writer.WriteString("module_id", module.ModuleId);

            var limitKey = module.LimitKey ?? module.ModuleType;
            writer.WriteString("limit_key", limitKey);

            if (maxInFlight is not null && maxInFlight.TryGetValue(limitKey, out var max))
            {
                writer.WriteNumber("max_in_flight", max);
            }
            else
            {
                writer.WriteNull("max_in_flight");
            }

            writer.WriteEndObject();
        }

        writer.WriteEndArray();

        writer.WriteEndObject();
    }

    private static string BuildVariantsSortKey(Dictionary<string, string>? variants)
    {
        if (variants is null || variants.Count == 0)
        {
            return string.Empty;
        }

        if (variants.Count == 1)
        {
            foreach (var pair in variants)
            {
                return string.Concat(pair.Key, "=", pair.Value);
            }
        }

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

            var builder = new StringBuilder(capacity: filledCount * 16);

            for (var i = 0; i < filledCount; i++)
            {
                if (i != 0)
                {
                    builder.Append('|');
                }

                builder.Append(rented[i].Key);
                builder.Append('=');
                builder.Append(rented[i].Value);
            }

            return builder.ToString();
        }
        finally
        {
            Array.Clear(rented, 0, filledCount);
            ArrayPool<KeyValuePair<string, string>>.Shared.Return(rented);
        }
    }

    private readonly struct PreviewMatrixEntry
    {
        public int Index { get; }

        public string SortKey { get; }

        public PreviewMatrixEntry(int index, string sortKey)
        {
            Index = index;
            SortKey = sortKey;
        }
    }

    private sealed class PreviewMatrixEntryComparer : IComparer<PreviewMatrixEntry>
    {
        public static readonly PreviewMatrixEntryComparer Instance = new();

        public int Compare(PreviewMatrixEntry x, PreviewMatrixEntry y)
        {
            var c = string.CompareOrdinal(x.SortKey, y.SortKey);
            if (c != 0)
            {
                return c;
            }

            return x.Index.CompareTo(y.Index);
        }
    }

    private sealed class StagePatchByNameComparer : IComparer<PatchEvaluatorV1.StagePatchV1>
    {
        public static readonly StagePatchByNameComparer Instance = new();

        public int Compare(PatchEvaluatorV1.StagePatchV1 x, PatchEvaluatorV1.StagePatchV1 y)
        {
            return string.CompareOrdinal(x.StageName, y.StageName);
        }
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

    private static void WriteExplainPatchParams(
        Utf8JsonWriter writer,
        PatchEvaluatorV1.FlowPatchEvaluationV1 evaluation,
        FlowRequestOptions requestOptions,
        QosTier qosTier)
    {
        writer.WritePropertyName("params");
        writer.WriteStartObject();

        if (!evaluation.TryGetFlowPatch(out var flowPatch) || flowPatch.ValueKind != JsonValueKind.Object)
        {
            writer.WriteNull("effective");
            writer.WriteNull("sources");
            writer.WriteEndObject();
            return;
        }

        if (!TryCollectParamsMergeInputs(flowPatch, requestOptions, qosTier, out var hasBase, out var baseParams, out var overlays, out var overlayCount))
        {
            writer.WriteNull("effective");
            writer.WriteNull("sources");
            writer.WriteEndObject();
            return;
        }

        var sources = new List<ParamsSourceEntry>(capacity: 8);

        writer.WritePropertyName("effective");
        WriteMergedParamsObject(writer, hasBase, baseParams, overlays, overlayCount, pathPrefix: null, sources);

        if (sources.Count > 1)
        {
            sources.Sort(ParamsSourceEntryByPathComparer.Instance);
        }

        writer.WritePropertyName("sources");
        writer.WriteStartArray();

        for (var i = 0; i < sources.Count; i++)
        {
            WriteParamsSourceEntry(writer, sources[i]);
        }

        writer.WriteEndArray();
        writer.WriteEndObject();
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

    private static void WriteMergedParamsObject(
        Utf8JsonWriter writer,
        bool hasBase,
        JsonElement baseObject,
        ParamsOverlay[] overlays,
        int overlayCount,
        string? pathPrefix,
        List<ParamsSourceEntry> sources)
    {
        writer.WriteStartObject();

        var names = new PropertyNameBuffer(initialCapacity: 16);

        if (hasBase)
        {
            foreach (var property in baseObject.EnumerateObject())
            {
                names.Add(property.Name);
            }
        }

        for (var i = 0; i < overlayCount; i++)
        {
            foreach (var property in overlays[i].Object.EnumerateObject())
            {
                names.Add(property.Name);
            }
        }

        var nameArray = names.Items;

        if (names.Count > 1)
        {
            Array.Sort(nameArray, 0, names.Count, StringComparer.Ordinal);
        }

        for (var nameIndex = 0; nameIndex < names.Count; nameIndex++)
        {
            var name = nameArray[nameIndex];

            var baseValue = default(JsonElement);
            var hasBaseValue = hasBase && baseObject.TryGetProperty(name, out baseValue);

            var lastOverlayIndex = -1;
            JsonElement lastOverlayValue = default;
            ParamsSourceDescriptor lastOverlaySource = default;

            for (var i = overlayCount - 1; i >= 0; i--)
            {
                if (!overlays[i].Object.TryGetProperty(name, out var overlayValue))
                {
                    continue;
                }

                lastOverlayIndex = i;
                lastOverlayValue = overlayValue;
                lastOverlaySource = overlays[i].Source;
                break;
            }

            if (lastOverlayIndex < 0)
            {
                if (!hasBaseValue)
                {
                    continue;
                }

                writer.WritePropertyName(name);

                if (baseValue.ValueKind == JsonValueKind.Object)
                {
                    WriteMergedParamsObject(writer, true, baseValue, Array.Empty<ParamsOverlay>(), 0, CombinePath(pathPrefix, name), sources);
                }
                else
                {
                    baseValue.WriteTo(writer);
                    sources.Add(new ParamsSourceEntry(CombinePath(pathPrefix, name), ParamsSourceDescriptor.Base));
                }

                continue;
            }

            if (lastOverlayValue.ValueKind != JsonValueKind.Object)
            {
                writer.WritePropertyName(name);
                lastOverlayValue.WriteTo(writer);
                sources.Add(new ParamsSourceEntry(CombinePath(pathPrefix, name), lastOverlaySource));
                continue;
            }

            var resetIndex = -1;

            for (var i = 0; i <= lastOverlayIndex; i++)
            {
                if (!overlays[i].Object.TryGetProperty(name, out var overlayValue))
                {
                    continue;
                }

                if (overlayValue.ValueKind != JsonValueKind.Object)
                {
                    resetIndex = i;
                }
            }

            var nestedHasBase = hasBaseValue && baseValue.ValueKind == JsonValueKind.Object && resetIndex < 0;
            var nestedBase = nestedHasBase ? baseValue : default;

            var maxNestedCount = lastOverlayIndex - resetIndex;
            var rented = ArrayPool<ParamsOverlay>.Shared.Rent(maxNestedCount);
            var nestedCount = 0;

            try
            {
                for (var i = resetIndex + 1; i <= lastOverlayIndex; i++)
                {
                    if (!overlays[i].Object.TryGetProperty(name, out var overlayValue))
                    {
                        continue;
                    }

                    if (overlayValue.ValueKind != JsonValueKind.Object)
                    {
                        continue;
                    }

                    rented[nestedCount] = new ParamsOverlay(overlayValue, overlays[i].Source);
                    nestedCount++;
                }

                writer.WritePropertyName(name);
                WriteMergedParamsObject(writer, nestedHasBase, nestedBase, rented, nestedCount, CombinePath(pathPrefix, name), sources);
            }
            finally
            {
                Array.Clear(rented, 0, nestedCount);
                ArrayPool<ParamsOverlay>.Shared.Return(rented);
            }
        }

        writer.WriteEndObject();
    }

    private static string CombinePath(string? prefix, string name)
    {
        if (string.IsNullOrEmpty(prefix))
        {
            return name;
        }

        return string.Concat(prefix, ".", name);
    }

    private static bool TryCollectParamsMergeInputs(
        JsonElement flowPatch,
        FlowRequestOptions requestOptions,
        QosTier qosTier,
        out bool hasBase,
        out JsonElement baseParams,
        out ParamsOverlay[] overlays,
        out int overlayCount)
    {
        hasBase = false;
        baseParams = default;

        var buffer = new ParamsOverlayBuffer(initialCapacity: 4);

        if (flowPatch.TryGetProperty("params", out var baseParamsPatch))
        {
            if (baseParamsPatch.ValueKind == JsonValueKind.Object)
            {
                hasBase = true;
                baseParams = baseParamsPatch;
            }
            else if (baseParamsPatch.ValueKind != JsonValueKind.Undefined)
            {
                throw new FormatException("params must be a JSON object.");
            }
        }

        var variants = requestOptions.Variants;

        if (variants is not null
            && variants.Count != 0
            && flowPatch.TryGetProperty("experiments", out var experimentsPatch)
            && experimentsPatch.ValueKind == JsonValueKind.Array)
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
                    buffer.Add(
                        new ParamsOverlay(
                            experimentParamsPatch,
                            new ParamsSourceDescriptor(
                                layer: "experiment",
                                experimentLayer: layer,
                                experimentVariant: variant,
                                qosTier: null)));
                }
                else if (experimentParamsPatch.ValueKind != JsonValueKind.Undefined)
                {
                    throw new FormatException("experiments[].patch.params must be a JSON object.");
                }
            }
        }

        var qosTierName = GetQosTierString(qosTier);

        if (flowPatch.TryGetProperty("qos", out var qosElement)
            && qosElement.ValueKind == JsonValueKind.Object
            && qosElement.TryGetProperty("tiers", out var tiersElement)
            && tiersElement.ValueKind == JsonValueKind.Object
            && tiersElement.TryGetProperty(qosTierName, out var tierElement)
            && tierElement.ValueKind == JsonValueKind.Object
            && tierElement.TryGetProperty("patch", out var tierPatch)
            && tierPatch.ValueKind == JsonValueKind.Object
            && tierPatch.TryGetProperty("params", out var qosParamsPatch))
        {
            if (qosParamsPatch.ValueKind == JsonValueKind.Object)
            {
                buffer.Add(
                    new ParamsOverlay(
                        qosParamsPatch,
                        new ParamsSourceDescriptor(
                            layer: "qos",
                            experimentLayer: null,
                            experimentVariant: null,
                            qosTier: qosTierName)));
            }
            else if (qosParamsPatch.ValueKind != JsonValueKind.Undefined)
            {
                throw new FormatException("qos.tiers[].patch.params must be a JSON object.");
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
                buffer.Add(
                    new ParamsOverlay(
                        emergencyParamsPatch,
                        new ParamsSourceDescriptor(
                            layer: "emergency",
                            experimentLayer: null,
                            experimentVariant: null,
                            qosTier: null)));
            }
            else if (emergencyParamsPatch.ValueKind != JsonValueKind.Undefined)
            {
                throw new FormatException("emergency.patch.params must be a JSON object.");
            }
        }

        overlays = buffer.Items;
        overlayCount = buffer.Count;
        return hasBase || overlayCount != 0;
    }

    private readonly struct ParamsOverlay
    {
        public JsonElement Object { get; }

        public ParamsSourceDescriptor Source { get; }

        public ParamsOverlay(JsonElement @object, ParamsSourceDescriptor source)
        {
            Object = @object;
            Source = source;
        }
    }

    private readonly struct ParamsSourceDescriptor
    {
        public static readonly ParamsSourceDescriptor Base = new(layer: "base", experimentLayer: null, experimentVariant: null, qosTier: null);

        public string Layer { get; }

        public string? ExperimentLayer { get; }

        public string? ExperimentVariant { get; }

        public string? QosTier { get; }

        public ParamsSourceDescriptor(string layer, string? experimentLayer, string? experimentVariant, string? qosTier)
        {
            Layer = layer;
            ExperimentLayer = experimentLayer;
            ExperimentVariant = experimentVariant;
            QosTier = qosTier;
        }
    }

    private readonly struct ParamsSourceEntry
    {
        public string Path { get; }

        public string Layer { get; }

        public string? ExperimentLayer { get; }

        public string? ExperimentVariant { get; }

        public string? QosTier { get; }

        public ParamsSourceEntry(string path, ParamsSourceDescriptor source)
        {
            Path = path;
            Layer = source.Layer;
            ExperimentLayer = source.ExperimentLayer;
            ExperimentVariant = source.ExperimentVariant;
            QosTier = source.QosTier;
        }
    }

    private sealed class ParamsSourceEntryByPathComparer : IComparer<ParamsSourceEntry>
    {
        public static readonly ParamsSourceEntryByPathComparer Instance = new();

        public int Compare(ParamsSourceEntry x, ParamsSourceEntry y)
        {
            return string.CompareOrdinal(x.Path, y.Path);
        }
    }

    private struct ParamsOverlayBuffer
    {
        private ParamsOverlay[]? _items;
        private int _count;

        public int Count => _count;

        public ParamsOverlay[] Items => _items ?? Array.Empty<ParamsOverlay>();

        public ParamsOverlayBuffer(int initialCapacity)
        {
            _items = initialCapacity <= 0 ? null : new ParamsOverlay[initialCapacity];
            _count = 0;
        }

        public void Add(ParamsOverlay item)
        {
            if (_items is null)
            {
                _items = new ParamsOverlay[4];
            }
            else if ((uint)_count >= (uint)_items.Length)
            {
                var newItems = new ParamsOverlay[_items.Length * 2];
                Array.Copy(_items, 0, newItems, 0, _items.Length);
                _items = newItems;
            }

            _items[_count] = item;
            _count++;
        }
    }

    private struct PropertyNameBuffer
    {
        private string[]? _items;
        private int _count;

        public int Count => _count;

        public string[] Items => _items ?? Array.Empty<string>();

        public PropertyNameBuffer(int initialCapacity)
        {
            _items = initialCapacity <= 0 ? null : new string[initialCapacity];
            _count = 0;
        }

        public void Add(string name)
        {
            if (_count != 0)
            {
                var items = _items;
                if (items is not null)
                {
                    for (var i = 0; i < _count; i++)
                    {
                        if (string.Equals(items[i], name, StringComparison.Ordinal))
                        {
                            return;
                        }
                    }
                }
            }

            if (_items is null)
            {
                _items = new string[4];
            }
            else if ((uint)_count >= (uint)_items.Length)
            {
                var newItems = new string[_items.Length * 2];
                Array.Copy(_items, 0, newItems, 0, _items.Length);
                _items = newItems;
            }

            _items[_count] = name;
            _count++;
        }
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

        private KeyValuePairByKeyComparer()
        {
        }

        public int Compare(KeyValuePair<string, string> x, KeyValuePair<string, string> y)
        {
            return string.Compare(x.Key, y.Key, StringComparison.Ordinal);
        }
    }

    private static void WriteExplainPatchStage(
        Utf8JsonWriter writer,
        PatchEvaluatorV1.StagePatchV1 stage,
        FlowRequestOptions requestOptions,
        SelectorRegistry? selectorRegistry,
        Dictionary<string, int>? maxInFlight,
        ref FlowContext? selectorFlowContext)
    {
        var modules = stage.Modules;
        var moduleCount = modules.Count;

        var decisionKinds = moduleCount == 0 ? Array.Empty<byte>() : new byte[moduleCount];
        var decisionCodes = moduleCount == 0 ? Array.Empty<string>() : new string[moduleCount];
        var gateDecisionCodes = moduleCount == 0 ? Array.Empty<string?>() : new string?[moduleCount];
        var gateReasonCodes = moduleCount == 0 ? Array.Empty<string?>() : new string?[moduleCount];
        var gateSelectorNames = moduleCount == 0 ? Array.Empty<string?>() : new string?[moduleCount];

        var candidates = moduleCount == 0 ? Array.Empty<StageModuleCandidate>() : new StageModuleCandidate[moduleCount];
        var candidateCount = 0;

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

            if (module.HasGate)
            {
                var gateDecision = EvaluateGateDecision(module.Gate, requestOptions, selectorRegistry, ref selectorFlowContext, out var gateSelectorName);
                gateDecisionCodes[i] = gateDecision.Code;
                gateReasonCodes[i] = gateDecision.ReasonCode;
                gateSelectorNames[i] = gateSelectorName;

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

            var limitKey = module.LimitKey ?? module.ModuleType;
            writer.WriteString("limit_key", limitKey);

            if (maxInFlight is not null && maxInFlight.TryGetValue(limitKey, out var max))
            {
                writer.WriteNumber("max_in_flight", max);
            }
            else
            {
                writer.WriteNull("max_in_flight");
            }

            writer.WriteBoolean("enabled", module.Enabled);
            writer.WriteBoolean("disabled_by_emergency", module.DisabledByEmergency);
            writer.WriteNumber("priority", module.Priority);

            if (gateDecisionCodes[i] is null)
            {
                writer.WriteNull("gate_decision_code");
            }
            else
            {
                writer.WriteString("gate_decision_code", gateDecisionCodes[i]);
            }

            if (string.IsNullOrEmpty(gateReasonCodes[i]))
            {
                writer.WriteNull("gate_reason_code");
            }
            else
            {
                writer.WriteString("gate_reason_code", gateReasonCodes[i]);
            }

            if (gateSelectorNames[i] is null)
            {
                writer.WriteNull("gate_selector_name");
            }
            else
            {
                writer.WriteString("gate_selector_name", gateSelectorNames[i]);
            }

            writer.WriteString("decision_kind", kind);
            writer.WriteString("decision_code", code);
            writer.WriteEndObject();
        }

        writer.WriteEndArray();

        writer.WritePropertyName("shadow_modules");
        writer.WriteStartArray();

        var shadowModules = stage.ShadowModules;
        var shadowCount = shadowModules.Count;

        for (var i = 0; i < shadowCount; i++)
        {
            var module = shadowModules[i];

            string? shadowGateDecisionCode = null;
            string? shadowGateReasonCode = null;
            string? shadowGateSelectorName = null;

            string kind;
            string code;

            if (!module.Enabled)
            {
                kind = "skip";
                code = ExecutionEngine.DisabledCode;
            }
            else
            {
                if (module.HasGate)
                {
                    var gateDecision = EvaluateGateDecision(module.Gate, requestOptions, selectorRegistry, ref selectorFlowContext, out var gateSelectorName);
                    shadowGateDecisionCode = gateDecision.Code;
                    shadowGateReasonCode = gateDecision.ReasonCode;
                    shadowGateSelectorName = gateSelectorName;

                    if (!gateDecision.Allowed)
                    {
                        kind = "skip";
                        code = ExecutionEngine.GateFalseCode;
                        goto WriteShadowModule;
                    }
                }

                if (!ShouldExecuteShadow(module.ShadowSampleBps, userId, module.ModuleId))
                {
                    kind = "skip";
                    code = ExecutionEngine.ShadowNotSampledCode;
                }
                else
                {
                    kind = "execute";
                    code = SelectedDecisionCode;
                }
            }

        WriteShadowModule:
            writer.WriteStartObject();
            writer.WriteString("module_id", module.ModuleId);
            writer.WriteString("module_type", module.ModuleType);

            var limitKey = module.LimitKey ?? module.ModuleType;
            writer.WriteString("limit_key", limitKey);

            if (maxInFlight is not null && maxInFlight.TryGetValue(limitKey, out var max))
            {
                writer.WriteNumber("max_in_flight", max);
            }
            else
            {
                writer.WriteNull("max_in_flight");
            }

            writer.WriteBoolean("enabled", module.Enabled);
            writer.WriteBoolean("disabled_by_emergency", module.DisabledByEmergency);
            writer.WriteNumber("priority", module.Priority);
            writer.WriteNumber("shadow_sample_rate_bps", module.ShadowSampleBps);

            if (shadowGateDecisionCode is null)
            {
                writer.WriteNull("gate_decision_code");
            }
            else
            {
                writer.WriteString("gate_decision_code", shadowGateDecisionCode);
            }

            if (string.IsNullOrEmpty(shadowGateReasonCode))
            {
                writer.WriteNull("gate_reason_code");
            }
            else
            {
                writer.WriteString("gate_reason_code", shadowGateReasonCode);
            }

            if (shadowGateSelectorName is null)
            {
                writer.WriteNull("gate_selector_name");
            }
            else
            {
                writer.WriteString("gate_selector_name", shadowGateSelectorName);
            }

            writer.WriteString("decision_kind", kind);
            writer.WriteString("decision_code", code);
            writer.WriteEndObject();
        }

        writer.WriteEndArray();
        writer.WriteEndObject();
    }

    private static GateDecision EvaluateGateDecision(
        JsonElement gateElement,
        FlowRequestOptions requestOptions,
        SelectorRegistry? selectorRegistry,
        ref FlowContext? selectorFlowContext,
        out string? gateSelectorName)
    {
        Gate? gate;

        if (selectorRegistry is not null)
        {
            if (!GateJsonV1.TryParseOptional(gateElement, "$.gate", selectorRegistry, out gate, out var finding))
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

            return EvaluateNonSelectorGateDecision(gate, requestOptions);
        }

        if (!GateJsonV1.TryParseOptional(gateElement, "$.gate", out gate, out var findingWithoutRegistry))
        {
            throw new FormatException(findingWithoutRegistry.Message);
        }

        if (gate is null)
        {
            gateSelectorName = null;
            return GateDecision.AllowedDecision;
        }

        gateSelectorName = gate is SelectorGate selectorGateWithoutRegistry ? selectorGateWithoutRegistry.SelectorName : null;

        if (GateContainsSelectorGate(gate))
        {
            throw new InvalidOperationException("SelectorRegistry is required to evaluate SelectorGate.");
        }

        return EvaluateNonSelectorGateDecision(gate, requestOptions);
    }

    private static GateDecision EvaluateNonSelectorGateDecision(Gate gate, FlowRequestOptions requestOptions)
    {
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

    private static readonly DateTimeOffset SelectorFlowContextDeadline =
        new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

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

    private static string BuildExplainPatchMermaid(
        PatchEvaluatorV1.FlowPatchEvaluationV1 evaluation,
        FlowRequestOptions requestOptions,
        SelectorRegistry? selectorRegistry)
    {
        var stages = evaluation.Stages;
        var builder = new StringBuilder(256);
        builder.Append("flowchart TD\n");

        var moduleIndex = 0;
        FlowContext? selectorFlowContext = null;

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

            if (moduleCount != 0)
            {
                var decisionKinds = new byte[moduleCount];
                var decisionCodes = new string[moduleCount];
                var candidates = new StageModuleCandidate[moduleCount];
                var candidateCount = 0;

                for (var i = 0; i < moduleCount; i++)
                {
                    var module = modules[i];

                    if (!module.Enabled)
                    {
                        decisionKinds[i] = 0;
                        decisionCodes[i] = ExecutionEngine.DisabledCode;
                        continue;
                    }

                    if (module.HasGate)
                    {
                        var gateDecision = EvaluateGateDecision(module.Gate, requestOptions, selectorRegistry, ref selectorFlowContext, out _);

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

            var shadowModules = stage.ShadowModules;
            var shadowCount = shadowModules.Count;
            var shadowUserId = requestOptions.UserId;

            for (var i = 0; i < shadowCount; i++)
            {
                var module = shadowModules[i];

                string kind;
                string code;

                if (!module.Enabled)
                {
                    kind = "skip";
                    code = ExecutionEngine.DisabledCode;
                }
                else
                {
                    if (module.HasGate)
                    {
                        var gateDecision = EvaluateGateDecision(module.Gate, requestOptions, selectorRegistry, ref selectorFlowContext, out _);

                        if (!gateDecision.Allowed)
                        {
                            kind = "skip";
                            code = ExecutionEngine.GateFalseCode;
                            goto AppendShadowModule;
                        }
                    }

                    if (!ShouldExecuteShadow(module.ShadowSampleBps, shadowUserId, module.ModuleId))
                    {
                        kind = "skip";
                        code = ExecutionEngine.ShadowNotSampledCode;
                    }
                    else
                    {
                        kind = "execute";
                        code = SelectedDecisionCode;
                    }
                }

            AppendShadowModule:
                builder.Append("  s");
                builder.Append(stageIndex);
                builder.Append(" -.-> m");
                builder.Append(moduleIndex);
                builder.Append("[\"");
                builder.Append(module.ModuleId);
                builder.Append("\\nshadow\\nsample_bps=");
                builder.Append(module.ShadowSampleBps.ToString(System.Globalization.CultureInfo.InvariantCulture));
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
        writer.WriteString("tooling_json_version", ToolingJsonVersion);
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
        writer.WriteString("tooling_json_version", ToolingJsonVersion);
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

        if (node.Kind == Rockestra.Core.Blueprint.BlueprintNodeKind.Step)
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
        writer.WriteString("tooling_json_version", ToolingJsonVersion);

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
        writer.WriteString("tooling_json_version", ToolingJsonVersion);
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
        PatchQosTierDiffReport qosReport,
        PatchLimitDiffReport limitReport,
        PatchParamDiffReport paramReport,
        PatchFanoutMaxDiffReport fanoutReport,
        PatchEmergencyDiffReport emergencyReport,
        RiskReport riskReport)
    {
        var output = new ArrayBufferWriter<byte>(512);
        using var writer = new Utf8JsonWriter(output, new JsonWriterOptions { Indented = false });

        writer.WriteStartObject();
        writer.WriteString("kind", "diff");
        writer.WriteString("tooling_json_version", ToolingJsonVersion);

        WriteModuleDiffs(writer, moduleReport.Diffs);
        WriteQosTierDiffs(writer, qosReport.Diffs);
        WriteLimitDiffs(writer, limitReport.Diffs);
        WriteParamDiffs(writer, paramReport.Diffs);
        WriteFanoutMaxDiffs(writer, fanoutReport.Diffs);
        WriteEmergencyDiffs(writer, emergencyReport.Diffs);
        WriteRiskReport(writer, riskReport);

        writer.WriteEndObject();
        writer.Flush();

        return Encoding.UTF8.GetString(output.WrittenSpan);
    }

    private enum RiskLevel
    {
        Low = 1,
        Medium = 2,
        High = 3,
    }

    private readonly struct RiskReport
    {
        public RiskLevel Level { get; }

        public int FanoutIncreaseCount { get; }

        public int ModuleAddedCount { get; }

        public int ModuleRemovedCount { get; }

        public int ShadowChangeCount { get; }

        public int LimitKeyChangeCount { get; }

        public int LimitChangeCount { get; }

        public int QosChangeCount { get; }

        public int ParamChangeCount { get; }

        public int EmergencyChangeCount { get; }

        public RiskReport(
            RiskLevel level,
            int fanoutIncreaseCount,
            int moduleAddedCount,
            int moduleRemovedCount,
            int shadowChangeCount,
            int limitKeyChangeCount,
            int limitChangeCount,
            int qosChangeCount,
            int paramChangeCount,
            int emergencyChangeCount)
        {
            Level = level;
            FanoutIncreaseCount = fanoutIncreaseCount;
            ModuleAddedCount = moduleAddedCount;
            ModuleRemovedCount = moduleRemovedCount;
            ShadowChangeCount = shadowChangeCount;
            LimitKeyChangeCount = limitKeyChangeCount;
            LimitChangeCount = limitChangeCount;
            QosChangeCount = qosChangeCount;
            ParamChangeCount = paramChangeCount;
            EmergencyChangeCount = emergencyChangeCount;
        }
    }

    private static RiskReport AnalyzeRiskReport(
        string oldPatchJson,
        string newPatchJson,
        PatchModuleDiffReport moduleReport,
        PatchQosTierDiffReport qosReport,
        PatchLimitDiffReport limitReport,
        PatchParamDiffReport paramReport,
        PatchFanoutMaxDiffReport fanoutReport,
        PatchEmergencyDiffReport emergencyReport)
    {
        var moduleAddedCount = 0;
        var moduleRemovedCount = 0;
        var shadowChangeCount = 0;
        var limitKeyChangeCount = 0;

        var moduleDiffs = moduleReport.Diffs;
        for (var i = 0; i < moduleDiffs.Count; i++)
        {
            var diff = moduleDiffs[i];

            if (diff.Kind == PatchModuleDiffKind.Added)
            {
                moduleAddedCount++;
            }
            else if (diff.Kind == PatchModuleDiffKind.Removed)
            {
                moduleRemovedCount++;
            }
            else if (diff.Kind == PatchModuleDiffKind.ShadowAdded
                || diff.Kind == PatchModuleDiffKind.ShadowRemoved
                || diff.Kind == PatchModuleDiffKind.ShadowSampleChanged)
            {
                shadowChangeCount++;
            }
            else if (diff.Kind == PatchModuleDiffKind.LimitKeyAdded
                || diff.Kind == PatchModuleDiffKind.LimitKeyRemoved
                || diff.Kind == PatchModuleDiffKind.LimitKeyChanged)
            {
                limitKeyChangeCount++;
            }
        }

        var limitChangeCount = limitReport.Diffs.Count;
        var qosChangeCount = qosReport.Diffs.Count;
        var paramChangeCount = paramReport.Diffs.Count;
        var emergencyChangeCount = emergencyReport.Diffs.Count;
        var fanoutIncreaseCount = ComputeFanoutIncreaseCount(oldPatchJson, newPatchJson, fanoutReport.Diffs);

        var level = RiskLevel.Low;

        if (fanoutIncreaseCount != 0 || moduleRemovedCount != 0 || emergencyChangeCount != 0 || limitChangeCount != 0)
        {
            level = RiskLevel.High;
        }
        else if (moduleAddedCount != 0 || shadowChangeCount != 0 || limitKeyChangeCount != 0 || qosChangeCount != 0 || paramChangeCount != 0)
        {
            level = RiskLevel.Medium;
        }

        return new RiskReport(
            level,
            fanoutIncreaseCount,
            moduleAddedCount,
            moduleRemovedCount,
            shadowChangeCount,
            limitKeyChangeCount,
            limitChangeCount,
            qosChangeCount,
            paramChangeCount,
            emergencyChangeCount);
    }

    private static void WriteRiskReport(Utf8JsonWriter writer, RiskReport report)
    {
        writer.WritePropertyName("risk_report");
        writer.WriteStartObject();
        writer.WriteString("level", GetRiskLevelString(report.Level));
        writer.WriteNumber("fanout_increase_count", report.FanoutIncreaseCount);
        writer.WriteNumber("module_added_count", report.ModuleAddedCount);
        writer.WriteNumber("module_removed_count", report.ModuleRemovedCount);
        writer.WriteNumber("shadow_change_count", report.ShadowChangeCount);
        writer.WriteNumber("limit_key_change_count", report.LimitKeyChangeCount);
        writer.WriteNumber("limit_change_count", report.LimitChangeCount);
        writer.WriteNumber("qos_change_count", report.QosChangeCount);
        writer.WriteNumber("param_change_count", report.ParamChangeCount);
        writer.WriteNumber("emergency_change_count", report.EmergencyChangeCount);
        writer.WriteEndObject();
    }

    private static string GetRiskLevelString(RiskLevel level)
    {
        return level switch
        {
            RiskLevel.Low => "low",
            RiskLevel.Medium => "medium",
            RiskLevel.High => "high",
            _ => "unknown",
        };
    }

    private static int ComputeFanoutIncreaseCount(
        string oldPatchJson,
        string newPatchJson,
        IReadOnlyList<PatchFanoutMaxDiff> diffs)
    {
        if (diffs.Count == 0)
        {
            return 0;
        }

        var oldMap = CollectFanoutMaxMap(oldPatchJson);
        var newMap = CollectFanoutMaxMap(newPatchJson);

        if (oldMap is null)
        {
            oldMap = EmptyFanoutMaxMap;
        }

        if (newMap is null)
        {
            newMap = EmptyFanoutMaxMap;
        }

        var count = 0;

        for (var i = 0; i < diffs.Count; i++)
        {
            var diff = diffs[i];
            var key = new FanoutMaxKey(diff.FlowName, diff.StageName, diff.ExperimentLayer, diff.ExperimentVariant);

            var oldExists = oldMap.TryGetValue(key, out var oldValue);
            var newExists = newMap.TryGetValue(key, out var newValue);

            if (oldExists)
            {
                if (!newExists || newValue > oldValue)
                {
                    count++;
                }
            }
        }

        return count;
    }

    private static readonly Dictionary<FanoutMaxKey, int> EmptyFanoutMaxMap = new(capacity: 0);

    private static Dictionary<FanoutMaxKey, int>? CollectFanoutMaxMap(string patchJson)
    {
        if (patchJson.Length == 0)
        {
            return null;
        }

        using var document = JsonDocument.Parse(patchJson);

        var root = document.RootElement;

        if (root.ValueKind != JsonValueKind.Object)
        {
            throw new FormatException("patchJson must be a JSON object.");
        }

        if (!root.TryGetProperty("schemaVersion", out var schemaVersion)
            || schemaVersion.ValueKind != JsonValueKind.String
            || !schemaVersion.ValueEquals("v1"))
        {
            throw new FormatException("patchJson schemaVersion is missing or unsupported.");
        }

        if (!root.TryGetProperty("flows", out var flowsElement) || flowsElement.ValueKind != JsonValueKind.Object)
        {
            throw new FormatException("flows must be an object.");
        }

        Dictionary<FanoutMaxKey, int>? map = null;

        foreach (var flowProperty in flowsElement.EnumerateObject())
        {
            var flowName = flowProperty.Name;
            var flowPatch = flowProperty.Value;

            if (flowPatch.ValueKind != JsonValueKind.Object)
            {
                throw new FormatException(string.Concat("Flow patch must be an object. Flow: ", flowName));
            }

            CollectFlowFanoutMax(flowName, flowPatch, experimentLayer: null, experimentVariant: null, ref map);

            if (!flowPatch.TryGetProperty("experiments", out var experimentsElement) || experimentsElement.ValueKind == JsonValueKind.Null)
            {
                continue;
            }

            if (experimentsElement.ValueKind != JsonValueKind.Array)
            {
                throw new FormatException(string.Concat("experiments must be an array. Flow: ", flowName));
            }

            foreach (var experimentMapping in experimentsElement.EnumerateArray())
            {
                if (experimentMapping.ValueKind != JsonValueKind.Object)
                {
                    throw new FormatException(string.Concat("experiments must be an array of objects. Flow: ", flowName));
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

                CollectFlowFanoutMax(flowName, patch, layer, variant, ref map);
            }
        }

        return map;
    }

    private static void CollectFlowFanoutMax(
        string flowName,
        JsonElement flowPatch,
        string? experimentLayer,
        string? experimentVariant,
        ref Dictionary<FanoutMaxKey, int>? map)
    {
        if (!flowPatch.TryGetProperty("stages", out var stagesElement) || stagesElement.ValueKind != JsonValueKind.Object)
        {
            return;
        }

        foreach (var stageProperty in stagesElement.EnumerateObject())
        {
            var stageName = stageProperty.Name;
            var stagePatch = stageProperty.Value;

            if (stagePatch.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            if (!stagePatch.TryGetProperty("fanoutMax", out var fanoutElement))
            {
                continue;
            }

            if (!TryGetFanoutMaxValue(fanoutElement, out var fanoutMax))
            {
                continue;
            }

            map ??= new Dictionary<FanoutMaxKey, int>(capacity: 4);
            map[new FanoutMaxKey(flowName, stageName, experimentLayer, experimentVariant)] = fanoutMax;
        }
    }

    private static bool TryGetFanoutMaxValue(JsonElement fanoutElement, out int value)
    {
        value = 0;

        if (fanoutElement.ValueKind != JsonValueKind.Number)
        {
            return false;
        }

        if (fanoutElement.TryGetInt32(out value))
        {
            return true;
        }

        if (fanoutElement.TryGetInt64(out var value64))
        {
            if (value64 < int.MinValue || value64 > int.MaxValue)
            {
                value = 0;
                return false;
            }

            value = (int)value64;
            return true;
        }

        return false;
    }

    private readonly struct FanoutMaxKey : IEquatable<FanoutMaxKey>
    {
        private readonly string _flowName;
        private readonly string _stageName;
        private readonly string? _experimentLayer;
        private readonly string? _experimentVariant;

        public FanoutMaxKey(string flowName, string stageName, string? experimentLayer, string? experimentVariant)
        {
            _flowName = flowName;
            _stageName = stageName;
            _experimentLayer = experimentLayer;
            _experimentVariant = experimentVariant;
        }

        public bool Equals(FanoutMaxKey other)
        {
            return string.Equals(_flowName, other._flowName, StringComparison.Ordinal)
                && string.Equals(_stageName, other._stageName, StringComparison.Ordinal)
                && string.Equals(_experimentLayer, other._experimentLayer, StringComparison.Ordinal)
                && string.Equals(_experimentVariant, other._experimentVariant, StringComparison.Ordinal);
        }

        public override bool Equals(object? obj)
        {
            return obj is FanoutMaxKey other && Equals(other);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = StringComparer.Ordinal.GetHashCode(_flowName);
                hashCode = (hashCode * 397) ^ StringComparer.Ordinal.GetHashCode(_stageName);
                hashCode = (hashCode * 397) ^ (_experimentLayer is null ? 0 : StringComparer.Ordinal.GetHashCode(_experimentLayer));
                hashCode = (hashCode * 397) ^ (_experimentVariant is null ? 0 : StringComparer.Ordinal.GetHashCode(_experimentVariant));
                return hashCode;
            }
        }
    }

    private static string BuildDiffErrorJson(string code, string message)
    {
        var output = new ArrayBufferWriter<byte>(256);
        using var writer = new Utf8JsonWriter(output, new JsonWriterOptions { Indented = false });

        writer.WriteStartObject();
        writer.WriteString("kind", "diff");
        writer.WriteString("tooling_json_version", ToolingJsonVersion);
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

    private static void WriteQosTierDiffs(Utf8JsonWriter writer, IReadOnlyList<PatchQosTierDiff> diffs)
    {
        writer.WritePropertyName("qos_tier_diffs");
        writer.WriteStartArray();

        if (diffs.Count == 1)
        {
            WriteQosTierDiff(writer, diffs[0]);
        }
        else if (diffs.Count > 1)
        {
            var sorted = new PatchQosTierDiff[diffs.Count];

            for (var i = 0; i < sorted.Length; i++)
            {
                sorted[i] = diffs[i];
            }

            Array.Sort(sorted, PatchQosTierDiffComparer.Instance);

            for (var i = 0; i < sorted.Length; i++)
            {
                WriteQosTierDiff(writer, sorted[i]);
            }
        }

        writer.WriteEndArray();
    }

    private static void WriteQosTierDiff(Utf8JsonWriter writer, PatchQosTierDiff diff)
    {
        writer.WriteStartObject();
        writer.WriteString("kind", GetPatchDiffKindString(diff.Kind));
        writer.WriteString("flow_name", diff.FlowName);
        writer.WriteString("qos_tier", diff.TierName);
        writer.WriteString("path", diff.Path);
        writer.WriteEndObject();
    }

    private static void WriteLimitDiffs(Utf8JsonWriter writer, IReadOnlyList<PatchLimitDiff> diffs)
    {
        writer.WritePropertyName("limit_diffs");
        writer.WriteStartArray();

        if (diffs.Count == 1)
        {
            WriteLimitDiff(writer, diffs[0]);
        }
        else if (diffs.Count > 1)
        {
            var sorted = new PatchLimitDiff[diffs.Count];

            for (var i = 0; i < sorted.Length; i++)
            {
                sorted[i] = diffs[i];
            }

            Array.Sort(sorted, PatchLimitDiffComparer.Instance);

            for (var i = 0; i < sorted.Length; i++)
            {
                WriteLimitDiff(writer, sorted[i]);
            }
        }

        writer.WriteEndArray();
    }

    private static void WriteLimitDiff(Utf8JsonWriter writer, PatchLimitDiff diff)
    {
        writer.WriteStartObject();
        writer.WriteString("kind", GetPatchDiffKindString(diff.Kind));
        writer.WriteString("limit_key", diff.LimitKey);
        writer.WriteString("path", diff.Path);

        if (diff.OldMaxInFlight.HasValue)
        {
            writer.WriteNumber("old_max_in_flight", diff.OldMaxInFlight.Value);
        }
        else
        {
            writer.WriteNull("old_max_in_flight");
        }

        if (diff.NewMaxInFlight.HasValue)
        {
            writer.WriteNumber("new_max_in_flight", diff.NewMaxInFlight.Value);
        }
        else
        {
            writer.WriteNull("new_max_in_flight");
        }

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
            PatchModuleDiffKind.ShadowAdded => "shadow_added",
            PatchModuleDiffKind.ShadowRemoved => "shadow_removed",
            PatchModuleDiffKind.ShadowSampleChanged => "shadow_sample_changed",
            PatchModuleDiffKind.LimitKeyAdded => "limit_key_added",
            PatchModuleDiffKind.LimitKeyRemoved => "limit_key_removed",
            PatchModuleDiffKind.LimitKeyChanged => "limit_key_changed",
            _ => "unknown",
        };
    }

    private static string GetPatchDiffKindString(PatchQosTierDiffKind kind)
    {
        return kind switch
        {
            PatchQosTierDiffKind.Added => "added",
            PatchQosTierDiffKind.Removed => "removed",
            PatchQosTierDiffKind.Changed => "changed",
            _ => "unknown",
        };
    }

    private static string GetPatchDiffKindString(PatchLimitDiffKind kind)
    {
        return kind switch
        {
            PatchLimitDiffKind.Added => "added",
            PatchLimitDiffKind.Removed => "removed",
            PatchLimitDiffKind.Changed => "changed",
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

    private sealed class PatchQosTierDiffComparer : IComparer<PatchQosTierDiff>
    {
        public static PatchQosTierDiffComparer Instance { get; } = new();

        public int Compare(PatchQosTierDiff x, PatchQosTierDiff y)
        {
            var c = string.CompareOrdinal(x.FlowName, y.FlowName);
            if (c != 0)
            {
                return c;
            }

            c = string.CompareOrdinal(x.TierName, y.TierName);
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

    private sealed class PatchLimitDiffComparer : IComparer<PatchLimitDiff>
    {
        public static PatchLimitDiffComparer Instance { get; } = new();

        public int Compare(PatchLimitDiff x, PatchLimitDiff y)
        {
            var c = string.CompareOrdinal(x.LimitKey, y.LimitKey);
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

    private static string GetNodeKindString(Rockestra.Core.Blueprint.BlueprintNodeKind kind)
    {
        return kind switch
        {
            Rockestra.Core.Blueprint.BlueprintNodeKind.Step => "step",
            Rockestra.Core.Blueprint.BlueprintNodeKind.Join => "join",
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

