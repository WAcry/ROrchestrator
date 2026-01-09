using System.Buffers;
using System.Text;
using System.Text.Json;
using ROrchestrator.Core;
using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Tooling;

public static class ExecExplainJsonV1
{
    private const string ToolingJsonVersion = "v1";

    public static string ExportJson(ExecExplain explain)
    {
        if (explain is null)
        {
            throw new ArgumentNullException(nameof(explain));
        }

        var output = new ArrayBufferWriter<byte>(512);
        using var writer = new Utf8JsonWriter(
            output,
            new JsonWriterOptions
            {
                Indented = false,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            });

        writer.WriteStartObject();
        writer.WriteString("kind", "exec_explain");
        writer.WriteString("tooling_json_version", ToolingJsonVersion);
        writer.WriteString("flow_name", explain.FlowName);
        writer.WriteString("level", GetExplainLevelString(explain.Level));
        writer.WriteString("plan_hash", explain.PlanHash.ToString("X16"));

        if (explain.TryGetConfigVersion(out var configVersion))
        {
            writer.WriteNumber("config_version", configVersion);
        }
        else
        {
            writer.WriteNull("config_version");
        }

        WriteSortedStringDictionaryOrNull(writer, "variants", explain.Variants);

        writer.WritePropertyName("overlays_applied");
        writer.WriteStartArray();

        var overlays = explain.OverlaysApplied;
        for (var i = 0; i < overlays.Count; i++)
        {
            WriteOverlay(writer, overlays[i]);
        }

        writer.WriteEndArray();

        writer.WritePropertyName("nodes");
        writer.WriteStartArray();

        var nodes = explain.Nodes;
        for (var i = 0; i < nodes.Count; i++)
        {
            WriteNode(writer, nodes[i]);
        }

        writer.WriteEndArray();

        writer.WritePropertyName("stage_modules");
        writer.WriteStartArray();

        var stageModules = explain.StageModules;
        for (var i = 0; i < stageModules.Count; i++)
        {
            WriteStageModule(writer, stageModules[i]);
        }

        writer.WriteEndArray();

        writer.WriteEndObject();
        writer.Flush();

        return Encoding.UTF8.GetString(output.WrittenSpan);
    }

    private static void WriteOverlay(Utf8JsonWriter writer, PatchEvaluatorV1.PatchOverlayAppliedV1 overlay)
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

    private static void WriteNode(Utf8JsonWriter writer, ExecExplainNode node)
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

        writer.WriteString("outcome_kind", GetOutcomeKindString(node.OutcomeKind));

        if (string.IsNullOrEmpty(node.OutcomeCode))
        {
            writer.WriteNull("outcome_code");
        }
        else
        {
            writer.WriteString("outcome_code", node.OutcomeCode);
        }

        writer.WriteEndObject();
    }

    private static void WriteStageModule(Utf8JsonWriter writer, ExecExplainStageModule stageModule)
    {
        writer.WriteStartObject();
        writer.WriteString("stage_name", stageModule.StageName);
        writer.WriteString("module_id", stageModule.ModuleId);
        writer.WriteString("module_type", stageModule.ModuleType);
        writer.WriteNumber("priority", stageModule.Priority);

        writer.WriteString("outcome_kind", GetOutcomeKindString(stageModule.OutcomeKind));

        if (string.IsNullOrEmpty(stageModule.OutcomeCode))
        {
            writer.WriteNull("outcome_code");
        }
        else
        {
            writer.WriteString("outcome_code", stageModule.OutcomeCode);
        }

        if (string.IsNullOrEmpty(stageModule.GateDecisionCode))
        {
            writer.WriteNull("gate_decision_code");
        }
        else
        {
            writer.WriteString("gate_decision_code", stageModule.GateDecisionCode);
        }

        if (string.IsNullOrEmpty(stageModule.GateSelectorName))
        {
            writer.WriteNull("gate_selector_name");
        }
        else
        {
            writer.WriteString("gate_selector_name", stageModule.GateSelectorName);
        }

        writer.WriteBoolean("is_shadow", stageModule.IsShadow);

        if (stageModule.IsShadow)
        {
            writer.WriteNumber("shadow_sample_bps", stageModule.ShadowSampleBps);
        }
        else
        {
            writer.WriteNull("shadow_sample_bps");
        }

        writer.WriteBoolean("is_override", stageModule.IsOverride);
        writer.WriteEndObject();
    }

    private static void WriteSortedStringDictionaryOrNull(
        Utf8JsonWriter writer,
        string propertyName,
        IReadOnlyDictionary<string, string> dictionary)
    {
        if (dictionary is null)
        {
            throw new ArgumentNullException(nameof(dictionary));
        }

        if (dictionary.Count == 0)
        {
            writer.WriteNull(propertyName);
            return;
        }

        writer.WritePropertyName(propertyName);
        writer.WriteStartObject();

        var rented = ArrayPool<KeyValuePair<string, string>>.Shared.Rent(dictionary.Count);
        var filledCount = 0;

        try
        {
            foreach (var pair in dictionary)
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

        writer.WriteEndObject();
    }

    private static string GetExplainLevelString(ExplainLevel level)
    {
        return level switch
        {
            ExplainLevel.Minimal => "minimal",
            ExplainLevel.Standard => "standard",
            ExplainLevel.Full => "full",
            _ => "unknown",
        };
    }

    private static string GetNodeKindString(BlueprintNodeKind kind)
    {
        return kind switch
        {
            BlueprintNodeKind.Step => "step",
            BlueprintNodeKind.Join => "join",
            _ => "unknown",
        };
    }

    private static string GetOutcomeKindString(OutcomeKind kind)
    {
        return kind switch
        {
            OutcomeKind.Unspecified => "unspecified",
            OutcomeKind.Ok => "ok",
            OutcomeKind.Error => "error",
            OutcomeKind.Timeout => "timeout",
            OutcomeKind.Skipped => "skipped",
            OutcomeKind.Fallback => "fallback",
            OutcomeKind.Canceled => "canceled",
            _ => "unknown",
        };
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
}

