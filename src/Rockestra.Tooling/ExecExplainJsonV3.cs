using System.Buffers;
using System.Text;
using System.Text.Json;
using Rockestra.Core;
using Rockestra.Core.Blueprint;

namespace Rockestra.Tooling;

public static class ExecExplainJsonV3
{
    private const string ToolingJsonVersion = "v3";

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

        if (explain.TryGetTrace(out var traceId, out var spanId))
        {
            writer.WriteString("trace_id", traceId.ToString());
            writer.WriteString("span_id", spanId.ToString());
        }
        else
        {
            writer.WriteNull("trace_id");
            writer.WriteNull("span_id");
        }

        writer.WriteString("deadline_utc", explain.DeadlineUtc);
        writer.WriteNumber("budget_remaining_ms_at_start", explain.BudgetRemainingMsAtStart);

        writer.WriteString("level", GetExplainLevelString(explain.Level));
        WriteExplainOptions(writer, explain);
        writer.WriteString("plan_hash", explain.PlanHash.ToString("X16"));

        if (explain.TryGetConfigVersion(out var configVersion))
        {
            writer.WriteNumber("config_version", configVersion);
        }
        else
        {
            writer.WriteNull("config_version");
        }

        WriteConfigSnapshotMeta(writer, explain);

        WriteTiming(writer, explain.DurationStopwatchTicks, explain.GetDuration().TotalMilliseconds);

        writer.WritePropertyName("qos");
        writer.WriteStartObject();
        writer.WriteString("selected_tier", GetQosTierString(explain.QosSelectedTier));

        if (string.IsNullOrEmpty(explain.QosReasonCode))
        {
            writer.WriteNull("reason_code");
        }
        else
        {
            writer.WriteString("reason_code", explain.QosReasonCode);
        }

        WriteSortedStringDictionaryOrNull(writer, "signals", explain.QosSignals);
        writer.WriteEndObject();

        WriteSortedStringDictionaryOrNull(writer, "variants", explain.Variants);

        if (string.IsNullOrEmpty(explain.EmergencyOverlayIgnoredReasonCode))
        {
            writer.WriteNull("emergency_ignored_reason_code");
        }
        else
        {
            writer.WriteString("emergency_ignored_reason_code", explain.EmergencyOverlayIgnoredReasonCode);
        }

        writer.WritePropertyName("overlays_applied");
        writer.WriteStartArray();

        var overlays = explain.OverlaysApplied;
        for (var i = 0; i < overlays.Count; i++)
        {
            WriteOverlay(writer, overlays[i]);
        }

        writer.WriteEndArray();

        WriteParams(writer, explain);

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

        WriteStageSnapshots(writer, stageModules);

        writer.WriteEndObject();
        writer.Flush();

        var redacted = ExplainRedactor.Redact(output.WrittenMemory, explain.Policy);
        return Encoding.UTF8.GetString(redacted);
    }

    private static void WriteExplainOptions(Utf8JsonWriter writer, ExecExplain explain)
    {
        writer.WritePropertyName("explain_options");
        writer.WriteStartObject();
        writer.WriteString("requested_level", GetExplainLevelString(explain.RequestedLevel));
        writer.WriteString("effective_level", GetExplainLevelString(explain.Level));

        if (string.IsNullOrEmpty(explain.ExplainReason))
        {
            writer.WriteNull("reason");
        }
        else
        {
            writer.WriteString("reason", explain.ExplainReason);
        }

        if (string.IsNullOrEmpty(explain.LevelDowngradeReasonCode))
        {
            writer.WriteNull("downgrade_reason");
        }
        else
        {
            writer.WriteString("downgrade_reason", explain.LevelDowngradeReasonCode);
        }

        writer.WriteString("policy", GetRedactionPolicyString(explain.Policy));
        writer.WriteEndObject();
    }

    private static void WriteConfigSnapshotMeta(Utf8JsonWriter writer, ExecExplain explain)
    {
        if (!explain.TryGetConfigSnapshotMeta(out var meta))
        {
            writer.WriteNull("config_snapshot_meta");
            return;
        }

        writer.WritePropertyName("config_snapshot_meta");
        writer.WriteStartObject();
        writer.WriteString("source", meta.Source);
        writer.WriteString("timestamp_utc", meta.TimestampUtc);

        writer.WritePropertyName("overlays");
        writer.WriteStartArray();

        var overlays = meta.Overlays;
        for (var i = 0; i < overlays.Length; i++)
        {
            writer.WriteStringValue(overlays[i]);
        }

        writer.WriteEndArray();

        writer.WritePropertyName("lkg");

        if (meta.TryGetLkgFallbackEvidence(out var evidence))
        {
            writer.WriteStartObject();
            writer.WriteBoolean("fallback", evidence.Fallback);
            writer.WriteNumber("last_good_config_version", evidence.LastGoodConfigVersion);

            if (evidence.HasCandidateConfigVersion)
            {
                writer.WriteNumber("candidate_config_version", evidence.CandidateConfigVersion);
            }
            else
            {
                writer.WriteNull("candidate_config_version");
            }

            writer.WriteEndObject();
        }
        else
        {
            writer.WriteNullValue();
        }

        writer.WriteEndObject();
    }

    private static void WriteTiming(Utf8JsonWriter writer, long durationStopwatchTicks, double durationMs)
    {
        writer.WritePropertyName("timing");
        writer.WriteStartObject();
        writer.WriteNumber("duration_ticks", durationStopwatchTicks);
        writer.WriteNumber("duration_ms", durationMs);
        writer.WriteEndObject();
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

    private static void WriteParams(Utf8JsonWriter writer, ExecExplain explain)
    {
        if (!explain.TryGetParamsExplain(out var paramsExplain))
        {
            writer.WriteNull("params");
            return;
        }

        writer.WritePropertyName("params");
        writer.WriteStartObject();
        writer.WriteString("hash", paramsExplain.ParamsHash);

        var effective = paramsExplain.EffectiveJsonUtf8;

        if (effective is null || effective.Length == 0)
        {
            writer.WriteNull("effective");
        }
        else
        {
            writer.WritePropertyName("effective");
            writer.WriteRawValue(effective);
        }

        var sources = paramsExplain.Sources;

        if (sources is null || sources.Length == 0)
        {
            writer.WriteNull("sources");
        }
        else
        {
            writer.WritePropertyName("sources");
            writer.WriteStartArray();

            for (var i = 0; i < sources.Length; i++)
            {
                var source = sources[i];
                writer.WriteStartObject();
                writer.WriteString("path", source.Path);
                writer.WriteString("layer", source.Layer);

                if (source.ExperimentLayer is null)
                {
                    writer.WriteNull("experiment_layer");
                }
                else
                {
                    writer.WriteString("experiment_layer", source.ExperimentLayer);
                }

                if (source.ExperimentVariant is null)
                {
                    writer.WriteNull("experiment_variant");
                }
                else
                {
                    writer.WriteString("experiment_variant", source.ExperimentVariant);
                }

                if (source.QosTier is null)
                {
                    writer.WriteNull("qos_tier");
                }
                else
                {
                    writer.WriteString("qos_tier", source.QosTier);
                }

                writer.WriteEndObject();
            }

            writer.WriteEndArray();
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

        WriteTiming(writer, node.DurationStopwatchTicks, node.GetDuration().TotalMilliseconds);

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

        if (string.IsNullOrEmpty(stageModule.LimitKey))
        {
            writer.WriteNull("limit_key");
        }
        else
        {
            writer.WriteString("limit_key", stageModule.LimitKey);
        }

        writer.WriteNumber("priority", stageModule.Priority);
        WriteTiming(writer, stageModule.DurationStopwatchTicks, stageModule.GetDuration().TotalMilliseconds);

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

        if (string.IsNullOrEmpty(stageModule.GateReasonCode))
        {
            writer.WriteNull("gate_reason_code");
        }
        else
        {
            writer.WriteString("gate_reason_code", stageModule.GateReasonCode);
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
        writer.WriteBoolean("memo_hit", stageModule.MemoHit);
        writer.WriteEndObject();
    }

    private static void WriteStageSnapshots(Utf8JsonWriter writer, IReadOnlyList<ExecExplainStageModule> stageModules)
    {
        writer.WritePropertyName("stage_snapshots");

        if (stageModules.Count == 0)
        {
            writer.WriteStartArray();
            writer.WriteEndArray();
            return;
        }

        var stageNameToIndices = new Dictionary<string, List<int>>(capacity: 4);
        var stageNames = new List<string>(capacity: 4);

        for (var i = 0; i < stageModules.Count; i++)
        {
            var stageName = stageModules[i].StageName;

            if (!stageNameToIndices.TryGetValue(stageName, out var indices))
            {
                indices = new List<int>(capacity: 4);
                stageNameToIndices.Add(stageName, indices);
                stageNames.Add(stageName);
            }

            indices.Add(i);
        }

        var stageNameArray = stageNames.ToArray();
        if (stageNameArray.Length > 1)
        {
            Array.Sort(stageNameArray, StringComparer.Ordinal);
        }

        writer.WriteStartArray();

        for (var stageIndex = 0; stageIndex < stageNameArray.Length; stageIndex++)
        {
            var stageName = stageNameArray[stageIndex];
            var indices = stageNameToIndices[stageName];

            writer.WriteStartObject();
            writer.WriteString("stage_name", stageName);

            writer.WritePropertyName("selected_modules");
            writer.WriteStartArray();
            WriteStageSnapshotModules(writer, stageModules, indices, includeShadow: false, includeSkipped: false);
            writer.WriteEndArray();

            writer.WritePropertyName("skipped_modules");
            writer.WriteStartArray();
            WriteStageSnapshotModules(writer, stageModules, indices, includeShadow: false, includeSkipped: true);
            writer.WriteEndArray();

            writer.WritePropertyName("selected_shadow_modules");
            writer.WriteStartArray();
            WriteStageSnapshotModules(writer, stageModules, indices, includeShadow: true, includeSkipped: false);
            writer.WriteEndArray();

            writer.WritePropertyName("skipped_shadow_modules");
            writer.WriteStartArray();
            WriteStageSnapshotModules(writer, stageModules, indices, includeShadow: true, includeSkipped: true);
            writer.WriteEndArray();

            writer.WriteEndObject();
        }

        writer.WriteEndArray();
    }

    private static void WriteStageSnapshotModules(
        Utf8JsonWriter writer,
        IReadOnlyList<ExecExplainStageModule> stageModules,
        List<int> indices,
        bool includeShadow,
        bool includeSkipped)
    {
        for (var i = 0; i < indices.Count; i++)
        {
            var module = stageModules[indices[i]];

            if (module.IsShadow != includeShadow)
            {
                continue;
            }

            var isSkipped = module.OutcomeKind == OutcomeKind.Skipped;

            if (!includeSkipped && isSkipped)
            {
                continue;
            }

            if (includeSkipped && !isSkipped)
            {
                continue;
            }

            writer.WriteStartObject();
            writer.WriteString("module_id", module.ModuleId);
            writer.WriteString("module_type", module.ModuleType);

            if (string.IsNullOrEmpty(module.LimitKey))
            {
                writer.WriteNull("limit_key");
            }
            else
            {
                writer.WriteString("limit_key", module.LimitKey);
            }

            writer.WriteString("outcome_kind", GetOutcomeKindString(module.OutcomeKind));

            if (string.IsNullOrEmpty(module.OutcomeCode))
            {
                writer.WriteNull("outcome_code");
            }
            else
            {
                writer.WriteString("outcome_code", module.OutcomeCode);
            }

            if (includeShadow)
            {
                writer.WriteNumber("shadow_sample_bps", module.ShadowSampleBps);
            }

            writer.WriteEndObject();
        }
    }

    private static void WriteSortedStringDictionaryOrNull(
        Utf8JsonWriter writer,
        string propertyName,
        IReadOnlyDictionary<string, string>? dictionary)
    {
        if (dictionary is null || dictionary.Count == 0)
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

    private static string GetRedactionPolicyString(ExplainRedactionPolicy policy)
    {
        return policy switch
        {
            ExplainRedactionPolicy.Default => "default",
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

    private static string GetQosTierString(QosTier tier)
    {
        return tier switch
        {
            QosTier.Full => "full",
            QosTier.Conserve => "conserve",
            QosTier.Emergency => "emergency",
            QosTier.Fallback => "fallback",
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
