using System.Buffers;
using System.Text;
using System.Text.Json;
using ROrchestrator.Core;
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
