using System.Buffers;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.Loader;
using System.Text;
using System.Text.Json;
using ROrchestrator.Core;
using ROrchestrator.Core.Selectors;
using ROrchestrator.Tooling;

namespace ROrchestrator.Cli;

public static class ROrchestratorCliApp
{
    public static int Run(string[] args, TextWriter stdout, TextWriter stderr)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(stdout);
        ArgumentNullException.ThrowIfNull(stderr);

        if (args.Length == 0 || IsHelpToken(args[0]))
        {
            WriteJson(stdout, BuildHelpJson());
            return 0;
        }

        var command = args[0];

        if (args.Length >= 2 && IsHelpToken(args[1]))
        {
            WriteJson(stdout, BuildCommandHelpJson(command));
            return 0;
        }

        ToolingCommandResult result;
        string? errorSummary;

        try
        {
            result = command switch
            {
                "validate" => RunValidate(args),
                "explain-flow" => RunExplainFlow(args),
                "explain-patch" => RunExplainPatch(args),
                "diff-patch" => RunDiffPatch(args),
                "preview-matrix" => RunPreviewMatrix(args),
                _ => CreateCliErrorResult("CLI_UNKNOWN_COMMAND", $"Unknown command: '{command}'."),
            };

            errorSummary = result.ExitCode == 0 ? null : $"error: {command} failed (exit_code={result.ExitCode}).";
        }
        catch (Exception ex) when (ShouldHandle(ex))
        {
            result = CreateCliErrorResult(
                "CLI_INTERNAL_ERROR",
                string.IsNullOrEmpty(ex.Message) ? "Internal error." : ex.Message,
                exitCode: 1);
            errorSummary = $"error: {command} failed (exit_code=1).";
        }

        WriteJson(stdout, result.Json);

        if (!string.IsNullOrEmpty(errorSummary))
        {
            stderr.WriteLine(errorSummary);
        }

        return result.ExitCode;
    }

    private static ToolingCommandResult RunValidate(string[] args)
    {
        string? patchPath = null;
        string? patchJson = null;
        string? bootstrapperAssemblyPath = null;
        string? bootstrapperTypeName = null;

        for (var i = 1; i < args.Length; i++)
        {
            var a = args[i];

            if (IsHelpToken(a))
            {
                return new ToolingCommandResult(0, BuildCommandHelpJson("validate"));
            }

            if (TryReadOption(args, ref i, "--patch", out var v))
            {
                patchPath = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--patch-json", out v))
            {
                patchJson = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--bootstrapper", out v))
            {
                bootstrapperAssemblyPath = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--bootstrapper-type", out v))
            {
                bootstrapperTypeName = v;
                continue;
            }

            return CreateCliErrorResult("CLI_USAGE_INVALID", $"Unknown argument: '{a}'.");
        }

        if (!TryResolveSingleJsonInput(patchPath, patchJson, "--patch", "--patch-json", out var resolvedPatchJson, out var inputError))
        {
            return CreateCliErrorResult("CLI_USAGE_INVALID", inputError);
        }

        if (string.IsNullOrEmpty(bootstrapperTypeName))
        {
            return CreateCliErrorResult(
                "CLI_USAGE_INVALID",
                "Missing required option: --bootstrapper-type <type>.");
        }

        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();
        var selectors = new SelectorRegistry();

        if (!TryInvokeBootstrapper(bootstrapperAssemblyPath, bootstrapperTypeName, registry, catalog, selectors, out var bootstrapError))
        {
            return CreateCliErrorResult("CLI_BOOTSTRAP_FAILED", bootstrapError);
        }

        return ToolingJsonV1.ValidatePatchJson(resolvedPatchJson, registry, catalog, selectors);
    }

    private static ToolingCommandResult RunExplainFlow(string[] args)
    {
        string? flowName = null;
        string? bootstrapperAssemblyPath = null;
        string? bootstrapperTypeName = null;
        var includeMermaid = false;

        for (var i = 1; i < args.Length; i++)
        {
            var a = args[i];

            if (IsHelpToken(a))
            {
                return new ToolingCommandResult(0, BuildCommandHelpJson("explain-flow"));
            }

            if (TryReadOption(args, ref i, "--flow", out var v))
            {
                flowName = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--bootstrapper", out v))
            {
                bootstrapperAssemblyPath = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--bootstrapper-type", out v))
            {
                bootstrapperTypeName = v;
                continue;
            }

            if (a == "--include-mermaid")
            {
                includeMermaid = true;
                continue;
            }

            return CreateCliErrorResult("CLI_USAGE_INVALID", $"Unknown argument: '{a}'.");
        }

        if (string.IsNullOrEmpty(flowName))
        {
            return CreateCliErrorResult("CLI_USAGE_INVALID", "Missing required option: --flow <name>.");
        }

        if (string.IsNullOrEmpty(bootstrapperTypeName))
        {
            return CreateCliErrorResult(
                "CLI_USAGE_INVALID",
                "Missing required option: --bootstrapper-type <type>.");
        }

        var registry = new FlowRegistry();
        var catalog = new ModuleCatalog();
        var selectors = new SelectorRegistry();

        if (!TryInvokeBootstrapper(bootstrapperAssemblyPath, bootstrapperTypeName, registry, catalog, selectors, out var bootstrapError))
        {
            return CreateCliErrorResult("CLI_BOOTSTRAP_FAILED", bootstrapError);
        }

        return ToolingJsonV1.ExplainFlowJson(flowName, registry, catalog, includeMermaid);
    }

    private static ToolingCommandResult RunExplainPatch(string[] args)
    {
        string? flowName = null;
        string? patchPath = null;
        string? patchJson = null;
        string? userId = null;
        var requestAttributes = new Dictionary<string, string>();
        var variants = new Dictionary<string, string>();
        string? bootstrapperAssemblyPath = null;
        string? bootstrapperTypeName = null;
        var includeMermaid = false;
        var qosTier = QosTier.Full;

        for (var i = 1; i < args.Length; i++)
        {
            var a = args[i];

            if (IsHelpToken(a))
            {
                return new ToolingCommandResult(0, BuildCommandHelpJson("explain-patch"));
            }

            if (TryReadOption(args, ref i, "--flow", out var v))
            {
                flowName = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--bootstrapper", out v))
            {
                bootstrapperAssemblyPath = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--bootstrapper-type", out v))
            {
                bootstrapperTypeName = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--patch", out v))
            {
                patchPath = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--patch-json", out v))
            {
                patchJson = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--variant", out v))
            {
                if (!TryParseKeyValuePair(v, out var key, out var value))
                {
                    return CreateCliErrorResult("CLI_USAGE_INVALID", $"Invalid --variant value: '{v}'. Expected '<key>=<value>'.");
                }

                variants[key] = value;
                continue;
            }

            if (TryReadOption(args, ref i, "--user-id", out v))
            {
                userId = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--request-attr", out v))
            {
                if (!TryParseKeyValuePair(v, out var key, out var value))
                {
                    return CreateCliErrorResult("CLI_USAGE_INVALID", $"Invalid --request-attr value: '{v}'. Expected '<key>=<value>'.");
                }

                requestAttributes[key] = value;
                continue;
            }

            if (TryReadOption(args, ref i, "--qos-tier", out v))
            {
                if (!TryParseQosTier(v, out qosTier))
                {
                    return CreateCliErrorResult("CLI_USAGE_INVALID", $"Invalid --qos-tier value: '{v}'. Expected: full|conserve|emergency|fallback.");
                }

                continue;
            }

            if (a == "--include-mermaid")
            {
                includeMermaid = true;
                continue;
            }

            return CreateCliErrorResult("CLI_USAGE_INVALID", $"Unknown argument: '{a}'.");
        }

        if (string.IsNullOrEmpty(flowName))
        {
            return CreateCliErrorResult("CLI_USAGE_INVALID", "Missing required option: --flow <name>.");
        }

        if (!TryResolveSingleJsonInput(patchPath, patchJson, "--patch", "--patch-json", out var resolvedPatchJson, out var inputError))
        {
            return CreateCliErrorResult("CLI_USAGE_INVALID", inputError);
        }

        FlowRequestOptions options;
        options = new FlowRequestOptions(
            variants: variants.Count == 0 ? null : variants,
            userId: userId,
            requestAttributes: requestAttributes.Count == 0 ? null : requestAttributes);

        SelectorRegistry? selectors = null;

        if (!string.IsNullOrEmpty(bootstrapperTypeName))
        {
            var registry = new FlowRegistry();
            var catalog = new ModuleCatalog();
            selectors = new SelectorRegistry();

            if (!TryInvokeBootstrapper(bootstrapperAssemblyPath, bootstrapperTypeName, registry, catalog, selectors, out var bootstrapError))
            {
                return CreateCliErrorResult("CLI_BOOTSTRAP_FAILED", bootstrapError);
            }
        }
        else
        {
            selectors = SelectorRegistry.Empty;
        }

        return ToolingJsonV1.ExplainPatchJson(flowName, resolvedPatchJson, options, includeMermaid, selectors, qosTier);
    }

    private static ToolingCommandResult RunDiffPatch(string[] args)
    {
        string? oldPath = null;
        string? oldJson = null;
        string? newPath = null;
        string? newJson = null;

        for (var i = 1; i < args.Length; i++)
        {
            var a = args[i];

            if (IsHelpToken(a))
            {
                return new ToolingCommandResult(0, BuildCommandHelpJson("diff-patch"));
            }

            if (TryReadOption(args, ref i, "--old", out var v))
            {
                oldPath = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--old-json", out v))
            {
                oldJson = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--new", out v))
            {
                newPath = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--new-json", out v))
            {
                newJson = v;
                continue;
            }

            return CreateCliErrorResult("CLI_USAGE_INVALID", $"Unknown argument: '{a}'.");
        }

        if (!TryResolveSingleJsonInput(oldPath, oldJson, "--old", "--old-json", out var resolvedOldJson, out var oldError))
        {
            return CreateCliErrorResult("CLI_USAGE_INVALID", oldError);
        }

        if (!TryResolveSingleJsonInput(newPath, newJson, "--new", "--new-json", out var resolvedNewJson, out var newError))
        {
            return CreateCliErrorResult("CLI_USAGE_INVALID", newError);
        }

        return ToolingJsonV1.DiffPatchJson(resolvedOldJson, resolvedNewJson);
    }

    private static ToolingCommandResult RunPreviewMatrix(string[] args)
    {
        string? flowName = null;
        string? patchPath = null;
        string? patchJson = null;
        string? matrixPath = null;
        string? matrixJson = null;
        string? userId = null;
        var requestAttributes = new Dictionary<string, string>();
        string? bootstrapperAssemblyPath = null;
        string? bootstrapperTypeName = null;
        var includeMermaid = false;
        var qosTier = QosTier.Full;

        for (var i = 1; i < args.Length; i++)
        {
            var a = args[i];

            if (IsHelpToken(a))
            {
                return new ToolingCommandResult(0, BuildCommandHelpJson("preview-matrix"));
            }

            if (TryReadOption(args, ref i, "--flow", out var v))
            {
                flowName = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--bootstrapper", out v))
            {
                bootstrapperAssemblyPath = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--bootstrapper-type", out v))
            {
                bootstrapperTypeName = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--patch", out v))
            {
                patchPath = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--patch-json", out v))
            {
                patchJson = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--matrix", out v))
            {
                matrixPath = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--matrix-json", out v))
            {
                matrixJson = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--user-id", out v))
            {
                userId = v;
                continue;
            }

            if (TryReadOption(args, ref i, "--request-attr", out v))
            {
                if (!TryParseKeyValuePair(v, out var key, out var value))
                {
                    return CreateCliErrorResult("CLI_USAGE_INVALID", $"Invalid --request-attr value: '{v}'. Expected '<key>=<value>'.");
                }

                requestAttributes[key] = value;
                continue;
            }

            if (TryReadOption(args, ref i, "--qos-tier", out v))
            {
                if (!TryParseQosTier(v, out qosTier))
                {
                    return CreateCliErrorResult("CLI_USAGE_INVALID", $"Invalid --qos-tier value: '{v}'. Expected: full|conserve|emergency|fallback.");
                }

                continue;
            }

            if (a == "--include-mermaid")
            {
                includeMermaid = true;
                continue;
            }

            return CreateCliErrorResult("CLI_USAGE_INVALID", $"Unknown argument: '{a}'.");
        }

        if (string.IsNullOrEmpty(flowName))
        {
            return CreateCliErrorResult("CLI_USAGE_INVALID", "Missing required option: --flow <name>.");
        }

        if (!TryResolveSingleJsonInput(patchPath, patchJson, "--patch", "--patch-json", out var resolvedPatchJson, out var patchError))
        {
            return CreateCliErrorResult("CLI_USAGE_INVALID", patchError);
        }

        if (!TryResolveSingleJsonInput(matrixPath, matrixJson, "--matrix", "--matrix-json", out var resolvedMatrixJson, out var matrixError))
        {
            return CreateCliErrorResult("CLI_USAGE_INVALID", matrixError);
        }

        if (!TryParseVariantsMatrix(resolvedMatrixJson, out var matrix, out var parseError))
        {
            return CreateCliErrorResult("CLI_USAGE_INVALID", parseError);
        }

        var options = new FlowRequestOptions(
            variants: null,
            userId: userId,
            requestAttributes: requestAttributes.Count == 0 ? null : requestAttributes);

        SelectorRegistry? selectors;

        if (!string.IsNullOrEmpty(bootstrapperTypeName))
        {
            var registry = new FlowRegistry();
            var catalog = new ModuleCatalog();
            selectors = new SelectorRegistry();

            if (!TryInvokeBootstrapper(bootstrapperAssemblyPath, bootstrapperTypeName, registry, catalog, selectors, out var bootstrapError))
            {
                return CreateCliErrorResult("CLI_BOOTSTRAP_FAILED", bootstrapError);
            }
        }
        else
        {
            selectors = SelectorRegistry.Empty;
        }

        return ToolingJsonV1.PreviewMatrixJson(flowName, resolvedPatchJson, matrix, selectors, qosTier, options, includeMermaid);
    }

    private static bool TryParseVariantsMatrix(
        string json,
        out IReadOnlyList<Dictionary<string, string>> matrix,
        out string error)
    {
        try
        {
            var parsed = JsonSerializer.Deserialize<Dictionary<string, string>[]>(json);
            if (parsed is null)
            {
                matrix = Array.Empty<Dictionary<string, string>>();
                error = "Invalid --matrix input: JSON payload must be a non-null array.";
                return false;
            }

            matrix = parsed;
            error = string.Empty;
            return true;
        }
        catch (JsonException ex)
        {
            matrix = Array.Empty<Dictionary<string, string>>();
            error = string.Concat("Invalid --matrix input: ", ex.Message);
            return false;
        }
    }

    private static bool TryInvokeBootstrapper(
        string? assemblyPath,
        string typeName,
        FlowRegistry registry,
        ModuleCatalog catalog,
        SelectorRegistry selectors,
        out string error)
    {
        try
        {
            Type? type = null;

            if (!string.IsNullOrEmpty(assemblyPath))
            {
                var fullPath = Path.GetFullPath(assemblyPath);
                if (!File.Exists(fullPath))
                {
                    error = $"Bootstrapper assembly not found: '{fullPath}'.";
                    return false;
                }

                var assembly = AssemblyLoadContext.Default.LoadFromAssemblyPath(fullPath);
                type = assembly.GetType(typeName, throwOnError: false, ignoreCase: false);
            }
            else
            {
                var assemblies = AppDomain.CurrentDomain.GetAssemblies();
                for (var i = 0; i < assemblies.Length; i++)
                {
                    type = assemblies[i].GetType(typeName, throwOnError: false, ignoreCase: false);
                    if (type is not null)
                    {
                        break;
                    }
                }
            }

            if (type is null)
            {
                error = $"Bootstrapper type not found: '{typeName}'.";
                return false;
            }

            var method = type.GetMethod(
                name: "Configure",
                bindingAttr: BindingFlags.Public | BindingFlags.Static,
                binder: null,
                types: new[] { typeof(FlowRegistry), typeof(ModuleCatalog), typeof(SelectorRegistry) },
                modifiers: null);

            if (method is not null)
            {
                method.Invoke(obj: null, parameters: new object[] { registry, catalog, selectors });
                error = string.Empty;
                return true;
            }

            method = type.GetMethod(
                name: "Configure",
                bindingAttr: BindingFlags.Public | BindingFlags.Static,
                binder: null,
                types: new[] { typeof(FlowRegistry), typeof(ModuleCatalog) },
                modifiers: null);

            if (method is null)
            {
                error =
                    $"Bootstrapper type '{typeName}' must define a public static method: Configure(FlowRegistry, ModuleCatalog) or Configure(FlowRegistry, ModuleCatalog, SelectorRegistry).";
                return false;
            }

            method.Invoke(obj: null, parameters: new object[] { registry, catalog });

            error = string.Empty;
            return true;
        }
        catch (Exception ex) when (ShouldHandle(ex))
        {
            error = string.IsNullOrEmpty(ex.Message) ? "Failed to invoke bootstrapper." : ex.Message;
            return false;
        }
    }

    private static bool TryResolveSingleJsonInput(
        string? path,
        string? inlineJson,
        string pathOptionName,
        string jsonOptionName,
        out string json,
        out string error)
    {
        if (!string.IsNullOrEmpty(path) && !string.IsNullOrEmpty(inlineJson))
        {
            json = string.Empty;
            error = $"Provide exactly one of: {pathOptionName} <path> or {jsonOptionName} <json>.";
            return false;
        }

        if (!string.IsNullOrEmpty(inlineJson))
        {
            json = inlineJson;
            error = string.Empty;
            return true;
        }

        if (string.IsNullOrEmpty(path))
        {
            json = string.Empty;
            error = $"Missing required option: {pathOptionName} <path> or {jsonOptionName} <json>.";
            return false;
        }

        try
        {
            json = File.ReadAllText(path, Encoding.UTF8);
            error = string.Empty;
            return true;
        }
        catch (Exception ex) when (ShouldHandle(ex))
        {
            json = string.Empty;
            error = string.Concat("Failed to read file: ", ex.Message);
            return false;
        }
    }

    private static bool TryParseKeyValuePair(string input, out string key, out string value)
    {
        var idx = input.IndexOf('=', StringComparison.Ordinal);
        if (idx <= 0 || idx == input.Length - 1)
        {
            key = string.Empty;
            value = string.Empty;
            return false;
        }

        key = input.Substring(0, idx);
        value = input.Substring(idx + 1);
        return true;
    }

    private static bool TryReadOption(string[] args, ref int index, string optionName, out string value)
    {
        var current = args[index];

        if (current == optionName)
        {
            if (index + 1 >= args.Length)
            {
                value = string.Empty;
                return true;
            }

            index++;
            value = args[index];
            return true;
        }

        if (current.StartsWith(optionName, StringComparison.Ordinal) && current.Length > optionName.Length && current[optionName.Length] == '=')
        {
            value = current.Substring(optionName.Length + 1);
            return true;
        }

        value = string.Empty;
        return false;
    }

    private static bool TryParseQosTier(string value, out QosTier tier)
    {
        if (string.Equals(value, "full", StringComparison.OrdinalIgnoreCase))
        {
            tier = QosTier.Full;
            return true;
        }

        if (string.Equals(value, "conserve", StringComparison.OrdinalIgnoreCase))
        {
            tier = QosTier.Conserve;
            return true;
        }

        if (string.Equals(value, "emergency", StringComparison.OrdinalIgnoreCase))
        {
            tier = QosTier.Emergency;
            return true;
        }

        if (string.Equals(value, "fallback", StringComparison.OrdinalIgnoreCase))
        {
            tier = QosTier.Fallback;
            return true;
        }

        tier = QosTier.Full;
        return false;
    }

    private static bool IsHelpToken(string token)
    {
        return token is "--help" or "-h" or "-?";
    }

    private static bool ShouldHandle(Exception exception)
    {
        return exception is not OutOfMemoryException
            and not StackOverflowException
            and not AccessViolationException;
    }

    private static void WriteJson(TextWriter writer, string json)
    {
        writer.WriteLine(json);
    }

    private static ToolingCommandResult CreateCliErrorResult(string code, string message, int exitCode = 2)
    {
        return new ToolingCommandResult(exitCode, BuildCliErrorJson(code, message));
    }

    private static string BuildCliErrorJson(string code, string message)
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
        writer.WriteString("kind", "cli_error");
        writer.WriteString("code", code);
        writer.WriteString("message", message);
        writer.WriteEndObject();
        writer.Flush();

        return Encoding.UTF8.GetString(output.WrittenSpan);
    }

    private static string BuildHelpJson()
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
        writer.WriteString("kind", "help");

        writer.WritePropertyName("commands");
        writer.WriteStartArray();

        WriteCommandHelp(writer, "validate");
        WriteCommandHelp(writer, "explain-flow");
        WriteCommandHelp(writer, "explain-patch");
        WriteCommandHelp(writer, "diff-patch");
        WriteCommandHelp(writer, "preview-matrix");

        writer.WriteEndArray();
        writer.WriteEndObject();
        writer.Flush();

        return Encoding.UTF8.GetString(output.WrittenSpan);
    }

    private static string BuildCommandHelpJson(string command)
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
        writer.WriteString("kind", "help");
        writer.WritePropertyName("command");
        WriteCommandHelp(writer, command);
        writer.WriteEndObject();
        writer.Flush();

        return Encoding.UTF8.GetString(output.WrittenSpan);
    }

    private static void WriteCommandHelp(Utf8JsonWriter writer, string command)
    {
        writer.WriteStartObject();
        writer.WriteString("name", command);

        writer.WriteString(
            "usage",
            command switch
            {
                "validate" =>
                    "rorchestrator validate --bootstrapper <path> --bootstrapper-type <type> (--patch <path> | --patch-json <json>)",
                "explain-flow" =>
                    "rorchestrator explain-flow --bootstrapper <path> --bootstrapper-type <type> --flow <name> [--include-mermaid]",
                "explain-patch" =>
                    "rorchestrator explain-patch --flow <name> (--patch <path> | --patch-json <json>) [--variant <k=v> ...] [--user-id <value>] [--request-attr <k=v> ...] [--qos-tier <tier>] [--include-mermaid] [--bootstrapper <path>] [--bootstrapper-type <type>]",
                "diff-patch" =>
                    "rorchestrator diff-patch (--old <path> | --old-json <json>) (--new <path> | --new-json <json>)",
                "preview-matrix" =>
                    "rorchestrator preview-matrix --flow <name> (--patch <path> | --patch-json <json>) (--matrix <path> | --matrix-json <json>) [--user-id <value>] [--request-attr <k=v> ...] [--qos-tier <tier>] [--include-mermaid] [--bootstrapper <path>] [--bootstrapper-type <type>]",
                _ => "rorchestrator <command> [options]",
            });

        writer.WriteEndObject();
    }
}
