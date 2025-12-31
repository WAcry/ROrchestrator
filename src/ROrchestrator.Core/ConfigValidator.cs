using System.Text.Json;

namespace ROrchestrator.Core;

public sealed class ConfigValidator
{
    private const string SupportedSchemaVersion = "v1";

    private const string CodeParseError = "CFG_PARSE_ERROR";
    private const string CodeSchemaVersionUnsupported = "CFG_SCHEMA_VERSION_UNSUPPORTED";
    private const string CodeUnknownField = "CFG_UNKNOWN_FIELD";
    private const string CodeFlowNotRegistered = "CFG_FLOW_NOT_REGISTERED";

    private readonly FlowRegistry _flowRegistry;

    public ConfigValidator(FlowRegistry flowRegistry)
    {
        _flowRegistry = flowRegistry ?? throw new ArgumentNullException(nameof(flowRegistry));
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

            if (hasFlows && flowsElement.ValueKind == JsonValueKind.Object)
            {
                foreach (var flowProperty in flowsElement.EnumerateObject())
                {
                    var flowName = flowProperty.Name;

                    if (flowName.Length == 0 || !_flowRegistry.IsRegistered(flowName))
                    {
                        findings.Add(
                            new ValidationFinding(
                                ValidationSeverity.Error,
                                code: CodeFlowNotRegistered,
                                path: string.Concat("$.flows.", flowName),
                                message: string.Concat("Flow is not registered: ", flowName)));
                    }

                    if (flowProperty.Value.ValueKind == JsonValueKind.Object)
                    {
                        ValidateFlowPatchTopLevelFields(flowName, flowProperty.Value, ref findings);
                    }
                }
            }

            return findings.IsEmpty ? ValidationReport.Empty : new ValidationReport(findings.ToArray());
        }
    }

    private static void ValidateFlowPatchTopLevelFields(string flowName, JsonElement flowPatch, ref FindingBuffer findings)
    {
        foreach (var flowField in flowPatch.EnumerateObject())
        {
            if (flowField.NameEquals("params")
                || flowField.NameEquals("stages")
                || flowField.NameEquals("experiments")
                || flowField.NameEquals("emergency"))
            {
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
