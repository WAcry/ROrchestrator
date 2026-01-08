using System.Text.Json;
using ROrchestrator.Core;

namespace ROrchestrator.Core.Gates;

public static class GateJsonV1
{
    public const int DefaultMaxDepth = 10;

    public const string CodeUnknownType = "CFG_GATE_UNKNOWN_TYPE";
    public const string CodeEmptyComposite = "CFG_GATE_EMPTY_COMPOSITE";
    public const string CodeTooDeep = "CFG_GATE_TOO_DEEP";
    public const string CodeExperimentInvalid = "CFG_GATE_EXPERIMENT_INVALID";

    public const string MessageUnknownType = "gate type is unknown or unsupported.";
    public const string MessageEmptyComposite = "gate composite must be a non-empty array.";
    public const string MessageTooDeep = "gate nesting is too deep.";
    public const string MessageExperimentLayerInvalid = "gate.experiment.layer is required and must be a non-empty string.";
    public const string MessageExperimentInInvalid = "gate.experiment.in is required and must be a non-empty array of strings.";

    public static bool TryParseOptional(JsonElement gateElement, string gatePath, out Gate? gate, out ValidationFinding finding)
    {
        if (gatePath is null)
        {
            throw new ArgumentNullException(nameof(gatePath));
        }

        gate = null;
        finding = default;

        var kind = gateElement.ValueKind;

        if (kind == JsonValueKind.Null || kind == JsonValueKind.Undefined)
        {
            return true;
        }

        return TryParseInternal(gateElement, gatePath, depth: 0, out gate, out finding);
    }

    private static bool TryParseInternal(JsonElement gateElement, string gatePath, int depth, out Gate? gate, out ValidationFinding finding)
    {
        gate = null;
        finding = default;

        if (depth > DefaultMaxDepth)
        {
            finding = new ValidationFinding(ValidationSeverity.Error, CodeTooDeep, gatePath, MessageTooDeep);
            return false;
        }

        if (gateElement.ValueKind != JsonValueKind.Object)
        {
            finding = new ValidationFinding(ValidationSeverity.Error, CodeUnknownType, gatePath, MessageUnknownType);
            return false;
        }

        var hasUnknownField = false;
        var gateType = GateType.Unknown;
        JsonElement gateTypeValue = default;
        var gateTypeCount = 0;

        foreach (var property in gateElement.EnumerateObject())
        {
            if (property.NameEquals("experiment"))
            {
                gateType = GateType.Experiment;
                gateTypeValue = property.Value;
                gateTypeCount++;
                continue;
            }

            if (property.NameEquals("all"))
            {
                gateType = GateType.All;
                gateTypeValue = property.Value;
                gateTypeCount++;
                continue;
            }

            if (property.NameEquals("any"))
            {
                gateType = GateType.Any;
                gateTypeValue = property.Value;
                gateTypeCount++;
                continue;
            }

            if (property.NameEquals("not"))
            {
                gateType = GateType.Not;
                gateTypeValue = property.Value;
                gateTypeCount++;
                continue;
            }

            hasUnknownField = true;
        }

        if (gateTypeCount != 1 || hasUnknownField)
        {
            finding = new ValidationFinding(ValidationSeverity.Error, CodeUnknownType, gatePath, MessageUnknownType);
            return false;
        }

        switch (gateType)
        {
            case GateType.Experiment:
                return TryParseExperiment(gateTypeValue, gatePath, out gate, out finding);
            case GateType.All:
                return TryParseComposite(GateType.All, gateTypeValue, string.Concat(gatePath, ".all"), depth, out gate, out finding);
            case GateType.Any:
                return TryParseComposite(GateType.Any, gateTypeValue, string.Concat(gatePath, ".any"), depth, out gate, out finding);
            case GateType.Not:
                return TryParseNot(gateTypeValue, string.Concat(gatePath, ".not"), depth, out gate, out finding);
            default:
                finding = new ValidationFinding(ValidationSeverity.Error, CodeUnknownType, gatePath, MessageUnknownType);
                return false;
        }
    }

    private static bool TryParseExperiment(JsonElement element, string gatePath, out Gate? gate, out ValidationFinding finding)
    {
        gate = null;
        finding = default;

        if (element.ValueKind != JsonValueKind.Object)
        {
            finding = new ValidationFinding(ValidationSeverity.Error, CodeExperimentInvalid, string.Concat(gatePath, ".experiment"), MessageExperimentLayerInvalid);
            return false;
        }

        string? layer = null;
        var hasLayer = false;

        var hasIn = false;
        JsonElement inElement = default;

        foreach (var property in element.EnumerateObject())
        {
            if (property.NameEquals("layer"))
            {
                hasLayer = true;
                if (property.Value.ValueKind == JsonValueKind.String)
                {
                    layer = property.Value.GetString();
                }
                else
                {
                    layer = null;
                }

                continue;
            }

            if (property.NameEquals("in"))
            {
                hasIn = true;
                inElement = property.Value;
                continue;
            }
        }

        if (!hasLayer || string.IsNullOrEmpty(layer))
        {
            finding = new ValidationFinding(
                ValidationSeverity.Error,
                CodeExperimentInvalid,
                string.Concat(gatePath, ".experiment.layer"),
                MessageExperimentLayerInvalid);
            return false;
        }

        if (!hasIn || inElement.ValueKind != JsonValueKind.Array || inElement.GetArrayLength() == 0)
        {
            finding = new ValidationFinding(
                ValidationSeverity.Error,
                CodeExperimentInvalid,
                string.Concat(gatePath, ".experiment.in"),
                MessageExperimentInInvalid);
            return false;
        }

        var variantsCount = inElement.GetArrayLength();
        var variants = new string[variantsCount];

        var index = 0;
        foreach (var item in inElement.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.String)
            {
                finding = new ValidationFinding(
                    ValidationSeverity.Error,
                    CodeExperimentInvalid,
                    string.Concat(gatePath, ".experiment.in[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "]"),
                    MessageExperimentInInvalid);
                return false;
            }

            var value = item.GetString();

            if (string.IsNullOrEmpty(value))
            {
                finding = new ValidationFinding(
                    ValidationSeverity.Error,
                    CodeExperimentInvalid,
                    string.Concat(gatePath, ".experiment.in[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "]"),
                    MessageExperimentInInvalid);
                return false;
            }

            variants[index] = value;
            index++;
        }

        gate = new ExperimentGate(layer, variants);
        return true;
    }

    private static bool TryParseComposite(GateType gateType, JsonElement element, string pathPrefix, int depth, out Gate? gate, out ValidationFinding finding)
    {
        gate = null;
        finding = default;

        if (element.ValueKind != JsonValueKind.Array || element.GetArrayLength() == 0)
        {
            finding = new ValidationFinding(ValidationSeverity.Error, CodeEmptyComposite, pathPrefix, MessageEmptyComposite);
            return false;
        }

        var length = element.GetArrayLength();
        var children = new Gate[length];

        var index = 0;
        foreach (var item in element.EnumerateArray())
        {
            var childPath = string.Concat(pathPrefix, "[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "]");

            if (!TryParseInternal(item, childPath, depth + 1, out var childGate, out finding))
            {
                return false;
            }

            children[index] = childGate!;
            index++;
        }

        gate = gateType == GateType.All
            ? new AllGate(children)
            : new AnyGate(children);

        return true;
    }

    private static bool TryParseNot(JsonElement element, string pathPrefix, int depth, out Gate? gate, out ValidationFinding finding)
    {
        gate = null;
        finding = default;

        if (!TryParseInternal(element, pathPrefix, depth + 1, out var child, out finding))
        {
            return false;
        }

        gate = new NotGate(child!);
        return true;
    }

    private enum GateType
    {
        Unknown = 0,
        Experiment = 1,
        All = 2,
        Any = 3,
        Not = 4,
    }
}

