using System.Text.Json;
using Rockestra.Core;
using Rockestra.Core.Selectors;

namespace Rockestra.Core.Gates;

public static class GateJsonV1
{
    public const int DefaultMaxDepth = 10;

    public const string CodeUnknownField = "CFG_UNKNOWN_FIELD";
    public const string CodeUnknownType = "CFG_GATE_UNKNOWN_TYPE";
    public const string CodeEmptyComposite = "CFG_GATE_EMPTY_COMPOSITE";
    public const string CodeTooDeep = "CFG_GATE_TOO_DEEP";
    public const string CodeExperimentInvalid = "CFG_GATE_EXPERIMENT_INVALID";
    public const string CodeRolloutInvalid = "CFG_GATE_ROLLOUT_INVALID";
    public const string CodeRequestInvalid = "CFG_GATE_REQUEST_INVALID";
    public const string CodeRequestFieldNotAllowed = "CFG_GATE_REQUEST_FIELD_NOT_ALLOWED";
    public const string CodeSelectorInvalid = "CFG_GATE_SELECTOR_INVALID";
    public const string CodeSelectorNotRegistered = "CFG_SELECTOR_NOT_REGISTERED";

    public const string MessageUnknownType = "gate type is unknown or unsupported.";
    public const string MessageEmptyComposite = "gate composite must be a non-empty array.";
    public const string MessageTooDeep = "gate nesting is too deep.";
    public const string MessageExperimentLayerInvalid = "gate.experiment.layer is required and must be a non-empty string.";
    public const string MessageExperimentInInvalid = "gate.experiment.in is required and must be a non-empty array of strings.";
    public const string MessageRolloutPercentInvalid = "gate.rollout.percent must be a number between 0 and 100.";
    public const string MessageRolloutSaltInvalid = "gate.rollout.salt is required and must be a non-empty string.";
    public const string MessageRequestFieldInvalid = "gate.request.field is required and must be a non-empty string.";
    public const string MessageRequestInInvalid = "gate.request.in is required and must be a non-empty array of strings.";
    public const string MessageRequestFieldNotAllowed = "gate.request.field is not allowed.";
    public const string MessageSelectorInvalid = "gate.selector is required and must be a non-empty string.";

    private static readonly string[] AllowedRequestFields =
    {
        "region",
        "device",
        "appVersion",
    };

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

        return TryParseInternal(gateElement, gatePath, depth: 0, selectorRegistry: null, out gate, out finding);
    }

    public static bool TryParseOptional(JsonElement gateElement, string gatePath, SelectorRegistry selectorRegistry, out Gate? gate, out ValidationFinding finding)
    {
        if (gatePath is null)
        {
            throw new ArgumentNullException(nameof(gatePath));
        }

        if (selectorRegistry is null)
        {
            throw new ArgumentNullException(nameof(selectorRegistry));
        }

        gate = null;
        finding = default;

        var kind = gateElement.ValueKind;

        if (kind == JsonValueKind.Null || kind == JsonValueKind.Undefined)
        {
            return true;
        }

        return TryParseInternal(gateElement, gatePath, depth: 0, selectorRegistry, out gate, out finding);
    }

    private static bool TryParseInternal(
        JsonElement gateElement,
        string gatePath,
        int depth,
        SelectorRegistry? selectorRegistry,
        out Gate? gate,
        out ValidationFinding finding)
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

            if (property.NameEquals("rollout"))
            {
                gateType = GateType.Rollout;
                gateTypeValue = property.Value;
                gateTypeCount++;
                continue;
            }

            if (property.NameEquals("request"))
            {
                gateType = GateType.Request;
                gateTypeValue = property.Value;
                gateTypeCount++;
                continue;
            }

            if (property.NameEquals("selector"))
            {
                gateType = GateType.Selector;
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
            case GateType.Rollout:
                return TryParseRollout(gateTypeValue, gatePath, out gate, out finding);
            case GateType.Request:
                return TryParseRequest(gateTypeValue, gatePath, out gate, out finding);
            case GateType.Selector:
                return TryParseSelector(gateTypeValue, gatePath, selectorRegistry, out gate, out finding);
            case GateType.All:
                return TryParseComposite(GateType.All, gateTypeValue, string.Concat(gatePath, ".all"), depth, selectorRegistry, out gate, out finding);
            case GateType.Any:
                return TryParseComposite(GateType.Any, gateTypeValue, string.Concat(gatePath, ".any"), depth, selectorRegistry, out gate, out finding);
            case GateType.Not:
                return TryParseNot(gateTypeValue, string.Concat(gatePath, ".not"), depth, selectorRegistry, out gate, out finding);
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

        string? unknownField = null;

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

            unknownField = property.Name;
            break;
        }

        if (unknownField is not null)
        {
            finding = new ValidationFinding(
                ValidationSeverity.Error,
                CodeUnknownField,
                string.Concat(gatePath, ".experiment.", unknownField),
                string.Concat("Unknown field: ", unknownField));
            return false;
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

    private static bool TryParseRollout(JsonElement element, string gatePath, out Gate? gate, out ValidationFinding finding)
    {
        gate = null;
        finding = default;

        if (element.ValueKind != JsonValueKind.Object)
        {
            finding = new ValidationFinding(ValidationSeverity.Error, CodeRolloutInvalid, string.Concat(gatePath, ".rollout"), MessageUnknownType);
            return false;
        }

        var hasPercent = false;
        JsonElement percentElement = default;
        string? salt = null;
        var hasSalt = false;
        string? unknownField = null;

        foreach (var property in element.EnumerateObject())
        {
            if (property.NameEquals("percent"))
            {
                hasPercent = true;
                percentElement = property.Value;
                continue;
            }

            if (property.NameEquals("salt"))
            {
                hasSalt = true;
                if (property.Value.ValueKind == JsonValueKind.String)
                {
                    salt = property.Value.GetString();
                }
                else
                {
                    salt = null;
                }

                continue;
            }

            unknownField = property.Name;
            break;
        }

        if (unknownField is not null)
        {
            finding = new ValidationFinding(
                ValidationSeverity.Error,
                CodeUnknownField,
                string.Concat(gatePath, ".rollout.", unknownField),
                string.Concat("Unknown field: ", unknownField));
            return false;
        }

        if (!hasPercent || percentElement.ValueKind != JsonValueKind.Number || !percentElement.TryGetDouble(out var percent))
        {
            finding = new ValidationFinding(
                ValidationSeverity.Error,
                CodeRolloutInvalid,
                string.Concat(gatePath, ".rollout.percent"),
                MessageRolloutPercentInvalid);
            return false;
        }

        if (percent < 0 || percent > 100)
        {
            finding = new ValidationFinding(
                ValidationSeverity.Error,
                CodeRolloutInvalid,
                string.Concat(gatePath, ".rollout.percent"),
                MessageRolloutPercentInvalid);
            return false;
        }

        if (!hasSalt || string.IsNullOrEmpty(salt))
        {
            finding = new ValidationFinding(
                ValidationSeverity.Error,
                CodeRolloutInvalid,
                string.Concat(gatePath, ".rollout.salt"),
                MessageRolloutSaltInvalid);
            return false;
        }

        gate = new RolloutGate(percent, salt);
        return true;
    }

    private static bool TryParseRequest(JsonElement element, string gatePath, out Gate? gate, out ValidationFinding finding)
    {
        gate = null;
        finding = default;

        if (element.ValueKind != JsonValueKind.Object)
        {
            finding = new ValidationFinding(ValidationSeverity.Error, CodeRequestInvalid, string.Concat(gatePath, ".request"), MessageUnknownType);
            return false;
        }

        string? field = null;
        var hasField = false;

        var hasIn = false;
        JsonElement inElement = default;
        string? unknownField = null;

        foreach (var property in element.EnumerateObject())
        {
            if (property.NameEquals("field"))
            {
                hasField = true;
                if (property.Value.ValueKind == JsonValueKind.String)
                {
                    field = property.Value.GetString();
                }
                else
                {
                    field = null;
                }

                continue;
            }

            if (property.NameEquals("in"))
            {
                hasIn = true;
                inElement = property.Value;
                continue;
            }

            unknownField = property.Name;
            break;
        }

        if (unknownField is not null)
        {
            finding = new ValidationFinding(
                ValidationSeverity.Error,
                CodeUnknownField,
                string.Concat(gatePath, ".request.", unknownField),
                string.Concat("Unknown field: ", unknownField));
            return false;
        }

        if (!hasField || string.IsNullOrEmpty(field))
        {
            finding = new ValidationFinding(
                ValidationSeverity.Error,
                CodeRequestInvalid,
                string.Concat(gatePath, ".request.field"),
                MessageRequestFieldInvalid);
            return false;
        }

        if (!IsAllowedRequestField(field))
        {
            finding = new ValidationFinding(
                ValidationSeverity.Error,
                CodeRequestFieldNotAllowed,
                string.Concat(gatePath, ".request.field"),
                MessageRequestFieldNotAllowed);
            return false;
        }

        if (!hasIn || inElement.ValueKind != JsonValueKind.Array || inElement.GetArrayLength() == 0)
        {
            finding = new ValidationFinding(
                ValidationSeverity.Error,
                CodeRequestInvalid,
                string.Concat(gatePath, ".request.in"),
                MessageRequestInInvalid);
            return false;
        }

        var valuesCount = inElement.GetArrayLength();
        var values = new string[valuesCount];

        var index = 0;
        foreach (var item in inElement.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.String)
            {
                finding = new ValidationFinding(
                    ValidationSeverity.Error,
                    CodeRequestInvalid,
                    string.Concat(gatePath, ".request.in[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "]"),
                    MessageRequestInInvalid);
                return false;
            }

            var value = item.GetString();

            if (string.IsNullOrEmpty(value))
            {
                finding = new ValidationFinding(
                    ValidationSeverity.Error,
                    CodeRequestInvalid,
                    string.Concat(gatePath, ".request.in[", index.ToString(System.Globalization.CultureInfo.InvariantCulture), "]"),
                    MessageRequestInInvalid);
                return false;
            }

            values[index] = value;
            index++;
        }

        gate = new RequestAttrGate(field, values);
        return true;
    }

    private static bool TryParseSelector(
        JsonElement element,
        string gatePath,
        SelectorRegistry? selectorRegistry,
        out Gate? gate,
        out ValidationFinding finding)
    {
        gate = null;
        finding = default;

        if (element.ValueKind != JsonValueKind.String)
        {
            finding = new ValidationFinding(
                ValidationSeverity.Error,
                CodeSelectorInvalid,
                string.Concat(gatePath, ".selector"),
                MessageSelectorInvalid);
            return false;
        }

        var selectorName = element.GetString();

        if (string.IsNullOrEmpty(selectorName))
        {
            finding = new ValidationFinding(
                ValidationSeverity.Error,
                CodeSelectorInvalid,
                string.Concat(gatePath, ".selector"),
                MessageSelectorInvalid);
            return false;
        }

        if (selectorRegistry is not null && !selectorRegistry.IsRegistered(selectorName))
        {
            finding = new ValidationFinding(
                ValidationSeverity.Error,
                CodeSelectorNotRegistered,
                string.Concat(gatePath, ".selector"),
                string.Concat("selector is not registered: ", selectorName));
            return false;
        }

        gate = new SelectorGate(selectorName);
        return true;
    }

    private static bool IsAllowedRequestField(string field)
    {
        for (var i = 0; i < AllowedRequestFields.Length; i++)
        {
            if (string.Equals(field, AllowedRequestFields[i], StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static bool TryParseComposite(
        GateType gateType,
        JsonElement element,
        string pathPrefix,
        int depth,
        SelectorRegistry? selectorRegistry,
        out Gate? gate,
        out ValidationFinding finding)
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

            if (!TryParseInternal(item, childPath, depth + 1, selectorRegistry, out var childGate, out finding))
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

    private static bool TryParseNot(
        JsonElement element,
        string pathPrefix,
        int depth,
        SelectorRegistry? selectorRegistry,
        out Gate? gate,
        out ValidationFinding finding)
    {
        gate = null;
        finding = default;

        if (!TryParseInternal(element, pathPrefix, depth + 1, selectorRegistry, out var child, out finding))
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
        Rollout = 2,
        Request = 3,
        All = 4,
        Any = 5,
        Not = 6,
        Selector = 7,
    }
}

