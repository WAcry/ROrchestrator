using System.Buffers;
using System.Text.Json;

namespace Rockestra.Core;

public static class ExplainRedactor
{
    private const string RedactedValue = "[REDACTED]";

    public static byte[] Redact(ReadOnlySpan<byte> jsonUtf8, ExplainRedactionPolicy policy = ExplainRedactionPolicy.Default)
    {
        var copied = jsonUtf8.ToArray();
        return Redact(copied.AsMemory(), policy);
    }

    public static byte[] Redact(ReadOnlyMemory<byte> jsonUtf8, ExplainRedactionPolicy policy = ExplainRedactionPolicy.Default)
    {
        if ((uint)policy > (uint)ExplainRedactionPolicy.Default)
        {
            throw new ArgumentOutOfRangeException(nameof(policy), policy, "Unsupported explain redaction policy.");
        }

        using var document = JsonDocument.Parse(jsonUtf8);

        var output = new ArrayBufferWriter<byte>(jsonUtf8.Length);
        using var writer = new Utf8JsonWriter(
            output,
            new JsonWriterOptions
            {
                Indented = false,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            });

        WriteRedactedValue(writer, document.RootElement, inParams: false, redactAllValues: false);
        writer.Flush();

        return output.WrittenSpan.ToArray();
    }

    public static byte[] RedactParamsEffective(ReadOnlySpan<byte> jsonUtf8, ExplainRedactionPolicy policy = ExplainRedactionPolicy.Default)
    {
        var copied = jsonUtf8.ToArray();
        return RedactParamsEffective(copied.AsMemory(), policy);
    }

    public static byte[] RedactParamsEffective(ReadOnlyMemory<byte> jsonUtf8, ExplainRedactionPolicy policy = ExplainRedactionPolicy.Default)
    {
        if ((uint)policy > (uint)ExplainRedactionPolicy.Default)
        {
            throw new ArgumentOutOfRangeException(nameof(policy), policy, "Unsupported explain redaction policy.");
        }

        using var document = JsonDocument.Parse(jsonUtf8);

        var output = new ArrayBufferWriter<byte>(jsonUtf8.Length);
        using var writer = new Utf8JsonWriter(
            output,
            new JsonWriterOptions
            {
                Indented = false,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            });

        WriteRedactedValue(writer, document.RootElement, inParams: false, redactAllValues: true);
        writer.Flush();

        return output.WrittenSpan.ToArray();
    }

    private static void WriteRedactedValue(Utf8JsonWriter writer, JsonElement element, bool inParams, bool redactAllValues)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                writer.WriteStartObject();

                foreach (var property in element.EnumerateObject())
                {
                    var name = property.Name;

                    writer.WritePropertyName(name);

                    if (ShouldRedactKey(name))
                    {
                        writer.WriteStringValue(RedactedValue);
                        continue;
                    }

                    var nextInParams = inParams || property.NameEquals("params");
                    var nextRedactAllValues = redactAllValues || (nextInParams && property.NameEquals("effective"));

                    WriteRedactedValue(writer, property.Value, nextInParams, nextRedactAllValues);
                }

                writer.WriteEndObject();
                return;

            case JsonValueKind.Array:
                writer.WriteStartArray();

                foreach (var item in element.EnumerateArray())
                {
                    WriteRedactedValue(writer, item, inParams, redactAllValues);
                }

                writer.WriteEndArray();
                return;

            case JsonValueKind.String:
                if (redactAllValues)
                {
                    writer.WriteStringValue(RedactedValue);
                    return;
                }

                writer.WriteStringValue(element.GetString());
                return;

            case JsonValueKind.Number:
            case JsonValueKind.True:
            case JsonValueKind.False:
                if (redactAllValues)
                {
                    writer.WriteStringValue(RedactedValue);
                    return;
                }

                element.WriteTo(writer);
                return;

            case JsonValueKind.Null:
                writer.WriteNullValue();
                return;

            case JsonValueKind.Undefined:
                writer.WriteNullValue();
                return;

            default:
                if (redactAllValues)
                {
                    writer.WriteStringValue(RedactedValue);
                    return;
                }

                element.WriteTo(writer);
                return;
        }
    }

    private static bool ShouldRedactKey(string name)
    {
        if (name.Length == 0)
        {
            return false;
        }

        if (name.IndexOf("token", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            return true;
        }

        if (name.IndexOf("password", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            return true;
        }

        if (name.IndexOf("secret", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            return true;
        }

        if (name.IndexOf("api_key", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            return true;
        }

        if (name.IndexOf("apikey", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            return true;
        }

        if (name.IndexOf("authorization", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            return true;
        }

        if (name.IndexOf("cookie", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            return true;
        }

        if (name.IndexOf("credential", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            return true;
        }

        if (name.IndexOf("session", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            return true;
        }

        return false;
    }
}
