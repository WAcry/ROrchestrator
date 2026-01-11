using System.Buffers;
using System.Text;
using System.Text.Json;

namespace Rockestra.Tooling.Tests;

internal static class JsonExecExplainV3Normalizer
{
    public static string Normalize(string json)
    {
        if (json is null)
        {
            throw new ArgumentNullException(nameof(json));
        }

        using var doc = JsonDocument.Parse(json);

        var output = new ArrayBufferWriter<byte>(json.Length);
        using var writer = new Utf8JsonWriter(
            output,
            new JsonWriterOptions
            {
                Indented = false,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            });

        WriteElement(writer, doc.RootElement);
        writer.Flush();

        return Encoding.UTF8.GetString(output.WrittenSpan);
    }

    private static void WriteElement(Utf8JsonWriter writer, JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
            {
                writer.WriteStartObject();

                foreach (var property in element.EnumerateObject())
                {
                    if (property.NameEquals("timing") && property.Value.ValueKind == JsonValueKind.Object)
                    {
                        WriteNormalizedTiming(writer, property.Name, property.Value);
                        continue;
                    }

                    if (property.NameEquals("budget_remaining_ms_at_start") && property.Value.ValueKind == JsonValueKind.Number)
                    {
                        writer.WriteNumber(property.Name, 0);
                        continue;
                    }

                    if (property.NameEquals("trace_id"))
                    {
                        writer.WritePropertyName(property.Name);

                        if (property.Value.ValueKind == JsonValueKind.String)
                        {
                            writer.WriteStringValue("TRACE_ID");
                        }
                        else
                        {
                            WriteElement(writer, property.Value);
                        }

                        continue;
                    }

                    if (property.NameEquals("span_id"))
                    {
                        writer.WritePropertyName(property.Name);

                        if (property.Value.ValueKind == JsonValueKind.String)
                        {
                            writer.WriteStringValue("SPAN_ID");
                        }
                        else
                        {
                            WriteElement(writer, property.Value);
                        }

                        continue;
                    }

                    writer.WritePropertyName(property.Name);
                    WriteElement(writer, property.Value);
                }

                writer.WriteEndObject();
                break;
            }
            case JsonValueKind.Array:
            {
                writer.WriteStartArray();

                foreach (var item in element.EnumerateArray())
                {
                    WriteElement(writer, item);
                }

                writer.WriteEndArray();
                break;
            }
            default:
                element.WriteTo(writer);
                break;
        }
    }

    private static void WriteNormalizedTiming(Utf8JsonWriter writer, string propertyName, JsonElement timing)
    {
        writer.WritePropertyName(propertyName);
        writer.WriteStartObject();

        foreach (var property in timing.EnumerateObject())
        {
            if (property.NameEquals("duration_ticks"))
            {
                writer.WriteNumber(property.Name, 0);
                continue;
            }

            if (property.NameEquals("duration_ms"))
            {
                writer.WriteNumber(property.Name, 0);
                continue;
            }

            writer.WritePropertyName(property.Name);
            WriteElement(writer, property.Value);
        }

        writer.WriteEndObject();
    }
}
