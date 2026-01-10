using System.Buffers;
using System.Text.Json;

namespace Rockestra.Core;

internal static class JsonMergeV1
{
    public static byte[] Merge(JsonElement baseObject, JsonElement[] overlays, int overlayCount)
    {
        if (overlays is null)
        {
            throw new ArgumentNullException(nameof(overlays));
        }

        if ((uint)overlayCount > (uint)overlays.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(overlayCount), overlayCount, "overlayCount must be within overlays bounds.");
        }

        if (baseObject.ValueKind != JsonValueKind.Object)
        {
            throw new ArgumentException("baseObject must be a JSON object.", nameof(baseObject));
        }

        for (var i = 0; i < overlayCount; i++)
        {
            if (overlays[i].ValueKind != JsonValueKind.Object)
            {
                throw new ArgumentException("All overlays must be JSON objects.", nameof(overlays));
            }
        }

        var output = new ArrayBufferWriter<byte>(256);
        using var writer = new Utf8JsonWriter(
            output,
            new JsonWriterOptions
            {
                Indented = false,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            });

        WriteMergedObject(writer, hasBase: true, baseObject, overlays, overlayCount);
        writer.Flush();

        return output.WrittenSpan.ToArray();
    }

    private static void WriteMergedObject(
        Utf8JsonWriter writer,
        bool hasBase,
        JsonElement baseObject,
        JsonElement[] overlays,
        int overlayCount)
    {
        writer.WriteStartObject();

        var names = new PropertyNameBuffer(initialCapacity: 16);

        if (hasBase && baseObject.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in baseObject.EnumerateObject())
            {
                names.Add(property.Name);
            }
        }

        for (var i = 0; i < overlayCount; i++)
        {
            foreach (var property in overlays[i].EnumerateObject())
            {
                names.Add(property.Name);
            }
        }

        var nameArray = names.Items;

        for (var nameIndex = 0; nameIndex < names.Count; nameIndex++)
        {
            var name = nameArray[nameIndex];

            var baseValue = default(JsonElement);
            var hasBaseValue = hasBase
                && baseObject.ValueKind == JsonValueKind.Object
                && baseObject.TryGetProperty(name, out baseValue);

            var lastOverlayIndex = -1;
            JsonElement lastOverlayValue = default;

            for (var i = overlayCount - 1; i >= 0; i--)
            {
                if (!overlays[i].TryGetProperty(name, out var overlayValue))
                {
                    continue;
                }

                lastOverlayIndex = i;
                lastOverlayValue = overlayValue;
                break;
            }

            if (lastOverlayIndex < 0)
            {
                if (!hasBaseValue)
                {
                    continue;
                }

                writer.WritePropertyName(name);
                baseValue.WriteTo(writer);
                continue;
            }

            if (lastOverlayValue.ValueKind != JsonValueKind.Object)
            {
                writer.WritePropertyName(name);
                lastOverlayValue.WriteTo(writer);
                continue;
            }

            var resetIndex = -1;

            for (var i = 0; i <= lastOverlayIndex; i++)
            {
                if (!overlays[i].TryGetProperty(name, out var overlayValue))
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
            var rented = ArrayPool<JsonElement>.Shared.Rent(maxNestedCount);
            var nestedCount = 0;

            try
            {
                for (var i = resetIndex + 1; i <= lastOverlayIndex; i++)
                {
                    if (!overlays[i].TryGetProperty(name, out var overlayValue))
                    {
                        continue;
                    }

                    if (overlayValue.ValueKind != JsonValueKind.Object)
                    {
                        continue;
                    }

                    rented[nestedCount] = overlayValue;
                    nestedCount++;
                }

                writer.WritePropertyName(name);
                WriteMergedObject(writer, nestedHasBase, nestedBase, rented, nestedCount);
            }
            finally
            {
                Array.Clear(rented, 0, nestedCount);
                ArrayPool<JsonElement>.Shared.Return(rented);
            }
        }

        writer.WriteEndObject();
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
}

