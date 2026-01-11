using System.Buffers;
using System.Text.Json;

namespace Rockestra.Core;

public static class FlowParamsResolver
{
    private const ulong OffsetBasis = 14695981039346656037ul;
    private const ulong Prime = 1099511628211ul;

    private static readonly ParamsSourceDescriptor DefaultSource = new(layer: "default", experimentLayer: null, experimentVariant: null, qosTier: null);
    private static readonly ParamsSourceDescriptor BaseSource = new(layer: "base", experimentLayer: null, experimentVariant: null, qosTier: null);
    private static readonly ParamsSourceDescriptor EmergencySource = new(layer: "emergency", experimentLayer: null, experimentVariant: null, qosTier: null);

    public static bool TryComputeParamsHash(
        JsonElement defaultParamsObject,
        JsonElement flowPatch,
        IReadOnlyDictionary<string, string>? variants,
        QosTier qosTier,
        out ulong hash,
        DateTimeOffset? configTimestampUtc = null)
    {
        if (defaultParamsObject.ValueKind != JsonValueKind.Object)
        {
            throw new ArgumentException("defaultParamsObject must be a JSON object.", nameof(defaultParamsObject));
        }

        var overlayBuffer = new ParamsOverlayBuffer(initialCapacity: 4);
        CollectOverlays(flowPatch, variants, qosTier, configTimestampUtc, ref overlayBuffer);

        var output = new ArrayBufferWriter<byte>(256);
        using var writer = new Utf8JsonWriter(
            output,
            new JsonWriterOptions
            {
                Indented = false,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            });

        WriteMergedParamsObject(
            writer,
            hasBase: true,
            baseObject: defaultParamsObject,
            baseSource: DefaultSource,
            overlays: overlayBuffer.Items,
            overlayCount: overlayBuffer.Count,
            pathPrefix: null,
            sources: null);
        writer.Flush();

        hash = ComputeFnv1a64(output.WrittenSpan);
        return true;
    }

    public static bool TryComputeExplainFull(
        JsonElement defaultParamsObject,
        JsonElement flowPatch,
        IReadOnlyDictionary<string, string>? variants,
        QosTier qosTier,
        out byte[] effectiveJsonUtf8,
        out ParamsSourceEntry[] sources,
        out ulong hash,
        DateTimeOffset? configTimestampUtc = null)
    {
        if (defaultParamsObject.ValueKind != JsonValueKind.Object)
        {
            throw new ArgumentException("defaultParamsObject must be a JSON object.", nameof(defaultParamsObject));
        }

        var overlayBuffer = new ParamsOverlayBuffer(initialCapacity: 4);
        CollectOverlays(flowPatch, variants, qosTier, configTimestampUtc, ref overlayBuffer);

        var sourcesList = new List<ParamsSourceEntry>(capacity: 32);

        var output = new ArrayBufferWriter<byte>(256);
        using var writer = new Utf8JsonWriter(
            output,
            new JsonWriterOptions
            {
                Indented = false,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            });

        WriteMergedParamsObject(
            writer,
            hasBase: true,
            baseObject: defaultParamsObject,
            baseSource: DefaultSource,
            overlays: overlayBuffer.Items,
            overlayCount: overlayBuffer.Count,
            pathPrefix: null,
            sources: sourcesList);
        writer.Flush();

        hash = ComputeFnv1a64(output.WrittenSpan);
        effectiveJsonUtf8 = output.WrittenSpan.ToArray();

        if (sourcesList.Count > 1)
        {
            sourcesList.Sort(ParamsSourceEntryByPathComparer.Instance);
        }

        sources = sourcesList.Count == 0 ? Array.Empty<ParamsSourceEntry>() : sourcesList.ToArray();
        return true;
    }

    private static ulong ComputeFnv1a64(ReadOnlySpan<byte> bytes)
    {
        var hash = OffsetBasis;

        for (var i = 0; i < bytes.Length; i++)
        {
            hash ^= bytes[i];
            hash *= Prime;
        }

        return hash;
    }

    private static void CollectOverlays(
        JsonElement flowPatch,
        IReadOnlyDictionary<string, string>? variants,
        QosTier qosTier,
        DateTimeOffset? configTimestampUtc,
        ref ParamsOverlayBuffer overlays)
    {
        if (flowPatch.ValueKind != JsonValueKind.Object)
        {
            return;
        }

        if (flowPatch.TryGetProperty("params", out var baseParamsPatch))
        {
            if (baseParamsPatch.ValueKind == JsonValueKind.Object)
            {
                overlays.Add(new ParamsOverlay(baseParamsPatch, BaseSource));
            }
            else if (baseParamsPatch.ValueKind != JsonValueKind.Undefined)
            {
                throw new FormatException("params must be a JSON object.");
            }
        }

        if (variants is not null
            && variants.Count != 0
            && flowPatch.TryGetProperty("experiments", out var experimentsPatch)
            && experimentsPatch.ValueKind == JsonValueKind.Array)
        {
            foreach (var experimentMapping in experimentsPatch.EnumerateArray())
            {
                if (experimentMapping.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                string? layer = null;
                string? variant = null;
                JsonElement patch = default;
                var hasPatch = false;

                foreach (var field in experimentMapping.EnumerateObject())
                {
                    if (field.NameEquals("layer"))
                    {
                        layer = field.Value.ValueKind == JsonValueKind.String ? field.Value.GetString() : null;
                        continue;
                    }

                    if (field.NameEquals("variant"))
                    {
                        variant = field.Value.ValueKind == JsonValueKind.String ? field.Value.GetString() : null;
                        continue;
                    }

                    if (field.NameEquals("patch"))
                    {
                        if (field.Value.ValueKind == JsonValueKind.Object)
                        {
                            patch = field.Value;
                            hasPatch = true;
                        }

                        continue;
                    }
                }

                if (string.IsNullOrEmpty(layer) || string.IsNullOrEmpty(variant) || !hasPatch)
                {
                    continue;
                }

                if (!variants.TryGetValue(layer, out var selectedVariant)
                    || !string.Equals(selectedVariant, variant, StringComparison.Ordinal))
                {
                    continue;
                }

                if (!patch.TryGetProperty("params", out var experimentParamsPatch))
                {
                    continue;
                }

                if (experimentParamsPatch.ValueKind == JsonValueKind.Object)
                {
                    overlays.Add(
                        new ParamsOverlay(
                            experimentParamsPatch,
                            new ParamsSourceDescriptor(
                                layer: "experiment",
                                experimentLayer: layer,
                                experimentVariant: variant,
                                qosTier: null)));
                }
                else if (experimentParamsPatch.ValueKind != JsonValueKind.Undefined)
                {
                    throw new FormatException("experiments[].patch.params must be a JSON object.");
                }
            }
        }

        var qosTierName = GetQosTierString(qosTier);

        if (flowPatch.TryGetProperty("qos", out var qosElement)
            && qosElement.ValueKind == JsonValueKind.Object
            && qosElement.TryGetProperty("tiers", out var tiersElement)
            && tiersElement.ValueKind == JsonValueKind.Object
            && tiersElement.TryGetProperty(qosTierName, out var tierElement)
            && tierElement.ValueKind == JsonValueKind.Object
            && tierElement.TryGetProperty("patch", out var tierPatch)
            && tierPatch.ValueKind == JsonValueKind.Object
            && tierPatch.TryGetProperty("params", out var qosParamsPatch))
        {
            if (qosParamsPatch.ValueKind == JsonValueKind.Object)
            {
                overlays.Add(
                    new ParamsOverlay(
                        qosParamsPatch,
                        new ParamsSourceDescriptor(
                            layer: "qos",
                            experimentLayer: null,
                            experimentVariant: null,
                            qosTier: qosTierName)));
            }
            else if (qosParamsPatch.ValueKind != JsonValueKind.Undefined)
            {
                throw new FormatException("qos.tiers[].patch.params must be a JSON object.");
            }
        }

        if (flowPatch.TryGetProperty("emergency", out var emergencyPatch)
            && emergencyPatch.ValueKind == JsonValueKind.Object
            && emergencyPatch.TryGetProperty("patch", out var emergencyPatchBody)
            && emergencyPatchBody.ValueKind == JsonValueKind.Object
            && emergencyPatchBody.TryGetProperty("params", out var emergencyParamsPatch))
        {
            if (emergencyParamsPatch.ValueKind == JsonValueKind.Object)
            {
                var isExpired = false;

                if (configTimestampUtc.HasValue)
                {
                    var nowUtcTicks = DateTimeOffset.UtcNow.UtcTicks;
                    isExpired = EmergencyOverlayTtlV1.IsExpired(emergencyPatch, configTimestampUtc.Value, nowUtcTicks);
                }

                if (!isExpired)
                {
                    overlays.Add(new ParamsOverlay(emergencyParamsPatch, EmergencySource));
                }
            }
            else if (emergencyParamsPatch.ValueKind != JsonValueKind.Undefined)
            {
                throw new FormatException("emergency.patch.params must be a JSON object.");
            }
        }
    }

    private static string GetQosTierString(QosTier tier)
    {
        return tier switch
        {
            QosTier.Full => "full",
            QosTier.Conserve => "conserve",
            QosTier.Emergency => "emergency",
            QosTier.Fallback => "fallback",
            _ => "full",
        };
    }

    private static void WriteMergedParamsObject(
        Utf8JsonWriter writer,
        bool hasBase,
        JsonElement baseObject,
        ParamsSourceDescriptor baseSource,
        ParamsOverlay[] overlays,
        int overlayCount,
        string? pathPrefix,
        List<ParamsSourceEntry>? sources)
    {
        writer.WriteStartObject();

        var names = new PropertyNameBuffer(initialCapacity: 16);

        if (hasBase)
        {
            foreach (var property in baseObject.EnumerateObject())
            {
                names.Add(property.Name);
            }
        }

        for (var i = 0; i < overlayCount; i++)
        {
            foreach (var property in overlays[i].Object.EnumerateObject())
            {
                names.Add(property.Name);
            }
        }

        var nameArray = names.Items;

        if (names.Count > 1)
        {
            Array.Sort(nameArray, 0, names.Count, StringComparer.Ordinal);
        }

        for (var nameIndex = 0; nameIndex < names.Count; nameIndex++)
        {
            var name = nameArray[nameIndex];

            var baseValue = default(JsonElement);
            var hasBaseValue = hasBase && baseObject.TryGetProperty(name, out baseValue);

            var lastOverlayIndex = -1;
            JsonElement lastOverlayValue = default;
            ParamsSourceDescriptor lastOverlaySource = default;

            for (var i = overlayCount - 1; i >= 0; i--)
            {
                if (!overlays[i].Object.TryGetProperty(name, out var overlayValue))
                {
                    continue;
                }

                lastOverlayIndex = i;
                lastOverlayValue = overlayValue;
                lastOverlaySource = overlays[i].Source;
                break;
            }

            if (lastOverlayIndex < 0)
            {
                if (!hasBaseValue)
                {
                    continue;
                }

                writer.WritePropertyName(name);

                if (baseValue.ValueKind == JsonValueKind.Object)
                {
                    WriteMergedParamsObject(
                        writer,
                        hasBase: true,
                        baseObject: baseValue,
                        baseSource: baseSource,
                        overlays: Array.Empty<ParamsOverlay>(),
                        overlayCount: 0,
                        pathPrefix: CombinePath(pathPrefix, name),
                        sources: sources);
                }
                else
                {
                    baseValue.WriteTo(writer);
                    sources?.Add(new ParamsSourceEntry(CombinePath(pathPrefix, name), baseSource));
                }

                continue;
            }

            if (lastOverlayValue.ValueKind != JsonValueKind.Object)
            {
                writer.WritePropertyName(name);
                lastOverlayValue.WriteTo(writer);
                sources?.Add(new ParamsSourceEntry(CombinePath(pathPrefix, name), lastOverlaySource));
                continue;
            }

            var resetIndex = -1;

            for (var i = 0; i <= lastOverlayIndex; i++)
            {
                if (!overlays[i].Object.TryGetProperty(name, out var overlayValue))
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
            var rented = ArrayPool<ParamsOverlay>.Shared.Rent(maxNestedCount);
            var nestedCount = 0;

            try
            {
                for (var i = resetIndex + 1; i <= lastOverlayIndex; i++)
                {
                    if (!overlays[i].Object.TryGetProperty(name, out var overlayValue))
                    {
                        continue;
                    }

                    if (overlayValue.ValueKind != JsonValueKind.Object)
                    {
                        continue;
                    }

                    rented[nestedCount] = new ParamsOverlay(overlayValue, overlays[i].Source);
                    nestedCount++;
                }

                writer.WritePropertyName(name);
                WriteMergedParamsObject(
                    writer,
                    hasBase: nestedHasBase,
                    baseObject: nestedBase,
                    baseSource: baseSource,
                    overlays: rented,
                    overlayCount: nestedCount,
                    pathPrefix: CombinePath(pathPrefix, name),
                    sources: sources);
            }
            finally
            {
                Array.Clear(rented, 0, nestedCount);
                ArrayPool<ParamsOverlay>.Shared.Return(rented);
            }
        }

        writer.WriteEndObject();
    }

    private static string CombinePath(string? prefix, string name)
    {
        if (string.IsNullOrEmpty(prefix))
        {
            return name;
        }

        return string.Concat(prefix, ".", name);
    }

    private readonly struct ParamsOverlay
    {
        public JsonElement Object { get; }

        public ParamsSourceDescriptor Source { get; }

        public ParamsOverlay(JsonElement @object, ParamsSourceDescriptor source)
        {
            Object = @object;
            Source = source;
        }
    }

    private struct ParamsOverlayBuffer
    {
        private ParamsOverlay[]? _items;
        private int _count;

        public int Count => _count;

        public ParamsOverlay[] Items => _items ?? Array.Empty<ParamsOverlay>();

        public ParamsOverlayBuffer(int initialCapacity)
        {
            _items = initialCapacity <= 0 ? null : new ParamsOverlay[initialCapacity];
            _count = 0;
        }

        public void Add(ParamsOverlay item)
        {
            if (_items is null)
            {
                _items = new ParamsOverlay[4];
            }
            else if ((uint)_count >= (uint)_items.Length)
            {
                var newItems = new ParamsOverlay[_items.Length * 2];
                Array.Copy(_items, 0, newItems, 0, _items.Length);
                _items = newItems;
            }

            _items[_count] = item;
            _count++;
        }
    }

    private sealed class ParamsSourceEntryByPathComparer : IComparer<ParamsSourceEntry>
    {
        public static readonly ParamsSourceEntryByPathComparer Instance = new();

        public int Compare(ParamsSourceEntry x, ParamsSourceEntry y)
        {
            return string.CompareOrdinal(x.Path, y.Path);
        }
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
