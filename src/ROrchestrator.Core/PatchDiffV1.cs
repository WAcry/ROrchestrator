using System.Globalization;
using System.Text.Json;

namespace ROrchestrator.Core;

public static class PatchDiffV1
{
    private const string SupportedSchemaVersion = "v1";

    public static PatchModuleDiffReport DiffModules(string oldPatchJson, string newPatchJson)
    {
        if (oldPatchJson is null)
        {
            throw new ArgumentNullException(nameof(oldPatchJson));
        }

        if (newPatchJson is null)
        {
            throw new ArgumentNullException(nameof(newPatchJson));
        }

        JsonDocument oldDocument;

        try
        {
            oldDocument = JsonDocument.Parse(oldPatchJson);
        }
        catch (JsonException ex)
        {
            throw new FormatException("oldPatchJson is not a valid JSON document.", ex);
        }

        using (oldDocument)
        {
            EnsureSupportedSchemaVersion(oldDocument.RootElement);

            JsonDocument newDocument;

            try
            {
                newDocument = JsonDocument.Parse(newPatchJson);
            }
            catch (JsonException ex)
            {
                throw new FormatException("newPatchJson is not a valid JSON document.", ex);
            }

            using (newDocument)
            {
                EnsureSupportedSchemaVersion(newDocument.RootElement);

                Dictionary<ModuleKey, ModuleInfo>? oldModuleMap = null;
                Dictionary<ModuleKey, ModuleInfo>? newModuleMap = null;

                CollectModules(oldDocument.RootElement, ref oldModuleMap);
                CollectModules(newDocument.RootElement, ref newModuleMap);

                if (oldModuleMap is null && newModuleMap is null)
                {
                    return PatchModuleDiffReport.Empty;
                }

                var buffer = new ModuleDiffBuffer();

                if (oldModuleMap is not null)
                {
                    foreach (var pair in oldModuleMap)
                    {
                        var key = pair.Key;
                        var oldModule = pair.Value;

                        if (newModuleMap is null || !newModuleMap.TryGetValue(key, out var newModule))
                        {
                            buffer.Add(
                                PatchModuleDiff.CreateRemoved(
                                    key.FlowName,
                                    key.StageName,
                                    key.ModuleId,
                                    BuildModulePath(key.FlowName, key.StageName, oldModule.Index)));
                            continue;
                        }

                        if (!string.Equals(oldModule.Use, newModule.Use, StringComparison.Ordinal))
                        {
                            buffer.Add(
                                PatchModuleDiff.CreateUseChanged(
                                    key.FlowName,
                                    key.StageName,
                                    key.ModuleId,
                                    string.Concat(BuildModulePath(key.FlowName, key.StageName, newModule.Index), ".use")));
                        }

                        if (!JsonElementDeepEquals(oldModule.With, newModule.With))
                        {
                            buffer.Add(
                                PatchModuleDiff.CreateWithChanged(
                                    key.FlowName,
                                    key.StageName,
                                    key.ModuleId,
                                    string.Concat(BuildModulePath(key.FlowName, key.StageName, newModule.Index), ".with")));
                        }
                    }
                }

                if (newModuleMap is not null)
                {
                    foreach (var pair in newModuleMap)
                    {
                        var key = pair.Key;

                        if (oldModuleMap is not null && oldModuleMap.ContainsKey(key))
                        {
                            continue;
                        }

                        var newModule = pair.Value;

                        buffer.Add(
                            PatchModuleDiff.CreateAdded(
                                key.FlowName,
                                key.StageName,
                                key.ModuleId,
                                BuildModulePath(key.FlowName, key.StageName, newModule.Index)));
                    }
                }

                var diffs = buffer.ToArray();

                if (diffs.Length == 0)
                {
                    return PatchModuleDiffReport.Empty;
                }

                if (diffs.Length > 1)
                {
                    Array.Sort(diffs, PatchModuleDiffComparer.Instance);
                }

                return new PatchModuleDiffReport(diffs);
            }
        }
    }

    private static void EnsureSupportedSchemaVersion(JsonElement root)
    {
        if (root.ValueKind != JsonValueKind.Object)
        {
            throw new NotSupportedException("schemaVersion is missing or unsupported. Supported: v1.");
        }

        if (!root.TryGetProperty("schemaVersion", out var schemaVersionElement)
            || schemaVersionElement.ValueKind != JsonValueKind.String
            || !schemaVersionElement.ValueEquals(SupportedSchemaVersion))
        {
            throw new NotSupportedException("schemaVersion is missing or unsupported. Supported: v1.");
        }
    }

    private static void CollectModules(JsonElement root, ref Dictionary<ModuleKey, ModuleInfo>? moduleMap)
    {
        if (!root.TryGetProperty("flows", out var flowsElement))
        {
            return;
        }

        if (flowsElement.ValueKind != JsonValueKind.Object)
        {
            throw new FormatException("flows must be an object.");
        }

        foreach (var flowProperty in flowsElement.EnumerateObject())
        {
            var flowName = flowProperty.Name;
            var flowPatch = flowProperty.Value;

            if (flowPatch.ValueKind != JsonValueKind.Object)
            {
                throw new FormatException(string.Concat("Flow patch must be an object. Flow: ", flowName));
            }

            if (!flowPatch.TryGetProperty("stages", out var stagesElement) || stagesElement.ValueKind == JsonValueKind.Null)
            {
                continue;
            }

            if (stagesElement.ValueKind != JsonValueKind.Object)
            {
                throw new FormatException(string.Concat("stages must be an object. Flow: ", flowName));
            }

            foreach (var stageProperty in stagesElement.EnumerateObject())
            {
                var stageName = stageProperty.Name;
                var stagePatch = stageProperty.Value;

                if (stagePatch.ValueKind != JsonValueKind.Object)
                {
                    throw new FormatException(
                        string.Concat("Stage patch must be an object. Flow: ", flowName, ", Stage: ", stageName));
                }

                if (!stagePatch.TryGetProperty("modules", out var modulesElement) || modulesElement.ValueKind == JsonValueKind.Null)
                {
                    continue;
                }

                if (modulesElement.ValueKind != JsonValueKind.Array)
                {
                    throw new FormatException(
                        string.Concat("modules must be an array. Flow: ", flowName, ", Stage: ", stageName));
                }

                var index = 0;

                foreach (var moduleElement in modulesElement.EnumerateArray())
                {
                    if (moduleElement.ValueKind != JsonValueKind.Object)
                    {
                        throw new FormatException(
                            string.Concat("modules must be an array of objects. Flow: ", flowName, ", Stage: ", stageName));
                    }

                    if (!moduleElement.TryGetProperty("id", out var idElement) || idElement.ValueKind != JsonValueKind.String)
                    {
                        throw new FormatException(
                            string.Concat("modules[].id is required and must be a string. Flow: ", flowName, ", Stage: ", stageName));
                    }

                    var moduleId = idElement.GetString();

                    if (string.IsNullOrEmpty(moduleId))
                    {
                        throw new FormatException(
                            string.Concat("modules[].id is required and must be non-empty. Flow: ", flowName, ", Stage: ", stageName));
                    }

                    if (!moduleElement.TryGetProperty("use", out var useElement) || useElement.ValueKind != JsonValueKind.String)
                    {
                        throw new FormatException(
                            string.Concat("modules[].use is required and must be a string. Flow: ", flowName, ", Stage: ", stageName, ", ModuleId: ", moduleId));
                    }

                    var moduleUse = useElement.GetString();

                    if (string.IsNullOrEmpty(moduleUse))
                    {
                        throw new FormatException(
                            string.Concat("modules[].use is required and must be non-empty. Flow: ", flowName, ", Stage: ", stageName, ", ModuleId: ", moduleId));
                    }

                    if (!moduleElement.TryGetProperty("with", out var withElement) || withElement.ValueKind == JsonValueKind.Null)
                    {
                        throw new FormatException(
                            string.Concat("modules[].with is required. Flow: ", flowName, ", Stage: ", stageName, ", ModuleId: ", moduleId));
                    }

                    moduleMap ??= new Dictionary<ModuleKey, ModuleInfo>(4);

                    var key = new ModuleKey(flowName, stageName, moduleId);

                    if (!moduleMap.TryAdd(key, new ModuleInfo(moduleUse, withElement, index)))
                    {
                        throw new FormatException(
                            string.Concat("Duplicate module id within stage. Flow: ", flowName, ", Stage: ", stageName, ", ModuleId: ", moduleId));
                    }

                    index++;
                }
            }
        }
    }

    private static string BuildModulePath(string flowName, string stageName, int index)
    {
        return string.Concat(
            "$.flows.",
            flowName,
            ".stages.",
            stageName,
            ".modules[",
            index.ToString(CultureInfo.InvariantCulture),
            "]");
    }

    private static bool JsonElementDeepEquals(JsonElement left, JsonElement right)
    {
        var leftKind = left.ValueKind;
        var rightKind = right.ValueKind;

        if (leftKind != rightKind)
        {
            return false;
        }

        switch (leftKind)
        {
            case JsonValueKind.Object:
            {
                var leftCount = 0;

                foreach (var leftProperty in left.EnumerateObject())
                {
                    leftCount++;

                    if (!right.TryGetProperty(leftProperty.Name, out var rightValue))
                    {
                        return false;
                    }

                    if (!JsonElementDeepEquals(leftProperty.Value, rightValue))
                    {
                        return false;
                    }
                }

                var rightCount = 0;

                foreach (var _ in right.EnumerateObject())
                {
                    rightCount++;
                }

                return leftCount == rightCount;
            }

            case JsonValueKind.Array:
            {
                if (left.GetArrayLength() != right.GetArrayLength())
                {
                    return false;
                }

                var leftEnumerator = left.EnumerateArray();
                var rightEnumerator = right.EnumerateArray();

                while (leftEnumerator.MoveNext())
                {
                    if (!rightEnumerator.MoveNext())
                    {
                        return false;
                    }

                    if (!JsonElementDeepEquals(leftEnumerator.Current, rightEnumerator.Current))
                    {
                        return false;
                    }
                }

                return !rightEnumerator.MoveNext();
            }

            case JsonValueKind.String:
                return string.Equals(left.GetString(), right.GetString(), StringComparison.Ordinal);

            case JsonValueKind.Number:
                return string.Equals(left.GetRawText(), right.GetRawText(), StringComparison.Ordinal);

            case JsonValueKind.True:
            case JsonValueKind.False:
                return left.GetBoolean() == right.GetBoolean();

            case JsonValueKind.Null:
            case JsonValueKind.Undefined:
                return true;

            default:
                return string.Equals(left.GetRawText(), right.GetRawText(), StringComparison.Ordinal);
        }
    }

    private readonly struct ModuleKey : IEquatable<ModuleKey>
    {
        public readonly string FlowName;
        public readonly string StageName;
        public readonly string ModuleId;

        public ModuleKey(string flowName, string stageName, string moduleId)
        {
            FlowName = flowName;
            StageName = stageName;
            ModuleId = moduleId;
        }

        public bool Equals(ModuleKey other)
        {
            return string.Equals(FlowName, other.FlowName, StringComparison.Ordinal)
                && string.Equals(StageName, other.StageName, StringComparison.Ordinal)
                && string.Equals(ModuleId, other.ModuleId, StringComparison.Ordinal);
        }

        public override bool Equals(object? obj)
        {
            return obj is ModuleKey other && Equals(other);
        }

        public override int GetHashCode()
        {
            var hash = new HashCode();
            hash.Add(FlowName);
            hash.Add(StageName);
            hash.Add(ModuleId);
            return hash.ToHashCode();
        }
    }

    private readonly struct ModuleInfo
    {
        public readonly string Use;
        public readonly JsonElement With;
        public readonly int Index;

        public ModuleInfo(string use, JsonElement with, int index)
        {
            Use = use;
            With = with;
            Index = index;
        }
    }

    private struct ModuleDiffBuffer
    {
        private PatchModuleDiff[]? _items;
        private int _count;

        public void Add(PatchModuleDiff item)
        {
            var items = _items;

            if (items is null)
            {
                items = new PatchModuleDiff[4];
                _items = items;
            }
            else if ((uint)_count >= (uint)items.Length)
            {
                var newItems = new PatchModuleDiff[items.Length * 2];
                Array.Copy(items, 0, newItems, 0, items.Length);
                items = newItems;
                _items = items;
            }

            items[_count] = item;
            _count++;
        }

        public PatchModuleDiff[] ToArray()
        {
            if (_count == 0)
            {
                return Array.Empty<PatchModuleDiff>();
            }

            var items = _items!;

            if (_count == items.Length)
            {
                return items;
            }

            var trimmed = new PatchModuleDiff[_count];
            Array.Copy(items, 0, trimmed, 0, _count);
            return trimmed;
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

            return ((int)x.Kind).CompareTo((int)y.Kind);
        }
    }
}

public sealed class PatchModuleDiffReport
{
    public static PatchModuleDiffReport Empty { get; } = new(Array.Empty<PatchModuleDiff>());

    private readonly PatchModuleDiff[] _diffs;

    public IReadOnlyList<PatchModuleDiff> Diffs => _diffs;

    internal PatchModuleDiffReport(PatchModuleDiff[] diffs)
    {
        _diffs = diffs ?? throw new ArgumentNullException(nameof(diffs));
    }
}

public enum PatchModuleDiffKind
{
    Added = 1,
    Removed = 2,
    UseChanged = 3,
    WithChanged = 4,
}

public readonly struct PatchModuleDiff
{
    public PatchModuleDiffKind Kind { get; }

    public string FlowName { get; }

    public string StageName { get; }

    public string ModuleId { get; }

    public string Path { get; }

    private PatchModuleDiff(
        PatchModuleDiffKind kind,
        string flowName,
        string stageName,
        string moduleId,
        string path)
    {
        Kind = kind;
        FlowName = flowName;
        StageName = stageName;
        ModuleId = moduleId;
        Path = path;
    }

    internal static PatchModuleDiff CreateAdded(string flowName, string stageName, string moduleId, string path)
    {
        return new PatchModuleDiff(PatchModuleDiffKind.Added, flowName, stageName, moduleId, path);
    }

    internal static PatchModuleDiff CreateRemoved(string flowName, string stageName, string moduleId, string path)
    {
        return new PatchModuleDiff(PatchModuleDiffKind.Removed, flowName, stageName, moduleId, path);
    }

    internal static PatchModuleDiff CreateUseChanged(string flowName, string stageName, string moduleId, string path)
    {
        return new PatchModuleDiff(PatchModuleDiffKind.UseChanged, flowName, stageName, moduleId, path);
    }

    internal static PatchModuleDiff CreateWithChanged(string flowName, string stageName, string moduleId, string path)
    {
        return new PatchModuleDiff(PatchModuleDiffKind.WithChanged, flowName, stageName, moduleId, path);
    }
}
