using System.Buffers;
using System.Text.Json;

namespace ROrchestrator.Core;

public sealed class FileLkgSnapshotStore : ILkgSnapshotStore
{
    private const int DefaultBufferSize = 4096;

    private readonly string _path;
    private readonly string _tempPath;

    public FileLkgSnapshotStore(string path)
    {
        if (string.IsNullOrEmpty(path))
        {
            throw new ArgumentException("Path must be non-empty.", nameof(path));
        }

        _path = path;
        _tempPath = string.Concat(path, ".tmp");
    }

    public LkgSnapshotLoadResultKind TryLoad(out ConfigSnapshot snapshot)
    {
        snapshot = default;

        try
        {
            if (!File.Exists(_path))
            {
                return LkgSnapshotLoadResultKind.NotFound;
            }

            var bytes = File.ReadAllBytes(_path);
            using var document = JsonDocument.Parse(bytes);

            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                return LkgSnapshotLoadResultKind.Corrupt;
            }

            if (!root.TryGetProperty("config_version", out var configVersionElement)
                || configVersionElement.ValueKind != JsonValueKind.Number
                || !configVersionElement.TryGetUInt64(out var configVersion))
            {
                return LkgSnapshotLoadResultKind.Corrupt;
            }

            if (!root.TryGetProperty("patch_json", out var patchJsonElement)
                || patchJsonElement.ValueKind != JsonValueKind.String)
            {
                return LkgSnapshotLoadResultKind.Corrupt;
            }

            var patchJson = patchJsonElement.GetString();
            if (patchJson is null)
            {
                return LkgSnapshotLoadResultKind.Corrupt;
            }

            snapshot = new ConfigSnapshot(configVersion, patchJson);
            return LkgSnapshotLoadResultKind.Loaded;
        }
        catch (JsonException)
        {
            return LkgSnapshotLoadResultKind.Corrupt;
        }
        catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
        {
            return LkgSnapshotLoadResultKind.Error;
        }
    }

    public bool TryStore(in ConfigSnapshot snapshot)
    {
        try
        {
            var directory = Path.GetDirectoryName(_path);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var output = new ArrayBufferWriter<byte>(512);
            using (var writer = new Utf8JsonWriter(
                       output,
                       new JsonWriterOptions
                       {
                           Indented = false,
                           Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
                       }))
            {
                writer.WriteStartObject();
                writer.WriteNumber("config_version", snapshot.ConfigVersion);
                writer.WriteString("patch_json", snapshot.PatchJson);
                writer.WriteEndObject();
                writer.Flush();
            }

            using (var stream = new FileStream(
                       _tempPath,
                       FileMode.Create,
                       FileAccess.Write,
                       FileShare.None,
                       DefaultBufferSize,
                       options: FileOptions.WriteThrough))
            {
                stream.Write(output.WrittenSpan);
                stream.Flush(flushToDisk: true);
            }

            if (File.Exists(_path))
            {
                File.Replace(_tempPath, _path, destinationBackupFileName: null, ignoreMetadataErrors: true);
            }
            else
            {
                File.Move(_tempPath, _path, overwrite: true);
            }

            return true;
        }
        catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
        {
            try
            {
                if (File.Exists(_tempPath))
                {
                    File.Delete(_tempPath);
                }
            }
            catch
            {
            }

            return false;
        }
    }
}

