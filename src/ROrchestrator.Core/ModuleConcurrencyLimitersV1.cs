using System.Text.Json;

namespace ROrchestrator.Core;

internal sealed class ModuleConcurrencyLimitersV1
{
    private readonly Lock _gate = new();
    private Dictionary<string, Limiter>? _limiters;
    private ulong _configuredConfigVersion;
    private string? _configuredPatchJson;

    public void EnsureConfigured(string patchJson, ulong configVersion)
    {
        if (patchJson is null)
        {
            throw new ArgumentNullException(nameof(patchJson));
        }

        var currentLimiters = Volatile.Read(ref _limiters);
        var currentVersion = Volatile.Read(ref _configuredConfigVersion);
        var currentPatchJson = Volatile.Read(ref _configuredPatchJson);

        if (configVersion == currentVersion && ReferenceEquals(patchJson, currentPatchJson))
        {
            return;
        }

        lock (_gate)
        {
            currentLimiters = _limiters;
            currentVersion = _configuredConfigVersion;
            currentPatchJson = _configuredPatchJson;

            if (configVersion == currentVersion && ReferenceEquals(patchJson, currentPatchJson))
            {
                return;
            }

            Dictionary<string, Limiter>? parsed;

            if (patchJson.Length == 0)
            {
                parsed = null;
            }
            else
            {
                parsed = Parse(patchJson, currentLimiters);
            }

            _configuredConfigVersion = configVersion;
            _configuredPatchJson = patchJson;
            Volatile.Write(ref _limiters, parsed);
        }
    }

    public bool TryEnter(string key, out Lease lease)
    {
        var limiters = Volatile.Read(ref _limiters);

        if (limiters is null || string.IsNullOrEmpty(key) || !limiters.TryGetValue(key, out var limiter))
        {
            lease = default;
            return true;
        }

        if (!limiter.TryAcquire())
        {
            lease = default;
            return false;
        }

        lease = new Lease(limiter);
        return true;
    }

    private static Dictionary<string, Limiter>? Parse(string patchJson, Dictionary<string, Limiter>? previous)
    {
        JsonDocument document;

        try
        {
            document = JsonDocument.Parse(patchJson);
        }
        catch (JsonException ex)
        {
            throw new FormatException("patchJson is not a valid JSON document.", ex);
        }

        using (document)
        {
            var root = document.RootElement;

            if (!root.TryGetProperty("schemaVersion", out var schemaVersion)
                || schemaVersion.ValueKind != JsonValueKind.String
                || !schemaVersion.ValueEquals("v1"))
            {
                throw new FormatException("patchJson schemaVersion is missing or unsupported.");
            }

            if (!root.TryGetProperty("limits", out var limitsElement) || limitsElement.ValueKind != JsonValueKind.Object)
            {
                return null;
            }

            if (!limitsElement.TryGetProperty("moduleConcurrency", out var moduleConcurrency) || moduleConcurrency.ValueKind != JsonValueKind.Object)
            {
                return null;
            }

            if (!moduleConcurrency.TryGetProperty("maxInFlight", out var maxInFlightElement) || maxInFlightElement.ValueKind != JsonValueKind.Object)
            {
                return null;
            }

            var count = 0;

            foreach (var _ in maxInFlightElement.EnumerateObject())
            {
                count++;
            }

            if (count == 0)
            {
                return null;
            }

            var limiters = new Dictionary<string, Limiter>(capacity: count);

            foreach (var entry in maxInFlightElement.EnumerateObject())
            {
                var key = entry.Name;

                if (string.IsNullOrEmpty(key))
                {
                    continue;
                }

                if (entry.Value.ValueKind != JsonValueKind.Number || !entry.Value.TryGetInt32(out var max) || max <= 0)
                {
                    continue;
                }

                Limiter limiter;

                if (previous is not null && previous.TryGetValue(key, out var existing))
                {
                    existing.SetMax(max);
                    limiter = existing;
                }
                else
                {
                    limiter = new Limiter(max);
                }

                limiters.Add(key, limiter);
            }

            return limiters.Count == 0 ? null : limiters;
        }
    }

    internal readonly struct Lease : IDisposable
    {
        private readonly Limiter? _limiter;

        public Lease(Limiter limiter)
        {
            _limiter = limiter;
        }

        public void Dispose()
        {
            _limiter?.Release();
        }
    }

    internal sealed class Limiter
    {
        private int _inFlight;
        private int _max;

        public Limiter(int max)
        {
            if (max <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(max), max, "Max must be positive.");
            }

            _inFlight = 0;
            _max = max;
        }

        public void SetMax(int max)
        {
            if (max <= 0)
            {
                max = 0;
            }

            Volatile.Write(ref _max, max);
        }

        public bool TryAcquire()
        {
            while (true)
            {
                var max = Volatile.Read(ref _max);
                if (max <= 0)
                {
                    return true;
                }

                var current = Volatile.Read(ref _inFlight);

                if (current >= max)
                {
                    return false;
                }

                if (Interlocked.CompareExchange(ref _inFlight, current + 1, current) == current)
                {
                    return true;
                }
            }
        }

        public void Release()
        {
            Interlocked.Decrement(ref _inFlight);
        }
    }
}
