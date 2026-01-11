namespace Rockestra.Core;

public readonly struct ConfigSnapshot
{
    public ulong ConfigVersion { get; }

    public string PatchJson { get; }

    public ConfigSnapshotMeta Meta { get; }

    public ConfigSnapshot(ulong configVersion, string patchJson, ConfigSnapshotMeta meta)
    {
        if (patchJson is null)
        {
            throw new ArgumentNullException(nameof(patchJson));
        }

        ConfigVersion = configVersion;
        PatchJson = patchJson;
        Meta = meta;
    }
}

public readonly struct ConfigSnapshotMeta
{
    private readonly string[] _overlays;
    private readonly bool _hasLkgFallbackEvidence;
    private readonly ConfigSnapshotLkgFallbackEvidence _lkgFallbackEvidence;

    public string Source { get; }

    public DateTimeOffset TimestampUtc { get; }

    public ReadOnlySpan<string> Overlays => _overlays;

    public ConfigSnapshotMeta(string source, DateTimeOffset timestampUtc, string[]? overlays = null)
        : this(source, timestampUtc, overlays, hasLkgFallbackEvidence: false, lkgFallbackEvidence: default)
    {
    }

    internal ConfigSnapshotMeta(
        string source,
        DateTimeOffset timestampUtc,
        string[]? overlays,
        bool hasLkgFallbackEvidence,
        ConfigSnapshotLkgFallbackEvidence lkgFallbackEvidence)
    {
        if (string.IsNullOrWhiteSpace(source))
        {
            throw new ArgumentException("Source must be non-empty.", nameof(source));
        }

        if (timestampUtc == default)
        {
            throw new ArgumentException("TimestampUtc must be non-default.", nameof(timestampUtc));
        }

        Source = source;
        TimestampUtc = timestampUtc.ToUniversalTime();

        if (overlays is null || overlays.Length == 0)
        {
            _overlays = Array.Empty<string>();
        }
        else
        {
            for (var i = 0; i < overlays.Length; i++)
            {
                if (string.IsNullOrEmpty(overlays[i]))
                {
                    throw new ArgumentException("Overlay values must be non-empty.", nameof(overlays));
                }
            }

            _overlays = overlays;
        }

        _hasLkgFallbackEvidence = hasLkgFallbackEvidence;
        _lkgFallbackEvidence = lkgFallbackEvidence;
    }

    public bool TryGetLkgFallbackEvidence(out ConfigSnapshotLkgFallbackEvidence evidence)
    {
        if (_hasLkgFallbackEvidence)
        {
            evidence = _lkgFallbackEvidence;
            return true;
        }

        evidence = default;
        return false;
    }

    internal string[] OverlaysArray => _overlays;

    internal ConfigSnapshotMeta WithSourceAndLkgFallbackEvidence(string source, in ConfigSnapshotLkgFallbackEvidence evidence)
    {
        return new ConfigSnapshotMeta(source, TimestampUtc, _overlays, hasLkgFallbackEvidence: true, evidence);
    }
}

public readonly struct ConfigSnapshotLkgFallbackEvidence
{
    public bool Fallback { get; }

    public bool HasCandidateConfigVersion { get; }

    public ulong CandidateConfigVersion { get; }

    public ulong LastGoodConfigVersion { get; }

    public ConfigSnapshotLkgFallbackEvidence(
        bool fallback,
        ulong lastGoodConfigVersion,
        bool hasCandidateConfigVersion,
        ulong candidateConfigVersion)
    {
        Fallback = fallback;
        LastGoodConfigVersion = lastGoodConfigVersion;
        HasCandidateConfigVersion = hasCandidateConfigVersion;
        CandidateConfigVersion = candidateConfigVersion;
    }
}

public interface IConfigProvider
{
    ValueTask<ConfigSnapshot> GetSnapshotAsync(FlowContext context);
}


