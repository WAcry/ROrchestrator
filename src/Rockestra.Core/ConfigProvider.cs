namespace Rockestra.Core;

public readonly struct ConfigSnapshot
{
    public ulong ConfigVersion { get; }

    public string PatchJson { get; }

    public ConfigSnapshot(ulong configVersion, string patchJson)
    {
        if (patchJson is null)
        {
            throw new ArgumentNullException(nameof(patchJson));
        }

        ConfigVersion = configVersion;
        PatchJson = patchJson;
    }
}

public interface IConfigProvider
{
    ValueTask<ConfigSnapshot> GetSnapshotAsync(FlowContext context);
}


