namespace Rockestra.Core;

public readonly struct ParamsExplain
{
    public string ParamsHash { get; }

    public byte[]? EffectiveJsonUtf8 { get; }

    public ParamsSourceEntry[]? Sources { get; }

    internal ParamsExplain(string paramsHash, byte[]? effectiveJsonUtf8, ParamsSourceEntry[]? sources)
    {
        ParamsHash = paramsHash;
        EffectiveJsonUtf8 = effectiveJsonUtf8;
        Sources = sources;
    }
}

public readonly struct ParamsSourceEntry
{
    public string Path { get; }

    public string Layer { get; }

    public string? ExperimentLayer { get; }

    public string? ExperimentVariant { get; }

    public string? QosTier { get; }

    internal ParamsSourceEntry(string path, ParamsSourceDescriptor source)
    {
        Path = path;
        Layer = source.Layer;
        ExperimentLayer = source.ExperimentLayer;
        ExperimentVariant = source.ExperimentVariant;
        QosTier = source.QosTier;
    }

    internal ParamsSourceEntry(string path, string layer, string? experimentLayer, string? experimentVariant, string? qosTier)
    {
        Path = path;
        Layer = layer;
        ExperimentLayer = experimentLayer;
        ExperimentVariant = experimentVariant;
        QosTier = qosTier;
    }
}

internal readonly struct ParamsSourceDescriptor
{
    public string Layer { get; }

    public string? ExperimentLayer { get; }

    public string? ExperimentVariant { get; }

    public string? QosTier { get; }

    public ParamsSourceDescriptor(string layer, string? experimentLayer, string? experimentVariant, string? qosTier)
    {
        Layer = layer;
        ExperimentLayer = experimentLayer;
        ExperimentVariant = experimentVariant;
        QosTier = qosTier;
    }
}

