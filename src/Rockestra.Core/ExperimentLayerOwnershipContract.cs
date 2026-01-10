namespace Rockestra.Core;

public sealed class ExperimentLayerOwnershipContract
{
    private readonly Dictionary<string, LayerOwnership> _layers;

    internal ExperimentLayerOwnershipContract(Dictionary<string, LayerOwnership> layers)
    {
        _layers = layers ?? throw new ArgumentNullException(nameof(layers));
    }

    internal bool TryGetLayerOwnership(string layer, out LayerOwnership ownership)
    {
        if (string.IsNullOrEmpty(layer))
        {
            throw new ArgumentException("Layer must be non-empty.", nameof(layer));
        }

        return _layers.TryGetValue(layer, out ownership);
    }

    internal readonly struct LayerOwnership
    {
        private readonly string[] _ownedParamPathPrefixes;
        private readonly string[] _ownedModuleIds;

        public LayerOwnership(string[] ownedParamPathPrefixes, string[] ownedModuleIds)
        {
            _ownedParamPathPrefixes = ownedParamPathPrefixes ?? throw new ArgumentNullException(nameof(ownedParamPathPrefixes));
            _ownedModuleIds = ownedModuleIds ?? throw new ArgumentNullException(nameof(ownedModuleIds));
        }

        public bool OwnsParamPath(string path)
        {
            if (string.IsNullOrEmpty(path))
            {
                return false;
            }

            var prefixes = _ownedParamPathPrefixes;

            for (var i = 0; i < prefixes.Length; i++)
            {
                var prefix = prefixes[i];

                if (string.IsNullOrEmpty(prefix))
                {
                    continue;
                }

                if (path.Length == prefix.Length)
                {
                    if (string.Equals(path, prefix, StringComparison.Ordinal))
                    {
                        return true;
                    }

                    continue;
                }

                if (path.Length > prefix.Length
                    && path[prefix.Length] == '.'
                    && path.StartsWith(prefix, StringComparison.Ordinal))
                {
                    return true;
                }
            }

            return false;
        }

        public bool OwnsModuleId(string moduleId)
        {
            if (string.IsNullOrEmpty(moduleId))
            {
                return false;
            }

            var ids = _ownedModuleIds;

            for (var i = 0; i < ids.Length; i++)
            {
                if (string.Equals(ids[i], moduleId, StringComparison.Ordinal))
                {
                    return true;
                }
            }

            return false;
        }
    }
}

public sealed class ExperimentLayerOwnershipContractBuilder
{
    private static readonly Dictionary<string, ExperimentLayerOwnershipContract.LayerOwnership> EmptyLayers = new();

    private Dictionary<string, ExperimentLayerOwnershipContract.LayerOwnership>? _layers;

    public ExperimentLayerOwnershipContractBuilder AddLayer(
        string layer,
        string[] ownedParamPathPrefixes,
        string[] ownedModuleIds)
    {
        if (string.IsNullOrEmpty(layer))
        {
            throw new ArgumentException("Layer must be non-empty.", nameof(layer));
        }

        if (ownedParamPathPrefixes is null)
        {
            throw new ArgumentNullException(nameof(ownedParamPathPrefixes));
        }

        if (ownedModuleIds is null)
        {
            throw new ArgumentNullException(nameof(ownedModuleIds));
        }

        _layers ??= new Dictionary<string, ExperimentLayerOwnershipContract.LayerOwnership>();

        if (!_layers.TryAdd(layer, new ExperimentLayerOwnershipContract.LayerOwnership(ownedParamPathPrefixes, ownedModuleIds)))
        {
            throw new ArgumentException($"Layer '{layer}' is already registered.", nameof(layer));
        }

        return this;
    }

    public ExperimentLayerOwnershipContract Build()
    {
        if (_layers is null || _layers.Count == 0)
        {
            return new ExperimentLayerOwnershipContract(EmptyLayers);
        }

        return new ExperimentLayerOwnershipContract(new Dictionary<string, ExperimentLayerOwnershipContract.LayerOwnership>(_layers));
    }
}

