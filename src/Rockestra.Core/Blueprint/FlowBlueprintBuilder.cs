using System.Collections.Frozen;
using Rockestra.Core;

namespace Rockestra.Core.Blueprint;

public sealed class FlowBlueprintBuilder<TReq, TResp>
{
    private readonly string _name;
    private readonly List<BlueprintNode> _nodes;
    private readonly HashSet<string> _nodeNames;
    private readonly HashSet<string> _stageNames;
    private readonly List<StageContractEntry> _stageContracts;

    internal FlowBlueprintBuilder(string name)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentException("Flow name must be non-empty.", nameof(name));
        }

        _name = name;
        _nodes = new List<BlueprintNode>();
        _nodeNames = new HashSet<string>();
        _stageNames = new HashSet<string>();
        _stageContracts = new List<StageContractEntry>();
    }

    public string Name => _name;

    public FlowBlueprintBuilder<TReq, TResp> Stage(string name, Action<StageBuilder> configure)
    {
        return StageCore(name, configureContract: null, configure);
    }

    public FlowBlueprintBuilder<TReq, TResp> Stage(
        string name,
        Action<StageContractBuilder> configureContract,
        Action<StageBuilder> configure)
    {
        if (configureContract is null)
        {
            throw new ArgumentNullException(nameof(configureContract));
        }

        return StageCore(name, configureContract, configure);
    }

    private FlowBlueprintBuilder<TReq, TResp> StageCore(
        string name,
        Action<StageContractBuilder>? configureContract,
        Action<StageBuilder> configure)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentException("Stage name must be non-empty.", nameof(name));
        }

        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        if (!_stageNames.Add(name))
        {
            throw new ArgumentException($"Flow '{_name}' already contains stage '{name}'.", nameof(name));
        }

        var stageIndex = _stageContracts.Count;
        _stageContracts.Add(new StageContractEntry(name, StageContract.Default));

        var nodesBefore = _nodes.Count;

        try
        {
            if (configureContract is not null)
            {
                var contractBuilder = new StageContractBuilder();
                configureContract(contractBuilder);
                SetStageContract(stageIndex, contractBuilder.Build());
            }

            configure(new StageBuilder(this, name, stageIndex));

            if (_nodes.Count == nodesBefore)
            {
                throw new InvalidOperationException($"Flow '{_name}' stage '{name}' must contain at least one node.");
            }
        }
        catch
        {
            for (var i = _nodes.Count - 1; i >= nodesBefore; i--)
            {
                _nodeNames.Remove(_nodes[i].Name);
                _nodes.RemoveAt(i);
            }

            if ((uint)stageIndex < (uint)_stageContracts.Count)
            {
                _stageContracts.RemoveAt(stageIndex);
            }

            _stageNames.Remove(name);
            throw;
        }

        return this;
    }

    public FlowBlueprintBuilder<TReq, TResp> Step(string name, string moduleType)
    {
        AddStep(name, stageName: null, moduleType);
        return this;
    }

    public FlowBlueprintBuilder<TReq, TResp> Join<TOut>(
        string name,
        Func<FlowContext, ValueTask<Outcome<TOut>>> join)
    {
        AddJoin(name, stageName: null, join);
        return this;
    }

    public FlowBlueprintBuilder<TReq, TResp> Join<TOut>(
        string name,
        Func<FlowContext, Outcome<TOut>> join)
    {
        if (join is null)
        {
            throw new ArgumentNullException(nameof(join));
        }

        AddJoin(
            name,
            stageName: null,
            ctx => new ValueTask<Outcome<TOut>>(join(ctx)));
        return this;
    }

    public FlowBlueprint<TReq, TResp> Build()
    {
        if (_nodes.Count == 0)
        {
            throw new InvalidOperationException($"Flow '{_name}' must contain at least one node.");
        }

        var nodes = _nodes.ToArray();
        var nameToIndex = new Dictionary<string, int>(nodes.Length);

        for (var i = 0; i < nodes.Length; i++)
        {
            nameToIndex.Add(nodes[i].Name, i);
        }

        var frozenNameToIndex = nameToIndex.ToFrozenDictionary();
        var stageContracts = _stageContracts.Count == 0 ? Array.Empty<StageContractEntry>() : _stageContracts.ToArray();
        return new FlowBlueprint<TReq, TResp>(_name, nodes, frozenNameToIndex, stageContracts);
    }

    internal void AddStep(string name, string? stageName, string moduleType)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentException("Node name must be non-empty.", nameof(name));
        }

        if (string.IsNullOrEmpty(moduleType))
        {
            throw new ArgumentException("Module type must be non-empty.", nameof(moduleType));
        }

        if (!_nodeNames.Add(name))
        {
            throw new ArgumentException($"Flow '{_name}' already contains node '{name}'.", nameof(name));
        }

        var index = _nodes.Count;
        _nodes.Add(BlueprintNode.CreateStep(index, name, stageName, moduleType));
    }

    internal void AddJoin<TOut>(
        string name,
        string? stageName,
        Func<FlowContext, ValueTask<Outcome<TOut>>> join)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentException("Node name must be non-empty.", nameof(name));
        }

        if (join is null)
        {
            throw new ArgumentNullException(nameof(join));
        }

        if (!_nodeNames.Add(name))
        {
            throw new ArgumentException($"Flow '{_name}' already contains node '{name}'.", nameof(name));
        }

        var index = _nodes.Count;
        _nodes.Add(BlueprintNode.CreateJoin(index, name, stageName, join));
    }

    internal void SetStageContract(int stageIndex, StageContract contract)
    {
        if ((uint)stageIndex >= (uint)_stageContracts.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(stageIndex), stageIndex, "Stage index is out of range.");
        }

        var stageName = _stageContracts[stageIndex].StageName;
        _stageContracts[stageIndex] = new StageContractEntry(stageName, contract);
    }

    public readonly struct StageBuilder
    {
        private readonly FlowBlueprintBuilder<TReq, TResp> _owner;
        private readonly string _stageName;
        private readonly int _stageIndex;

        internal StageBuilder(FlowBlueprintBuilder<TReq, TResp> owner, string stageName, int stageIndex)
        {
            _owner = owner;
            _stageName = stageName;
            _stageIndex = stageIndex;
        }

        public StageBuilder Contract(Action<StageContractBuilder> configureContract)
        {
            if (configureContract is null)
            {
                throw new ArgumentNullException(nameof(configureContract));
            }

            var builder = new StageContractBuilder();
            configureContract(builder);
            _owner.SetStageContract(_stageIndex, builder.Build());
            return this;
        }

        public StageBuilder Step(string name, string moduleType)
        {
            _owner.AddStep(name, _stageName, moduleType);
            return this;
        }

        public StageBuilder Join<TOut>(
            string name,
            Func<FlowContext, ValueTask<Outcome<TOut>>> join)
        {
            _owner.AddJoin(name, _stageName, join);
            return this;
        }

        public StageBuilder Join<TOut>(
            string name,
            Func<FlowContext, Outcome<TOut>> join)
        {
            if (join is null)
            {
                throw new ArgumentNullException(nameof(join));
            }

            _owner.AddJoin(
                name,
                _stageName,
                ctx => new ValueTask<Outcome<TOut>>(join(ctx)));
            return this;
        }
    }
}

