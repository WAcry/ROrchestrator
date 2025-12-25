using ROrchestrator.Core;

namespace ROrchestrator.Core.Blueprint;

public sealed class FlowBlueprintBuilder<TReq, TResp>
{
    private readonly string _name;
    private readonly List<BlueprintNode> _nodes;
    private readonly HashSet<string> _nodeNames;
    private readonly HashSet<string> _stageNames;

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
    }

    public string Name => _name;

    public FlowBlueprintBuilder<TReq, TResp> Stage(string name, Action<StageBuilder> configure)
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

        var nodesBefore = _nodes.Count;

        try
        {
            configure(new StageBuilder(this, name));

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

    public FlowBlueprint<TReq, TResp> Build()
    {
        if (_nodes.Count == 0)
        {
            throw new InvalidOperationException($"Flow '{_name}' must contain at least one node.");
        }

        return new FlowBlueprint<TReq, TResp>(_name, _nodes.ToArray());
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

        _nodes.Add(BlueprintNode.CreateStep(name, stageName, moduleType));
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

        _nodes.Add(BlueprintNode.CreateJoin(name, stageName, join));
    }

    public readonly struct StageBuilder
    {
        private readonly FlowBlueprintBuilder<TReq, TResp> _owner;
        private readonly string _stageName;

        internal StageBuilder(FlowBlueprintBuilder<TReq, TResp> owner, string stageName)
        {
            _owner = owner;
            _stageName = stageName;
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
    }
}
