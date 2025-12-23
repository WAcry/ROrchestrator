namespace ROrchestrator.Core.Blueprint;

public sealed class FlowBlueprint<TReq, TResp>
{
    private readonly BlueprintNode[] _nodes;

    public string Name { get; }

    public IReadOnlyList<BlueprintNode> Nodes => _nodes;

    internal FlowBlueprint(string name, BlueprintNode[] nodes)
    {
        Name = name;
        _nodes = nodes;
    }
}

