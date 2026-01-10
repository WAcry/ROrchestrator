namespace Rockestra.Core.Blueprint;

public sealed class FlowBlueprint<TReq, TResp>
{
    private readonly BlueprintNode[] _nodes;
    private readonly IReadOnlyDictionary<string, int> _nodeNameToIndex;

    public string Name { get; }

    public IReadOnlyList<BlueprintNode> Nodes => _nodes;

    internal IReadOnlyDictionary<string, int> NodeNameToIndex => _nodeNameToIndex;

    internal FlowBlueprint(string name, BlueprintNode[] nodes, IReadOnlyDictionary<string, int> nodeNameToIndex)
    {
        Name = name;
        _nodes = nodes;
        _nodeNameToIndex = nodeNameToIndex;
    }
}


