namespace Rockestra.Core.Blueprint;

public sealed class FlowBlueprint<TReq, TResp>
{
    private readonly BlueprintNode[] _nodes;
    private readonly IReadOnlyDictionary<string, int> _nodeNameToIndex;
    private readonly StageContractEntry[] _stageContracts;

    public string Name { get; }

    public IReadOnlyList<BlueprintNode> Nodes => _nodes;

    internal IReadOnlyDictionary<string, int> NodeNameToIndex => _nodeNameToIndex;

    internal StageContractEntry[] StageContracts => _stageContracts;

    internal FlowBlueprint(
        string name,
        BlueprintNode[] nodes,
        IReadOnlyDictionary<string, int> nodeNameToIndex,
        StageContractEntry[] stageContracts)
    {
        Name = name;
        _nodes = nodes;
        _nodeNameToIndex = nodeNameToIndex;
        _stageContracts = stageContracts ?? throw new ArgumentNullException(nameof(stageContracts));
    }
}


