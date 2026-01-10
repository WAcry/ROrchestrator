using Rockestra.Core.Blueprint;

namespace Rockestra.Core;

public sealed class PlanTemplate<TReq, TResp>
{
    private readonly PlanNodeTemplate[] _nodes;
    private readonly IReadOnlyDictionary<string, int> _nodeNameToIndex;
    private readonly StageContractEntry[] _stageContracts;

    public string Name { get; }

    public ulong PlanHash { get; }

    public IReadOnlyList<PlanNodeTemplate> Nodes => _nodes;

    internal IReadOnlyDictionary<string, int> NodeNameToIndex => _nodeNameToIndex;

    internal PlanTemplate(
        string name,
        ulong planHash,
        PlanNodeTemplate[] nodes,
        IReadOnlyDictionary<string, int> nodeNameToIndex,
        StageContractEntry[] stageContracts)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentException("Name must be non-empty.", nameof(name));
        }

        if (nodes is null)
        {
            throw new ArgumentNullException(nameof(nodes));
        }

        if (nodes.Length == 0)
        {
            throw new ArgumentException("Nodes must be non-empty.", nameof(nodes));
        }

        Name = name;
        PlanHash = planHash;
        _nodes = nodes;
        _nodeNameToIndex = nodeNameToIndex;
        _stageContracts = stageContracts ?? throw new ArgumentNullException(nameof(stageContracts));
    }

    internal bool TryGetStageContract(string stageName, out StageContract contract)
    {
        if (string.IsNullOrEmpty(stageName))
        {
            throw new ArgumentException("StageName must be non-empty.", nameof(stageName));
        }

        for (var i = 0; i < _stageContracts.Length; i++)
        {
            if (string.Equals(_stageContracts[i].StageName, stageName, StringComparison.Ordinal))
            {
                contract = _stageContracts[i].Contract;
                return true;
            }
        }

        contract = StageContract.Default;
        return false;
    }
}

public readonly struct PlanNodeTemplate
{
    public BlueprintNodeKind Kind { get; }

    public int Index { get; }

    public string Name { get; }

    public string? StageName { get; }

    public string? ModuleType { get; }

    public Delegate? Join { get; }

    public Type OutputType { get; }

    private PlanNodeTemplate(
        BlueprintNodeKind kind,
        int index,
        string name,
        string? stageName,
        string? moduleType,
        Delegate? join,
        Type outputType)
    {
        Kind = kind;
        Index = index;
        Name = name;
        StageName = stageName;
        ModuleType = moduleType;
        Join = join;
        OutputType = outputType;
    }

    internal static PlanNodeTemplate CreateStep(int index, string name, string? stageName, string moduleType, Type outputType)
    {
        return new PlanNodeTemplate(
            BlueprintNodeKind.Step,
            index,
            name,
            stageName,
            moduleType,
            join: null,
            outputType);
    }

    internal static PlanNodeTemplate CreateJoin(int index, string name, string? stageName, Delegate join, Type outputType)
    {
        return new PlanNodeTemplate(
            BlueprintNodeKind.Join,
            index,
            name,
            stageName,
            moduleType: null,
            join,
            outputType);
    }
}

