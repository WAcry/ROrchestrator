using Rockestra.Core.Blueprint;

namespace Rockestra.Core;

public sealed class PlanTemplate<TReq, TResp>
{
    private readonly PlanNodeTemplate[] _nodes;
    private readonly IReadOnlyDictionary<string, int> _nodeNameToIndex;

    public string Name { get; }

    public ulong PlanHash { get; }

    public IReadOnlyList<PlanNodeTemplate> Nodes => _nodes;

    internal IReadOnlyDictionary<string, int> NodeNameToIndex => _nodeNameToIndex;

    internal PlanTemplate(string name, ulong planHash, PlanNodeTemplate[] nodes, IReadOnlyDictionary<string, int> nodeNameToIndex)
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

