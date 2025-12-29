using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Core;

public sealed class PlanTemplate<TReq, TResp>
{
    private readonly PlanNodeTemplate[] _nodes;

    public string Name { get; }

    public ulong PlanHash { get; }

    public IReadOnlyList<PlanNodeTemplate> Nodes => _nodes;

    internal PlanTemplate(string name, ulong planHash, PlanNodeTemplate[] nodes)
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
    }
}

public readonly struct PlanNodeTemplate
{
    public BlueprintNodeKind Kind { get; }

    public string Name { get; }

    public string? StageName { get; }

    public string? ModuleType { get; }

    public Delegate? Join { get; }

    public Type OutputType { get; }

    private PlanNodeTemplate(
        BlueprintNodeKind kind,
        string name,
        string? stageName,
        string? moduleType,
        Delegate? join,
        Type outputType)
    {
        Kind = kind;
        Name = name;
        StageName = stageName;
        ModuleType = moduleType;
        Join = join;
        OutputType = outputType;
    }

    internal static PlanNodeTemplate CreateStep(string name, string? stageName, string moduleType, Type outputType)
    {
        return new PlanNodeTemplate(
            BlueprintNodeKind.Step,
            name,
            stageName,
            moduleType,
            join: null,
            outputType);
    }

    internal static PlanNodeTemplate CreateJoin(string name, string? stageName, Delegate join, Type outputType)
    {
        return new PlanNodeTemplate(
            BlueprintNodeKind.Join,
            name,
            stageName,
            moduleType: null,
            join,
            outputType);
    }
}
