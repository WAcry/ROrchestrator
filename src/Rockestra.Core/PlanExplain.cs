using Rockestra.Core.Blueprint;

namespace Rockestra.Core;

public sealed class PlanExplain
{
    private readonly PlanExplainNode[] _nodes;

    public string FlowName { get; }

    public ulong PlanTemplateHash { get; }

    public IReadOnlyList<PlanExplainNode> Nodes => _nodes;

    internal PlanExplain(string flowName, ulong planTemplateHash, PlanExplainNode[] nodes)
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        if (nodes is null)
        {
            throw new ArgumentNullException(nameof(nodes));
        }

        if (nodes.Length == 0)
        {
            throw new ArgumentException("Nodes must be non-empty.", nameof(nodes));
        }

        FlowName = flowName;
        PlanTemplateHash = planTemplateHash;
        _nodes = nodes;
    }
}

public readonly struct PlanExplainNode
{
    public BlueprintNodeKind Kind { get; }

    public string Name { get; }

    public string? StageName { get; }

    public string? ModuleType { get; }

    public Type OutputType { get; }

    private PlanExplainNode(
        BlueprintNodeKind kind,
        string name,
        string? stageName,
        string? moduleType,
        Type outputType)
    {
        Kind = kind;
        Name = name;
        StageName = stageName;
        ModuleType = moduleType;
        OutputType = outputType;
    }

    internal static PlanExplainNode CreateStep(string name, string? stageName, string moduleType, Type outputType)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentException("Name must be non-empty.", nameof(name));
        }

        if (string.IsNullOrEmpty(moduleType))
        {
            throw new ArgumentException("ModuleType must be non-empty.", nameof(moduleType));
        }

        if (outputType is null)
        {
            throw new ArgumentNullException(nameof(outputType));
        }

        return new PlanExplainNode(
            BlueprintNodeKind.Step,
            name,
            stageName,
            moduleType,
            outputType);
    }

    internal static PlanExplainNode CreateJoin(string name, string? stageName, Type outputType)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentException("Name must be non-empty.", nameof(name));
        }

        if (outputType is null)
        {
            throw new ArgumentNullException(nameof(outputType));
        }

        return new PlanExplainNode(
            BlueprintNodeKind.Join,
            name,
            stageName,
            moduleType: null,
            outputType);
    }
}


