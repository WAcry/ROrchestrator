using ROrchestrator.Core;

namespace ROrchestrator.Core.Blueprint;

public readonly struct BlueprintNode
{
    public BlueprintNodeKind Kind { get; }

    public string Name { get; }

    public string? StageName { get; }

    public string? ModuleType { get; }

    public Delegate? Join { get; }

    public Type? JoinOutputType { get; }

    private BlueprintNode(
        BlueprintNodeKind kind,
        string name,
        string? stageName,
        string? moduleType,
        Delegate? join,
        Type? joinOutputType)
    {
        Kind = kind;
        Name = name;
        StageName = stageName;
        ModuleType = moduleType;
        Join = join;
        JoinOutputType = joinOutputType;
    }

    internal static BlueprintNode CreateStep(string name, string? stageName, string moduleType)
    {
        return new BlueprintNode(BlueprintNodeKind.Step, name, stageName, moduleType, join: null, joinOutputType: null);
    }

    internal static BlueprintNode CreateJoin<TOut>(
        string name,
        string? stageName,
        Func<FlowContext, ValueTask<Outcome<TOut>>> join)
    {
        return new BlueprintNode(BlueprintNodeKind.Join, name, stageName, moduleType: null, join, typeof(TOut));
    }
}

