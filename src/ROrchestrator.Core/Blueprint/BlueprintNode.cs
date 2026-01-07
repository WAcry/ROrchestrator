using ROrchestrator.Core;

namespace ROrchestrator.Core.Blueprint;

public readonly struct BlueprintNode
{
    public BlueprintNodeKind Kind { get; }

    public int Index { get; }

    public string Name { get; }

    public string? StageName { get; }

    public string? ModuleType { get; }

    public Delegate? Join { get; }

    public Type? JoinOutputType { get; }

    private BlueprintNode(
        BlueprintNodeKind kind,
        int index,
        string name,
        string? stageName,
        string? moduleType,
        Delegate? join,
        Type? joinOutputType)
    {
        Kind = kind;
        Index = index;
        Name = name;
        StageName = stageName;
        ModuleType = moduleType;
        Join = join;
        JoinOutputType = joinOutputType;
    }

    internal static BlueprintNode CreateStep(int index, string name, string? stageName, string moduleType)
    {
        return new BlueprintNode(BlueprintNodeKind.Step, index, name, stageName, moduleType, join: null, joinOutputType: null);
    }

    internal static BlueprintNode CreateJoin<TOut>(
        int index,
        string name,
        string? stageName,
        Func<FlowContext, ValueTask<Outcome<TOut>>> join)
    {
        return new BlueprintNode(BlueprintNodeKind.Join, index, name, stageName, moduleType: null, join, typeof(TOut));
    }
}

