using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Core;

public readonly struct FlowDefinition<TReq, TResp, TParams, TPatch>
    where TParams : notnull
{
    public FlowBlueprint<TReq, TResp> Blueprint { get; }

    public Type ParamsType { get; }

    public Type PatchType { get; }

    public TParams DefaultParams { get; }

    internal FlowDefinition(
        FlowBlueprint<TReq, TResp> blueprint,
        Type paramsType,
        Type patchType,
        TParams defaultParams)
    {
        Blueprint = blueprint ?? throw new ArgumentNullException(nameof(blueprint));
        ParamsType = paramsType ?? throw new ArgumentNullException(nameof(paramsType));
        PatchType = patchType ?? throw new ArgumentNullException(nameof(patchType));
        DefaultParams = defaultParams;
    }
}

public sealed class FlowRegistry
{
    private readonly Dictionary<string, Entry> _flows;

    public FlowRegistry()
    {
        _flows = new Dictionary<string, Entry>();
    }

    public bool IsRegistered(string flowName)
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        return _flows.ContainsKey(flowName);
    }

    internal bool TryGetStageNameSet(string flowName, out string[] stageNameSet)
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        if (_flows.TryGetValue(flowName, out var entry))
        {
            stageNameSet = entry.StageNameSet;
            return true;
        }

        stageNameSet = Array.Empty<string>();
        return false;
    }

    internal bool TryGetStageNameSetAndPatchType(
        string flowName,
        out string[] stageNameSet,
        out string[] nodeNameSet,
        out Type? patchType,
        out ExperimentLayerOwnershipContract? experimentLayerOwnershipContract)
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        if (_flows.TryGetValue(flowName, out var entry))
        {
            stageNameSet = entry.StageNameSet;
            nodeNameSet = entry.NodeNameSet;
            patchType = entry.PatchType;
            experimentLayerOwnershipContract = entry.ExperimentLayerOwnershipContract;
            return true;
        }

        stageNameSet = Array.Empty<string>();
        nodeNameSet = Array.Empty<string>();
        patchType = null;
        experimentLayerOwnershipContract = null;
        return false;
    }

    internal bool TryGetParamsBinding(string flowName, out Type paramsType, out Type patchType, out object defaultParams)
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        if (_flows.TryGetValue(flowName, out var entry)
            && entry.ParamsType is not null
            && entry.PatchType is not null
            && entry.DefaultParams is not null)
        {
            paramsType = entry.ParamsType;
            patchType = entry.PatchType;
            defaultParams = entry.DefaultParams;
            return true;
        }

        paramsType = null!;
        patchType = null!;
        defaultParams = null!;
        return false;
    }

    public void Register<TReq, TResp>(
        string flowName,
        FlowBlueprint<TReq, TResp> blueprint,
        ExperimentLayerOwnershipContract? experimentLayerOwnershipContract = null)
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        if (blueprint is null)
        {
            throw new ArgumentNullException(nameof(blueprint));
        }

        var stageNameSet = BuildStageNameSet(blueprint);
        var nodeNameSet = BuildNodeNameSet(blueprint);

        if (!_flows.TryAdd(
            flowName,
            new Entry(
                requestType: typeof(TReq),
                responseType: typeof(TResp),
                blueprint: blueprint,
                stageNameSet: stageNameSet,
                nodeNameSet: nodeNameSet,
                experimentLayerOwnershipContract: experimentLayerOwnershipContract,
                explainCompiler: CompilePlanExplain<TReq, TResp>)))
        {
            throw new ArgumentException($"FlowName '{flowName}' is already registered.", nameof(flowName));
        }
    }

    public void Register<TReq, TResp, TParams, TPatch>(
        string flowName,
        FlowBlueprint<TReq, TResp> blueprint,
        TParams defaultParams,
        ExperimentLayerOwnershipContract? experimentLayerOwnershipContract = null)
        where TParams : notnull
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        if (blueprint is null)
        {
            throw new ArgumentNullException(nameof(blueprint));
        }

        if (defaultParams is null)
        {
            throw new ArgumentNullException(nameof(defaultParams));
        }

        var stageNameSet = BuildStageNameSet(blueprint);
        var nodeNameSet = BuildNodeNameSet(blueprint);

        if (!_flows.TryAdd(
            flowName,
            new Entry(
                requestType: typeof(TReq),
                responseType: typeof(TResp),
                paramsType: typeof(TParams),
                patchType: typeof(TPatch),
                blueprint: blueprint,
                defaultParams: defaultParams,
                stageNameSet: stageNameSet,
                nodeNameSet: nodeNameSet,
                experimentLayerOwnershipContract: experimentLayerOwnershipContract,
                explainCompiler: CompilePlanExplain<TReq, TResp>)))
        {
            throw new ArgumentException($"FlowName '{flowName}' is already registered.", nameof(flowName));
        }
    }

    public FlowBlueprint<TReq, TResp> Get<TReq, TResp>(string flowName)
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        if (!_flows.TryGetValue(flowName, out var entry))
        {
            throw new InvalidOperationException($"Flow '{flowName}' is not registered.");
        }

        if (entry.RequestType != typeof(TReq) || entry.ResponseType != typeof(TResp))
        {
            throw new InvalidOperationException($"Flow '{flowName}' has a different signature.");
        }

        return (FlowBlueprint<TReq, TResp>)entry.Blueprint;
    }

    public FlowDefinition<TReq, TResp, TParams, TPatch> Get<TReq, TResp, TParams, TPatch>(string flowName)
        where TParams : notnull
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        if (!_flows.TryGetValue(flowName, out var entry))
        {
            throw new InvalidOperationException($"Flow '{flowName}' is not registered.");
        }

        if (entry.RequestType != typeof(TReq)
            || entry.ResponseType != typeof(TResp)
            || entry.ParamsType != typeof(TParams)
            || entry.PatchType != typeof(TPatch))
        {
            throw new InvalidOperationException($"Flow '{flowName}' has a different signature.");
        }

        return new FlowDefinition<TReq, TResp, TParams, TPatch>(
            blueprint: (FlowBlueprint<TReq, TResp>)entry.Blueprint,
            paramsType: entry.ParamsType!,
            patchType: entry.PatchType!,
            defaultParams: (TParams)entry.DefaultParams!);
    }

    public bool TryGet<TReq, TResp>(string flowName, out FlowBlueprint<TReq, TResp> blueprint)
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        if (_flows.TryGetValue(flowName, out var entry))
        {
            if (entry.RequestType != typeof(TReq) || entry.ResponseType != typeof(TResp))
            {
                throw new InvalidOperationException($"Flow '{flowName}' has a different signature.");
            }

            blueprint = (FlowBlueprint<TReq, TResp>)entry.Blueprint;
            return true;
        }

        blueprint = default!;
        return false;
    }

    public bool TryGet<TReq, TResp, TParams, TPatch>(
        string flowName,
        out FlowDefinition<TReq, TResp, TParams, TPatch> definition)
        where TParams : notnull
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        if (_flows.TryGetValue(flowName, out var entry))
        {
            if (entry.RequestType != typeof(TReq)
                || entry.ResponseType != typeof(TResp)
                || entry.ParamsType != typeof(TParams)
                || entry.PatchType != typeof(TPatch))
            {
                throw new InvalidOperationException($"Flow '{flowName}' has a different signature.");
            }

            definition = new FlowDefinition<TReq, TResp, TParams, TPatch>(
                blueprint: (FlowBlueprint<TReq, TResp>)entry.Blueprint,
                paramsType: entry.ParamsType!,
                patchType: entry.PatchType!,
                defaultParams: (TParams)entry.DefaultParams!);
            return true;
        }

        definition = default;
        return false;
    }

    public PlanExplain Explain(string flowName, ModuleCatalog catalog)
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        if (catalog is null)
        {
            throw new ArgumentNullException(nameof(catalog));
        }

        if (!_flows.TryGetValue(flowName, out var entry))
        {
            throw new InvalidOperationException($"Flow '{flowName}' is not registered.");
        }

        return entry.ExplainCompiler(entry.Blueprint, catalog);
    }

    private static PlanExplain CompilePlanExplain<TReq, TResp>(object blueprint, ModuleCatalog catalog)
    {
        var typedBlueprint = (FlowBlueprint<TReq, TResp>)blueprint;
        var result = PlanCompiler.CompileWithExplain(typedBlueprint, catalog);
        return result.Explain;
    }

    private readonly struct Entry
    {
        public Type RequestType { get; }

        public Type ResponseType { get; }

        public Type? ParamsType { get; }

        public Type? PatchType { get; }

        public object Blueprint { get; }

        public object? DefaultParams { get; }

        public string[] StageNameSet { get; }

        public string[] NodeNameSet { get; }

        public ExperimentLayerOwnershipContract? ExperimentLayerOwnershipContract { get; }

        public Func<object, ModuleCatalog, PlanExplain> ExplainCompiler { get; }

        public Entry(
            Type requestType,
            Type responseType,
            object blueprint,
            string[] stageNameSet,
            string[] nodeNameSet,
            ExperimentLayerOwnershipContract? experimentLayerOwnershipContract,
            Func<object, ModuleCatalog, PlanExplain> explainCompiler)
        {
            RequestType = requestType;
            ResponseType = responseType;
            ParamsType = null;
            PatchType = null;
            Blueprint = blueprint;
            DefaultParams = null;
            StageNameSet = stageNameSet;
            NodeNameSet = nodeNameSet;
            ExperimentLayerOwnershipContract = experimentLayerOwnershipContract;
            ExplainCompiler = explainCompiler;
        }

        public Entry(
            Type requestType,
            Type responseType,
            Type paramsType,
            Type patchType,
            object blueprint,
            object defaultParams,
            string[] stageNameSet,
            string[] nodeNameSet,
            ExperimentLayerOwnershipContract? experimentLayerOwnershipContract,
            Func<object, ModuleCatalog, PlanExplain> explainCompiler)
        {
            RequestType = requestType;
            ResponseType = responseType;
            ParamsType = paramsType;
            PatchType = patchType;
            Blueprint = blueprint;
            DefaultParams = defaultParams;
            StageNameSet = stageNameSet;
            NodeNameSet = nodeNameSet;
            ExperimentLayerOwnershipContract = experimentLayerOwnershipContract;
            ExplainCompiler = explainCompiler;
        }
    }

    private static string[] BuildStageNameSet<TReq, TResp>(FlowBlueprint<TReq, TResp> blueprint)
    {
        var nodes = blueprint.Nodes;

        string[]? stageNames = null;
        var stageNameCount = 0;

        for (var i = 0; i < nodes.Count; i++)
        {
            var stageName = nodes[i].StageName;
            if (stageName is null)
            {
                continue;
            }

            if (stageName.Length == 0)
            {
                continue;
            }

            if (stageNameCount != 0)
            {
                var found = false;
                var buffer = stageNames!;

                for (var j = 0; j < stageNameCount; j++)
                {
                    if (string.Equals(buffer[j], stageName, StringComparison.Ordinal))
                    {
                        found = true;
                        break;
                    }
                }

                if (found)
                {
                    continue;
                }
            }

            if (stageNames is null)
            {
                stageNames = new string[4];
            }
            else if ((uint)stageNameCount >= (uint)stageNames.Length)
            {
                var newItems = new string[stageNames.Length * 2];
                Array.Copy(stageNames, 0, newItems, 0, stageNames.Length);
                stageNames = newItems;
            }

            stageNames[stageNameCount] = stageName;
            stageNameCount++;
        }

        if (stageNameCount == 0)
        {
            return Array.Empty<string>();
        }

        var items = stageNames!;

        if (stageNameCount == items.Length)
        {
            return items;
        }

        var trimmed = new string[stageNameCount];
        Array.Copy(items, 0, trimmed, 0, stageNameCount);
        return trimmed;
    }

    private static string[] BuildNodeNameSet<TReq, TResp>(FlowBlueprint<TReq, TResp> blueprint)
    {
        var nodes = blueprint.Nodes;

        if (nodes.Count == 0)
        {
            return Array.Empty<string>();
        }

        var buffer = new string[nodes.Count];
        var count = 0;

        for (var i = 0; i < nodes.Count; i++)
        {
            var name = nodes[i].Name;

            if (name.Length == 0)
            {
                continue;
            }

            if (count != 0)
            {
                var found = false;

                for (var j = 0; j < count; j++)
                {
                    if (string.Equals(buffer[j], name, StringComparison.Ordinal))
                    {
                        found = true;
                        break;
                    }
                }

                if (found)
                {
                    continue;
                }
            }

            buffer[count] = name;
            count++;
        }

        if (count == 0)
        {
            return Array.Empty<string>();
        }

        if (count == buffer.Length)
        {
            return buffer;
        }

        var trimmed = new string[count];
        Array.Copy(buffer, 0, trimmed, 0, count);
        return trimmed;
    }
}
