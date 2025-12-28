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

    public void Register<TReq, TResp>(string flowName, FlowBlueprint<TReq, TResp> blueprint)
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        if (blueprint is null)
        {
            throw new ArgumentNullException(nameof(blueprint));
        }

        if (!_flows.TryAdd(flowName, new Entry(typeof(TReq), typeof(TResp), blueprint)))
        {
            throw new ArgumentException($"FlowName '{flowName}' is already registered.", nameof(flowName));
        }
    }

    public void Register<TReq, TResp, TParams, TPatch>(
        string flowName,
        FlowBlueprint<TReq, TResp> blueprint,
        TParams defaultParams)
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

        if (!_flows.TryAdd(
            flowName,
            new Entry(
                requestType: typeof(TReq),
                responseType: typeof(TResp),
                paramsType: typeof(TParams),
                patchType: typeof(TPatch),
                blueprint: blueprint,
                defaultParams: defaultParams)))
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

    private readonly struct Entry
    {
        public Type RequestType { get; }

        public Type ResponseType { get; }

        public Type? ParamsType { get; }

        public Type? PatchType { get; }

        public object Blueprint { get; }

        public object? DefaultParams { get; }

        public Entry(Type requestType, Type responseType, object blueprint)
        {
            RequestType = requestType;
            ResponseType = responseType;
            ParamsType = null;
            PatchType = null;
            Blueprint = blueprint;
            DefaultParams = null;
        }

        public Entry(
            Type requestType,
            Type responseType,
            Type paramsType,
            Type patchType,
            object blueprint,
            object defaultParams)
        {
            RequestType = requestType;
            ResponseType = responseType;
            ParamsType = paramsType;
            PatchType = patchType;
            Blueprint = blueprint;
            DefaultParams = defaultParams;
        }
    }
}
