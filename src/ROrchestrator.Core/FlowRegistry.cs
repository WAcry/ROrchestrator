using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Core;

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

    private readonly struct Entry
    {
        public Type RequestType { get; }

        public Type ResponseType { get; }

        public object Blueprint { get; }

        public Entry(Type requestType, Type responseType, object blueprint)
        {
            RequestType = requestType;
            ResponseType = responseType;
            Blueprint = blueprint;
        }
    }
}

