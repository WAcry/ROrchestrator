using System.Threading;

namespace ROrchestrator.Core;

public sealed class FlowHost
{
    private readonly FlowRegistry _registry;
    private readonly ModuleCatalog _catalog;
    private readonly ExecutionEngine _engine;

    private readonly Lock _cacheGate;
    private Dictionary<string, object>? _templateCache;

    public int CachedPlanTemplateCount
    {
        get
        {
            var cache = Volatile.Read(ref _templateCache);
            return cache?.Count ?? 0;
        }
    }

    public FlowHost(FlowRegistry registry, ModuleCatalog catalog)
    {
        _registry = registry ?? throw new ArgumentNullException(nameof(registry));
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
        _engine = new ExecutionEngine(catalog);

        _cacheGate = new();
    }

    public ValueTask<Outcome<TResp>> ExecuteAsync<TReq, TResp>(string flowName, TReq request, FlowContext flowContext)
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        if (flowContext is null)
        {
            throw new ArgumentNullException(nameof(flowContext));
        }

        if (!typeof(TReq).IsValueType && request is null)
        {
            throw new ArgumentNullException(nameof(request));
        }

        var cache = Volatile.Read(ref _templateCache);

        if (cache is not null && cache.TryGetValue(flowName, out var cached))
        {
            if (cached is PlanTemplate<TReq, TResp> typedTemplate)
            {
                return _engine.ExecuteAsync(typedTemplate, request, flowContext);
            }

            _registry.Get<TReq, TResp>(flowName);
            throw new InvalidOperationException($"Flow '{flowName}' has a different signature.");
        }

        PlanTemplate<TReq, TResp> template;

        lock (_cacheGate)
        {
            cache = _templateCache;

            if (cache is not null && cache.TryGetValue(flowName, out cached))
            {
                if (cached is PlanTemplate<TReq, TResp> typedTemplate)
                {
                    template = typedTemplate;
                }
                else
                {
                    _registry.Get<TReq, TResp>(flowName);
                    throw new InvalidOperationException($"Flow '{flowName}' has a different signature.");
                }
            }
            else
            {
                var blueprint = _registry.Get<TReq, TResp>(flowName);
                template = PlanCompiler.Compile(blueprint, _catalog);

                Dictionary<string, object> newCache;

                if (cache is null || cache.Count == 0)
                {
                    newCache = new Dictionary<string, object>(1);
                }
                else
                {
                    newCache = new Dictionary<string, object>(cache.Count + 1);

                    foreach (var pair in cache)
                    {
                        newCache.Add(pair.Key, pair.Value);
                    }
                }

                newCache.Add(flowName, template);
                Volatile.Write(ref _templateCache, newCache);
            }
        }

        return _engine.ExecuteAsync(template, request, flowContext);
    }
}
