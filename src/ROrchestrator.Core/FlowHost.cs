using System.Threading;

namespace ROrchestrator.Core;

public sealed class FlowHost
{
    private readonly FlowRegistry _registry;
    private readonly ModuleCatalog _catalog;
    private readonly ExecutionEngine _engine;
    private readonly IConfigProvider _configProvider;
    private readonly IPlanCompiler _planCompiler;

    private readonly Lock _cacheGate;
    private Dictionary<PlanTemplateCacheKey, object>? _templateCache;

    public int CachedPlanTemplateCount
    {
        get
        {
            var cache = Volatile.Read(ref _templateCache);
            return cache?.Count ?? 0;
        }
    }

    public FlowHost(FlowRegistry registry, ModuleCatalog catalog)
        : this(registry, catalog, EmptyConfigProvider.Instance, DefaultPlanCompiler.Instance)
    {
    }

    public FlowHost(FlowRegistry registry, ModuleCatalog catalog, IConfigProvider configProvider)
        : this(registry, catalog, configProvider, DefaultPlanCompiler.Instance)
    {
    }

    internal FlowHost(
        FlowRegistry registry,
        ModuleCatalog catalog,
        IConfigProvider configProvider,
        IPlanCompiler planCompiler)
    {
        _registry = registry ?? throw new ArgumentNullException(nameof(registry));
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
        _engine = new ExecutionEngine(catalog);
        _configProvider = configProvider ?? throw new ArgumentNullException(nameof(configProvider));
        _planCompiler = planCompiler ?? throw new ArgumentNullException(nameof(planCompiler));

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

        var snapshotTask = flowContext.GetConfigSnapshotAsync(_configProvider);
        if (!snapshotTask.IsCompletedSuccessfully)
        {
            return ExecuteWithSnapshotAsync<TReq, TResp>(flowName, request, flowContext, snapshotTask);
        }

        return ExecuteWithSnapshot<TReq, TResp>(flowName, request, flowContext, snapshotTask.Result);
    }

    private async ValueTask<Outcome<TResp>> ExecuteWithSnapshotAsync<TReq, TResp>(
        string flowName,
        TReq request,
        FlowContext flowContext,
        ValueTask<ConfigSnapshot> snapshotTask)
    {
        var snapshot = await snapshotTask.ConfigureAwait(false);
        return await ExecuteWithSnapshot<TReq, TResp>(flowName, request, flowContext, snapshot).ConfigureAwait(false);
    }

    private ValueTask<Outcome<TResp>> ExecuteWithSnapshot<TReq, TResp>(
        string flowName,
        TReq request,
        FlowContext flowContext,
        ConfigSnapshot snapshot)
    {
        var key = new PlanTemplateCacheKey(flowName, snapshot.ConfigVersion);

        var cache = Volatile.Read(ref _templateCache);

        if (cache is not null && cache.TryGetValue(key, out var cached))
        {
            if (cached is PlanTemplate<TReq, TResp> typedTemplate)
            {
                if (_registry.TryGetParamsBinding(flowName, out var paramsType, out var patchType, out var defaultParams))
                {
                    flowContext.ConfigureFlowBinding(flowName, paramsType, patchType, defaultParams);
                }

                return _engine.ExecuteAsync(typedTemplate, request, flowContext);
            }

            _registry.Get<TReq, TResp>(flowName);
            throw new InvalidOperationException($"Flow '{flowName}' has a different signature.");
        }

        PlanTemplate<TReq, TResp> template;

        lock (_cacheGate)
        {
            cache = _templateCache;

            if (cache is not null && cache.TryGetValue(key, out cached))
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
                template = _planCompiler.Compile(blueprint, _catalog);

                Dictionary<PlanTemplateCacheKey, object> newCache;

                if (cache is null || cache.Count == 0)
                {
                    newCache = new Dictionary<PlanTemplateCacheKey, object>(1);
                }
                else
                {
                    newCache = new Dictionary<PlanTemplateCacheKey, object>(cache.Count + 1);

                    foreach (var pair in cache)
                    {
                        newCache.Add(pair.Key, pair.Value);
                    }
                }

                newCache.Add(key, template);
                Volatile.Write(ref _templateCache, newCache);
            }
        }

        if (_registry.TryGetParamsBinding(flowName, out var configuredParamsType, out var configuredPatchType, out var configuredDefaultParams))
        {
            flowContext.ConfigureFlowBinding(flowName, configuredParamsType, configuredPatchType, configuredDefaultParams);
        }

        return _engine.ExecuteAsync(template, request, flowContext);
    }

    private readonly struct PlanTemplateCacheKey : IEquatable<PlanTemplateCacheKey>
    {
        public string FlowName { get; }

        public ulong ConfigVersion { get; }

        public PlanTemplateCacheKey(string flowName, ulong configVersion)
        {
            FlowName = flowName;
            ConfigVersion = configVersion;
        }

        public bool Equals(PlanTemplateCacheKey other)
        {
            return ConfigVersion == other.ConfigVersion
                && string.Equals(FlowName, other.FlowName, StringComparison.Ordinal);
        }

        public override bool Equals(object? obj)
        {
            return obj is PlanTemplateCacheKey other && Equals(other);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hash = StringComparer.Ordinal.GetHashCode(FlowName);
                hash = (hash * 397) ^ ((int)ConfigVersion);
                hash = (hash * 397) ^ ((int)(ConfigVersion >> 32));
                return hash;
            }
        }
    }

    private sealed class EmptyConfigProvider : IConfigProvider
    {
        public static readonly EmptyConfigProvider Instance = new();

        private static readonly ConfigSnapshot Snapshot = new(configVersion: 0, patchJson: string.Empty);

        private EmptyConfigProvider()
        {
        }

        public ValueTask<ConfigSnapshot> GetSnapshotAsync(FlowContext context)
        {
            return new ValueTask<ConfigSnapshot>(Snapshot);
        }
    }
}
