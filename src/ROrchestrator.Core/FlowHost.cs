using System.Threading;
using ROrchestrator.Core.Selectors;

namespace ROrchestrator.Core;

public sealed class FlowHost
{
    private readonly FlowRegistry _registry;
    private readonly ModuleCatalog _catalog;
    private readonly ExecutionEngine _engine;
    private readonly IConfigProvider _configProvider;
    private readonly IQosTierProvider? _qosTierProvider;
    private readonly IPlanCompiler _planCompiler;

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
        : this(registry, catalog, SelectorRegistry.Empty, EmptyConfigProvider.Instance, DefaultPlanCompiler.Instance, qosTierProvider: null)
    {
    }

    public FlowHost(FlowRegistry registry, ModuleCatalog catalog, IConfigProvider configProvider)
        : this(registry, catalog, SelectorRegistry.Empty, configProvider, DefaultPlanCompiler.Instance, qosTierProvider: null)
    {
    }

    public FlowHost(FlowRegistry registry, ModuleCatalog catalog, SelectorRegistry selectorRegistry)
        : this(registry, catalog, selectorRegistry, EmptyConfigProvider.Instance, DefaultPlanCompiler.Instance, qosTierProvider: null)
    {
    }

    public FlowHost(FlowRegistry registry, ModuleCatalog catalog, SelectorRegistry selectorRegistry, IConfigProvider configProvider)
        : this(registry, catalog, selectorRegistry, configProvider, DefaultPlanCompiler.Instance, qosTierProvider: null)
    {
    }

    public FlowHost(FlowRegistry registry, ModuleCatalog catalog, IQosTierProvider qosTierProvider)
        : this(registry, catalog, SelectorRegistry.Empty, EmptyConfigProvider.Instance, DefaultPlanCompiler.Instance, qosTierProvider)
    {
    }

    public FlowHost(FlowRegistry registry, ModuleCatalog catalog, IConfigProvider configProvider, IQosTierProvider qosTierProvider)
        : this(registry, catalog, SelectorRegistry.Empty, configProvider, DefaultPlanCompiler.Instance, qosTierProvider)
    {
    }

    public FlowHost(FlowRegistry registry, ModuleCatalog catalog, SelectorRegistry selectorRegistry, IQosTierProvider qosTierProvider)
        : this(registry, catalog, selectorRegistry, EmptyConfigProvider.Instance, DefaultPlanCompiler.Instance, qosTierProvider)
    {
    }

    public FlowHost(
        FlowRegistry registry,
        ModuleCatalog catalog,
        SelectorRegistry selectorRegistry,
        IConfigProvider configProvider,
        IQosTierProvider qosTierProvider)
        : this(registry, catalog, selectorRegistry, configProvider, DefaultPlanCompiler.Instance, qosTierProvider)
    {
    }

    internal FlowHost(
        FlowRegistry registry,
        ModuleCatalog catalog,
        IConfigProvider configProvider,
        IPlanCompiler planCompiler)
        : this(registry, catalog, SelectorRegistry.Empty, configProvider, planCompiler, qosTierProvider: null)
    {
    }

    internal FlowHost(
        FlowRegistry registry,
        ModuleCatalog catalog,
        SelectorRegistry selectorRegistry,
        IConfigProvider configProvider,
        IPlanCompiler planCompiler)
        : this(registry, catalog, selectorRegistry, configProvider, planCompiler, qosTierProvider: null)
    {
    }

    internal FlowHost(
        FlowRegistry registry,
        ModuleCatalog catalog,
        SelectorRegistry selectorRegistry,
        IConfigProvider configProvider,
        IPlanCompiler planCompiler,
        IQosTierProvider? qosTierProvider)
    {
        _registry = registry ?? throw new ArgumentNullException(nameof(registry));
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
        _engine = new ExecutionEngine(catalog, selectorRegistry ?? throw new ArgumentNullException(nameof(selectorRegistry)));
        _configProvider = WrapConfigProvider(configProvider ?? throw new ArgumentNullException(nameof(configProvider)), _registry, _catalog, selectorRegistry);
        _qosTierProvider = qosTierProvider;
        _planCompiler = planCompiler ?? throw new ArgumentNullException(nameof(planCompiler));

        _cacheGate = new();
    }

    private static IConfigProvider WrapConfigProvider(
        IConfigProvider provider,
        FlowRegistry registry,
        ModuleCatalog catalog,
        SelectorRegistry selectorRegistry)
    {
        if (provider is EmptyConfigProvider || provider is LkgConfigProvider)
        {
            return provider;
        }

        var validator = new ConfigValidator(registry, catalog, selectorRegistry);
        return new LkgConfigProvider(provider, validator);
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

        flowContext.SetFlowNameHint(flowName);

        var snapshotTask = flowContext.GetConfigSnapshotAsync(_configProvider);
        if (!snapshotTask.IsCompletedSuccessfully)
        {
            return ExecuteWithSnapshotAsync<TReq, TResp>(flowName, request, flowContext, snapshotTask);
        }

        _ = snapshotTask.Result;
        return ExecuteWithSnapshot<TReq, TResp>(flowName, request, flowContext);
    }

    private async ValueTask<Outcome<TResp>> ExecuteWithSnapshotAsync<TReq, TResp>(
        string flowName,
        TReq request,
        FlowContext flowContext,
        ValueTask<ConfigSnapshot> snapshotTask)
    {
        _ = await snapshotTask.ConfigureAwait(false);
        return await ExecuteWithSnapshot<TReq, TResp>(flowName, request, flowContext).ConfigureAwait(false);
    }

    private ValueTask<Outcome<TResp>> ExecuteWithSnapshot<TReq, TResp>(
        string flowName,
        TReq request,
        FlowContext flowContext)
    {
        SetQosSelectedTier(flowName, flowContext);

        var cache = Volatile.Read(ref _templateCache);

        if (cache is not null && cache.TryGetValue(flowName, out var cached))
        {
            if (cached is PlanTemplate<TReq, TResp> typedTemplate)
            {
                if (_registry.TryGetParamsBinding(flowName, out var paramsType, out var patchType, out var defaultParams))
                {
                    flowContext.ConfigureFlowBinding(flowName, paramsType, patchType, defaultParams);
                }

                return _engine.ExecuteAsync(flowName, typedTemplate, request, flowContext);
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
                template = _planCompiler.Compile(blueprint, _catalog);

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

        if (_registry.TryGetParamsBinding(flowName, out var configuredParamsType, out var configuredPatchType, out var configuredDefaultParams))
        {
            flowContext.ConfigureFlowBinding(flowName, configuredParamsType, configuredPatchType, configuredDefaultParams);
        }

        return _engine.ExecuteAsync(flowName, template, request, flowContext);
    }

    private void SetQosSelectedTier(string flowName, FlowContext context)
    {
        var provider = _qosTierProvider;

        QosTier selectedTier;

        if (provider is null)
        {
            selectedTier = QosTier.Full;
        }
        else
        {
            try
            {
                selectedTier = provider.SelectTier(flowName, context);
            }
            catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
            {
                _ = ex;
                selectedTier = QosTier.Full;
            }

            selectedTier = NormalizeQosTier(selectedTier);
        }

        context.SetQosSelectedTier(selectedTier);
        Observability.FlowMetricsV1.RecordQosTierSelected(flowName, selectedTier);
    }

    private static QosTier NormalizeQosTier(QosTier tier)
    {
        return tier switch
        {
            QosTier.Full => QosTier.Full,
            QosTier.Conserve => QosTier.Conserve,
            QosTier.Emergency => QosTier.Emergency,
            QosTier.Fallback => QosTier.Fallback,
            _ => QosTier.Full,
        };
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
