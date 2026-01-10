using System.Threading;
using ROrchestrator.Core.Selectors;

namespace ROrchestrator.Core;

public sealed class FlowHost
{
    private readonly FlowRegistry _registry;
    private readonly ModuleCatalog _catalog;
    private readonly ExecutionEngine _engine;
    private readonly IConfigProvider _configProvider;
    private readonly IQosProvider? _qosProvider;
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
        : this(registry, catalog, SelectorRegistry.Empty, EmptyConfigProvider.Instance, DefaultPlanCompiler.Instance, qosProvider: null)
    {
    }

    public FlowHost(FlowRegistry registry, ModuleCatalog catalog, IConfigProvider configProvider)
        : this(registry, catalog, SelectorRegistry.Empty, configProvider, DefaultPlanCompiler.Instance, qosProvider: null)
    {
    }

    public FlowHost(FlowRegistry registry, ModuleCatalog catalog, SelectorRegistry selectorRegistry)
        : this(registry, catalog, selectorRegistry, EmptyConfigProvider.Instance, DefaultPlanCompiler.Instance, qosProvider: null)
    {
    }

    public FlowHost(FlowRegistry registry, ModuleCatalog catalog, SelectorRegistry selectorRegistry, IConfigProvider configProvider)
        : this(registry, catalog, selectorRegistry, configProvider, DefaultPlanCompiler.Instance, qosProvider: null)
    {
    }

    public FlowHost(FlowRegistry registry, ModuleCatalog catalog, IQosTierProvider qosTierProvider)
        : this(registry, catalog, SelectorRegistry.Empty, EmptyConfigProvider.Instance, DefaultPlanCompiler.Instance, new QosTierProviderAdapter(qosTierProvider))
    {
    }

    public FlowHost(FlowRegistry registry, ModuleCatalog catalog, IConfigProvider configProvider, IQosTierProvider qosTierProvider)
        : this(registry, catalog, SelectorRegistry.Empty, configProvider, DefaultPlanCompiler.Instance, new QosTierProviderAdapter(qosTierProvider))
    {
    }

    public FlowHost(FlowRegistry registry, ModuleCatalog catalog, SelectorRegistry selectorRegistry, IQosTierProvider qosTierProvider)
        : this(registry, catalog, selectorRegistry, EmptyConfigProvider.Instance, DefaultPlanCompiler.Instance, new QosTierProviderAdapter(qosTierProvider))
    {
    }

    public FlowHost(
        FlowRegistry registry,
        ModuleCatalog catalog,
        SelectorRegistry selectorRegistry,
        IConfigProvider configProvider,
        IQosTierProvider qosTierProvider)
        : this(registry, catalog, selectorRegistry, configProvider, DefaultPlanCompiler.Instance, new QosTierProviderAdapter(qosTierProvider))
    {
    }

    public FlowHost(FlowRegistry registry, ModuleCatalog catalog, IQosProvider qosProvider)
        : this(registry, catalog, SelectorRegistry.Empty, EmptyConfigProvider.Instance, DefaultPlanCompiler.Instance, qosProvider)
    {
    }

    public FlowHost(FlowRegistry registry, ModuleCatalog catalog, IConfigProvider configProvider, IQosProvider qosProvider)
        : this(registry, catalog, SelectorRegistry.Empty, configProvider, DefaultPlanCompiler.Instance, qosProvider)
    {
    }

    public FlowHost(FlowRegistry registry, ModuleCatalog catalog, SelectorRegistry selectorRegistry, IQosProvider qosProvider)
        : this(registry, catalog, selectorRegistry, EmptyConfigProvider.Instance, DefaultPlanCompiler.Instance, qosProvider)
    {
    }

    public FlowHost(
        FlowRegistry registry,
        ModuleCatalog catalog,
        SelectorRegistry selectorRegistry,
        IConfigProvider configProvider,
        IQosProvider qosProvider)
        : this(registry, catalog, selectorRegistry, configProvider, DefaultPlanCompiler.Instance, qosProvider)
    {
    }

    internal FlowHost(
        FlowRegistry registry,
        ModuleCatalog catalog,
        IConfigProvider configProvider,
        IPlanCompiler planCompiler)
        : this(registry, catalog, SelectorRegistry.Empty, configProvider, planCompiler, qosProvider: null)
    {
    }

    internal FlowHost(
        FlowRegistry registry,
        ModuleCatalog catalog,
        SelectorRegistry selectorRegistry,
        IConfigProvider configProvider,
        IPlanCompiler planCompiler)
        : this(registry, catalog, selectorRegistry, configProvider, planCompiler, qosProvider: null)
    {
    }

    internal FlowHost(
        FlowRegistry registry,
        ModuleCatalog catalog,
        SelectorRegistry selectorRegistry,
        IConfigProvider configProvider,
        IPlanCompiler planCompiler,
        IQosProvider? qosProvider)
    {
        _registry = registry ?? throw new ArgumentNullException(nameof(registry));
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
        _engine = new ExecutionEngine(catalog, selectorRegistry ?? throw new ArgumentNullException(nameof(selectorRegistry)));
        _configProvider = WrapConfigProvider(configProvider ?? throw new ArgumentNullException(nameof(configProvider)), _registry, _catalog, selectorRegistry);
        _qosProvider = qosProvider;
        _planCompiler = planCompiler ?? throw new ArgumentNullException(nameof(planCompiler));

        _cacheGate = new();
    }

    private static IConfigProvider WrapConfigProvider(
        IConfigProvider provider,
        FlowRegistry registry,
        ModuleCatalog catalog,
        SelectorRegistry selectorRegistry)
    {
        if (provider is EmptyConfigProvider || provider is LkgConfigProvider || provider is PersistedLkgConfigProvider)
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
        SetQosDecision(flowName, flowContext);

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

    private const int MaxQosReasonCodeLength = 64;

    private void SetQosDecision(string flowName, FlowContext context)
    {
        var provider = _qosProvider;

        QosTier selectedTier;
        string? reasonCode;
        IReadOnlyDictionary<string, string>? signals;

        if (provider is null)
        {
            selectedTier = QosTier.Full;
            reasonCode = null;
            signals = null;
        }
        else
        {
            var collectSignals = context.ShouldCollectQosSignals();
            var selectContext = new QosSelectContext(collectSignals);

            try
            {
                var decision = provider.Select(flowName, context, selectContext);
                selectedTier = decision.SelectedTier;
                reasonCode = NormalizeReasonCode(decision.ReasonCode);
                signals = collectSignals ? NormalizeSignals(decision.Signals) : null;
            }
            catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
            {
                _ = ex;
                selectedTier = QosTier.Full;
                reasonCode = "PROVIDER_ERROR";
                signals = null;
            }

            selectedTier = NormalizeQosTier(selectedTier);
        }

        context.SetQosDecision(selectedTier, reasonCode, signals);
        Observability.FlowMetricsV1.RecordQosTierSelected(flowName, selectedTier);
    }

    private static string? NormalizeReasonCode(string? reasonCode)
    {
        if (string.IsNullOrWhiteSpace(reasonCode))
        {
            return null;
        }

        if (reasonCode.Length <= MaxQosReasonCodeLength)
        {
            return reasonCode;
        }

        return reasonCode.Substring(0, MaxQosReasonCodeLength);
    }

    private static IReadOnlyDictionary<string, string>? NormalizeSignals(IReadOnlyDictionary<string, string>? signals)
    {
        if (signals is null || signals.Count == 0)
        {
            return null;
        }

        var copy = new Dictionary<string, string>(capacity: signals.Count);
        foreach (var pair in signals)
        {
            copy.Add(pair.Key, pair.Value);
        }

        return new System.Collections.ObjectModel.ReadOnlyDictionary<string, string>(copy);
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

    private sealed class QosTierProviderAdapter : IQosProvider
    {
        private readonly IQosTierProvider _inner;

        public QosTierProviderAdapter(IQosTierProvider inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        }

        public QosDecision Select(string flowName, FlowContext context, QosSelectContext selectContext)
        {
            _ = selectContext;
            return new QosDecision(_inner.SelectTier(flowName, context), reasonCode: null, signals: null);
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
