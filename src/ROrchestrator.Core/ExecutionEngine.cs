using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Threading;
using ROrchestrator.Core.Blueprint;
using ROrchestrator.Core.Observability;

namespace ROrchestrator.Core;

public sealed class ExecutionEngine
{
    public const string DeadlineExceededCode = "DEADLINE_EXCEEDED";
    public const string UnhandledExceptionCode = "UNHANDLED_EXCEPTION";
    public const string UpstreamCanceledCode = "UPSTREAM_CANCELED";

    private readonly ModuleCatalog _catalog;

    public ExecutionEngine(ModuleCatalog catalog)
    {
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
    }

    private static bool IsDeadlineExceeded(DateTimeOffset deadline)
    {
        return DateTimeOffset.UtcNow >= deadline;
    }

    public async ValueTask<Outcome<TResp>> ExecuteAsync<TReq, TResp>(
        FlowBlueprint<TReq, TResp> blueprint,
        TReq request,
        FlowContext flowContext)
    {
        if (blueprint is null)
        {
            throw new ArgumentNullException(nameof(blueprint));
        }

        if (flowContext is null)
        {
            throw new ArgumentNullException(nameof(flowContext));
        }

        if (!typeof(TReq).IsValueType && request is null)
        {
            throw new ArgumentNullException(nameof(request));
        }

        var nodes = blueprint.Nodes;
        var nodeCount = nodes.Count;

        if (nodeCount == 0)
        {
            throw new InvalidOperationException($"Flow '{blueprint.Name}' must contain at least one node.");
        }

        if (IsDeadlineExceeded(flowContext.Deadline))
        {
            return Outcome<TResp>.Timeout(DeadlineExceededCode);
        }

        if (flowContext.CancellationToken.IsCancellationRequested)
        {
            return Outcome<TResp>.Canceled(UpstreamCanceledCode);
        }

        flowContext.PrepareForExecution(blueprint.NodeNameToIndex, nodeCount);

        for (var i = 0; i < nodeCount; i++)
        {
            if (IsDeadlineExceeded(flowContext.Deadline))
            {
                return Outcome<TResp>.Timeout(DeadlineExceededCode);
            }

            if (flowContext.CancellationToken.IsCancellationRequested)
            {
                return Outcome<TResp>.Canceled(UpstreamCanceledCode);
            }

            var node = nodes[i];

            if (node.Kind == BlueprintNodeKind.Step)
            {
                var moduleType = node.ModuleType!;

                if (!_catalog.TryGetSignature(moduleType, out var argsType, out var outType))
                {
                    throw new InvalidOperationException($"Module type '{moduleType}' is not registered.");
                }

                if (argsType != typeof(TReq))
                {
                    throw new InvalidOperationException($"Module type '{moduleType}' has a different signature.");
                }

                var executor = StepExecutorCache<TReq>.Get(outType);
                await executor(this, node, request, flowContext).ConfigureAwait(false);
                continue;
            }

            if (node.Kind == BlueprintNodeKind.Join)
            {
                var joinOutType = node.JoinOutputType!;
                var executor = JoinExecutorCache.Get(joinOutType);
                await executor(node, flowContext).ConfigureAwait(false);
                continue;
            }

            throw new InvalidOperationException($"Unsupported node kind: '{node.Kind}'.");
        }

        if (IsDeadlineExceeded(flowContext.Deadline))
        {
            return Outcome<TResp>.Timeout(DeadlineExceededCode);
        }

        if (flowContext.CancellationToken.IsCancellationRequested)
        {
            return Outcome<TResp>.Canceled(UpstreamCanceledCode);
        }

        var lastNode = nodes[nodeCount - 1];

        EnsureFinalOutputType<TReq, TResp>(blueprint, lastNode);

        if (!flowContext.TryGetNodeOutcome<TResp>(lastNode.Name, out var finalOutcome))
        {
            throw new InvalidOperationException($"Outcome for node '{lastNode.Name}' has not been recorded.");
        }

        return finalOutcome;
    }

    public async ValueTask<Outcome<TResp>> ExecuteAsync<TReq, TResp>(
        PlanTemplate<TReq, TResp> template,
        TReq request,
        FlowContext context)
    {
        if (template is null)
        {
            throw new ArgumentNullException(nameof(template));
        }

        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        if (!typeof(TReq).IsValueType && request is null)
        {
            throw new ArgumentNullException(nameof(request));
        }

        var flowName = template.Name;
        var recordFlowMetrics = FlowMetricsV1.IsFlowEnabled;
        var recordStepMetrics = FlowMetricsV1.IsStepEnabled;
        var recordStepSkipReasonMetrics = FlowMetricsV1.IsStepSkipReasonEnabled;
        var recordJoinMetrics = FlowMetricsV1.IsJoinEnabled;
        var flowStartTimestamp = recordFlowMetrics ? FlowMetricsV1.StartFlowTimer() : 0;

        Outcome<TResp> ReturnWithFlowMetrics(Outcome<TResp> outcome)
        {
            if (recordFlowMetrics)
            {
                FlowMetricsV1.RecordFlow(flowStartTimestamp, flowName, outcome.Kind);
            }

            return outcome;
        }

        var nodes = template.Nodes;
        var nodeCount = nodes.Count;

        if (nodeCount == 0)
        {
            throw new InvalidOperationException($"Flow '{flowName}' must contain at least one node.");
        }

        if (IsDeadlineExceeded(context.Deadline))
        {
            return ReturnWithFlowMetrics(Outcome<TResp>.Timeout(DeadlineExceededCode));
        }

        if (context.CancellationToken.IsCancellationRequested)
        {
            return ReturnWithFlowMetrics(Outcome<TResp>.Canceled(UpstreamCanceledCode));
        }

        context.PrepareForExecution(template.NodeNameToIndex, nodeCount);

        var activitySource = FlowActivitySource.Instance;

        if (activitySource.HasListeners())
        {
            using var flowActivity = activitySource.StartActivity(FlowActivitySource.FlowActivityName, ActivityKind.Internal);

            if (flowActivity is not null)
            {
                var planHashTagValue = template.PlanHash.ToString(FlowActivitySource.PlanHashFormat);
                flowActivity.SetTag(FlowActivitySource.TagFlowName, flowName);
                flowActivity.SetTag(FlowActivitySource.TagPlanHash, planHashTagValue);

                string? configVersionTagValue = null;
                if (context.TryGetConfigVersion(out var configVersion))
                {
                    configVersionTagValue = configVersion.ToString(CultureInfo.InvariantCulture);
                    flowActivity.SetTag(FlowActivitySource.TagConfigVersion, configVersionTagValue);
                }

                for (var i = 0; i < nodeCount; i++)
                {
                    if (IsDeadlineExceeded(context.Deadline))
                    {
                        return ReturnWithFlowMetrics(Outcome<TResp>.Timeout(DeadlineExceededCode));
                    }

                    if (context.CancellationToken.IsCancellationRequested)
                    {
                        return ReturnWithFlowMetrics(Outcome<TResp>.Canceled(UpstreamCanceledCode));
                    }

                    var node = nodes[i];
                    var nodeStartTimestamp = 0L;
                    var recordNodeMetrics = false;
                    var recordSkipReasonMetric = false;

                    Activity? nodeActivity;

                    if (node.Kind == BlueprintNodeKind.Step)
                    {
                        recordNodeMetrics = recordStepMetrics;
                        recordSkipReasonMetric = recordStepSkipReasonMetrics;
                        nodeActivity = activitySource.StartActivity(FlowActivitySource.StepActivityName, ActivityKind.Internal);
                    }
                    else if (node.Kind == BlueprintNodeKind.Join)
                    {
                        recordNodeMetrics = recordJoinMetrics;
                        nodeActivity = activitySource.StartActivity(FlowActivitySource.JoinActivityName, ActivityKind.Internal);
                    }
                    else
                    {
                        throw new InvalidOperationException($"Unsupported node kind: '{node.Kind}'.");
                    }

                    if (nodeActivity is not null)
                    {
                        nodeActivity.SetTag(FlowActivitySource.TagFlowName, flowName);
                        nodeActivity.SetTag(FlowActivitySource.TagPlanHash, planHashTagValue);
                        if (configVersionTagValue is not null)
                        {
                            nodeActivity.SetTag(FlowActivitySource.TagConfigVersion, configVersionTagValue);
                        }
                        nodeActivity.SetTag(FlowActivitySource.TagNodeName, node.Name);
                        nodeActivity.SetTag(FlowActivitySource.TagNodeKind, FlowActivitySource.GetNodeKindTagValue(node.Kind));

                        if (!string.IsNullOrEmpty(node.StageName))
                        {
                            nodeActivity.SetTag(FlowActivitySource.TagStageName, node.StageName);
                        }

                        if (node.Kind == BlueprintNodeKind.Step && !string.IsNullOrEmpty(node.ModuleType))
                        {
                            nodeActivity.SetTag(FlowActivitySource.TagModuleType, node.ModuleType);
                        }
                    }

                    try
                    {
                        if (recordNodeMetrics)
                        {
                            nodeStartTimestamp = node.Kind == BlueprintNodeKind.Step
                                ? FlowMetricsV1.StartStepTimer()
                                : FlowMetricsV1.StartJoinTimer();
                        }

                        if (node.Kind == BlueprintNodeKind.Step)
                        {
                            var outType = node.OutputType;
                            var executor = StepTemplateExecutorCache<TReq>.Get(outType);
                            await executor(this, node, request, context).ConfigureAwait(false);
                            continue;
                        }

                        var joinOutType = node.OutputType;
                        var joinExecutor = JoinTemplateExecutorCache.Get(joinOutType);
                        await joinExecutor(node, context).ConfigureAwait(false);
                    }
                    finally
                    {
                        if ((nodeActivity is not null || recordNodeMetrics || recordSkipReasonMetric)
                            && context.TryGetNodeOutcomeMetadata(node.Index, out var kind, out var code))
                        {
                            if (nodeActivity is not null)
                            {
                                nodeActivity.SetTag(FlowActivitySource.TagOutcomeKind, FlowActivitySource.GetOutcomeKindTagValue(kind));
                                nodeActivity.SetTag(FlowActivitySource.TagOutcomeCode, code);
                            }

                            if (recordNodeMetrics)
                            {
                                if (node.Kind == BlueprintNodeKind.Step)
                                {
                                    FlowMetricsV1.RecordStep(nodeStartTimestamp, flowName, node.ModuleType!, kind);
                                }
                                else if (node.Kind == BlueprintNodeKind.Join)
                                {
                                    FlowMetricsV1.RecordJoin(nodeStartTimestamp, flowName, kind);
                                }
                            }

                            if (recordSkipReasonMetric && kind == OutcomeKind.Skipped)
                            {
                                FlowMetricsV1.RecordStepSkipReason(flowName, code);
                            }
                        }

                        nodeActivity?.Dispose();
                    }
                }

                if (IsDeadlineExceeded(context.Deadline))
                {
                    return ReturnWithFlowMetrics(Outcome<TResp>.Timeout(DeadlineExceededCode));
                }

                if (context.CancellationToken.IsCancellationRequested)
                {
                    return ReturnWithFlowMetrics(Outcome<TResp>.Canceled(UpstreamCanceledCode));
                }

                var lastNode = nodes[nodeCount - 1];

                EnsureFinalOutputType<TReq, TResp>(template, lastNode);

                if (!context.TryGetNodeOutcome<TResp>(lastNode.Name, out var finalOutcome))
                {
                    throw new InvalidOperationException($"Outcome for node '{lastNode.Name}' has not been recorded.");
                }

                return ReturnWithFlowMetrics(finalOutcome);
            }
        }

        for (var i = 0; i < nodeCount; i++)
        {
            if (IsDeadlineExceeded(context.Deadline))
            {
                return ReturnWithFlowMetrics(Outcome<TResp>.Timeout(DeadlineExceededCode));
            }

            if (context.CancellationToken.IsCancellationRequested)
            {
                return ReturnWithFlowMetrics(Outcome<TResp>.Canceled(UpstreamCanceledCode));
            }

            var node = nodes[i];

            if (node.Kind == BlueprintNodeKind.Step)
            {
                var nodeStartTimestamp = recordStepMetrics ? FlowMetricsV1.StartStepTimer() : 0;
                var outType = node.OutputType;
                var executor = StepTemplateExecutorCache<TReq>.Get(outType);
                await executor(this, node, request, context).ConfigureAwait(false);

                if ((recordStepMetrics || recordStepSkipReasonMetrics)
                    && context.TryGetNodeOutcomeMetadata(node.Index, out var kind, out var code))
                {
                    if (recordStepMetrics)
                    {
                        FlowMetricsV1.RecordStep(nodeStartTimestamp, flowName, node.ModuleType!, kind);
                    }

                    if (recordStepSkipReasonMetrics && kind == OutcomeKind.Skipped)
                    {
                        FlowMetricsV1.RecordStepSkipReason(flowName, code);
                    }
                }

                continue;
            }

            if (node.Kind == BlueprintNodeKind.Join)
            {
                var nodeStartTimestamp = recordJoinMetrics ? FlowMetricsV1.StartJoinTimer() : 0;
                var joinOutType = node.OutputType;
                var executor = JoinTemplateExecutorCache.Get(joinOutType);
                await executor(node, context).ConfigureAwait(false);

                if (recordJoinMetrics && context.TryGetNodeOutcomeMetadata(node.Index, out var kind, out _))
                {
                    FlowMetricsV1.RecordJoin(nodeStartTimestamp, flowName, kind);
                }

                continue;
            }

            throw new InvalidOperationException($"Unsupported node kind: '{node.Kind}'.");
        }

        if (IsDeadlineExceeded(context.Deadline))
        {
            return ReturnWithFlowMetrics(Outcome<TResp>.Timeout(DeadlineExceededCode));
        }

        if (context.CancellationToken.IsCancellationRequested)
        {
            return ReturnWithFlowMetrics(Outcome<TResp>.Canceled(UpstreamCanceledCode));
        }

        var finalNode = nodes[nodeCount - 1];

        EnsureFinalOutputType<TReq, TResp>(template, finalNode);

        if (!context.TryGetNodeOutcome<TResp>(finalNode.Name, out var finalOutcomeResult))
        {
            throw new InvalidOperationException($"Outcome for node '{finalNode.Name}' has not been recorded.");
        }

        return ReturnWithFlowMetrics(finalOutcomeResult);
    }

    private static void EnsureFinalOutputType<TReq, TResp>(FlowBlueprint<TReq, TResp> blueprint, BlueprintNode lastNode)
    {
        if (lastNode.Kind == BlueprintNodeKind.Join)
        {
            if (lastNode.JoinOutputType != typeof(TResp))
            {
                throw new InvalidOperationException(
                    $"Flow '{blueprint.Name}' final node '{lastNode.Name}' has output type '{lastNode.JoinOutputType}', not '{typeof(TResp)}'.");
            }

            return;
        }

        if (lastNode.Kind == BlueprintNodeKind.Step)
        {
            throw new InvalidOperationException(
                $"Flow '{blueprint.Name}' final node '{lastNode.Name}' must be a join that produces '{typeof(TResp)}'.");
        }

        throw new InvalidOperationException(
            $"Flow '{blueprint.Name}' final node '{lastNode.Name}' has unsupported kind '{lastNode.Kind}'.");
    }

    private static void EnsureFinalOutputType<TReq, TResp>(PlanTemplate<TReq, TResp> template, PlanNodeTemplate lastNode)
    {
        if (lastNode.Kind == BlueprintNodeKind.Join)
        {
            if (lastNode.OutputType != typeof(TResp))
            {
                throw new InvalidOperationException(
                    $"Flow '{template.Name}' final node '{lastNode.Name}' has output type '{lastNode.OutputType}', not '{typeof(TResp)}'.");
            }

            return;
        }

        if (lastNode.Kind == BlueprintNodeKind.Step)
        {
            throw new InvalidOperationException(
                $"Flow '{template.Name}' final node '{lastNode.Name}' must be a join that produces '{typeof(TResp)}'.");
        }

        throw new InvalidOperationException(
            $"Flow '{template.Name}' final node '{lastNode.Name}' has unsupported kind '{lastNode.Kind}'.");
    }

    private static async ValueTask ExecuteStepAsyncCore<TArgs, TOut>(
        ExecutionEngine engine,
        BlueprintNode node,
        TArgs args,
        FlowContext flowContext)
    {
        var moduleType = node.ModuleType!;
        var module = engine._catalog.Create<TArgs, TOut>(moduleType, flowContext.Services);

        Outcome<TOut> outcome;

        try
        {
            outcome = await module.ExecuteAsync(new ModuleContext<TArgs>(node.Name, moduleType, args, flowContext))
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            outcome = IsDeadlineExceeded(flowContext.Deadline)
                ? Outcome<TOut>.Timeout(DeadlineExceededCode)
                : Outcome<TOut>.Canceled(UpstreamCanceledCode);
        }
        catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
        {
            outcome = Outcome<TOut>.Error(UnhandledExceptionCode);
        }

        flowContext.RecordNodeOutcome(node.Index, node.Name, outcome);
    }

    private static async ValueTask ExecuteStepTemplateAsyncCore<TArgs, TOut>(
        ExecutionEngine engine,
        PlanNodeTemplate node,
        TArgs args,
        FlowContext flowContext)
    {
        var moduleType = node.ModuleType!;
        var module = engine._catalog.Create<TArgs, TOut>(moduleType, flowContext.Services);

        Outcome<TOut> outcome;

        try
        {
            outcome = await module.ExecuteAsync(new ModuleContext<TArgs>(node.Name, moduleType, args, flowContext))
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            outcome = IsDeadlineExceeded(flowContext.Deadline)
                ? Outcome<TOut>.Timeout(DeadlineExceededCode)
                : Outcome<TOut>.Canceled(UpstreamCanceledCode);
        }
        catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
        {
            outcome = Outcome<TOut>.Error(UnhandledExceptionCode);
        }

        flowContext.RecordNodeOutcome(node.Index, node.Name, outcome);
    }

    private static async ValueTask ExecuteJoinAsyncCore<TOut>(BlueprintNode node, FlowContext flowContext)
    {
        var join = (Func<FlowContext, ValueTask<Outcome<TOut>>>)node.Join!;

        Outcome<TOut> outcome;

        try
        {
            outcome = await join(flowContext).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            outcome = IsDeadlineExceeded(flowContext.Deadline)
                ? Outcome<TOut>.Timeout(DeadlineExceededCode)
                : Outcome<TOut>.Canceled(UpstreamCanceledCode);
        }
        catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
        {
            outcome = Outcome<TOut>.Error(UnhandledExceptionCode);
        }

        flowContext.RecordNodeOutcome(node.Index, node.Name, outcome);
    }

    private static async ValueTask ExecuteJoinTemplateAsyncCore<TOut>(PlanNodeTemplate node, FlowContext flowContext)
    {
        var join = (Func<FlowContext, ValueTask<Outcome<TOut>>>)node.Join!;

        Outcome<TOut> outcome;

        try
        {
            outcome = await join(flowContext).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            outcome = IsDeadlineExceeded(flowContext.Deadline)
                ? Outcome<TOut>.Timeout(DeadlineExceededCode)
                : Outcome<TOut>.Canceled(UpstreamCanceledCode);
        }
        catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
        {
            outcome = Outcome<TOut>.Error(UnhandledExceptionCode);
        }

        flowContext.RecordNodeOutcome(node.Index, node.Name, outcome);
    }

    private static class StepExecutorCache<TArgs>
    {
        private static readonly Lock _gate = new();
        private static Dictionary<Type, Func<ExecutionEngine, BlueprintNode, TArgs, FlowContext, ValueTask>>? _cache;
        private static readonly MethodInfo CoreMethod =
            typeof(ExecutionEngine).GetMethod(nameof(ExecuteStepAsyncCore), BindingFlags.NonPublic | BindingFlags.Static)!;

        public static Func<ExecutionEngine, BlueprintNode, TArgs, FlowContext, ValueTask> Get(Type outType)
        {
            var cache = Volatile.Read(ref _cache);

            if (cache is not null && cache.TryGetValue(outType, out var executor))
            {
                return executor;
            }

            lock (_gate)
            {
                cache = _cache;
                if (cache is not null && cache.TryGetValue(outType, out executor))
                {
                    return executor;
                }

                var closedCore = CoreMethod.MakeGenericMethod(typeof(TArgs), outType);
                executor = (Func<ExecutionEngine, BlueprintNode, TArgs, FlowContext, ValueTask>)closedCore.CreateDelegate(
                    typeof(Func<ExecutionEngine, BlueprintNode, TArgs, FlowContext, ValueTask>));

                Dictionary<Type, Func<ExecutionEngine, BlueprintNode, TArgs, FlowContext, ValueTask>> newCache;

                if (cache is null || cache.Count == 0)
                {
                    newCache = new Dictionary<Type, Func<ExecutionEngine, BlueprintNode, TArgs, FlowContext, ValueTask>>(1);
                }
                else
                {
                    newCache = new Dictionary<Type, Func<ExecutionEngine, BlueprintNode, TArgs, FlowContext, ValueTask>>(
                        cache.Count + 1);

                    foreach (var pair in cache)
                    {
                        newCache.Add(pair.Key, pair.Value);
                    }
                }

                newCache.Add(outType, executor);
                Volatile.Write(ref _cache, newCache);
                return executor;
            }
        }
    }

    private static class JoinExecutorCache
    {
        private static readonly Lock _gate = new();
        private static Dictionary<Type, Func<BlueprintNode, FlowContext, ValueTask>>? _cache;
        private static readonly MethodInfo CoreMethod =
            typeof(ExecutionEngine).GetMethod(nameof(ExecuteJoinAsyncCore), BindingFlags.NonPublic | BindingFlags.Static)!;

        public static Func<BlueprintNode, FlowContext, ValueTask> Get(Type outType)
        {
            var cache = Volatile.Read(ref _cache);

            if (cache is not null && cache.TryGetValue(outType, out var executor))
            {
                return executor;
            }

            lock (_gate)
            {
                cache = _cache;
                if (cache is not null && cache.TryGetValue(outType, out executor))
                {
                    return executor;
                }

                var closedCore = CoreMethod.MakeGenericMethod(outType);
                executor = (Func<BlueprintNode, FlowContext, ValueTask>)closedCore.CreateDelegate(
                    typeof(Func<BlueprintNode, FlowContext, ValueTask>));

                Dictionary<Type, Func<BlueprintNode, FlowContext, ValueTask>> newCache;

                if (cache is null || cache.Count == 0)
                {
                    newCache = new Dictionary<Type, Func<BlueprintNode, FlowContext, ValueTask>>(1);
                }
                else
                {
                    newCache = new Dictionary<Type, Func<BlueprintNode, FlowContext, ValueTask>>(cache.Count + 1);

                    foreach (var pair in cache)
                    {
                        newCache.Add(pair.Key, pair.Value);
                    }
                }

                newCache.Add(outType, executor);
                Volatile.Write(ref _cache, newCache);
                return executor;
            }
        }
    }

    private static class StepTemplateExecutorCache<TArgs>
    {
        private static readonly Lock _gate = new();
        private static Dictionary<Type, Func<ExecutionEngine, PlanNodeTemplate, TArgs, FlowContext, ValueTask>>? _cache;
        private static readonly MethodInfo CoreMethod =
            typeof(ExecutionEngine).GetMethod(nameof(ExecuteStepTemplateAsyncCore), BindingFlags.NonPublic | BindingFlags.Static)!;

        public static Func<ExecutionEngine, PlanNodeTemplate, TArgs, FlowContext, ValueTask> Get(Type outType)
        {
            var cache = Volatile.Read(ref _cache);

            if (cache is not null && cache.TryGetValue(outType, out var executor))
            {
                return executor;
            }

            lock (_gate)
            {
                cache = _cache;
                if (cache is not null && cache.TryGetValue(outType, out executor))
                {
                    return executor;
                }

                var closedCore = CoreMethod.MakeGenericMethod(typeof(TArgs), outType);
                executor = (Func<ExecutionEngine, PlanNodeTemplate, TArgs, FlowContext, ValueTask>)closedCore.CreateDelegate(
                    typeof(Func<ExecutionEngine, PlanNodeTemplate, TArgs, FlowContext, ValueTask>));

                Dictionary<Type, Func<ExecutionEngine, PlanNodeTemplate, TArgs, FlowContext, ValueTask>> newCache;

                if (cache is null || cache.Count == 0)
                {
                    newCache = new Dictionary<Type, Func<ExecutionEngine, PlanNodeTemplate, TArgs, FlowContext, ValueTask>>(1);
                }
                else
                {
                    newCache = new Dictionary<Type, Func<ExecutionEngine, PlanNodeTemplate, TArgs, FlowContext, ValueTask>>(
                        cache.Count + 1);

                    foreach (var pair in cache)
                    {
                        newCache.Add(pair.Key, pair.Value);
                    }
                }

                newCache.Add(outType, executor);
                Volatile.Write(ref _cache, newCache);
                return executor;
            }
        }
    }

    private static class JoinTemplateExecutorCache
    {
        private static readonly Lock _gate = new();
        private static Dictionary<Type, Func<PlanNodeTemplate, FlowContext, ValueTask>>? _cache;
        private static readonly MethodInfo CoreMethod =
            typeof(ExecutionEngine).GetMethod(nameof(ExecuteJoinTemplateAsyncCore), BindingFlags.NonPublic | BindingFlags.Static)!;

        public static Func<PlanNodeTemplate, FlowContext, ValueTask> Get(Type outType)
        {
            var cache = Volatile.Read(ref _cache);

            if (cache is not null && cache.TryGetValue(outType, out var executor))
            {
                return executor;
            }

            lock (_gate)
            {
                cache = _cache;
                if (cache is not null && cache.TryGetValue(outType, out executor))
                {
                    return executor;
                }

                var closedCore = CoreMethod.MakeGenericMethod(outType);
                executor = (Func<PlanNodeTemplate, FlowContext, ValueTask>)closedCore.CreateDelegate(
                    typeof(Func<PlanNodeTemplate, FlowContext, ValueTask>));

                Dictionary<Type, Func<PlanNodeTemplate, FlowContext, ValueTask>> newCache;

                if (cache is null || cache.Count == 0)
                {
                    newCache = new Dictionary<Type, Func<PlanNodeTemplate, FlowContext, ValueTask>>(1);
                }
                else
                {
                    newCache = new Dictionary<Type, Func<PlanNodeTemplate, FlowContext, ValueTask>>(cache.Count + 1);

                    foreach (var pair in cache)
                    {
                        newCache.Add(pair.Key, pair.Value);
                    }
                }

                newCache.Add(outType, executor);
                Volatile.Write(ref _cache, newCache);
                return executor;
            }
        }
    }
}
