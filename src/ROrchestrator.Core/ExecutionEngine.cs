using System.Reflection;
using System.Threading;
using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Core;

public sealed class ExecutionEngine
{
    public const string UnhandledExceptionCode = "UNHANDLED_EXCEPTION";

    private readonly ModuleCatalog _catalog;

    public ExecutionEngine(ModuleCatalog catalog)
    {
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
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

        flowContext.EnsureNodeOutcomesCapacity(nodeCount);

        for (var i = 0; i < nodeCount; i++)
        {
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

        var lastNode = nodes[nodeCount - 1];

        EnsureFinalOutputType<TReq, TResp>(blueprint, lastNode);

        if (!flowContext.TryGetNodeOutcome<TResp>(lastNode.Name, out var finalOutcome))
        {
            throw new InvalidOperationException($"Outcome for node '{lastNode.Name}' has not been recorded.");
        }

        return finalOutcome;
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
        catch (Exception)
        {
            outcome = Outcome<TOut>.Error(UnhandledExceptionCode);
        }

        flowContext.RecordNodeOutcome(node.Name, outcome);
    }

    private static async ValueTask ExecuteJoinAsyncCore<TOut>(BlueprintNode node, FlowContext flowContext)
    {
        var join = (Func<FlowContext, ValueTask<Outcome<TOut>>>)node.Join!;

        Outcome<TOut> outcome;

        try
        {
            outcome = await join(flowContext).ConfigureAwait(false);
        }
        catch (Exception)
        {
            outcome = Outcome<TOut>.Error(UnhandledExceptionCode);
        }

        flowContext.RecordNodeOutcome(node.Name, outcome);
    }

    private static class StepExecutorCache<TArgs>
    {
        private static readonly object Lock = new object();
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

            lock (Lock)
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
        private static readonly object Lock = new object();
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

            lock (Lock)
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
}
