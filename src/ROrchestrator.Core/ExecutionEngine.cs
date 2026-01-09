using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using ROrchestrator.Core.Blueprint;
using ROrchestrator.Core.Gates;
using ROrchestrator.Core.Observability;

namespace ROrchestrator.Core;

public sealed class ExecutionEngine
{
    public const string DeadlineExceededCode = "DEADLINE_EXCEEDED";
    public const string UnhandledExceptionCode = "UNHANDLED_EXCEPTION";
    public const string UpstreamCanceledCode = "UPSTREAM_CANCELED";

    public const string DisabledCode = "DISABLED";
    public const string GateFalseCode = "GATE_FALSE";
    public const string FanoutTrimCode = "FANOUT_TRIM";

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
        var execExplainCollector = context.ExecExplainCollector;
        execExplainCollector?.Clear();
        var recordFlowMetrics = FlowMetricsV1.IsFlowEnabled;
        var recordStepMetrics = FlowMetricsV1.IsStepEnabled;
        var recordStepSkipReasonMetrics = FlowMetricsV1.IsStepSkipReasonEnabled;
        var recordJoinMetrics = FlowMetricsV1.IsJoinEnabled;
        var flowStartTimestamp = recordFlowMetrics ? FlowMetricsV1.StartFlowTimer() : 0;

        Outcome<TResp> ReturnWithFlowMetrics(Outcome<TResp> outcome)
        {
            execExplainCollector?.Finish(context);
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
        execExplainCollector?.Start(flowName, template.PlanHash, nodes);

        using var patchEvaluation = MaybeEvaluatePatch(flowName, context);
        execExplainCollector?.RecordRouting(context.Variants, patchEvaluation?.OverlaysApplied);
        string? currentStageName = null;

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

                    if (patchEvaluation is not null)
                    {
                        var stageName = node.StageName;

                        if (string.IsNullOrEmpty(stageName))
                        {
                            currentStageName = null;
                        }
                        else if (!string.Equals(currentStageName, stageName, StringComparison.Ordinal))
                        {
                            currentStageName = stageName;
                            await ExecuteStageFanoutIfAnyAsync(stageName, patchEvaluation, context, execExplainCollector).ConfigureAwait(false);

                            if (IsDeadlineExceeded(context.Deadline))
                            {
                                return ReturnWithFlowMetrics(Outcome<TResp>.Timeout(DeadlineExceededCode));
                            }

                            if (context.CancellationToken.IsCancellationRequested)
                            {
                                return ReturnWithFlowMetrics(Outcome<TResp>.Canceled(UpstreamCanceledCode));
                            }
                        }
                    }

                    var nodeStartTimestamp = 0L;
                    var recordNodeMetrics = false;
                    var recordSkipReasonMetric = false;
                    var execExplainNodeStartTimestamp = 0L;

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
                        if (execExplainCollector is not null)
                        {
                            execExplainNodeStartTimestamp = Stopwatch.GetTimestamp();
                        }

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
                        var needsOutcomeMetadata = nodeActivity is not null
                            || recordNodeMetrics
                            || recordSkipReasonMetric
                            || execExplainCollector is not null;

                        if (needsOutcomeMetadata && context.TryGetNodeOutcomeMetadata(node.Index, out var kind, out var code))
                        {
                            if (execExplainCollector is not null)
                            {
                                var execExplainNodeEndTimestamp = Stopwatch.GetTimestamp();
                                execExplainCollector.RecordNode(node, execExplainNodeStartTimestamp, execExplainNodeEndTimestamp, kind, code);
                            }

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
                        else if (execExplainCollector is not null)
                        {
                            var execExplainNodeEndTimestamp = Stopwatch.GetTimestamp();
                            execExplainCollector.RecordNode(
                                node,
                                execExplainNodeStartTimestamp,
                                execExplainNodeEndTimestamp,
                                OutcomeKind.Unspecified,
                                string.Empty);
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

            if (patchEvaluation is not null)
            {
                var stageName = node.StageName;

                if (string.IsNullOrEmpty(stageName))
                {
                    currentStageName = null;
                }
                else if (!string.Equals(currentStageName, stageName, StringComparison.Ordinal))
                {
                    currentStageName = stageName;
                    await ExecuteStageFanoutIfAnyAsync(stageName, patchEvaluation, context, execExplainCollector).ConfigureAwait(false);

                    if (IsDeadlineExceeded(context.Deadline))
                    {
                        return ReturnWithFlowMetrics(Outcome<TResp>.Timeout(DeadlineExceededCode));
                    }

                    if (context.CancellationToken.IsCancellationRequested)
                    {
                        return ReturnWithFlowMetrics(Outcome<TResp>.Canceled(UpstreamCanceledCode));
                    }
                }
            }

            if (node.Kind == BlueprintNodeKind.Step)
            {
                var nodeStartTimestamp = recordStepMetrics ? FlowMetricsV1.StartStepTimer() : 0;
                var execExplainNodeStartTimestamp = execExplainCollector is not null ? Stopwatch.GetTimestamp() : 0;
                var outType = node.OutputType;
                var executor = StepTemplateExecutorCache<TReq>.Get(outType);
                await executor(this, node, request, context).ConfigureAwait(false);
                var execExplainNodeEndTimestamp = execExplainCollector is not null ? Stopwatch.GetTimestamp() : 0;

                var needsOutcomeMetadata = recordStepMetrics
                    || recordStepSkipReasonMetrics
                    || execExplainCollector is not null;

                if (needsOutcomeMetadata && context.TryGetNodeOutcomeMetadata(node.Index, out var kind, out var code))
                {
                    if (recordStepMetrics)
                    {
                        FlowMetricsV1.RecordStep(nodeStartTimestamp, flowName, node.ModuleType!, kind);
                    }

                    if (recordStepSkipReasonMetrics && kind == OutcomeKind.Skipped)
                    {
                        FlowMetricsV1.RecordStepSkipReason(flowName, code);
                    }

                    if (execExplainCollector is not null)
                    {
                        execExplainCollector.RecordNode(node, execExplainNodeStartTimestamp, execExplainNodeEndTimestamp, kind, code);
                    }
                }
                else if (execExplainCollector is not null)
                {
                    execExplainCollector.RecordNode(
                        node,
                        execExplainNodeStartTimestamp,
                        execExplainNodeEndTimestamp,
                        OutcomeKind.Unspecified,
                        string.Empty);
                }

                continue;
            }

            if (node.Kind == BlueprintNodeKind.Join)
            {
                var nodeStartTimestamp = recordJoinMetrics ? FlowMetricsV1.StartJoinTimer() : 0;
                var execExplainNodeStartTimestamp = execExplainCollector is not null ? Stopwatch.GetTimestamp() : 0;
                var joinOutType = node.OutputType;
                var executor = JoinTemplateExecutorCache.Get(joinOutType);
                await executor(node, context).ConfigureAwait(false);
                var execExplainNodeEndTimestamp = execExplainCollector is not null ? Stopwatch.GetTimestamp() : 0;

                var needsOutcomeMetadata = recordJoinMetrics || execExplainCollector is not null;

                if (needsOutcomeMetadata && context.TryGetNodeOutcomeMetadata(node.Index, out var kind, out var code))
                {
                    if (recordJoinMetrics)
                    {
                        FlowMetricsV1.RecordJoin(nodeStartTimestamp, flowName, kind);
                    }

                    if (execExplainCollector is not null)
                    {
                        execExplainCollector.RecordNode(node, execExplainNodeStartTimestamp, execExplainNodeEndTimestamp, kind, code);
                    }
                }
                else if (execExplainCollector is not null)
                {
                    execExplainCollector.RecordNode(
                        node,
                        execExplainNodeStartTimestamp,
                        execExplainNodeEndTimestamp,
                        OutcomeKind.Unspecified,
                        string.Empty);
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

    private static PatchEvaluatorV1.FlowPatchEvaluationV1? MaybeEvaluatePatch(string flowName, FlowContext context)
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        if (!context.TryGetConfigSnapshot(out var snapshot))
        {
            return null;
        }

        var patchJson = snapshot.PatchJson;
        if (patchJson.Length == 0)
        {
            return null;
        }

        return PatchEvaluatorV1.Evaluate(
            flowName,
            patchJson,
            new FlowRequestOptions(context.Variants, context.UserId, context.RequestAttributes));
    }

    private async ValueTask ExecuteStageFanoutIfAnyAsync(
        string stageName,
        PatchEvaluatorV1.FlowPatchEvaluationV1 patchEvaluation,
        FlowContext context,
        ExecExplainCollectorV1? execExplainCollector)
    {
        var stages = patchEvaluation.Stages;

        for (var i = 0; i < stages.Count; i++)
        {
            var stage = stages[i];

            if (string.Equals(stage.StageName, stageName, StringComparison.Ordinal))
            {
                await ExecuteStageFanoutAsync(patchEvaluation.FlowName, stageName, stage, context, execExplainCollector).ConfigureAwait(false);
                return;
            }
        }
    }

    private async ValueTask ExecuteStageFanoutAsync(
        string flowName,
        string stageName,
        PatchEvaluatorV1.StagePatchV1 stagePatch,
        FlowContext context,
        ExecExplainCollectorV1? execExplainCollector)
    {
        var modules = stagePatch.Modules;
        var moduleCount = modules.Count;

        if (moduleCount == 0)
        {
            return;
        }

        var fanoutMax = stagePatch.HasFanoutMax ? stagePatch.FanoutMax : int.MaxValue;

        if (fanoutMax < 0)
        {
            fanoutMax = 0;
        }

        var argsTypes = new Type[moduleCount];
        var outTypes = new Type[moduleCount];

        for (var i = 0; i < moduleCount; i++)
        {
            var moduleType = modules[i].ModuleType;

            if (!_catalog.TryGetSignature(moduleType, out var argsType, out var outType))
            {
                throw new InvalidOperationException($"Module type '{moduleType}' is not registered.");
            }

            argsTypes[i] = argsType;
            outTypes[i] = outType;
        }

        var results = new StageModuleOutcomeMetadata[moduleCount];
        var candidates = new StageModuleCandidate[moduleCount];
        var candidateCount = 0;

        for (var i = 0; i < moduleCount; i++)
        {
            var module = modules[i];

            if (!module.Enabled)
            {
                RecordStageModuleSkippedOutcome(outTypes[i], context, module.ModuleId, DisabledCode);
                results[i] = new StageModuleOutcomeMetadata(OutcomeKind.Skipped, DisabledCode, isOverride: false);
                continue;
            }

            if (module.HasGate && !EvaluateGateAllowed(module.Gate, context))
            {
                RecordStageModuleSkippedOutcome(outTypes[i], context, module.ModuleId, GateFalseCode);
                results[i] = new StageModuleOutcomeMetadata(OutcomeKind.Skipped, GateFalseCode, isOverride: false);
                continue;
            }

            candidates[candidateCount] = new StageModuleCandidate(moduleIndex: i, priority: module.Priority);
            candidateCount++;
        }

        SortStageModuleCandidates(candidates, candidateCount);

        var executeCount = candidateCount;

        if (fanoutMax < executeCount)
        {
            executeCount = fanoutMax;
        }

        if (executeCount < 0)
        {
            executeCount = 0;
        }

        var executeTasks = executeCount == 0 ? Array.Empty<ValueTask<StageModuleOutcomeMetadata>>() : new ValueTask<StageModuleOutcomeMetadata>[executeCount];
        var executeIndices = executeCount == 0 ? Array.Empty<int>() : new int[executeCount];
        var taskIndex = 0;

        for (var rank = 0; rank < candidateCount; rank++)
        {
            var moduleIndex = candidates[rank].ModuleIndex;
            var module = modules[moduleIndex];

            if (rank < executeCount)
            {
                var executor = StageModuleExecutorCache.Get(argsTypes[moduleIndex], outTypes[moduleIndex]);
                executeTasks[taskIndex] = executor(this, flowName, stageName, module.ModuleId, module.ModuleType, module.Args, context);
                executeIndices[taskIndex] = moduleIndex;
                taskIndex++;
                continue;
            }

            RecordStageModuleSkippedOutcome(outTypes[moduleIndex], context, module.ModuleId, FanoutTrimCode);
            results[moduleIndex] = new StageModuleOutcomeMetadata(OutcomeKind.Skipped, FanoutTrimCode, isOverride: false);
        }

        for (var i = 0; i < taskIndex; i++)
        {
            results[executeIndices[i]] = await executeTasks[i].ConfigureAwait(false);
        }

        var invocationSink = context.FlowTestInvocationSink;

        if (invocationSink is not null)
        {
            for (var i = 0; i < taskIndex; i++)
            {
                var moduleIndex = executeIndices[i];
                var module = modules[moduleIndex];
                var meta = results[moduleIndex];
                invocationSink.Record(module.ModuleId, module.ModuleType, meta.IsOverride, meta.Kind, meta.Code);
            }
        }

        if (execExplainCollector is not null)
        {
            for (var i = 0; i < moduleCount; i++)
            {
                var module = modules[i];
                var meta = results[i];
                execExplainCollector.RecordStageModule(stageName, module.ModuleId, module.ModuleType, module.Priority, meta.Kind, meta.Code, meta.IsOverride);
            }
        }

        RecordStageFanoutSnapshot(stageName, modules, executeIndices, taskIndex, results, context);
    }

    private static void RecordStageModuleSkippedOutcome(Type outType, FlowContext context, string moduleId, string code)
    {
        var recorder = StageModuleSkipOutcomeCache.Get(outType);
        recorder(context, moduleId, code);
    }

    private static void RecordStageFanoutSnapshot(
        string stageName,
        IReadOnlyList<PatchEvaluatorV1.StageModulePatchV1> modules,
        int[] executeIndices,
        int executeCount,
        StageModuleOutcomeMetadata[] results,
        FlowContext context)
    {
        if (executeCount < 0)
        {
            executeCount = 0;
        }

        var moduleCount = modules.Count;

        var enabledModuleIds = executeCount == 0 ? Array.Empty<string>() : new string[executeCount];

        for (var i = 0; i < executeCount; i++)
        {
            enabledModuleIds[i] = modules[executeIndices[i]].ModuleId;
        }

        StageFanoutSkippedModule[] skipped;

        if (moduleCount == executeCount)
        {
            skipped = Array.Empty<StageFanoutSkippedModule>();
        }
        else
        {
            var executedFlags = executeCount == 0 ? null : new bool[moduleCount];

            if (executedFlags is not null)
            {
                for (var i = 0; i < executeCount; i++)
                {
                    executedFlags[executeIndices[i]] = true;
                }
            }

            skipped = new StageFanoutSkippedModule[moduleCount - executeCount];
            var skippedCount = 0;

            for (var i = 0; i < moduleCount; i++)
            {
                if (executedFlags is not null && executedFlags[i])
                {
                    continue;
                }

                skipped[skippedCount] = new StageFanoutSkippedModule(modules[i].ModuleId, results[i].Code);
                skippedCount++;
            }

            if (skippedCount != skipped.Length)
            {
                var trimmed = new StageFanoutSkippedModule[skippedCount];
                Array.Copy(skipped, 0, trimmed, 0, skippedCount);
                skipped = trimmed;
            }
        }

        context.RecordStageFanoutSnapshot(stageName, enabledModuleIds, skipped);
    }

    private static bool EvaluateGateAllowed(JsonElement gateElement, FlowContext context)
    {
        if (!GateJsonV1.TryParseOptional(gateElement, "$.gate", out var gate, out var finding))
        {
            throw new InvalidOperationException(finding.Message);
        }

        if (gate is null)
        {
            return true;
        }

        return GateEvaluator.Evaluate(gate, context).Allowed;
    }

    private readonly struct StageModuleOutcomeMetadata
    {
        public OutcomeKind Kind { get; }

        public string Code { get; }

        public bool IsOverride { get; }

        public StageModuleOutcomeMetadata(OutcomeKind kind, string code, bool isOverride)
        {
            Kind = kind;
            Code = code;
            IsOverride = isOverride;
        }
    }

    private readonly struct StageModuleCandidate
    {
        public int ModuleIndex { get; }

        public int Priority { get; }

        public StageModuleCandidate(int moduleIndex, int priority)
        {
            ModuleIndex = moduleIndex;
            Priority = priority;
        }
    }

    private static void SortStageModuleCandidates(StageModuleCandidate[] candidates, int count)
    {
        for (var i = 1; i < count; i++)
        {
            var candidate = candidates[i];
            var j = i - 1;

            while (j >= 0
                   && (candidates[j].Priority < candidate.Priority
                       || (candidates[j].Priority == candidate.Priority && candidates[j].ModuleIndex > candidate.ModuleIndex)))
            {
                candidates[j + 1] = candidates[j];
                j--;
            }

            candidates[j + 1] = candidate;
        }
    }

    private static async ValueTask<StageModuleOutcomeMetadata> ExecuteStageModuleAsyncCore<TArgs, TOut>(
        ExecutionEngine engine,
        string flowName,
        string stageName,
        string moduleId,
        string moduleType,
        JsonElement argsJson,
        FlowContext flowContext)
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        if (string.IsNullOrEmpty(stageName))
        {
            throw new ArgumentException("StageName must be non-empty.", nameof(stageName));
        }

        if (string.IsNullOrEmpty(moduleId))
        {
            throw new ArgumentException("ModuleId must be non-empty.", nameof(moduleId));
        }

        if (string.IsNullOrEmpty(moduleType))
        {
            throw new ArgumentException("ModuleType must be non-empty.", nameof(moduleType));
        }

        if (engine is null)
        {
            throw new ArgumentNullException(nameof(engine));
        }

        if (flowContext is null)
        {
            throw new ArgumentNullException(nameof(flowContext));
        }

        Activity? moduleActivity = null;
        var activitySource = FlowActivitySource.Instance;

        if (activitySource.HasListeners())
        {
            moduleActivity = activitySource.StartActivity(FlowActivitySource.StageFanoutModuleActivityName, ActivityKind.Internal);

            if (moduleActivity is not null)
            {
                moduleActivity.SetTag(FlowActivitySource.TagFlowName, flowName);
                moduleActivity.SetTag(FlowActivitySource.TagStageName, stageName);
                moduleActivity.SetTag(FlowActivitySource.TagModuleId, moduleId);
                moduleActivity.SetTag(FlowActivitySource.TagModuleType, moduleType);

                if (flowContext.TryGetConfigVersion(out var configVersion))
                {
                    moduleActivity.SetTag(FlowActivitySource.TagConfigVersion, configVersion.ToString(CultureInfo.InvariantCulture));
                }
            }
        }

        var moduleStartTimestamp = FlowMetricsV1.StartStageFanoutModuleTimer();

        var overrideProvider = flowContext.FlowTestOverrideProvider;
        Outcome<TOut> outcome = default;
        var recordedOutcome = false;

        try
        {
            if (overrideProvider is not null && overrideProvider.TryGetOverride(moduleId, out var overrideEntry))
            {
                if (overrideEntry is FlowTestOverrideOutcome<TOut> fixedOverride)
                {
                    outcome = fixedOverride.Outcome;
                }
                else if (overrideEntry is FlowTestOverrideCompute<TArgs, TOut> computeOverride)
                {
                    var args = argsJson.Deserialize<TArgs>();

                    if (!typeof(TArgs).IsValueType && args is null)
                    {
                        throw new InvalidOperationException($"Module '{moduleId}' args binding produced null.");
                    }

                    var moduleContext = new ModuleContext<TArgs>(moduleId, moduleType, args!, flowContext);

                    try
                    {
                        outcome = await computeOverride.Compute(moduleContext).ConfigureAwait(false);
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
                }
                else
                {
                    throw new InvalidOperationException(
                        $"Override for moduleId '{moduleId}' has a different signature. Expected args '{typeof(TArgs)}' and output '{typeof(TOut)}'.");
                }

                flowContext.RecordNodeOutcome(moduleId, outcome);
                recordedOutcome = true;
                return new StageModuleOutcomeMetadata(outcome.Kind, outcome.Code, isOverride: true);
            }

            var argsValue = argsJson.Deserialize<TArgs>();

            if (!typeof(TArgs).IsValueType && argsValue is null)
            {
                throw new InvalidOperationException($"Module '{moduleId}' args binding produced null.");
            }

            var module = engine._catalog.Create<TArgs, TOut>(moduleType, flowContext.Services);

            try
            {
                outcome = await module.ExecuteAsync(new ModuleContext<TArgs>(moduleId, moduleType, argsValue!, flowContext))
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

            flowContext.RecordNodeOutcome(moduleId, outcome);
            recordedOutcome = true;
            return new StageModuleOutcomeMetadata(outcome.Kind, outcome.Code, isOverride: false);
        }
        finally
        {
            if (recordedOutcome)
            {
                FlowMetricsV1.RecordStageFanoutModule(moduleStartTimestamp, flowName, stageName, moduleType, outcome.Kind);

                if (moduleActivity is not null)
                {
                    moduleActivity.SetTag(FlowActivitySource.TagOutcomeKind, FlowActivitySource.GetOutcomeKindTagValue(outcome.Kind));
                    moduleActivity.SetTag(FlowActivitySource.TagOutcomeCode, outcome.Code);
                }
            }

            moduleActivity?.Dispose();
        }
    }

    private static void RecordStageModuleSkippedOutcomeCore<TOut>(FlowContext context, string moduleId, string code)
    {
        context.RecordNodeOutcome(moduleId, Outcome<TOut>.Skipped(code));
    }

    private static class StageModuleSkipOutcomeCache
    {
        private static readonly Lock _gate = new();
        private static Dictionary<Type, Action<FlowContext, string, string>>? _cache;
        private static readonly MethodInfo CoreMethod =
            typeof(ExecutionEngine).GetMethod(nameof(RecordStageModuleSkippedOutcomeCore), BindingFlags.NonPublic | BindingFlags.Static)!;

        public static Action<FlowContext, string, string> Get(Type outType)
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
                executor = (Action<FlowContext, string, string>)closedCore.CreateDelegate(typeof(Action<FlowContext, string, string>));

                Dictionary<Type, Action<FlowContext, string, string>> newCache;

                if (cache is null || cache.Count == 0)
                {
                    newCache = new Dictionary<Type, Action<FlowContext, string, string>>(1);
                }
                else
                {
                    newCache = new Dictionary<Type, Action<FlowContext, string, string>>(cache.Count + 1);

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

    private static class StageModuleExecutorCache
    {
        private static readonly Lock _gate = new();
        private static Dictionary<StageModuleExecutorCacheKey, Func<ExecutionEngine, string, string, string, string, JsonElement, FlowContext, ValueTask<StageModuleOutcomeMetadata>>>? _cache;
        private static readonly MethodInfo CoreMethod =
            typeof(ExecutionEngine).GetMethod(nameof(ExecuteStageModuleAsyncCore), BindingFlags.NonPublic | BindingFlags.Static)!;

        public static Func<ExecutionEngine, string, string, string, string, JsonElement, FlowContext, ValueTask<StageModuleOutcomeMetadata>> Get(
            Type argsType,
            Type outType)
        {
            var key = new StageModuleExecutorCacheKey(argsType, outType);
            var cache = Volatile.Read(ref _cache);

            if (cache is not null && cache.TryGetValue(key, out var executor))
            {
                return executor;
            }

            lock (_gate)
            {
                cache = _cache;

                if (cache is not null && cache.TryGetValue(key, out executor))
                {
                    return executor;
                }

                var closedCore = CoreMethod.MakeGenericMethod(argsType, outType);
                executor =
                    (Func<ExecutionEngine, string, string, string, string, JsonElement, FlowContext, ValueTask<StageModuleOutcomeMetadata>>)closedCore.CreateDelegate(
                        typeof(Func<ExecutionEngine, string, string, string, string, JsonElement, FlowContext, ValueTask<StageModuleOutcomeMetadata>>));

                Dictionary<StageModuleExecutorCacheKey, Func<ExecutionEngine, string, string, string, string, JsonElement, FlowContext, ValueTask<StageModuleOutcomeMetadata>>> newCache;

                if (cache is null || cache.Count == 0)
                {
                    newCache =
                        new Dictionary<StageModuleExecutorCacheKey, Func<ExecutionEngine, string, string, string, string, JsonElement, FlowContext, ValueTask<StageModuleOutcomeMetadata>>>(1);
                }
                else
                {
                    newCache = new Dictionary<StageModuleExecutorCacheKey, Func<ExecutionEngine, string, string, string, string, JsonElement, FlowContext, ValueTask<StageModuleOutcomeMetadata>>>(
                        cache.Count + 1);

                    foreach (var pair in cache)
                    {
                        newCache.Add(pair.Key, pair.Value);
                    }
                }

                newCache.Add(key, executor);
                Volatile.Write(ref _cache, newCache);
                return executor;
            }
        }

        private readonly struct StageModuleExecutorCacheKey : IEquatable<StageModuleExecutorCacheKey>
        {
            public Type ArgsType { get; }

            public Type OutType { get; }

            public StageModuleExecutorCacheKey(Type argsType, Type outType)
            {
                ArgsType = argsType;
                OutType = outType;
            }

            public bool Equals(StageModuleExecutorCacheKey other)
            {
                return ArgsType == other.ArgsType && OutType == other.OutType;
            }

            public override bool Equals(object? obj)
            {
                return obj is StageModuleExecutorCacheKey other && Equals(other);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (ArgsType.GetHashCode() * 397) ^ OutType.GetHashCode();
                }
            }
        }
    }

    private static async ValueTask ExecuteStepAsyncCore<TArgs, TOut>(
        ExecutionEngine engine,
        BlueprintNode node,
        TArgs args,
        FlowContext flowContext)
    {
        var moduleType = node.ModuleType!;

        var overrideProvider = flowContext.FlowTestOverrideProvider;
        if (overrideProvider is not null && overrideProvider.TryGetOverride(node.Name, out var overrideEntry))
        {
            Outcome<TOut> overriddenOutcome;

            if (overrideEntry is FlowTestOverrideOutcome<TOut> fixedOverride)
            {
                overriddenOutcome = fixedOverride.Outcome;
            }
            else if (overrideEntry is FlowTestOverrideCompute<TArgs, TOut> computeOverride)
            {
                var moduleContext = new ModuleContext<TArgs>(node.Name, moduleType, args, flowContext);

                try
                {
                    overriddenOutcome = await computeOverride.Compute(moduleContext).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    overriddenOutcome = IsDeadlineExceeded(flowContext.Deadline)
                        ? Outcome<TOut>.Timeout(DeadlineExceededCode)
                        : Outcome<TOut>.Canceled(UpstreamCanceledCode);
                }
                catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
                {
                    overriddenOutcome = Outcome<TOut>.Error(UnhandledExceptionCode);
                }
            }
            else
            {
                throw new InvalidOperationException(
                    $"Override for moduleId '{node.Name}' has a different signature. Expected args '{typeof(TArgs)}' and output '{typeof(TOut)}'.");
            }

            flowContext.FlowTestInvocationSink?.Record(
                node.Name,
                moduleType,
                isOverride: true,
                overriddenOutcome.Kind,
                overriddenOutcome.Code);
            flowContext.RecordNodeOutcome(node.Index, node.Name, overriddenOutcome);
            return;
        }

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

        flowContext.FlowTestInvocationSink?.Record(
            node.Name,
            moduleType,
            isOverride: false,
            outcome.Kind,
            outcome.Code);
        flowContext.RecordNodeOutcome(node.Index, node.Name, outcome);
    }

    private static async ValueTask ExecuteStepTemplateAsyncCore<TArgs, TOut>(
        ExecutionEngine engine,
        PlanNodeTemplate node,
        TArgs args,
        FlowContext flowContext)
    {
        var moduleType = node.ModuleType!;

        var overrideProvider = flowContext.FlowTestOverrideProvider;
        if (overrideProvider is not null && overrideProvider.TryGetOverride(node.Name, out var overrideEntry))
        {
            Outcome<TOut> overriddenOutcome;

            if (overrideEntry is FlowTestOverrideOutcome<TOut> fixedOverride)
            {
                overriddenOutcome = fixedOverride.Outcome;
            }
            else if (overrideEntry is FlowTestOverrideCompute<TArgs, TOut> computeOverride)
            {
                var moduleContext = new ModuleContext<TArgs>(node.Name, moduleType, args, flowContext);

                try
                {
                    overriddenOutcome = await computeOverride.Compute(moduleContext).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    overriddenOutcome = IsDeadlineExceeded(flowContext.Deadline)
                        ? Outcome<TOut>.Timeout(DeadlineExceededCode)
                        : Outcome<TOut>.Canceled(UpstreamCanceledCode);
                }
                catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
                {
                    overriddenOutcome = Outcome<TOut>.Error(UnhandledExceptionCode);
                }
            }
            else
            {
                throw new InvalidOperationException(
                    $"Override for moduleId '{node.Name}' has a different signature. Expected args '{typeof(TArgs)}' and output '{typeof(TOut)}'.");
            }

            flowContext.FlowTestInvocationSink?.Record(
                node.Name,
                moduleType,
                isOverride: true,
                overriddenOutcome.Kind,
                overriddenOutcome.Code);
            flowContext.RecordNodeOutcome(node.Index, node.Name, overriddenOutcome);
            return;
        }

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

        flowContext.FlowTestInvocationSink?.Record(
            node.Name,
            moduleType,
            isOverride: false,
            outcome.Kind,
            outcome.Code);
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
