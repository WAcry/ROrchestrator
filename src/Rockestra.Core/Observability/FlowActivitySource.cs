using System.Diagnostics;
using Rockestra.Core.Blueprint;

namespace Rockestra.Core.Observability;

internal static class FlowActivitySource
{
    internal const string ActivitySourceName = "Rockestra";

    internal const string PlanHashFormat = "X16";

    internal const string FlowActivityName = "rockestra.flow";
    internal const string StepActivityName = "rockestra.node.step";
    internal const string JoinActivityName = "rockestra.node.join";
    internal const string StageFanoutModuleActivityName = "rockestra.stage.fanout.module";

    internal const string TagFlowName = "flow.name";
    internal const string TagPlanHash = "plan.hash";
    internal const string TagConfigVersion = "config.version";
    internal const string TagNodeName = "node.name";
    internal const string TagNodeKind = "node.kind";
    internal const string TagStageName = "stage.name";
    internal const string TagModuleId = "module.id";
    internal const string TagModuleType = "module.type";
    internal const string TagOutcomeKind = "outcome.kind";
    internal const string TagOutcomeCode = "outcome.code";
    internal const string TagSkipCode = "skip.code";
    internal const string TagExecutionPath = "execution.path";
    internal const string TagShadowSampleRateBps = "shadow.sample_rate_bps";

    internal const string ExecutionPathPrimary = "primary";
    internal const string ExecutionPathShadow = "shadow";

    internal static readonly ActivitySource Instance = new(ActivitySourceName);

    internal static string GetNodeKindTagValue(BlueprintNodeKind kind)
    {
        return kind switch
        {
            BlueprintNodeKind.Step => "step",
            BlueprintNodeKind.Join => "join",
            _ => "unknown",
        };
    }

    internal static string GetOutcomeKindTagValue(OutcomeKind kind)
    {
        return kind switch
        {
            OutcomeKind.Ok => "ok",
            OutcomeKind.Error => "error",
            OutcomeKind.Timeout => "timeout",
            OutcomeKind.Skipped => "skipped",
            OutcomeKind.Fallback => "fallback",
            OutcomeKind.Canceled => "canceled",
            _ => "unspecified",
        };
    }
}

