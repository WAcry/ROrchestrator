using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace ROrchestrator.Core.Observability;

internal static class FlowMetricsV1
{
    private const string DurationUnit = "ms";
    private const string CountUnit = "count";

    private const string UnknownSkipCodeTagValue = "OTHER";
    private const int MaxSkipCodeLength = 64;

    private static readonly Meter Meter = new(FlowActivitySource.ActivitySourceName);

    private static readonly Histogram<double> FlowLatencyMs = Meter.CreateHistogram<double>(
        name: "rorchestrator.flow.latency.ms",
        unit: DurationUnit,
        description: "End-to-end flow latency for plan-template execution.");

    private static readonly Counter<long> FlowOutcomes = Meter.CreateCounter<long>(
        name: "rorchestrator.flow.outcomes",
        unit: CountUnit,
        description: "Flow outcomes count for plan-template execution.");

    private static readonly Histogram<double> StepLatencyMs = Meter.CreateHistogram<double>(
        name: "rorchestrator.step.latency.ms",
        unit: DurationUnit,
        description: "Step latency for plan-template execution.");

    private static readonly Counter<long> StepOutcomes = Meter.CreateCounter<long>(
        name: "rorchestrator.step.outcomes",
        unit: CountUnit,
        description: "Step outcomes count for plan-template execution.");

    private static readonly Counter<long> StepSkippedReasons = Meter.CreateCounter<long>(
        name: "rorchestrator.step.skipped.reasons",
        unit: CountUnit,
        description: "Step skip reasons count for plan-template execution.");

    private static readonly Histogram<double> JoinLatencyMs = Meter.CreateHistogram<double>(
        name: "rorchestrator.join.latency.ms",
        unit: DurationUnit,
        description: "Join latency for plan-template execution.");

    private static readonly Counter<long> JoinOutcomes = Meter.CreateCounter<long>(
        name: "rorchestrator.join.outcomes",
        unit: CountUnit,
        description: "Join outcomes count for plan-template execution.");

    internal static bool IsFlowEnabled => FlowLatencyMs.Enabled || FlowOutcomes.Enabled;

    internal static bool IsStepEnabled => StepLatencyMs.Enabled || StepOutcomes.Enabled;

    internal static bool IsStepSkipReasonEnabled => StepSkippedReasons.Enabled;

    internal static bool IsJoinEnabled => JoinLatencyMs.Enabled || JoinOutcomes.Enabled;

    internal static long StartFlowTimer()
    {
        return FlowLatencyMs.Enabled ? Stopwatch.GetTimestamp() : 0;
    }

    internal static long StartStepTimer()
    {
        return StepLatencyMs.Enabled ? Stopwatch.GetTimestamp() : 0;
    }

    internal static long StartJoinTimer()
    {
        return JoinLatencyMs.Enabled ? Stopwatch.GetTimestamp() : 0;
    }

    internal static void RecordFlow(long startTimestamp, string flowName, OutcomeKind outcomeKind)
    {
        var recordDuration = startTimestamp != 0 && FlowLatencyMs.Enabled;
        var recordOutcome = FlowOutcomes.Enabled;

        if (!recordDuration && !recordOutcome)
        {
            return;
        }

        TagList tags = default;
        tags.Add(FlowActivitySource.TagFlowName, flowName);
        tags.Add(FlowActivitySource.TagOutcomeKind, FlowActivitySource.GetOutcomeKindTagValue(outcomeKind));

        if (recordDuration)
        {
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp);
            FlowLatencyMs.Record(elapsed.TotalMilliseconds, tags);
        }

        if (recordOutcome)
        {
            FlowOutcomes.Add(1, tags);
        }
    }

    internal static void RecordStep(long startTimestamp, string flowName, string moduleType, OutcomeKind outcomeKind)
    {
        var recordDuration = startTimestamp != 0 && StepLatencyMs.Enabled;
        var recordOutcome = StepOutcomes.Enabled;

        if (!recordDuration && !recordOutcome)
        {
            return;
        }

        TagList tags = default;
        tags.Add(FlowActivitySource.TagFlowName, flowName);
        tags.Add(FlowActivitySource.TagModuleType, moduleType);
        tags.Add(FlowActivitySource.TagOutcomeKind, FlowActivitySource.GetOutcomeKindTagValue(outcomeKind));

        if (recordDuration)
        {
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp);
            StepLatencyMs.Record(elapsed.TotalMilliseconds, tags);
        }

        if (recordOutcome)
        {
            StepOutcomes.Add(1, tags);
        }
    }

    internal static void RecordStepSkipReason(string flowName, string code)
    {
        if (!StepSkippedReasons.Enabled)
        {
            return;
        }

        var codeTagValue = GetSkipCodeTagValue(code);

        TagList tags = default;
        tags.Add(FlowActivitySource.TagFlowName, flowName);
        tags.Add(FlowActivitySource.TagSkipCode, codeTagValue);

        StepSkippedReasons.Add(1, tags);
    }

    internal static void RecordJoin(long startTimestamp, string flowName, OutcomeKind outcomeKind)
    {
        var recordDuration = startTimestamp != 0 && JoinLatencyMs.Enabled;
        var recordOutcome = JoinOutcomes.Enabled;

        if (!recordDuration && !recordOutcome)
        {
            return;
        }

        TagList tags = default;
        tags.Add(FlowActivitySource.TagFlowName, flowName);
        tags.Add(FlowActivitySource.TagOutcomeKind, FlowActivitySource.GetOutcomeKindTagValue(outcomeKind));

        if (recordDuration)
        {
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp);
            JoinLatencyMs.Record(elapsed.TotalMilliseconds, tags);
        }

        if (recordOutcome)
        {
            JoinOutcomes.Add(1, tags);
        }
    }

    private static string GetSkipCodeTagValue(string code)
    {
        if (string.IsNullOrEmpty(code) || code.Length > MaxSkipCodeLength)
        {
            return UnknownSkipCodeTagValue;
        }

        var digitRun = 0;

        for (var i = 0; i < code.Length; i++)
        {
            var c = code[i];

            if (c >= 'A' && c <= 'Z')
            {
                digitRun = 0;
                continue;
            }

            if (c == '_')
            {
                digitRun = 0;
                continue;
            }

            if (c >= '0' && c <= '9')
            {
                digitRun++;
                if (digitRun >= 6)
                {
                    return UnknownSkipCodeTagValue;
                }

                continue;
            }

            return UnknownSkipCodeTagValue;
        }

        return code;
    }
}
