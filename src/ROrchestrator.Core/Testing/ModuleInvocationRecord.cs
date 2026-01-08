namespace ROrchestrator.Core.Testing;

public enum ModuleInvocationSource
{
    Real = 0,
    Override = 1,
}

public readonly struct ModuleInvocationRecord
{
    public string ModuleId { get; }

    public string ModuleType { get; }

    public ModuleInvocationSource Source { get; }

    public OutcomeKind OutcomeKind { get; }

    public string OutcomeCode { get; }

    public ModuleInvocationRecord(
        string moduleId,
        string moduleType,
        ModuleInvocationSource source,
        OutcomeKind outcomeKind,
        string outcomeCode)
    {
        ModuleId = moduleId;
        ModuleType = moduleType;
        Source = source;
        OutcomeKind = outcomeKind;
        OutcomeCode = outcomeCode;
    }
}

internal sealed class FlowTestInvocationCollector : IFlowTestInvocationSink
{
    private readonly List<ModuleInvocationRecord> _records;

    public FlowTestInvocationCollector()
    {
        _records = new List<ModuleInvocationRecord>();
    }

    public void Record(string moduleId, string moduleType, bool isOverride, OutcomeKind outcomeKind, string outcomeCode)
    {
        _records.Add(
            new ModuleInvocationRecord(
                moduleId,
                moduleType,
                isOverride ? ModuleInvocationSource.Override : ModuleInvocationSource.Real,
                outcomeKind,
                outcomeCode));
    }

    public ModuleInvocationRecord[] ToArray()
    {
        return _records.ToArray();
    }
}

