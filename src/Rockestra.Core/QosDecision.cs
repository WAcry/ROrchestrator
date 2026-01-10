namespace Rockestra.Core;

public readonly struct QosDecision
{
    public QosTier SelectedTier { get; }

    public string? ReasonCode { get; }

    public IReadOnlyDictionary<string, string>? Signals { get; }

    public QosDecision(QosTier selectedTier, string? reasonCode, IReadOnlyDictionary<string, string>? signals)
    {
        SelectedTier = selectedTier;
        ReasonCode = reasonCode;
        Signals = signals;
    }
}

public readonly struct QosSelectContext
{
    public bool CollectSignals { get; }

    public QosSelectContext(bool collectSignals)
    {
        CollectSignals = collectSignals;
    }
}

public interface IQosProvider
{
    QosDecision Select(string flowName, FlowContext context, QosSelectContext selectContext);
}


