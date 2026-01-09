namespace ROrchestrator.Core;

public enum QosTier
{
    Full = 0,
    Conserve = 1,
    Emergency = 2,
    Fallback = 3,
}

public interface IQosTierProvider
{
    QosTier SelectTier(string flowName, FlowContext context);
}

