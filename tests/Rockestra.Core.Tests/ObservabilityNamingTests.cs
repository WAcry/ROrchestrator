using System.Diagnostics.Metrics;

namespace Rockestra.Core.Tests;

public sealed class ObservabilityNamingTests
{
    [Fact]
    public void FlowActivitySource_ShouldUseUnifiedName()
    {
        Assert.Equal("Rockestra", Observability.FlowActivitySource.ActivitySourceName);
        Assert.Equal(Observability.FlowActivitySource.ActivitySourceName, Observability.FlowActivitySource.Instance.Name);
    }

    [Fact]
    public void FlowMetricsV1_ShouldUseUnifiedName()
    {
        string? meterName = null;

        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, _) =>
            {
                if (instrument.Name != "rockestra.flow.outcomes")
                {
                    return;
                }

                meterName = instrument.Meter.Name;
            },
        };

        listener.Start();

        _ = Observability.FlowMetricsV1.IsFlowEnabled;

        Assert.Equal("Rockestra", meterName);
    }
}


