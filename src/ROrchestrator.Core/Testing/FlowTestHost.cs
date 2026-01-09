namespace ROrchestrator.Core.Testing;

public sealed class FlowTestHost
{
    private readonly FlowHost _host;
    private readonly IServiceProvider _services;
    private readonly DateTimeOffset _deadline;
    private readonly IFlowTestOverrideProvider? _overrideProvider;

    internal FlowTestHost(FlowHost host, IServiceProvider services, DateTimeOffset deadline, IFlowTestOverrideProvider? overrideProvider)
    {
        _host = host ?? throw new ArgumentNullException(nameof(host));
        _services = services ?? throw new ArgumentNullException(nameof(services));
        _deadline = deadline == default ? throw new ArgumentException("Deadline must be non-default.", nameof(deadline)) : deadline;
        _overrideProvider = overrideProvider;
    }

    public static FlowTestHostBuilder Create(FlowRegistry registry, ModuleCatalog catalog)
    {
        return new FlowTestHostBuilder(registry, catalog);
    }

    public async ValueTask<TestRunResult<TResp>> RunAsync<TReq, TResp>(
        string flowName,
        TReq req,
        FlowRequestOptions? requestOptions = null,
        ConfigSnapshot? configSnapshot = null)
    {
        var flowContext = requestOptions.HasValue
            ? new FlowContext(_services, CancellationToken.None, _deadline, requestOptions.Value)
            : new FlowContext(_services, CancellationToken.None, _deadline);

        if (configSnapshot.HasValue)
        {
            flowContext.SetConfigSnapshotForTesting(configSnapshot.Value);
        }

        flowContext.EnableExecExplain(ExplainLevel.Standard);

        var invocationCollector = new FlowTestInvocationCollector();
        flowContext.ConfigureForTesting(_overrideProvider, invocationCollector);

        var outcome = await _host.ExecuteAsync<TReq, TResp>(flowName, req, flowContext).ConfigureAwait(false);

        if (!flowContext.TryGetExecExplain(out var explain))
        {
            throw new InvalidOperationException("ExecExplain was not recorded.");
        }

        return new TestRunResult<TResp>(outcome, explain, invocationCollector.ToArray());
    }
}
