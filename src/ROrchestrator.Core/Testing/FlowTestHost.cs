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

    public async ValueTask<TestRunResult<TResp>> RunAsync<TReq, TResp>(string flowName, TReq req)
    {
        var flowContext = new FlowContext(_services, CancellationToken.None, _deadline);
        flowContext.EnableExecExplain();

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

