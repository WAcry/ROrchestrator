namespace ROrchestrator.Core.Testing;

public sealed class ModuleContextFactory
{
    private static readonly DateTimeOffset DefaultDeadline =
        new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private IServiceProvider _services;
    private CancellationToken _cancellationToken;
    private DateTimeOffset _deadline;
    private IExplainSink? _explainSink;
    private FlowRequestOptions _requestOptions;
    private bool _hasRequestOptions;

    private ModuleContextFactory()
    {
        _services = EmptyServiceProvider.Instance;
        _cancellationToken = CancellationToken.None;
        _deadline = DefaultDeadline;
        _explainSink = null;
        _requestOptions = default;
        _hasRequestOptions = false;
    }

    public static ModuleContextFactory Create()
    {
        return new ModuleContextFactory();
    }

    public ModuleContextFactory WithServices(IServiceProvider services)
    {
        _services = services ?? throw new ArgumentNullException(nameof(services));
        return this;
    }

    public ModuleContextFactory WithCancellationToken(CancellationToken cancellationToken)
    {
        _cancellationToken = cancellationToken;
        return this;
    }

    public ModuleContextFactory WithDeadline(DateTimeOffset deadline)
    {
        if (deadline == default)
        {
            throw new ArgumentException("Deadline must be non-default.", nameof(deadline));
        }

        _deadline = deadline;
        return this;
    }

    public ModuleContextFactory WithExplainSink(IExplainSink explainSink)
    {
        _explainSink = explainSink ?? throw new ArgumentNullException(nameof(explainSink));
        return this;
    }

    public ModuleContextFactory WithRequestOptions(FlowRequestOptions requestOptions)
    {
        _requestOptions = requestOptions;
        _hasRequestOptions = true;
        return this;
    }

    public FlowContext CreateFlowContext()
    {
        var sink = _explainSink;

        if (_hasRequestOptions)
        {
            return new FlowContext(_services, _cancellationToken, _deadline, _requestOptions, sink);
        }

        return new FlowContext(_services, _cancellationToken, _deadline, sink);
    }

    public ModuleContext<TArgs> CreateModuleContext<TArgs>(string moduleId, string typeName, TArgs args)
    {
        var flowContext = CreateFlowContext();
        return new ModuleContext<TArgs>(moduleId, typeName, args, flowContext);
    }

    private sealed class EmptyServiceProvider : IServiceProvider
    {
        public static readonly EmptyServiceProvider Instance = new();

        private EmptyServiceProvider()
        {
        }

        public object? GetService(Type serviceType)
        {
            return null;
        }
    }
}

