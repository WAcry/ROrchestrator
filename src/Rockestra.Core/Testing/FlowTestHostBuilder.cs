namespace Rockestra.Core.Testing;

public sealed class FlowTestHostBuilder
{
    private static readonly DateTimeOffset DefaultDeadline = new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private readonly FlowRegistry _registry;
    private readonly ModuleCatalog _catalog;
    private IServiceProvider? _services;
    private DateTimeOffset _deadline;
    private Overrides? _overrides;

    internal FlowTestHostBuilder(FlowRegistry registry, ModuleCatalog catalog)
    {
        _registry = registry ?? throw new ArgumentNullException(nameof(registry));
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
        _deadline = DefaultDeadline;
    }

    public FlowTestHostBuilder WithServices(IServiceProvider services)
    {
        _services = services ?? throw new ArgumentNullException(nameof(services));
        return this;
    }

    public FlowTestHostBuilder WithDeadline(DateTimeOffset deadline)
    {
        if (deadline == default)
        {
            throw new ArgumentException("Deadline must be non-default.", nameof(deadline));
        }

        _deadline = deadline;
        return this;
    }

    public FlowTestHostBuilder WithOverrides(Action<Overrides> configure)
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        _overrides ??= new Overrides();
        configure(_overrides);
        return this;
    }

    public FlowTestHost Build()
    {
        var host = new FlowHost(_registry, _catalog);
        var services = _services ?? EmptyServiceProvider.Instance;
        return new FlowTestHost(host, services, _deadline, _overrides);
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


