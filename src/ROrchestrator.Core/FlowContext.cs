namespace ROrchestrator.Core;

public sealed class FlowContext
{
    public IServiceProvider Services { get; }

    public CancellationToken CancellationToken { get; }

    public DateTimeOffset Deadline { get; }

    public FlowContext(IServiceProvider services, CancellationToken cancellationToken, DateTimeOffset deadline)
    {
        Services = services ?? throw new ArgumentNullException(nameof(services));

        if (deadline == default)
        {
            throw new ArgumentException("Deadline must be non-default.", nameof(deadline));
        }

        CancellationToken = cancellationToken;
        Deadline = deadline;
    }
}

