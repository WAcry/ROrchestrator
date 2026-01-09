namespace ROrchestrator.Core;

public readonly struct ModuleContext<TArgs>
{
    public string ModuleId { get; }

    public string TypeName { get; }

    public TArgs Args { get; }

    public FlowContext FlowContext { get; }

    public IServiceProvider Services => FlowContext.Services;

    public CancellationToken CancellationToken => FlowContext.CancellationToken;

    public DateTimeOffset Deadline => FlowContext.Deadline;

    public IExplainSink ExplainSink => FlowContext.ExplainSink;

    public ModuleContext(string moduleId, string typeName, TArgs args, FlowContext flowContext)
    {
        if (string.IsNullOrEmpty(moduleId))
        {
            throw new ArgumentException("ModuleId must be non-empty.", nameof(moduleId));
        }

        if (string.IsNullOrEmpty(typeName))
        {
            throw new ArgumentException("TypeName must be non-empty.", nameof(typeName));
        }

        FlowContext = flowContext ?? throw new ArgumentNullException(nameof(flowContext));

        if (!typeof(TArgs).IsValueType && args is null)
        {
            throw new ArgumentNullException(nameof(args));
        }

        ModuleId = moduleId;
        TypeName = typeName;
        Args = args;
    }
}
