namespace ROrchestrator.Core.Testing;

public sealed record TestRunResult<TResp>(
    Outcome<TResp> Outcome,
    ExecExplain Explain,
    IReadOnlyList<ModuleInvocationRecord> Invocations);

