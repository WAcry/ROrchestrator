namespace ROrchestrator.Core;

public interface IModule<TArgs, TOut>
{
    ValueTask<Outcome<TOut>> ExecuteAsync(ModuleContext<TArgs> context);
}

