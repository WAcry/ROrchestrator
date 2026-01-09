namespace ROrchestrator.Core;

public interface IModuleArgsValidator<in TArgs>
{
    bool TryValidate(TArgs args, out string? path, out string message);
}

