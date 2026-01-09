namespace ROrchestrator.Core;

public sealed class StageFanoutSnapshot
{
    private readonly string[] _enabledModuleIds;
    private readonly StageFanoutSkippedModule[] _skippedModules;

    public string StageName { get; }

    public IReadOnlyList<string> EnabledModuleIds => _enabledModuleIds;

    public IReadOnlyList<StageFanoutSkippedModule> SkippedModules => _skippedModules;

    internal StageFanoutSnapshot(string stageName, string[] enabledModuleIds, StageFanoutSkippedModule[] skippedModules)
    {
        if (string.IsNullOrEmpty(stageName))
        {
            throw new ArgumentException("StageName must be non-empty.", nameof(stageName));
        }

        StageName = stageName;
        _enabledModuleIds = enabledModuleIds ?? throw new ArgumentNullException(nameof(enabledModuleIds));
        _skippedModules = skippedModules ?? throw new ArgumentNullException(nameof(skippedModules));
    }
}

public readonly struct StageFanoutSkippedModule
{
    public string ModuleId { get; }

    public string ReasonCode { get; }

    internal StageFanoutSkippedModule(string moduleId, string reasonCode)
    {
        ModuleId = moduleId;
        ReasonCode = reasonCode;
    }
}

