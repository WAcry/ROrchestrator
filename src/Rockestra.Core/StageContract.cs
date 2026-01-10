namespace Rockestra.Core;

public readonly struct StageContract
{
    internal static readonly StageContract Default =
        new(allowsDynamicModules: false, allowedModuleTypes: Array.Empty<string>(), maxModulesWarn: 0, maxModulesHard: 0);

    private readonly bool _allowsDynamicModules;
    private readonly string[] _allowedModuleTypes;
    private readonly int _maxModulesWarn;
    private readonly int _maxModulesHard;

    internal StageContract(bool allowsDynamicModules, string[] allowedModuleTypes, int maxModulesWarn, int maxModulesHard)
    {
        if (allowedModuleTypes is null)
        {
            throw new ArgumentNullException(nameof(allowedModuleTypes));
        }

        if (maxModulesWarn < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxModulesWarn), maxModulesWarn, "MaxModulesWarn must be >= 0.");
        }

        if (maxModulesHard < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxModulesHard), maxModulesHard, "MaxModulesHard must be >= 0.");
        }

        if (maxModulesWarn != 0 && maxModulesHard != 0 && maxModulesWarn > maxModulesHard)
        {
            throw new ArgumentException("MaxModulesWarn must be <= MaxModulesHard when both are set.");
        }

        _allowsDynamicModules = allowsDynamicModules;
        _allowedModuleTypes = allowedModuleTypes;
        _maxModulesWarn = maxModulesWarn;
        _maxModulesHard = maxModulesHard;
    }

    public bool AllowsDynamicModules => _allowsDynamicModules;

    public int MaxModulesWarn => _maxModulesWarn;

    public int MaxModulesHard => _maxModulesHard;

    internal string[] AllowedModuleTypes => _allowedModuleTypes;

    internal bool IsModuleTypeAllowed(string moduleType)
    {
        if (!_allowsDynamicModules)
        {
            return false;
        }

        var allowlist = _allowedModuleTypes;
        if (allowlist.Length == 0)
        {
            return true;
        }

        for (var i = 0; i < allowlist.Length; i++)
        {
            if (string.Equals(allowlist[i], moduleType, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}

public sealed class StageContractBuilder
{
    private bool _allowsDynamicModules;
    private int _maxModulesWarn;
    private int _maxModulesHard;
    private List<string>? _allowedModuleTypes;

    public StageContractBuilder()
    {
        _allowsDynamicModules = false;
        _maxModulesWarn = 0;
        _maxModulesHard = 0;
        _allowedModuleTypes = null;
    }

    public StageContractBuilder AllowDynamicModules()
    {
        _allowsDynamicModules = true;
        return this;
    }

    public StageContractBuilder DisallowDynamicModules()
    {
        _allowsDynamicModules = false;
        return this;
    }

    public StageContractBuilder AllowModuleTypes(params string[] moduleTypes)
    {
        if (moduleTypes is null)
        {
            throw new ArgumentNullException(nameof(moduleTypes));
        }

        if (moduleTypes.Length == 0)
        {
            return this;
        }

        _allowedModuleTypes ??= new List<string>(capacity: moduleTypes.Length);

        for (var i = 0; i < moduleTypes.Length; i++)
        {
            var moduleType = moduleTypes[i];

            if (string.IsNullOrEmpty(moduleType))
            {
                throw new ArgumentException("Allowed module type must be non-empty.", nameof(moduleTypes));
            }

            var list = _allowedModuleTypes;
            var found = false;

            for (var j = 0; j < list.Count; j++)
            {
                if (string.Equals(list[j], moduleType, StringComparison.Ordinal))
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                list.Add(moduleType);
            }
        }

        return this;
    }

    public StageContractBuilder MaxModules(int warn, int hard)
    {
        if (warn < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(warn), warn, "Warn must be >= 0.");
        }

        if (hard < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(hard), hard, "Hard must be >= 0.");
        }

        if (warn != 0 && hard != 0 && warn > hard)
        {
            throw new ArgumentException("Warn must be <= Hard when both are set.");
        }

        _maxModulesWarn = warn;
        _maxModulesHard = hard;
        return this;
    }

    internal StageContract Build()
    {
        string[] allowedModuleTypes;

        var list = _allowedModuleTypes;
        if (list is null || list.Count == 0)
        {
            allowedModuleTypes = Array.Empty<string>();
        }
        else
        {
            allowedModuleTypes = list.ToArray();
        }

        return new StageContract(_allowsDynamicModules, allowedModuleTypes, _maxModulesWarn, _maxModulesHard);
    }
}

internal readonly struct StageContractEntry
{
    public string StageName { get; }

    public StageContract Contract { get; }

    public StageContractEntry(string stageName, StageContract contract)
    {
        StageName = stageName ?? throw new ArgumentNullException(nameof(stageName));
        Contract = contract;
    }
}

