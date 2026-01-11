namespace Rockestra.Core;

public readonly struct StageContract
{
    internal const int MaxAllowedFanoutMax = 8;

    internal static readonly StageContract Default =
        new(
            allowsDynamicModules: false,
            allowsShadowModules: false,
            allowedModuleTypes: Array.Empty<string>(),
            maxModulesWarn: 0,
            maxModulesHard: 0,
            maxShadowModulesHard: 0,
            maxShadowSampleBps: 10000,
            minFanoutMax: 0,
            maxFanoutMax: MaxAllowedFanoutMax);

    private readonly bool _allowsDynamicModules;
    private readonly bool _allowsShadowModules;
    private readonly string[] _allowedModuleTypes;
    private readonly int _maxModulesWarn;
    private readonly int _maxModulesHard;
    private readonly int _maxShadowModulesHard;
    private readonly int _maxShadowSampleBps;
    private readonly int _minFanoutMax;
    private readonly int _maxFanoutMax;

    internal StageContract(
        bool allowsDynamicModules,
        bool allowsShadowModules,
        string[] allowedModuleTypes,
        int maxModulesWarn,
        int maxModulesHard,
        int maxShadowModulesHard,
        int maxShadowSampleBps,
        int minFanoutMax,
        int maxFanoutMax)
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
        _allowsShadowModules = allowsShadowModules;
        _allowedModuleTypes = allowedModuleTypes;
        _maxModulesWarn = maxModulesWarn;
        _maxModulesHard = maxModulesHard;

        if (maxShadowModulesHard < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxShadowModulesHard), maxShadowModulesHard, "MaxShadowModulesHard must be >= 0.");
        }

        if (maxShadowSampleBps < 0 || maxShadowSampleBps > 10000)
        {
            throw new ArgumentOutOfRangeException(nameof(maxShadowSampleBps), maxShadowSampleBps, "MaxShadowSampleBps must be within range 0..10000.");
        }

        if (minFanoutMax < 0 || minFanoutMax > MaxAllowedFanoutMax)
        {
            throw new ArgumentOutOfRangeException(nameof(minFanoutMax), minFanoutMax, $"MinFanoutMax must be within range 0..{MaxAllowedFanoutMax}.");
        }

        if (maxFanoutMax < 0 || maxFanoutMax > MaxAllowedFanoutMax)
        {
            throw new ArgumentOutOfRangeException(nameof(maxFanoutMax), maxFanoutMax, $"MaxFanoutMax must be within range 0..{MaxAllowedFanoutMax}.");
        }

        if (minFanoutMax > maxFanoutMax)
        {
            throw new ArgumentException("MinFanoutMax must be <= MaxFanoutMax.");
        }

        _maxShadowModulesHard = maxShadowModulesHard;
        _maxShadowSampleBps = maxShadowSampleBps;
        _minFanoutMax = minFanoutMax;
        _maxFanoutMax = maxFanoutMax;
    }

    public bool AllowsDynamicModules => _allowsDynamicModules;

    public bool AllowsShadowModules => _allowsShadowModules;

    public int MaxModulesWarn => _maxModulesWarn;

    public int MaxModulesHard => _maxModulesHard;

    public int MaxShadowModulesHard => _maxShadowModulesHard;

    public int MaxShadowSampleBps => _maxShadowSampleBps;

    public int MinFanoutMax => _minFanoutMax;

    public int MaxFanoutMax => _maxFanoutMax;

    public ReadOnlySpan<string> AllowedModuleTypes => _allowedModuleTypes;

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
    private bool _allowsShadowModules;
    private int _maxModulesWarn;
    private int _maxModulesHard;
    private int _maxShadowModulesHard;
    private int _maxShadowSampleBps;
    private int _minFanoutMax;
    private int _maxFanoutMax;
    private List<string>? _allowedModuleTypes;

    public StageContractBuilder()
    {
        _allowsDynamicModules = false;
        _allowsShadowModules = true;
        _maxModulesWarn = 0;
        _maxModulesHard = 0;
        _maxShadowModulesHard = 0;
        _maxShadowSampleBps = 10000;
        _minFanoutMax = 0;
        _maxFanoutMax = StageContract.MaxAllowedFanoutMax;
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

    public StageContractBuilder AllowShadowModules()
    {
        _allowsShadowModules = true;
        return this;
    }

    public StageContractBuilder DisallowShadowModules()
    {
        _allowsShadowModules = false;
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

    public StageContractBuilder MaxShadowModules(int hard)
    {
        if (hard < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(hard), hard, "Hard must be >= 0.");
        }

        _maxShadowModulesHard = hard;
        return this;
    }

    public StageContractBuilder MaxShadowSampleBps(int maxShadowSampleBps)
    {
        if (maxShadowSampleBps < 0 || maxShadowSampleBps > 10000)
        {
            throw new ArgumentOutOfRangeException(nameof(maxShadowSampleBps), maxShadowSampleBps, "MaxShadowSampleBps must be within range 0..10000.");
        }

        _maxShadowSampleBps = maxShadowSampleBps;
        return this;
    }

    public StageContractBuilder FanoutMaxRange(int min, int max)
    {
        if (min < 0 || min > StageContract.MaxAllowedFanoutMax)
        {
            throw new ArgumentOutOfRangeException(nameof(min), min, $"Min must be within range 0..{StageContract.MaxAllowedFanoutMax}.");
        }

        if (max < 0 || max > StageContract.MaxAllowedFanoutMax)
        {
            throw new ArgumentOutOfRangeException(nameof(max), max, $"Max must be within range 0..{StageContract.MaxAllowedFanoutMax}.");
        }

        if (min > max)
        {
            throw new ArgumentException("Min must be <= Max.");
        }

        _minFanoutMax = min;
        _maxFanoutMax = max;
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

        return new StageContract(
            _allowsDynamicModules,
            _allowsShadowModules,
            allowedModuleTypes,
            _maxModulesWarn,
            _maxModulesHard,
            _maxShadowModulesHard,
            _maxShadowSampleBps,
            _minFanoutMax,
            _maxFanoutMax);
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
