namespace ROrchestrator.Core;

internal interface IModuleArgsValidatorInvoker
{
    bool TryValidate(object args, out string? path, out string message);
}

public sealed class ModuleCatalog
{
    private readonly Dictionary<string, Entry> _modules;

    public ModuleCatalog()
    {
        _modules = new Dictionary<string, Entry>();
    }

    public void Register<TArgs, TOut>(
        string typeName,
        Func<IServiceProvider, IModule<TArgs, TOut>> factory,
        IModuleArgsValidator<TArgs>? argsValidator = null)
    {
        if (string.IsNullOrEmpty(typeName))
        {
            throw new ArgumentException("TypeName must be non-empty.", nameof(typeName));
        }

        if (factory is null)
        {
            throw new ArgumentNullException(nameof(factory));
        }

        IModuleArgsValidatorInvoker? argsValidatorInvoker = null;

        if (argsValidator is not null)
        {
            argsValidatorInvoker = new ModuleArgsValidatorInvoker<TArgs>(argsValidator);
        }

        if (!_modules.TryAdd(typeName, new Entry(typeof(TArgs), typeof(TOut), factory, argsValidatorInvoker)))
        {
            throw new ArgumentException($"TypeName '{typeName}' is already registered.", nameof(typeName));
        }
    }

    public IModule<TArgs, TOut> Create<TArgs, TOut>(string typeName, IServiceProvider services)
    {
        if (string.IsNullOrEmpty(typeName))
        {
            throw new ArgumentException("TypeName must be non-empty.", nameof(typeName));
        }

        if (services is null)
        {
            throw new ArgumentNullException(nameof(services));
        }

        if (!_modules.TryGetValue(typeName, out var entry))
        {
            throw new InvalidOperationException($"Module type '{typeName}' is not registered.");
        }

        if (entry.ArgsType != typeof(TArgs) || entry.OutType != typeof(TOut))
        {
            throw new InvalidOperationException($"Module type '{typeName}' has a different signature.");
        }

        var factory = (Func<IServiceProvider, IModule<TArgs, TOut>>)entry.Factory;

        IModule<TArgs, TOut>? module;

        try
        {
            module = factory(services);
        }
        catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
        {
            throw new InvalidOperationException($"Failed to create module type '{typeName}'.", ex);
        }

        if (module is null)
        {
            throw new InvalidOperationException($"Factory returned null for module type '{typeName}'.");
        }

        return module;
    }

    internal bool TryGetSignature(string typeName, out Type argsType, out Type outType)
    {
        if (string.IsNullOrEmpty(typeName))
        {
            throw new ArgumentException("TypeName must be non-empty.", nameof(typeName));
        }

        if (_modules.TryGetValue(typeName, out var entry))
        {
            argsType = entry.ArgsType;
            outType = entry.OutType;
            return true;
        }

        argsType = default!;
        outType = default!;
        return false;
    }

    internal bool TryGetSignature(string typeName, out Type argsType, out Type outType, out IModuleArgsValidatorInvoker? argsValidator)
    {
        if (string.IsNullOrEmpty(typeName))
        {
            throw new ArgumentException("TypeName must be non-empty.", nameof(typeName));
        }

        if (_modules.TryGetValue(typeName, out var entry))
        {
            argsType = entry.ArgsType;
            outType = entry.OutType;
            argsValidator = entry.ArgsValidator;
            return true;
        }

        argsType = default!;
        outType = default!;
        argsValidator = null;
        return false;
    }

    private readonly struct Entry
    {
        public Type ArgsType { get; }

        public Type OutType { get; }

        public Delegate Factory { get; }

        public IModuleArgsValidatorInvoker? ArgsValidator { get; }

        public Entry(Type argsType, Type outType, Delegate factory, IModuleArgsValidatorInvoker? argsValidator)
        {
            ArgsType = argsType;
            OutType = outType;
            Factory = factory;
            ArgsValidator = argsValidator;
        }
    }

    private sealed class ModuleArgsValidatorInvoker<TArgs> : IModuleArgsValidatorInvoker
    {
        private readonly IModuleArgsValidator<TArgs> _validator;

        public ModuleArgsValidatorInvoker(IModuleArgsValidator<TArgs> validator)
        {
            _validator = validator;
        }

        public bool TryValidate(object args, out string? path, out string message)
        {
            if (args is not TArgs typedArgs)
            {
                path = null;
                message = string.Concat("Expected module args type: ", typeof(TArgs).FullName);
                return false;
            }

            return _validator.TryValidate(typedArgs, out path, out message);
        }
    }
}
