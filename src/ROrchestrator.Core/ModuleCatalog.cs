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
        IModuleArgsValidator<TArgs>? argsValidator = null,
        ModuleLifetime lifetime = ModuleLifetime.Transient,
        ModuleThreadSafety threadSafety = ModuleThreadSafety.ThreadSafe)
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

        if (!_modules.TryAdd(typeName, new Entry(typeof(TArgs), typeof(TOut), factory, argsValidatorInvoker, lifetime, threadSafety)))
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

        return entry.Create<TArgs, TOut>(typeName, services);
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

    private sealed class Entry
    {
        public Type ArgsType { get; }

        public Type OutType { get; }

        public Delegate Factory { get; }

        public IModuleArgsValidatorInvoker? ArgsValidator { get; }

        public ModuleLifetime Lifetime { get; }

        public ModuleThreadSafety ThreadSafety { get; }

        private readonly Lock _singletonGate = new();
        private object? _singleton;

        public Entry(
            Type argsType,
            Type outType,
            Delegate factory,
            IModuleArgsValidatorInvoker? argsValidator,
            ModuleLifetime lifetime,
            ModuleThreadSafety threadSafety)
        {
            ArgsType = argsType;
            OutType = outType;
            Factory = factory;
            ArgsValidator = argsValidator;
            Lifetime = lifetime;
            ThreadSafety = threadSafety;
            _singleton = null;
        }

        public IModule<TArgs, TOut> Create<TArgs, TOut>(string typeName, IServiceProvider services)
        {
            if (Lifetime != ModuleLifetime.Singleton)
            {
                return CreateTransient<TArgs, TOut>(typeName, services);
            }

            var existing = Volatile.Read(ref _singleton);
            if (existing is not null)
            {
                return (IModule<TArgs, TOut>)existing;
            }

            lock (_singletonGate)
            {
                existing = _singleton;
                if (existing is not null)
                {
                    return (IModule<TArgs, TOut>)existing;
                }

                var created = CreateTransient<TArgs, TOut>(typeName, services);

                IModule<TArgs, TOut> instance = created;
                if (ThreadSafety == ModuleThreadSafety.NotThreadSafe)
                {
                    instance = new NotThreadSafeSingletonModuleWrapper<TArgs, TOut>(created);
                }

                _singleton = instance;
                return instance;
            }
        }

        private IModule<TArgs, TOut> CreateTransient<TArgs, TOut>(string typeName, IServiceProvider services)
        {
            var factory = (Func<IServiceProvider, IModule<TArgs, TOut>>)Factory;

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
    }

    private sealed class NotThreadSafeSingletonModuleWrapper<TArgs, TOut> : IModule<TArgs, TOut>
    {
        private readonly IModule<TArgs, TOut> _inner;
        private int _inUse;

        public NotThreadSafeSingletonModuleWrapper(IModule<TArgs, TOut> inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _inUse = 0;
        }

        public async ValueTask<Outcome<TOut>> ExecuteAsync(ModuleContext<TArgs> context)
        {
            if (Interlocked.CompareExchange(ref _inUse, 1, 0) != 0)
            {
                var flowName = context.FlowContext.TryGetFlowNameHint(out var found) ? found : "unknown";
                throw new ModuleConcurrencyViolationException(
                    $"Concurrent execution detected for singleton module marked not_thread_safe. flow='{flowName}', module_id='{context.ModuleId}', module_type='{context.TypeName}'.");
            }

            try
            {
                return await _inner.ExecuteAsync(context).ConfigureAwait(false);
            }
            finally
            {
                Volatile.Write(ref _inUse, 0);
            }
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
