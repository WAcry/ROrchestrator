namespace ROrchestrator.Core;

public sealed class ModuleCatalog
{
    private readonly Dictionary<string, Entry> _modules;

    public ModuleCatalog()
    {
        _modules = new Dictionary<string, Entry>(StringComparer.Ordinal);
    }

    public void Register<TArgs, TOut>(string typeName, Func<IServiceProvider, IModule<TArgs, TOut>> factory)
    {
        if (string.IsNullOrEmpty(typeName))
        {
            throw new ArgumentException("TypeName must be non-empty.", nameof(typeName));
        }

        if (factory is null)
        {
            throw new ArgumentNullException(nameof(factory));
        }

        if (!_modules.TryAdd(typeName, new Entry(typeof(TArgs), typeof(TOut), factory)))
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
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to create module type '{typeName}'.", ex);
        }

        if (module is null)
        {
            throw new InvalidOperationException($"Factory returned null for module type '{typeName}'.");
        }

        return module;
    }

    private readonly struct Entry
    {
        public Type ArgsType { get; }

        public Type OutType { get; }

        public Delegate Factory { get; }

        public Entry(Type argsType, Type outType, Delegate factory)
        {
            ArgsType = argsType;
            OutType = outType;
            Factory = factory;
        }
    }
}

