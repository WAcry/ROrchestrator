using ROrchestrator.Core;

namespace ROrchestrator.Core.Tests;

public sealed class ModuleCatalogTests
{
    [Fact]
    public void Create_ShouldCreateModule_FromRegisteredFactory()
    {
        var catalog = new ModuleCatalog();
        var services = new DummyServiceProvider();
        IServiceProvider? observedServices = null;

        catalog.Register<int, string>(
            typeName: "cg.offline_key_list",
            factory: sp =>
            {
                observedServices = sp;
                return new DummyModule();
            });

        var module = catalog.Create<int, string>("cg.offline_key_list", services);

        Assert.Same(services, observedServices);
        Assert.NotNull(module);
        Assert.IsType<DummyModule>(module);
    }

    [Fact]
    public void Register_ShouldThrow_WhenTypeNameIsDuplicated()
    {
        var catalog = new ModuleCatalog();
        catalog.Register<int, string>("cg.offline_key_list", _ => new DummyModule());

        var ex = Assert.Throws<ArgumentException>(
            () => catalog.Register<int, string>("cg.offline_key_list", _ => new DummyModule()));

        Assert.Equal("typeName", ex.ParamName);
    }

    [Fact]
    public void Create_ShouldThrow_WhenTypeNameIsNotRegistered()
    {
        var catalog = new ModuleCatalog();

        var ex = Assert.Throws<InvalidOperationException>(
            () => catalog.Create<int, string>("cg.offline_key_list", new DummyServiceProvider()));

        Assert.Contains("cg.offline_key_list", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Create_ShouldThrow_WhenFactoryFailsToResolveDependencies()
    {
        var catalog = new ModuleCatalog();
        catalog.Register<int, string>("cg.offline_key_list", _ => throw new InvalidOperationException("DI failed."));

        var ex = Assert.Throws<InvalidOperationException>(
            () => catalog.Create<int, string>("cg.offline_key_list", new DummyServiceProvider()));

        Assert.Contains("cg.offline_key_list", ex.Message, StringComparison.Ordinal);
        Assert.NotNull(ex.InnerException);
        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal("DI failed.", ex.InnerException.Message);
    }

    private sealed class DummyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType)
        {
            return null;
        }
    }

    private sealed class DummyModule : IModule<int, string>
    {
        public ValueTask<Outcome<string>> ExecuteAsync(ModuleContext<int> context)
        {
            return new ValueTask<Outcome<string>>(Outcome<string>.Ok("ok"));
        }
    }
}

