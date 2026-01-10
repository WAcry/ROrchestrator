using Rockestra.Core;
using Rockestra.Core.Blueprint;
using Rockestra.Core.Selectors;

namespace Rockestra.BootstrapperFixture;

public static class FixtureBootstrapper
{
    public const string FlowName = "HomeFeed";
    public const string ModuleType = "fixture.module";
    public const string SelectorName = "is_allowed";

    public static void Configure(FlowRegistry registry, ModuleCatalog catalog, SelectorRegistry selectors)
    {
        registry.Register(
            FlowName,
            FlowBlueprint
                .Define<FixtureArgs, int>(FlowName)
                .Stage(
                    "s1",
                    stage =>
                    {
                        stage
                            .Step("n1", moduleType: ModuleType)
                            .Join(
                                "join",
                                join: _ => new ValueTask<Outcome<int>>(Outcome<int>.Ok(0)));
                    })
                .Build());

        catalog.Register<FixtureArgs, int>(ModuleType, _ => new DummyModule());
        selectors.Register(SelectorName, _ => true);
    }
}

public static class SignatureMismatchBootstrapper
{
    public static void Configure()
    {
    }
}

public sealed class FixtureArgs
{
}

public sealed class DummyModule : IModule<FixtureArgs, int>
{
    public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<FixtureArgs> context)
    {
        _ = context;
        return new ValueTask<Outcome<int>>(Outcome<int>.Ok(0));
    }
}


