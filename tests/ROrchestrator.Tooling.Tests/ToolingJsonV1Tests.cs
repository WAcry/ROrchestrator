using ROrchestrator.Core;
using ROrchestrator.Core.Blueprint;
using ROrchestrator.Tooling;

namespace ROrchestrator.Tooling.Tests;

public sealed class ToolingJsonV1Tests
{
    [Fact]
    public void ValidatePatchJson_ShouldProduceStableJson()
    {
        var registry = CreateRegistry();
        var catalog = CreateCatalog();

        var result = ToolingJsonV1.ValidatePatchJson(
            patchJson: "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\"}]}}}}}",
            registry,
            catalog);

        Assert.Equal(2, result.ExitCode);

        Assert.Equal(
            "{\"kind\":\"validate\",\"is_valid\":false,\"findings\":[{\"severity\":\"error\",\"code\":\"CFG_MODULE_ARGS_MISSING\",\"path\":\"$.flows.HomeFeed.stages.s1.modules[0].with\",\"message\":\"modules[].with is required.\"}]}",
            result.Json);
    }

    [Fact]
    public void DiffPatchJson_ShouldProduceStableJson()
    {
        var result = ToolingJsonV1.DiffPatchJson(
            oldPatchJson: "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}}]}}}}}",
            newPatchJson: "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"test.module\",\"with\":{}},{\"id\":\"m2\",\"use\":\"test.module\",\"with\":{}}]}}}}}");

        Assert.Equal(0, result.ExitCode);

        Assert.Equal(
            "{\"kind\":\"diff\",\"module_diffs\":[{\"kind\":\"added\",\"flow_name\":\"HomeFeed\",\"stage_name\":\"s1\",\"module_id\":\"m2\",\"path\":\"$.flows.HomeFeed.stages.s1.modules[1]\",\"experiment_layer\":null,\"experiment_variant\":null}],\"param_diffs\":[],\"fanout_max_diffs\":[],\"emergency_diffs\":[]}",
            result.Json);
    }

    [Fact]
    public void DiffPatchJson_ShouldProduceStableErrorJson_WhenOldPatchJsonIsInvalid()
    {
        var result = ToolingJsonV1.DiffPatchJson(
            oldPatchJson: "not json",
            newPatchJson: "{\"schemaVersion\":\"v1\",\"flows\":{}}");

        Assert.Equal(2, result.ExitCode);

        Assert.Equal(
            "{\"kind\":\"diff\",\"error\":{\"code\":\"DIFF_INPUT_INVALID\",\"message\":\"oldPatchJson is not a valid JSON document.\"}}",
            result.Json);
    }

    private static FlowRegistry CreateRegistry()
    {
        var registry = new FlowRegistry();
        registry.Register("HomeFeed", CreateBlueprintWithStage<int, int>("TestFlow", stageName: "s1", okValue: 0));
        return registry;
    }

    private static ModuleCatalog CreateCatalog()
    {
        var catalog = new ModuleCatalog();
        catalog.Register<TestArgs, int>("test.module", _ => new DummyModule());
        return catalog;
    }

    private static FlowBlueprint<TReq, TResp> CreateBlueprintWithStage<TReq, TResp>(string name, string stageName, TResp okValue)
    {
        return FlowBlueprint
            .Define<TReq, TResp>(name)
            .Stage(
                stageName,
                stage =>
                {
                    stage
                        .Step("n1", moduleType: "test.module")
                        .Join(
                            "join",
                            join: _ => new ValueTask<Outcome<TResp>>(Outcome<TResp>.Ok(okValue)));
                })
            .Build();
    }

    private sealed class TestArgs
    {
    }

    private sealed class DummyModule : IModule<TestArgs, int>
    {
        public ValueTask<Outcome<int>> ExecuteAsync(ModuleContext<TestArgs> context)
        {
            return new ValueTask<Outcome<int>>(Outcome<int>.Ok(0));
        }
    }
}

