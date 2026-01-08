using ROrchestrator.Core.Blueprint;

namespace ROrchestrator.Core;

internal interface IPlanCompiler
{
    PlanTemplate<TReq, TResp> Compile<TReq, TResp>(FlowBlueprint<TReq, TResp> blueprint, ModuleCatalog catalog);
}

internal sealed class DefaultPlanCompiler : IPlanCompiler
{
    public static readonly DefaultPlanCompiler Instance = new();

    private DefaultPlanCompiler()
    {
    }

    public PlanTemplate<TReq, TResp> Compile<TReq, TResp>(FlowBlueprint<TReq, TResp> blueprint, ModuleCatalog catalog)
    {
        return PlanCompiler.Compile(blueprint, catalog);
    }
}

