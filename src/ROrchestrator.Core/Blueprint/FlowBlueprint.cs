namespace ROrchestrator.Core.Blueprint;

public static class FlowBlueprint
{
    public static FlowBlueprintBuilder<TReq, TResp> Define<TReq, TResp>(string name)
    {
        return new FlowBlueprintBuilder<TReq, TResp>(name);
    }

    public static FlowBlueprintBuilder<TReq, TResp> Define<TReq, TResp>()
    {
        return new FlowBlueprintBuilder<TReq, TResp>(typeof(TReq).Name);
    }
}

