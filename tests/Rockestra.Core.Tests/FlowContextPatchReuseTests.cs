namespace Rockestra.Core.Tests;

public sealed class FlowContextPatchReuseTests
{
    private static readonly DateTimeOffset FutureDeadline = new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Params_ShouldReuseActivePatchEvaluation_AndNotParsePatchJsonAgain()
    {
        const string flowName = "PatchReuseFlow";

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);

        flowContext.ConfigureFlowBinding(flowName, typeof(TestParams), typeof(TestParamsPatch), new TestParams { A = 0 });

        _ = await flowContext.GetConfigSnapshotAsync(new StaticConfigProvider(configVersion: 1, patchJson: "not json"));

        var validPatchJson = "{\"schemaVersion\":\"v1\",\"flows\":{\"PatchReuseFlow\":{\"params\":{\"A\":123}}}}";

        using var evaluation = PatchEvaluatorV1.Evaluate(flowName, validPatchJson, requestOptions: default);

        flowContext.SetActivePatchEvaluation(evaluation, configVersion: 1);

        try
        {
            var resolved = flowContext.Params<TestParams>();
            Assert.Equal(123, resolved.A);
        }
        finally
        {
            flowContext.ClearActivePatchEvaluation(evaluation);
        }
    }

    private sealed class TestParams
    {
        public int A { get; init; }
    }

    private sealed class TestParamsPatch
    {
        public int? A { get; init; }
    }

    private sealed class DummyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType)
        {
            return null;
        }
    }

    private sealed class StaticConfigProvider : IConfigProvider
    {
        private readonly ConfigSnapshot _snapshot;

        public StaticConfigProvider(ulong configVersion, string patchJson)
        {
            _snapshot = new ConfigSnapshot(
                configVersion,
                patchJson,
                new ConfigSnapshotMeta(source: "static", timestampUtc: DateTimeOffset.UtcNow));
        }

        public ValueTask<ConfigSnapshot> GetSnapshotAsync(FlowContext context)
        {
            return new ValueTask<ConfigSnapshot>(_snapshot);
        }
    }
}


