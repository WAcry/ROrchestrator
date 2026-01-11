using System.Text.Json;

namespace Rockestra.Core.Tests;

public sealed class FlowContextParamsTests
{
    private static readonly DateTimeOffset FutureDeadline = new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Params_ShouldMergeDefaultBaseExperimentQosEmergency_InOrder()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{" +
            "\"params\":{\"MaxCandidate\":10,\"Nested\":{\"Mode\":\"base\"}}," +
            "\"experiments\":[{\"layer\":\"l1\",\"variant\":\"B\",\"patch\":{\"params\":{\"MaxCandidate\":20,\"Nested\":{\"Threshold\":99}}}}]," +
            "\"qos\":{\"tiers\":{\"emergency\":{\"patch\":{\"params\":{\"MaxCandidate\":25,\"Nested\":{\"Mode\":\"qos\",\"Threshold\":123}}}}}}," +
            "\"emergency\":{\"reason\":\"r\",\"operator\":\"op\",\"ttl_minutes\":30,\"patch\":{\"params\":{\"MaxCandidate\":30,\"Nested\":{\"Mode\":\"emergency\"}}}}" +
            "}}}";

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(
            services,
            CancellationToken.None,
            FutureDeadline,
            requestOptions: new FlowRequestOptions(
                variants: new Dictionary<string, string>
                {
                    { "l1", "B" },
                }));

        flowContext.SetQosDecision(QosTier.Emergency, reasonCode: null, signals: null);

        flowContext.ConfigureFlowBinding(
            flowName: "HomeFeed",
            paramsType: typeof(TestParams),
            patchType: typeof(TestParamsPatch),
            defaultParams: new TestParams
            {
                MaxCandidate = 1,
                Nested = new TestNestedParams { Mode = "default", Threshold = 1 },
            });

        _ = await flowContext.GetConfigSnapshotAsync(new StaticConfigProvider(configVersion: 1, patchJson));

        var @params = flowContext.Params<TestParams>();

        Assert.Equal(30, @params.MaxCandidate);
        Assert.NotNull(@params.Nested);
        Assert.Equal("emergency", @params.Nested.Mode);
        Assert.Equal(123, @params.Nested.Threshold);

        var second = flowContext.Params<TestParams>();
        Assert.Same(@params, second);
    }

    [Fact]
    public async Task Params_ShouldThrowJsonException_WhenBindingFails()
    {
        var patchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{" +
            "\"params\":{\"MaxCandidate\":\"oops\"}" +
            "}}}";

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);

        flowContext.ConfigureFlowBinding(
            flowName: "HomeFeed",
            paramsType: typeof(TestParams),
            patchType: typeof(TestParamsPatch),
            defaultParams: new TestParams { MaxCandidate = 1, Nested = new TestNestedParams { Mode = "default", Threshold = 1 } });

        _ = await flowContext.GetConfigSnapshotAsync(new StaticConfigProvider(configVersion: 1, patchJson));

        _ = Assert.Throws<JsonException>(() => flowContext.Params<TestParams>());
    }

    [Fact]
    public async Task Params_ShouldThrowInvalidOperationException_WhenTypeDoesNotMatch()
    {
        var patchJson = "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"params\":{\"MaxCandidate\":10}}}}";

        var services = new DummyServiceProvider();
        var flowContext = new FlowContext(services, CancellationToken.None, FutureDeadline);

        flowContext.ConfigureFlowBinding(
            flowName: "HomeFeed",
            paramsType: typeof(TestParams),
            patchType: typeof(TestParamsPatch),
            defaultParams: new TestParams { MaxCandidate = 1, Nested = new TestNestedParams { Mode = "default", Threshold = 1 } });

        _ = await flowContext.GetConfigSnapshotAsync(new StaticConfigProvider(configVersion: 1, patchJson));

        _ = Assert.Throws<InvalidOperationException>(() => flowContext.Params<OtherParams>());
    }

    private sealed class TestParams
    {
        public int MaxCandidate { get; set; }

        public TestNestedParams Nested { get; set; } = new();
    }

    private sealed class TestNestedParams
    {
        public string Mode { get; set; } = string.Empty;

        public int Threshold { get; set; }
    }

    private sealed class TestParamsPatch
    {
        public int? MaxCandidate { get; set; }

        public TestNestedParamsPatch? Nested { get; set; }
    }

    private sealed class TestNestedParamsPatch
    {
        public string? Mode { get; set; }

        public int? Threshold { get; set; }
    }

    private sealed class OtherParams
    {
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

