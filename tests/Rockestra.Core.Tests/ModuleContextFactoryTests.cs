using Rockestra.Core.Testing;

namespace Rockestra.Core.Tests;

public sealed class ModuleContextFactoryTests
{
    [Fact]
    public void CreateModuleContext_ShouldUseConfiguredDeadlineCancellationAndExplainSink()
    {
        var services = new DummyServiceProvider();
        using var cts = new CancellationTokenSource();
        var deadline = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var explainSink = new RecordingExplainSink();

        var factory = ModuleContextFactory.Create()
            .WithServices(services)
            .WithCancellationToken(cts.Token)
            .WithDeadline(deadline)
            .WithExplainSink(explainSink);

        var ctx = factory.CreateModuleContext("m1", "test.module", args: 123);

        Assert.Same(services, ctx.Services);
        Assert.Equal(cts.Token, ctx.CancellationToken);
        Assert.Equal(deadline, ctx.Deadline);
        Assert.Same(explainSink, ctx.ExplainSink);

        ctx.ExplainSink.Add("k1", "v1");
        Assert.Equal(1, explainSink.Count);
        Assert.Equal("k1", explainSink.Entries[0].Key);
        Assert.Equal("v1", explainSink.Entries[0].Value);
    }

    private sealed class DummyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }

    private sealed class RecordingExplainSink : IExplainSink
    {
        private readonly Entry[] _entries;
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public IReadOnlyList<Entry> Entries => _entries;

        public RecordingExplainSink()
        {
            _entries = new Entry[4];
        }

        public void Add(string key, string value)
        {
            if (string.IsNullOrEmpty(key))
            {
                throw new ArgumentException("Key must be non-empty.", nameof(key));
            }

            if (value is null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            var index = _count;
            _entries[index] = new Entry(key, value);
            Volatile.Write(ref _count, index + 1);
        }

        public readonly struct Entry
        {
            public string Key { get; }
            public string Value { get; }

            public Entry(string key, string value)
            {
                Key = key;
                Value = value;
            }
        }
    }
}


