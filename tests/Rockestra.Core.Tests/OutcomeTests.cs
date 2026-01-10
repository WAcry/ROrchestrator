using Rockestra.Core;

namespace Rockestra.Core.Tests;

public class OutcomeTests
{
    [Fact]
    public void Default_ShouldBeUnspecified_AndHaveEmptyCode_AndNotExposeValue()
    {
        var outcome = default(Outcome<int>);

        Assert.Equal(OutcomeKind.Unspecified, outcome.Kind);
        Assert.False(outcome.IsOk);
        Assert.False(outcome.IsError);
        Assert.Equal(string.Empty, outcome.Code);

        Assert.Throws<InvalidOperationException>(() => { _ = outcome.Value; });
    }

    [Fact]
    public void Ok_ShouldExposeValueAndKind()
    {
        var outcome = Outcome<int>.Ok(42);

        Assert.Equal(OutcomeKind.Ok, outcome.Kind);
        Assert.True(outcome.IsOk);
        Assert.False(outcome.IsError);
        Assert.Equal("OK", outcome.Code);
        Assert.Equal(42, outcome.Value);
    }

    [Fact]
    public void Error_ShouldExposeCodeAndKind()
    {
        var outcome = Outcome<int>.Error("boom");

        Assert.Equal(OutcomeKind.Error, outcome.Kind);
        Assert.False(outcome.IsOk);
        Assert.True(outcome.IsError);
        Assert.Equal("boom", outcome.Code);

        Assert.Throws<InvalidOperationException>(() => { _ = outcome.Value; });
    }

    [Fact]
    public void Error_ShouldRejectNullOrEmptyCode()
    {
        Assert.Throws<ArgumentException>(() => { _ = Outcome<int>.Error(""); });
        Assert.Throws<ArgumentException>(() => { _ = Outcome<int>.Error(null!); });
    }

    [Fact]
    public void Ok_ShouldRejectNullValue()
    {
        Assert.Throws<ArgumentNullException>(() => { _ = Outcome<string>.Ok(null!); });
    }

    [Fact]
    public void Timeout_ShouldExposeCodeAndKind()
    {
        var outcome = Outcome<int>.Timeout("BUDGET_TIMEOUT");

        Assert.Equal(OutcomeKind.Timeout, outcome.Kind);
        Assert.False(outcome.IsOk);
        Assert.False(outcome.IsError);
        Assert.True(outcome.IsTimeout);
        Assert.Equal("BUDGET_TIMEOUT", outcome.Code);

        Assert.Throws<InvalidOperationException>(() => { _ = outcome.Value; });
    }

    [Fact]
    public void Timeout_ShouldRejectNullOrEmptyCode()
    {
        Assert.Throws<ArgumentException>(() => { _ = Outcome<int>.Timeout(""); });
        Assert.Throws<ArgumentException>(() => { _ = Outcome<int>.Timeout(null!); });
    }

    [Fact]
    public void Skipped_ShouldExposeCodeAndKind()
    {
        var outcome = Outcome<int>.Skipped("GATE_FALSE");

        Assert.Equal(OutcomeKind.Skipped, outcome.Kind);
        Assert.False(outcome.IsOk);
        Assert.False(outcome.IsError);
        Assert.True(outcome.IsSkipped);
        Assert.Equal("GATE_FALSE", outcome.Code);

        Assert.Throws<InvalidOperationException>(() => { _ = outcome.Value; });
    }

    [Fact]
    public void Skipped_ShouldRejectNullOrEmptyCode()
    {
        Assert.Throws<ArgumentException>(() => { _ = Outcome<int>.Skipped(""); });
        Assert.Throws<ArgumentException>(() => { _ = Outcome<int>.Skipped(null!); });
    }

    [Fact]
    public void Fallback_ShouldExposeValueCodeAndKind()
    {
        var outcome = Outcome<int>.Fallback(42, "PRIMARY_FAILED");

        Assert.Equal(OutcomeKind.Fallback, outcome.Kind);
        Assert.False(outcome.IsOk);
        Assert.False(outcome.IsError);
        Assert.True(outcome.IsFallback);
        Assert.Equal("PRIMARY_FAILED", outcome.Code);
        Assert.Equal(42, outcome.Value);
    }

    [Fact]
    public void Fallback_ShouldRejectNullValue()
    {
        Assert.Throws<ArgumentNullException>(() => { _ = Outcome<string>.Fallback(null!, "PRIMARY_FAILED"); });
    }

    [Fact]
    public void Fallback_ShouldRejectNullOrEmptyCode()
    {
        Assert.Throws<ArgumentException>(() => { _ = Outcome<int>.Fallback(42, ""); });
        Assert.Throws<ArgumentException>(() => { _ = Outcome<int>.Fallback(42, null!); });
    }

    [Fact]
    public void Canceled_ShouldExposeCodeAndKind()
    {
        var outcome = Outcome<int>.Canceled("UPSTREAM_CANCELED");

        Assert.Equal(OutcomeKind.Canceled, outcome.Kind);
        Assert.False(outcome.IsOk);
        Assert.False(outcome.IsError);
        Assert.True(outcome.IsCanceled);
        Assert.Equal("UPSTREAM_CANCELED", outcome.Code);

        Assert.Throws<InvalidOperationException>(() => { _ = outcome.Value; });
    }

    [Fact]
    public void Canceled_ShouldRejectNullOrEmptyCode()
    {
        Assert.Throws<ArgumentException>(() => { _ = Outcome<int>.Canceled(""); });
        Assert.Throws<ArgumentException>(() => { _ = Outcome<int>.Canceled(null!); });
    }
}

