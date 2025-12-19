namespace Flow.NET.Core.Tests;

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
}
