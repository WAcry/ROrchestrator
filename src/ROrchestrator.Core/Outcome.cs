namespace ROrchestrator.Core;

public readonly struct Outcome<T>
{
    private readonly T? _value;
    private readonly string? _code;

    public OutcomeKind Kind { get; }

    public bool IsOk => Kind == OutcomeKind.Ok;

    public bool IsError => Kind == OutcomeKind.Error;

    public bool IsTimeout => Kind == OutcomeKind.Timeout;

    public bool IsSkipped => Kind == OutcomeKind.Skipped;

    public bool IsFallback => Kind == OutcomeKind.Fallback;

    public bool IsCanceled => Kind == OutcomeKind.Canceled;

    public string Code => _code ?? string.Empty;

    public T Value
    {
        get
        {
            if (Kind != OutcomeKind.Ok && Kind != OutcomeKind.Fallback)
            {
                throw new InvalidOperationException("Outcome does not contain a value.");
            }

            return _value!;
        }
    }

    private Outcome(OutcomeKind kind, T? value, string? code)
    {
        Kind = kind;
        _value = value;
        _code = code;
    }

    public static Outcome<T> Ok(T value)
    {
        if (value is null)
        {
            throw new ArgumentNullException(nameof(value));
        }

        return new Outcome<T>(OutcomeKind.Ok, value, code: "OK");
    }

    public static Outcome<T> Error(string code)
    {
        if (string.IsNullOrEmpty(code))
        {
            throw new ArgumentException("Code must be non-empty.", nameof(code));
        }

        return new Outcome<T>(OutcomeKind.Error, value: default, code);
    }

    public static Outcome<T> Timeout(string code)
    {
        if (string.IsNullOrEmpty(code))
        {
            throw new ArgumentException("Code must be non-empty.", nameof(code));
        }

        return new Outcome<T>(OutcomeKind.Timeout, value: default, code);
    }

    public static Outcome<T> Skipped(string code)
    {
        if (string.IsNullOrEmpty(code))
        {
            throw new ArgumentException("Code must be non-empty.", nameof(code));
        }

        return new Outcome<T>(OutcomeKind.Skipped, value: default, code);
    }

    public static Outcome<T> Fallback(T value, string code)
    {
        if (value is null)
        {
            throw new ArgumentNullException(nameof(value));
        }

        if (string.IsNullOrEmpty(code))
        {
            throw new ArgumentException("Code must be non-empty.", nameof(code));
        }

        return new Outcome<T>(OutcomeKind.Fallback, value, code);
    }

    public static Outcome<T> Canceled(string code)
    {
        if (string.IsNullOrEmpty(code))
        {
            throw new ArgumentException("Code must be non-empty.", nameof(code));
        }

        return new Outcome<T>(OutcomeKind.Canceled, value: default, code);
    }
}
