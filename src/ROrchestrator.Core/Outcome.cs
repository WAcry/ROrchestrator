namespace ROrchestrator.Core;

public readonly struct Outcome<T>
{
    private readonly T? _value;
    private readonly string? _code;

    public OutcomeKind Kind { get; }

    public bool IsOk => Kind == OutcomeKind.Ok;

    public bool IsError => Kind == OutcomeKind.Error;

    public string Code => _code ?? string.Empty;

    public T Value
    {
        get
        {
            if (Kind != OutcomeKind.Ok)
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
}
