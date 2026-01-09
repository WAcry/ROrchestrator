namespace ROrchestrator.Core.Gates;

public abstract class Gate
{
    protected Gate()
    {
    }
}

public sealed class ExperimentGate : Gate
{
    private readonly string[] _variants;

    public string Layer { get; }

    public ReadOnlyMemory<string> Variants => _variants;

    public ExperimentGate(string layer, params string[] variants)
    {
        if (string.IsNullOrEmpty(layer))
        {
            throw new ArgumentException("Layer must be non-empty.", nameof(layer));
        }

        if (variants is null)
        {
            throw new ArgumentNullException(nameof(variants));
        }

        if (variants.Length == 0)
        {
            throw new ArgumentException("Variants must be non-empty.", nameof(variants));
        }

        for (var i = 0; i < variants.Length; i++)
        {
            if (string.IsNullOrEmpty(variants[i]))
            {
                throw new ArgumentException("Variants must contain only non-empty values.", nameof(variants));
            }
        }

        Layer = layer;
        _variants = variants;
    }
}

public sealed class AllGate : Gate
{
    private readonly Gate[] _children;

    public ReadOnlyMemory<Gate> Children => _children;

    public AllGate(params Gate[] children)
    {
        if (children is null)
        {
            throw new ArgumentNullException(nameof(children));
        }

        if (children.Length == 0)
        {
            throw new ArgumentException("Children must be non-empty.", nameof(children));
        }

        for (var i = 0; i < children.Length; i++)
        {
            if (children[i] is null)
            {
                throw new ArgumentException("Children must not contain null.", nameof(children));
            }
        }

        _children = children;
    }
}

public sealed class AnyGate : Gate
{
    private readonly Gate[] _children;

    public ReadOnlyMemory<Gate> Children => _children;

    public AnyGate(params Gate[] children)
    {
        if (children is null)
        {
            throw new ArgumentNullException(nameof(children));
        }

        if (children.Length == 0)
        {
            throw new ArgumentException("Children must be non-empty.", nameof(children));
        }

        for (var i = 0; i < children.Length; i++)
        {
            if (children[i] is null)
            {
                throw new ArgumentException("Children must not contain null.", nameof(children));
            }
        }

        _children = children;
    }
}

public sealed class NotGate : Gate
{
    public Gate Child { get; }

    public NotGate(Gate child)
    {
        Child = child ?? throw new ArgumentNullException(nameof(child));
    }
}

public sealed class RolloutGate : Gate
{
    public double Percent { get; }

    public string Salt { get; }

    public RolloutGate(double percent, string salt)
    {
        if (percent < 0 || percent > 100)
        {
            throw new ArgumentOutOfRangeException(nameof(percent), percent, "Percent must be between 0 and 100.");
        }

        Salt = string.IsNullOrEmpty(salt) ? throw new ArgumentException("Salt must be non-empty.", nameof(salt)) : salt;
        Percent = percent;
    }
}

public sealed class RequestAttrGate : Gate
{
    private readonly string[] _values;

    public string Field { get; }

    public ReadOnlyMemory<string> Values => _values;

    public RequestAttrGate(string field, params string[] values)
    {
        if (string.IsNullOrEmpty(field))
        {
            throw new ArgumentException("Field must be non-empty.", nameof(field));
        }

        if (values is null)
        {
            throw new ArgumentNullException(nameof(values));
        }

        if (values.Length == 0)
        {
            throw new ArgumentException("Values must be non-empty.", nameof(values));
        }

        for (var i = 0; i < values.Length; i++)
        {
            if (string.IsNullOrEmpty(values[i]))
            {
                throw new ArgumentException("Values must contain only non-empty values.", nameof(values));
            }
        }

        Field = field;
        _values = values;
    }
}
