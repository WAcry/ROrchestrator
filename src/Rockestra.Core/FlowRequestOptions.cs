namespace Rockestra.Core;

public readonly struct FlowRequestOptions
{
    public IReadOnlyDictionary<string, string>? Variants { get; }

    public string? UserId { get; }

    public IReadOnlyDictionary<string, string>? RequestAttributes { get; }

    public FlowRequestOptions(
        IReadOnlyDictionary<string, string>? variants = null,
        string? userId = null,
        IReadOnlyDictionary<string, string>? requestAttributes = null)
    {
        if (variants is not null && variants.Count != 0)
        {
            if (variants is Dictionary<string, string> variantDictionary)
            {
                foreach (var pair in variantDictionary)
                {
                    if (string.IsNullOrEmpty(pair.Key))
                    {
                        throw new ArgumentException("Variant layer name must be non-empty.", nameof(variants));
                    }

                    if (pair.Value is null)
                    {
                        throw new ArgumentException("Variant value must be non-null.", nameof(variants));
                    }
                }
            }
            else
            {
                foreach (var pair in variants)
                {
                    if (string.IsNullOrEmpty(pair.Key))
                    {
                        throw new ArgumentException("Variant layer name must be non-empty.", nameof(variants));
                    }

                    if (pair.Value is null)
                    {
                        throw new ArgumentException("Variant value must be non-null.", nameof(variants));
                    }
                }
            }
        }

        if (requestAttributes is not null && requestAttributes.Count != 0)
        {
            if (requestAttributes is Dictionary<string, string> requestAttributeDictionary)
            {
                foreach (var pair in requestAttributeDictionary)
                {
                    if (string.IsNullOrEmpty(pair.Key))
                    {
                        throw new ArgumentException("Request attribute name must be non-empty.", nameof(requestAttributes));
                    }

                    if (pair.Value is null)
                    {
                        throw new ArgumentException("Request attribute value must be non-null.", nameof(requestAttributes));
                    }
                }
            }
            else
            {
                foreach (var pair in requestAttributes)
                {
                    if (string.IsNullOrEmpty(pair.Key))
                    {
                        throw new ArgumentException("Request attribute name must be non-empty.", nameof(requestAttributes));
                    }

                    if (pair.Value is null)
                    {
                        throw new ArgumentException("Request attribute value must be non-null.", nameof(requestAttributes));
                    }
                }
            }
        }

        Variants = variants;
        UserId = string.IsNullOrEmpty(userId) ? null : userId;
        RequestAttributes = requestAttributes;
    }
}

