namespace ROrchestrator.Core;

internal readonly struct RequestMemoKey : IEquatable<RequestMemoKey>
{
    public string ModuleType { get; }

    public string MemoKey { get; }

    public RuntimeTypeHandle OutType { get; }

    public bool IsShadow { get; }

    public RequestMemoKey(string moduleType, string memoKey, RuntimeTypeHandle outType, bool isShadow)
    {
        ModuleType = moduleType;
        MemoKey = memoKey;
        OutType = outType;
        IsShadow = isShadow;
    }

    public bool Equals(RequestMemoKey other)
    {
        return ModuleType == other.ModuleType
               && MemoKey == other.MemoKey
               && OutType.Equals(other.OutType)
               && IsShadow == other.IsShadow;
    }

    public override bool Equals(object? obj)
    {
        return obj is RequestMemoKey other && Equals(other);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            var hash = ModuleType.GetHashCode();
            hash = (hash * 397) ^ MemoKey.GetHashCode();
            hash = (hash * 397) ^ OutType.GetHashCode();
            hash = (hash * 397) ^ IsShadow.GetHashCode();
            return hash;
        }
    }
}

internal sealed class RequestMemo
{
    private readonly Lock _gate = new();
    private Dictionary<RequestMemoKey, object>? _tasks;

    public (Task<TResult> Task, bool Hit) GetOrAdd<TResult>(RequestMemoKey key, Func<Task<TResult>> factory)
    {
        if (factory is null)
        {
            throw new ArgumentNullException(nameof(factory));
        }

        TaskCompletionSource<TResult>? tcs = null;

        lock (_gate)
        {
            if (_tasks is not null && _tasks.TryGetValue(key, out var existing))
            {
                if (existing is Task<TResult> typed)
                {
                    return (typed, Hit: true);
                }

                throw new InvalidOperationException("RequestMemo key already exists with a different result type.");
            }

            tcs = new TaskCompletionSource<TResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            (_tasks ??= new Dictionary<RequestMemoKey, object>(capacity: 4)).Add(key, tcs.Task);
        }

        _ = CompleteAsync(tcs, factory);
        return (tcs.Task, Hit: false);
    }

    private static async Task CompleteAsync<TResult>(TaskCompletionSource<TResult> tcs, Func<Task<TResult>> factory)
    {
        try
        {
            var result = await factory().ConfigureAwait(false);
            _ = tcs.TrySetResult(result);
        }
        catch (Exception ex)
        {
            _ = tcs.TrySetException(ex);
        }
    }
}
