using System.Text.Json;

namespace Rockestra.Core;

public sealed class PersistedLkgConfigProvider : IConfigProvider
{
    private static readonly FlowRequestOptions EmptyRequestOptions = new();

    private readonly IConfigProvider _inner;
    private readonly ConfigValidator _validator;
    private readonly ILkgSnapshotStore _store;
    private readonly Lock _gate;
    private SnapshotBox _lkg;
    private RejectedCandidate? _rejected;

    private int _validationAttemptCount;

    internal int ValidationAttemptCount => Volatile.Read(ref _validationAttemptCount);

    public PersistedLkgConfigProvider(IConfigProvider inner, ConfigValidator validator, ILkgSnapshotStore store)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _validator = validator ?? throw new ArgumentNullException(nameof(validator));
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _gate = new Lock();
        _lkg = new SnapshotBox(new ConfigSnapshot(configVersion: 0, patchJson: string.Empty));

        TryLoadInitialLkg();
    }

    public ValueTask<ConfigSnapshot> GetSnapshotAsync(FlowContext context)
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        ValueTask<ConfigSnapshot> snapshotTask;

        try
        {
            snapshotTask = _inner.GetSnapshotAsync(context);
        }
        catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
        {
            RecordFallback(context);
            return new ValueTask<ConfigSnapshot>(Volatile.Read(ref _lkg).Snapshot);
        }

        if (snapshotTask.IsCompletedSuccessfully)
        {
            return new ValueTask<ConfigSnapshot>(HandleCandidate(context, snapshotTask.Result));
        }

        return new ValueTask<ConfigSnapshot>(HandleCandidateAsync(context, snapshotTask));
    }

    private void TryLoadInitialLkg()
    {
        ConfigSnapshot snapshot;
        LkgSnapshotLoadResultKind resultKind;

        try
        {
            resultKind = _store.TryLoad(out snapshot);
        }
        catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
        {
            _ = ex;
            Observability.FlowMetricsV1.RecordConfigLkgSnapshotLoadFailure();
            return;
        }

        if (resultKind != LkgSnapshotLoadResultKind.Loaded)
        {
            if (resultKind != LkgSnapshotLoadResultKind.NotFound)
            {
                Observability.FlowMetricsV1.RecordConfigLkgSnapshotLoadFailure();
            }

            return;
        }

        try
        {
            if (!TryAcceptCandidate(snapshot))
            {
                Observability.FlowMetricsV1.RecordConfigLkgSnapshotLoadFailure();
                return;
            }
        }
        catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
        {
            _ = ex;
            Observability.FlowMetricsV1.RecordConfigLkgSnapshotLoadFailure();
            return;
        }

        _lkg = new SnapshotBox(snapshot);
    }

    private async Task<ConfigSnapshot> HandleCandidateAsync(FlowContext context, ValueTask<ConfigSnapshot> snapshotTask)
    {
        ConfigSnapshot candidate;

        try
        {
            candidate = await snapshotTask.ConfigureAwait(false);
        }
        catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
        {
            RecordFallback(context);
            return Volatile.Read(ref _lkg).Snapshot;
        }

        return HandleCandidate(context, candidate);
    }

    private ConfigSnapshot HandleCandidate(FlowContext context, ConfigSnapshot candidate)
    {
        var lkg = Volatile.Read(ref _lkg);

        if (candidate.ConfigVersion == lkg.Snapshot.ConfigVersion)
        {
            return lkg.Snapshot;
        }

        var rejected = Volatile.Read(ref _rejected);
        if (rejected is not null && rejected.ConfigVersion == candidate.ConfigVersion)
        {
            RecordFallback(context);
            return lkg.Snapshot;
        }

        lock (_gate)
        {
            lkg = _lkg;

            if (candidate.ConfigVersion == lkg.Snapshot.ConfigVersion)
            {
                return lkg.Snapshot;
            }

            rejected = _rejected;
            if (rejected is not null && rejected.ConfigVersion == candidate.ConfigVersion)
            {
                RecordFallback(context);
                return lkg.Snapshot;
            }

            Interlocked.Increment(ref _validationAttemptCount);

            if (!TryAcceptCandidate(candidate))
            {
                Volatile.Write(ref _rejected, new RejectedCandidate(candidate.ConfigVersion));
                RecordFallback(context);
                return lkg.Snapshot;
            }

            var accepted = new SnapshotBox(candidate);
            Volatile.Write(ref _lkg, accepted);
            Volatile.Write(ref _rejected, null);

            if (!_store.TryStore(candidate))
            {
                Observability.FlowMetricsV1.RecordConfigLkgSnapshotPersistFailure();

                try
                {
                    context.ExplainSink.Add("config_lkg_snapshot_persist_failure", "true");
                }
                catch
                {
                }
            }

            return candidate;
        }
    }

    private bool TryAcceptCandidate(ConfigSnapshot candidate)
    {
        var patchJson = candidate.PatchJson;

        if (patchJson.Length == 0)
        {
            return true;
        }

        ValidationReport report;

        try
        {
            report = _validator.ValidatePatchJson(patchJson);
        }
        catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
        {
            _ = ex;
            return false;
        }

        if (!report.IsValid)
        {
            return false;
        }

        return TryCompilePatch(patchJson, candidate.ConfigVersion);
    }

    private static bool TryCompilePatch(string patchJson, ulong configVersion)
    {
        JsonDocument document;

        try
        {
            document = JsonDocument.Parse(patchJson);
        }
        catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
        {
            _ = ex;
            return false;
        }

        using (document)
        {
            var root = document.RootElement;

            if (root.ValueKind != JsonValueKind.Object)
            {
                return false;
            }

            if (!root.TryGetProperty("flows", out var flowsElement))
            {
                return true;
            }

            if (flowsElement.ValueKind != JsonValueKind.Object)
            {
                return false;
            }

            foreach (var flowProperty in flowsElement.EnumerateObject())
            {
                var flowName = flowProperty.Name;

                try
                {
                    using var evaluation = PatchEvaluatorV1.Evaluate(flowName, patchJson, EmptyRequestOptions, configVersion);
                }
                catch (Exception ex) when (ExceptionGuard.ShouldHandle(ex))
                {
                    _ = ex;
                    return false;
                }
            }
        }

        return true;
    }

    private static void RecordFallback(FlowContext context)
    {
        try
        {
            context.ExplainSink.Add("config_lkg_fallback", "true");
        }
        catch
        {
        }

        if (context.TryGetFlowNameHint(out var flowName))
        {
            Observability.FlowMetricsV1.RecordConfigLkgFallback(flowName);
        }
    }

    private sealed class SnapshotBox
    {
        public ConfigSnapshot Snapshot { get; }

        public SnapshotBox(ConfigSnapshot snapshot)
        {
            Snapshot = snapshot;
        }
    }

    private sealed class RejectedCandidate
    {
        public ulong ConfigVersion { get; }

        public RejectedCandidate(ulong configVersion)
        {
            ConfigVersion = configVersion;
        }
    }
}


