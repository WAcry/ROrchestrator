namespace ROrchestrator.Core;

public enum LkgSnapshotLoadResultKind
{
    NotFound = 0,
    Loaded = 1,
    Corrupt = 2,
    Error = 3,
}

public interface ILkgSnapshotStore
{
    LkgSnapshotLoadResultKind TryLoad(out ConfigSnapshot snapshot);

    bool TryStore(in ConfigSnapshot snapshot);
}

