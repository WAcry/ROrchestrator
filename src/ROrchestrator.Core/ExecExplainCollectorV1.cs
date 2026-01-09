using System.Diagnostics;

namespace ROrchestrator.Core;

internal sealed class ExecExplainCollectorV1
{
    private ExecExplainNode[]? _nodes;
    private List<ExecExplainStageModule>? _stageModules;
    private ExecExplain? _explain;
    private string? _flowName;
    private ulong _planHash;
    private long _flowStartTimestamp;
    private long _flowEndTimestamp;
    private bool _active;

    public bool IsActive => _active;

    public void Clear()
    {
        _nodes = null;
        _stageModules = null;
        _explain = null;
        _flowName = null;
        _planHash = 0;
        _flowStartTimestamp = 0;
        _flowEndTimestamp = 0;
        _active = false;
    }

    public void Start(string flowName, ulong planHash, IReadOnlyList<PlanNodeTemplate> nodes)
    {
        if (string.IsNullOrEmpty(flowName))
        {
            throw new ArgumentException("FlowName must be non-empty.", nameof(flowName));
        }

        if (nodes is null)
        {
            throw new ArgumentNullException(nameof(nodes));
        }

        var nodeCount = nodes.Count;
        if (nodeCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(nodes), nodeCount, "Nodes must be non-empty.");
        }

        _flowName = flowName;
        _planHash = planHash;
        _flowStartTimestamp = Stopwatch.GetTimestamp();
        _flowEndTimestamp = 0;
        _active = true;
        _explain = null;

        if (_stageModules is not null)
        {
            _stageModules.Clear();
        }

        var explainNodes = new ExecExplainNode[nodeCount];
        for (var i = 0; i < nodeCount; i++)
        {
            var node = nodes[i];
            explainNodes[i] = new ExecExplainNode(
                node.Kind,
                node.Name,
                node.StageName,
                node.ModuleType,
                startTimestamp: 0,
                endTimestamp: 0,
                outcomeKind: OutcomeKind.Unspecified,
                outcomeCode: string.Empty);
        }

        _nodes = explainNodes;
    }

    public void RecordStageModule(
        string stageName,
        string moduleId,
        string moduleType,
        int priority,
        OutcomeKind outcomeKind,
        string outcomeCode,
        bool isOverride)
    {
        if (!_active)
        {
            return;
        }

        _stageModules ??= new List<ExecExplainStageModule>(capacity: 8);
        _stageModules.Add(new ExecExplainStageModule(stageName, moduleId, moduleType, priority, outcomeKind, outcomeCode, isOverride));
    }

    public void RecordNode(PlanNodeTemplate node, long startTimestamp, long endTimestamp, OutcomeKind outcomeKind, string outcomeCode)
    {
        if (!_active)
        {
            return;
        }

        _nodes![node.Index] = new ExecExplainNode(
            node.Kind,
            node.Name,
            node.StageName,
            node.ModuleType,
            startTimestamp,
            endTimestamp,
            outcomeKind,
            outcomeCode);
    }

    public void Finish(FlowContext context)
    {
        if (!_active)
        {
            return;
        }

        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        _flowEndTimestamp = Stopwatch.GetTimestamp();
        var hasConfigVersion = context.TryGetConfigVersion(out var configVersion);
        var nodes = _nodes;
        if (nodes is null)
        {
            throw new InvalidOperationException("ExecExplainCollectorV1 has not been started.");
        }

        var stageModules = _stageModules;
        var stageModulesArray = stageModules is null || stageModules.Count == 0
            ? Array.Empty<ExecExplainStageModule>()
            : stageModules.ToArray();

        _explain = new ExecExplain(
            _flowName!,
            _planHash,
            hasConfigVersion,
            configVersion,
            _flowStartTimestamp,
            _flowEndTimestamp,
            nodes,
            stageModulesArray);
        _active = false;
    }

    public bool TryGetExplain(out ExecExplain explain)
    {
        var current = _explain;
        if (current is null)
        {
            explain = null!;
            return false;
        }

        explain = current;
        return true;
    }
}
