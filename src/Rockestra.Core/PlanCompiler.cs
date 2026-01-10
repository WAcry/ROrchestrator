using System.Collections.Frozen;
using Rockestra.Core.Blueprint;

namespace Rockestra.Core;

public static class PlanCompiler
{
    public static PlanTemplate<TReq, TResp> Compile<TReq, TResp>(FlowBlueprint<TReq, TResp> blueprint, ModuleCatalog catalog)
    {
        return CompileCore(blueprint, catalog, includeExplain: false, out _);
    }

    public static (PlanTemplate<TReq, TResp> Template, PlanExplain Explain) CompileWithExplain<TReq, TResp>(
        FlowBlueprint<TReq, TResp> blueprint,
        ModuleCatalog catalog)
    {
        var template = CompileCore(blueprint, catalog, includeExplain: true, out var explain);
        return (template, explain!);
    }

    private static PlanTemplate<TReq, TResp> CompileCore<TReq, TResp>(
        FlowBlueprint<TReq, TResp> blueprint,
        ModuleCatalog catalog,
        bool includeExplain,
        out PlanExplain? explain)
    {
        if (blueprint is null)
        {
            throw new ArgumentNullException(nameof(blueprint));
        }

        if (catalog is null)
        {
            throw new ArgumentNullException(nameof(catalog));
        }

        if (string.IsNullOrEmpty(blueprint.Name))
        {
            throw new InvalidOperationException("Flow name must be non-empty.");
        }

        var nodes = blueprint.Nodes;
        var nodeCount = nodes.Count;

        if (nodeCount == 0)
        {
            throw new InvalidOperationException($"Flow '{blueprint.Name}' must contain at least one node.");
        }

        var planNodes = new PlanNodeTemplate[nodeCount];
        var explainNodes = includeExplain ? new PlanExplainNode[nodeCount] : null;
        var nodeNameToIndex = new Dictionary<string, int>(nodeCount);

        var hash = PlanHashBuilder.Create();
        PlanHashBuilder.AddString(ref hash, blueprint.Name);
        PlanHashBuilder.AddType(ref hash, typeof(TReq));
        PlanHashBuilder.AddType(ref hash, typeof(TResp));

        for (var i = 0; i < nodeCount; i++)
        {
            var node = nodes[i];
            nodeNameToIndex.Add(node.Name, i);

            PlanHashBuilder.AddInt32(ref hash, (int)node.Kind);
            PlanHashBuilder.AddString(ref hash, node.Name);
            PlanHashBuilder.AddString(ref hash, node.StageName);

            if (node.Kind == BlueprintNodeKind.Step)
            {
                var moduleType = node.ModuleType;

                if (string.IsNullOrEmpty(moduleType))
                {
                    throw new InvalidOperationException(
                        $"Flow '{blueprint.Name}' node '{node.Name}' must specify a non-empty module type.");
                }

                if (!catalog.TryGetSignature(moduleType, out var argsType, out var outType))
                {
                    throw new InvalidOperationException(
                        $"Flow '{blueprint.Name}' node '{node.Name}' references unregistered module type '{moduleType}'.");
                }

                if (argsType != typeof(TReq))
                {
                    throw new InvalidOperationException(
                        $"Flow '{blueprint.Name}' node '{node.Name}' module type '{moduleType}' has a different signature.");
                }

                planNodes[i] = PlanNodeTemplate.CreateStep(i, node.Name, node.StageName, moduleType, outType);

                if (explainNodes is not null)
                {
                    explainNodes[i] = PlanExplainNode.CreateStep(node.Name, node.StageName, moduleType, outType);
                }

                PlanHashBuilder.AddString(ref hash, moduleType);
                PlanHashBuilder.AddType(ref hash, outType);
                continue;
            }

            if (node.Kind == BlueprintNodeKind.Join)
            {
                var join = node.Join;
                var outType = node.JoinOutputType;

                if (join is null)
                {
                    throw new InvalidOperationException(
                        $"Flow '{blueprint.Name}' node '{node.Name}' must specify a non-null join delegate.");
                }

                if (outType is null)
                {
                    throw new InvalidOperationException(
                        $"Flow '{blueprint.Name}' node '{node.Name}' must specify a non-null join output type.");
                }

                planNodes[i] = PlanNodeTemplate.CreateJoin(i, node.Name, node.StageName, join, outType);

                if (explainNodes is not null)
                {
                    explainNodes[i] = PlanExplainNode.CreateJoin(node.Name, node.StageName, outType);
                }

                PlanHashBuilder.AddType(ref hash, outType);
                continue;
            }

            throw new InvalidOperationException($"Unsupported node kind: '{node.Kind}'.");
        }

        EnsureFinalOutputType<TResp>(blueprint.Name, planNodes[nodeCount - 1]);

        if (explainNodes is null)
        {
            explain = null;
        }
        else
        {
            explain = new PlanExplain(blueprint.Name, planTemplateHash: hash, explainNodes);
        }

        var frozenNodeNameToIndex = nodeNameToIndex.ToFrozenDictionary();
        return new PlanTemplate<TReq, TResp>(blueprint.Name, planHash: hash, planNodes, frozenNodeNameToIndex, blueprint.StageContracts);
    }

    private static void EnsureFinalOutputType<TResp>(string flowName, PlanNodeTemplate lastNode)
    {
        if (lastNode.Kind == BlueprintNodeKind.Join)
        {
            if (lastNode.OutputType != typeof(TResp))
            {
                throw new InvalidOperationException(
                    $"Flow '{flowName}' final node '{lastNode.Name}' has output type '{lastNode.OutputType}', not '{typeof(TResp)}'.");
            }

            return;
        }

        if (lastNode.Kind == BlueprintNodeKind.Step)
        {
            throw new InvalidOperationException(
                $"Flow '{flowName}' final node '{lastNode.Name}' must be a join that produces '{typeof(TResp)}'.");
        }

        throw new InvalidOperationException(
            $"Flow '{flowName}' final node '{lastNode.Name}' has unsupported kind '{lastNode.Kind}'.");
    }

    private static class PlanHashBuilder
    {
        private const ulong OffsetBasis = 14695981039346656037ul;
        private const ulong Prime = 1099511628211ul;

        public static ulong Create()
        {
            return OffsetBasis;
        }

        public static void AddByte(ref ulong hash, byte value)
        {
            hash ^= value;
            hash *= Prime;
        }

        public static void AddInt32(ref ulong hash, int value)
        {
            unchecked
            {
                AddByte(ref hash, (byte)value);
                AddByte(ref hash, (byte)(value >> 8));
                AddByte(ref hash, (byte)(value >> 16));
                AddByte(ref hash, (byte)(value >> 24));
            }
        }

        public static void AddUInt16(ref ulong hash, ushort value)
        {
            AddByte(ref hash, (byte)value);
            AddByte(ref hash, (byte)(value >> 8));
        }

        public static void AddString(ref ulong hash, string? value)
        {
            if (value is null)
            {
                AddByte(ref hash, 0);
                return;
            }

            AddByte(ref hash, 1);
            AddInt32(ref hash, value.Length);

            for (var i = 0; i < value.Length; i++)
            {
                AddUInt16(ref hash, value[i]);
            }
        }

        public static void AddType(ref ulong hash, Type type)
        {
            if (type is null)
            {
                AddByte(ref hash, 0);
                return;
            }

            AddByte(ref hash, 1);

            if (type.IsArray)
            {
                AddByte(ref hash, 2);
                AddInt32(ref hash, type.GetArrayRank());
                AddType(ref hash, type.GetElementType()!);
                return;
            }

            if (type.IsGenericType)
            {
                AddByte(ref hash, 3);

                var definition = type.GetGenericTypeDefinition();
                AddString(ref hash, definition.FullName ?? definition.Name);
                AddString(ref hash, definition.Assembly.GetName().Name);

                var args = type.GetGenericArguments();
                AddInt32(ref hash, args.Length);

                for (var i = 0; i < args.Length; i++)
                {
                    AddType(ref hash, args[i]);
                }

                return;
            }

            AddByte(ref hash, 4);
            AddString(ref hash, type.FullName ?? type.Name);
            AddString(ref hash, type.Assembly.GetName().Name);
        }
    }
}

