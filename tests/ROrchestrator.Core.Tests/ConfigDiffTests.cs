using ROrchestrator.Core;

namespace ROrchestrator.Core.Tests;

public sealed class ConfigDiffTests
{
    [Fact]
    public void DiffModules_ShouldReportAdded_WhenModuleIsAdded()
    {
        var oldPatchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"t1\",\"with\":{}}]}}}}}";

        var newPatchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"t1\",\"with\":{}},{\"id\":\"m2\",\"use\":\"t2\",\"with\":{}}]}}}}}";

        var report = PatchDiffV1.DiffModules(oldPatchJson, newPatchJson);

        var diff = Assert.Single(report.Diffs);
        Assert.Equal(PatchModuleDiffKind.Added, diff.Kind);
        Assert.Equal("HomeFeed", diff.FlowName);
        Assert.Null(diff.ExperimentLayer);
        Assert.Null(diff.ExperimentVariant);
        Assert.Equal("s1", diff.StageName);
        Assert.Equal("m2", diff.ModuleId);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[1]", diff.Path);
    }

    [Fact]
    public void DiffModules_ShouldReportRemoved_WhenModuleIsRemoved()
    {
        var oldPatchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"t1\",\"with\":{}},{\"id\":\"m2\",\"use\":\"t2\",\"with\":{}}]}}}}}";

        var newPatchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"t1\",\"with\":{}}]}}}}}";

        var report = PatchDiffV1.DiffModules(oldPatchJson, newPatchJson);

        var diff = Assert.Single(report.Diffs);
        Assert.Equal(PatchModuleDiffKind.Removed, diff.Kind);
        Assert.Equal("HomeFeed", diff.FlowName);
        Assert.Equal("s1", diff.StageName);
        Assert.Equal("m2", diff.ModuleId);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[1]", diff.Path);
    }

    [Fact]
    public void DiffModules_ShouldReportUseChanged_WhenModuleUseChanges()
    {
        var oldPatchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"t1\",\"with\":{}}]}}}}}";

        var newPatchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"t2\",\"with\":{}}]}}}}}";

        var report = PatchDiffV1.DiffModules(oldPatchJson, newPatchJson);

        var diff = Assert.Single(report.Diffs);
        Assert.Equal(PatchModuleDiffKind.UseChanged, diff.Kind);
        Assert.Equal("HomeFeed", diff.FlowName);
        Assert.Equal("s1", diff.StageName);
        Assert.Equal("m1", diff.ModuleId);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].use", diff.Path);
    }

    [Fact]
    public void DiffModules_ShouldReportWithChanged_WhenModuleWithChangesDeeply()
    {
        var oldPatchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"t1\",\"with\":{\"a\":{\"b\":1}}}]}}}}}";

        var newPatchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"t1\",\"with\":{\"a\":{\"b\":2}}}]}}}}}";

        var report = PatchDiffV1.DiffModules(oldPatchJson, newPatchJson);

        var diff = Assert.Single(report.Diffs);
        Assert.Equal(PatchModuleDiffKind.WithChanged, diff.Kind);
        Assert.Equal("HomeFeed", diff.FlowName);
        Assert.Equal("s1", diff.StageName);
        Assert.Equal("m1", diff.ModuleId);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0].with", diff.Path);
    }

    [Fact]
    public void DiffModules_ShouldBeEmpty_WhenOnlyModuleOrderChanges()
    {
        var oldPatchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"t1\",\"with\":{}},{\"id\":\"m2\",\"use\":\"t2\",\"with\":{}}]}}}}}";

        var newPatchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m2\",\"use\":\"t2\",\"with\":{}},{\"id\":\"m1\",\"use\":\"t1\",\"with\":{}}]}}}}}";

        var report = PatchDiffV1.DiffModules(oldPatchJson, newPatchJson);

        Assert.Empty(report.Diffs);
    }

    [Fact]
    public void DiffModules_ShouldReportRemovedAndAdded_WhenModuleMovesAcrossStages()
    {
        var oldPatchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"t1\",\"with\":{}}]}}}}}";

        var newPatchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"stages\":{\"s2\":{\"modules\":[{\"id\":\"m1\",\"use\":\"t1\",\"with\":{}}]}}}}}";

        var report = PatchDiffV1.DiffModules(oldPatchJson, newPatchJson);

        Assert.Equal(2, report.Diffs.Count);

        var removed = report.Diffs[0];
        Assert.Equal(PatchModuleDiffKind.Removed, removed.Kind);
        Assert.Equal("HomeFeed", removed.FlowName);
        Assert.Null(removed.ExperimentLayer);
        Assert.Null(removed.ExperimentVariant);
        Assert.Equal("s1", removed.StageName);
        Assert.Equal("m1", removed.ModuleId);
        Assert.Equal("$.flows.HomeFeed.stages.s1.modules[0]", removed.Path);

        var added = report.Diffs[1];
        Assert.Equal(PatchModuleDiffKind.Added, added.Kind);
        Assert.Equal("HomeFeed", added.FlowName);
        Assert.Null(added.ExperimentLayer);
        Assert.Null(added.ExperimentVariant);
        Assert.Equal("s2", added.StageName);
        Assert.Equal("m1", added.ModuleId);
        Assert.Equal("$.flows.HomeFeed.stages.s2.modules[0]", added.Path);
    }

    [Fact]
    public void DiffModules_ShouldReportUseChanged_WhenExperimentPatchModuleUseChanges()
    {
        var oldPatchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"t1\",\"with\":{}}]}}}}]}}}";

        var newPatchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"t2\",\"with\":{}}]}}}}]}}}";

        var report = PatchDiffV1.DiffModules(oldPatchJson, newPatchJson);

        var diff = Assert.Single(report.Diffs);
        Assert.Equal(PatchModuleDiffKind.UseChanged, diff.Kind);
        Assert.Equal("HomeFeed", diff.FlowName);
        Assert.Equal("l1", diff.ExperimentLayer);
        Assert.Equal("v1", diff.ExperimentVariant);
        Assert.Equal("s1", diff.StageName);
        Assert.Equal("m1", diff.ModuleId);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch.stages.s1.modules[0].use", diff.Path);
    }

    [Fact]
    public void DiffModules_ShouldBeEmpty_WhenOnlyExperimentOrderChanges()
    {
        var oldPatchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"t1\",\"with\":{}}]}}}},{\"layer\":\"l2\",\"variant\":\"v2\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m2\",\"use\":\"t2\",\"with\":{}}]}}}}]}}}";

        var newPatchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l2\",\"variant\":\"v2\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m2\",\"use\":\"t2\",\"with\":{}}]}}}},{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"t1\",\"with\":{}}]}}}}]}}}";

        var report = PatchDiffV1.DiffModules(oldPatchJson, newPatchJson);

        Assert.Empty(report.Diffs);
    }

    [Fact]
    public void DiffModules_ShouldReportRemovedAndAdded_WhenExperimentMappingIsRemovedOrAdded()
    {
        var oldPatchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l1\",\"variant\":\"v1\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m1\",\"use\":\"t1\",\"with\":{}}]}}}}]}}}";

        var newPatchJson =
            "{\"schemaVersion\":\"v1\",\"flows\":{\"HomeFeed\":{\"experiments\":[{\"layer\":\"l2\",\"variant\":\"v2\",\"patch\":{\"stages\":{\"s1\":{\"modules\":[{\"id\":\"m2\",\"use\":\"t2\",\"with\":{}}]}}}}]}}}";

        var report = PatchDiffV1.DiffModules(oldPatchJson, newPatchJson);

        Assert.Equal(2, report.Diffs.Count);

        var removed = report.Diffs[0];
        Assert.Equal(PatchModuleDiffKind.Removed, removed.Kind);
        Assert.Equal("HomeFeed", removed.FlowName);
        Assert.Equal("l1", removed.ExperimentLayer);
        Assert.Equal("v1", removed.ExperimentVariant);
        Assert.Equal("s1", removed.StageName);
        Assert.Equal("m1", removed.ModuleId);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch.stages.s1.modules[0]", removed.Path);

        var added = report.Diffs[1];
        Assert.Equal(PatchModuleDiffKind.Added, added.Kind);
        Assert.Equal("HomeFeed", added.FlowName);
        Assert.Equal("l2", added.ExperimentLayer);
        Assert.Equal("v2", added.ExperimentVariant);
        Assert.Equal("s1", added.StageName);
        Assert.Equal("m2", added.ModuleId);
        Assert.Equal("$.flows.HomeFeed.experiments[0].patch.stages.s1.modules[0]", added.Path);
    }

    [Fact]
    public void DiffModules_ShouldThrow_WhenJsonIsInvalid()
    {
        var validPatchJson = "{\"schemaVersion\":\"v1\",\"flows\":{}}";
        Assert.Throws<FormatException>(() => PatchDiffV1.DiffModules("{", validPatchJson));
    }

    [Fact]
    public void DiffModules_ShouldThrow_WhenNewJsonIsInvalid()
    {
        var validPatchJson = "{\"schemaVersion\":\"v1\",\"flows\":{}}";
        Assert.Throws<FormatException>(() => PatchDiffV1.DiffModules(validPatchJson, "{"));
    }

    [Fact]
    public void DiffModules_ShouldThrow_WhenSchemaVersionIsUnsupported()
    {
        var validPatchJson = "{\"schemaVersion\":\"v1\",\"flows\":{}}";
        Assert.Throws<NotSupportedException>(() => PatchDiffV1.DiffModules("{\"schemaVersion\":\"v999\",\"flows\":{}}", validPatchJson));
    }

    [Fact]
    public void DiffModules_ShouldThrowNotSupported_WhenOldSchemaUnsupported_EvenIfNewJsonIsInvalid()
    {
        Assert.Throws<NotSupportedException>(() => PatchDiffV1.DiffModules("{\"schemaVersion\":\"v999\",\"flows\":{}}", "{"));
    }
}
