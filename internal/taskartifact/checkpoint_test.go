package taskartifact

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	sandboxconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
)

func TestPrepareCheckpointRetainsLayoutAndActiveDiskContents(t *testing.T) {
	for _, carrier := range []string{"local", "bundle"} {
		for _, overlay := range []bool{false, true} {
			name := carrier + "/single"
			if overlay {
				name = carrier + "/overlay"
			}
			t.Run(name, func(t *testing.T) {
				source := taskCheckpointSource(t, overlay, true)
				dir, sandboxRef, snapshotRef, manifestConfig := writeTaskCheckpointArtifacts(t, carrier, source)
				for _, launch := range []struct {
					name string
					kind types.ResumeSourceKind
					mode types.LaunchMode
					ref  string
				}{
					{"sandbox cold", types.ResumeSourceSandbox, types.LaunchCold, sandboxRef},
					{"snapshot cold", types.ResumeSourceSnapshot, types.LaunchCold, snapshotRef},
					{"snapshot memory", types.ResumeSourceSnapshot, types.LaunchMemory, snapshotRef},
				} {
					t.Run(launch.name, func(t *testing.T) {
						result, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{
							RootSourceKind: string(launch.kind), LaunchMode: string(launch.mode), RootRef: launch.ref,
							RelativeDir: dir, ManifestConfig: manifestConfig, MaxRefs: 16,
						})
						if err != nil {
							t.Fatal(err)
						}
						cfg := result.SourceSandboxConfig
						if cfg == nil || !cfg.IsCheckpoint() {
							t.Fatal("artifact Open and preparation lost Checkpoint layout")
						}
						clone, err := cfg.Clone()
						if err != nil || !clone.IsCheckpoint() {
							t.Fatalf("clone lost Checkpoint layout: %v", err)
						}
						if err := clone.Validate(); err != nil {
							t.Fatalf("cloned Checkpoint is invalid: %v", err)
						}
						mode := types.ArtifactDiskSingle
						if overlay {
							mode = types.ArtifactDiskOverlay
						}
						want := types.ArtifactDiskTopology{
							Root: types.ArtifactDiskShape{Mode: mode, HasActiveBase: true},
							Disks: []types.ArtifactDiskShape{
								{Name: "workspace", Mode: mode, HasActiveBase: true},
								{Name: "scratch", Mode: mode, HasActiveBase: true},
							},
						}
						if !reflect.DeepEqual(result.Summary.DiskTopology, want) {
							t.Fatalf("Checkpoint topology = %+v, want %+v", result.Summary.DiskTopology, want)
						}
						_, err = checkpointBuildParams(clone).BuildColdConfig()
						if err == nil || !strings.Contains(err.Error(), "data disks are not supported") {
							t.Fatalf("Build data-disk restriction changed: %v", err)
						}
					})
				}
			})
		}
	}
}

func TestPrepareCheckpointSupportsBuildSourceInheritance(t *testing.T) {
	for _, carrier := range []string{"local", "bundle"} {
		t.Run(carrier, func(t *testing.T) {
			dir, ref, _, manifestConfig := writeTaskCheckpointArtifacts(t, carrier, taskCheckpointSource(t, false, false))
			result, err := Prepare(context.Background(), configsock.ArtifactPrepareSpec{
				RootSourceKind: string(types.ResumeSourceSandbox), LaunchMode: string(types.LaunchCold), RootRef: ref,
				RelativeDir: dir, ManifestConfig: manifestConfig, MaxRefs: 16, ReadSourceImageConfig: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			built, err := checkpointBuildParams(result.SourceSandboxConfig).BuildColdConfig()
			if err != nil {
				t.Fatalf("Build rejected Checkpoint source: %v", err)
			}
			if built.Metadata["checkpoint-inheritance"] != "retained" || built.Launch.Exec != "/bin/true" {
				t.Fatalf("Build did not inherit source defaults: %+v", built)
			}
			if !result.SourceSandboxConfig.IsCheckpoint() {
				t.Fatal("Build mutated source layout")
			}
		})
	}
}

func checkpointBuildParams(source *sandboxconfig.PortableSandboxConfig) sandboxcfg.BuildColdParams {
	return sandboxcfg.BuildColdParams{
		Profile: types.ProfileBare, Source: source,
		ImageRef: "file://build.image@digest:" + strings.Repeat("c", 64),
		Kernel:   "/kernel", Runtime: "/runtime", OverlayDiffTpl: "/overlay-template",
	}
}

func taskCheckpointSource(t testing.TB, overlay, dataDisks bool) sparse.Source {
	t.Helper()
	const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	root := sandboxconfig.PortableRootConfig{Base: "self"}
	rootBase := ""
	if overlay {
		root.Overlay = &sandboxconfig.PortableOverlayConfig{}
		rootBase = "file://root.image@digest:" + digest
	}
	cfg := &sandboxconfig.PortableSandboxConfig{
		Version: sandboxconfig.PortableSandboxConfigVersion,
		Resources: sandboxconfig.PortableResourcesConfig{
			Capacity:    sandboxconfig.CapacityConfig{CPU: 1, Memory: "64MiB"},
			Allocatable: sandboxconfig.AllocatableConfig{CPU: 1, Memory: "64MiB"},
		},
		Boot: sandboxconfig.PortableBootConfig{
			Kernel: "file://vmlinux@digest:" + digest, Runtime: "file://runtime.bundle@digest:" + digest,
			Root: root,
		},
		Launch:   sandboxconfig.PortableLaunchConfig{Exec: "/bin/true", Workdir: "/", Restart: "never"},
		Metadata: map[string]string{"checkpoint-inheritance": "retained"},
	}
	cp := &sandboxfile.Checkpoint{Version: 1, CheckpointID: "cp-prepare", Root: sandboxfile.CheckpointDisk{DiffID: "root-diff"}}
	if dataDisks {
		for _, name := range []string{"workspace", "scratch"} {
			disk := sandboxconfig.PortableDiskConfig{Name: name}
			if overlay {
				disk.Base = "file://" + name + ".image@digest:" + digest
				disk.Overlay = &sandboxconfig.PortableOverlayConfig{}
			}
			cfg.Boot.Disks = append(cfg.Boot.Disks, disk)
			cfg.Mounts = append(cfg.Mounts, sandboxconfig.MountConfig{Type: "disk", Source: name, Target: "/" + name})
			cp.Disks = append(cp.Disks, sandboxfile.CheckpointNamedDisk{Name: name, DiffID: name + "-diff"})
		}
	}
	cfg, err := cfg.Checkpointed(rootBase)
	if err != nil {
		t.Fatal(err)
	}
	source, err := sandboxfile.BuildCheckpointSource(context.Background(), cfg, cp)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func writeTaskCheckpointArtifacts(t testing.TB, carrier string, source sparse.Source) (dir, sandboxRef, snapshotRef, manifestConfig string) {
	t.Helper()
	dir = t.TempDir()
	if carrier == "bundle" {
		path, snapshotKey, sandboxKey, cfg := writeTaskManifestBundleSource(t, dir, nil, source, nil)
		return dir,
			manifest.Ref{Scheme: manifest.RefSchemeFile, Path: path, DigestScheme: "manifest", Digest: sandboxKey}.String(),
			manifest.Ref{Scheme: manifest.RefSchemeFile, Path: path, DigestScheme: "manifest", Digest: snapshotKey}.String(), cfg
	}
	sink := snapshot.NewFileSink(dir, "task-checkpoint", nil, false, nil)
	sandboxRef, sandboxPath, err := sink.AbsorbSandbox(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	_, snapshotPath, err := sink.AbsorbSnapshot(context.Background(), taskMemorySource(t, sandboxRef, nil))
	if err != nil {
		t.Fatal(err)
	}
	return dir, sandboxPath, snapshotPath, ""
}
