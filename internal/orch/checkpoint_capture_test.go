package orch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestCaptureCreatesFreshCheckpointIDForEveryInvocation(t *testing.T) {
	for _, mode := range []string{config.CheckpointLocal, config.CheckpointBundle} {
		for _, kind := range []types.CaptureKind{types.CaptureSnapshot, types.CaptureSandbox} {
			t.Run(mode+"/"+string(kind), func(t *testing.T) {
				o, sb, trace := newCaptureCommandFixture(t, mode)
				report, wantSource := captureTestReport(kind)
				t.Setenv("CAPTURE_FIRST_STDOUT", report)
				t.Setenv("CAPTURE_NEXT_STDOUT", report)
				request := sandboxcfg.CaptureRequest{Kind: kind}
				for i := 0; i < 2; i++ {
					source, err := o.snapshotLocal(context.Background(), sb, request)
					if err != nil || source != wantSource {
						t.Fatalf("capture %d = %+v, %v; want %+v", i, source, err, wantSource)
					}
				}
				_, first := splitNamedCheckpointArgs(t, readCheckpointArgs(t, trace+".1"))
				_, second := splitNamedCheckpointArgs(t, readCheckpointArgs(t, trace+".2"))
				if first == second {
					t.Fatalf("separate captures reused Checkpoint ID %s", first)
				}
				assertCaptureAttempts(t, trace, 2)
			})
		}
	}
}

func TestCaptureCheckpointUUIDFailureDoesNotStartCommand(t *testing.T) {
	o, sb, trace := newCaptureCommandFixture(t, config.CheckpointLocal)
	wantErr := errors.New("UUID entropy unavailable")
	_, err := o.snapshotLocalWithCheckpointID(context.Background(), sb, orchSnapshotCapture(sandboxcfg.SnapshotPolicy{}), func() (uuid.UUID, error) {
		return uuid.Nil, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("capture error = %v, want UUID generation error", err)
	}
	if _, err := os.Stat(trace + ".count"); !os.IsNotExist(err) {
		t.Fatalf("command started after UUID generation failure: %v", err)
	}
}

func TestCaptureUnsupportedRetriesCopyWithIndependentArgumentsAndOutput(t *testing.T) {
	for _, mode := range []string{config.CheckpointLocal, config.CheckpointBundle} {
		for _, kind := range []types.CaptureKind{types.CaptureSnapshot, types.CaptureSandbox} {
			t.Run(mode+"/"+string(kind), func(t *testing.T) {
				o, sb, trace := newCaptureCommandFixture(t, mode)
				report, wantSource := captureTestReport(kind)
				t.Setenv("CAPTURE_FIRST_EXIT", "78")
				t.Setenv("CAPTURE_FIRST_STDOUT", "incomplete named capture output")
				t.Setenv("CAPTURE_FIRST_STDERR", "checkpoint_unsupported")
				t.Setenv("CAPTURE_NEXT_STDOUT", report)
				request := sandboxcfg.CaptureRequest{Kind: kind}
				command := "export"
				if kind == types.CaptureSnapshot {
					command = "snapshot"
					request.SnapshotPolicy = sandboxcfg.SnapshotPolicy{MergeRef: orchCheckpointBool(false), DropCaches: orchCheckpointBool(true)}
				}
				source, err := o.snapshotLocal(context.Background(), sb, request)
				if err != nil || source != wantSource {
					t.Fatalf("copy fallback = %+v, %v; want %+v", source, err, wantSource)
				}
				first, _ := splitNamedCheckpointArgs(t, readCheckpointArgs(t, trace+".1"))
				second := readCheckpointArgs(t, trace+".2")
				want := []string{command, "--json", "--path-id", sb.ID, "--output", filepath.Join(sb.BaseDir, "checkpoint"), "--mode", mode, "--run-root", nodepath.SandboxRunRoot(o.cfg.Paths.RunRoot)}
				if kind == types.CaptureSnapshot {
					want = append(want, "--merge-ref=false", "--drop-caches=true")
				}
				if !reflect.DeepEqual(first, want) || !reflect.DeepEqual(second, want) {
					t.Fatalf("named argv without ID = %#v; fallback argv = %#v; want %#v", first, second, want)
				}
				assertCaptureAttempts(t, trace, 2)
			})
		}
	}
}

func TestCaptureFailuresDoNotDowngradeOrRetryIndefinitely(t *testing.T) {
	report, _ := captureTestReport(types.CaptureSnapshot)
	for _, tc := range []struct {
		name, firstExit, firstOut, firstErr, nextExit, nextOut, nextErr string
		wantAttempts                                                    int
		wantError, absentError                                          string
	}{
		{name: "disk full", firstExit: "1", firstErr: "ENOSPC", wantAttempts: 1, wantError: "ENOSPC"},
		{name: "I/O failure", firstExit: "5", firstErr: "I/O error", wantAttempts: 1, wantError: "I/O error"},
		{name: "unsupported text is not a protocol", firstExit: "1", firstErr: "checkpoint_unsupported", wantAttempts: 1},
		{name: "malformed successful report", firstOut: "not JSON", wantAttempts: 1},
		{name: "incomplete successful report", firstOut: `{"snapshotRef":"` + checkpointSnapshotRef + `"}`, wantAttempts: 1},
		{name: "trailing report", firstOut: report + `{}`, wantAttempts: 1},
		{name: "fallback is attempted once", firstExit: "78", nextExit: "78", wantAttempts: 2},
		{name: "fallback validates full report", firstExit: "78", nextOut: `{"snapshotRef":"` + checkpointSnapshotRef + `"}`, wantAttempts: 2},
		{name: "fallback stderr is independent", firstExit: "78", firstErr: "stale-unsupported-message", nextExit: "1", nextErr: "copy ENOSPC", wantAttempts: 2, wantError: "copy ENOSPC", absentError: "stale-unsupported-message"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, sb, trace := newCaptureCommandFixture(t, config.CheckpointLocal)
			t.Setenv("CAPTURE_FIRST_EXIT", tc.firstExit)
			t.Setenv("CAPTURE_FIRST_STDOUT", tc.firstOut)
			t.Setenv("CAPTURE_FIRST_STDERR", tc.firstErr)
			t.Setenv("CAPTURE_NEXT_EXIT", tc.nextExit)
			t.Setenv("CAPTURE_NEXT_STDOUT", tc.nextOut)
			t.Setenv("CAPTURE_NEXT_STDERR", tc.nextErr)
			_, err := o.snapshotLocal(context.Background(), sb, orchSnapshotCapture(sandboxcfg.SnapshotPolicy{}))
			if err == nil || (tc.wantError != "" && !strings.Contains(err.Error(), tc.wantError)) || (tc.absentError != "" && strings.Contains(err.Error(), tc.absentError)) {
				t.Fatalf("capture error = %v; require %q, exclude %q", err, tc.wantError, tc.absentError)
			}
			assertCaptureAttempts(t, trace, tc.wantAttempts)
		})
	}
}

func splitNamedCheckpointArgs(t testing.TB, args []string) ([]string, uuid.UUID) {
	t.Helper()
	var id uuid.UUID
	var plain []string
	count := 0
	for i := 0; i < len(args); i++ {
		if args[i] != "--checkpoint" {
			plain = append(plain, args[i])
			continue
		}
		count++
		i++
		if i == len(args) {
			t.Fatal("Checkpoint flag has no ID")
		}
		var err error
		id, err = uuid.Parse(args[i])
		if err != nil || id.Version() != 7 || id.Variant() != uuid.RFC4122 || id.String() != args[i] {
			t.Fatalf("Checkpoint ID %q is not a canonical RFC 9562 UUID v7: %v", args[i], err)
		}
	}
	if count != 1 {
		t.Fatalf("got %d Checkpoint IDs in argv %#v", count, args)
	}
	return plain, id
}

func newCaptureCommandFixture(t *testing.T, mode string) (*Orchestrator, *types.Sandbox, string) {
	t.Helper()
	argsPath := installCheckpointSandboxCtl(t)
	trace := filepath.Join(filepath.Dir(argsPath), "capture")
	t.Setenv("CAPTURE_TRACE", trace)
	for _, key := range []string{"CAPTURE_FIRST_EXIT", "CAPTURE_FIRST_STDOUT", "CAPTURE_FIRST_STDERR", "CAPTURE_NEXT_EXIT", "CAPTURE_NEXT_STDOUT", "CAPTURE_NEXT_STDERR"} {
		t.Setenv(key, "")
	}
	const script = `#!/bin/sh
count=0
if [ -f "$CAPTURE_TRACE.count" ]; then read -r count < "$CAPTURE_TRACE.count"; fi
count=$((count + 1))
printf '%s\n' "$count" > "$CAPTURE_TRACE.count"
printf '%s\n' "$@" > "$CAPTURE_TRACE.$count"
if [ "$count" = 1 ]; then
  printf '%s\n' "$CAPTURE_FIRST_STDOUT"
  printf '%s\n' "$CAPTURE_FIRST_STDERR" >&2
  exit "${CAPTURE_FIRST_EXIT:-0}"
fi
printf '%s\n' "$CAPTURE_NEXT_STDOUT"
printf '%s\n' "$CAPTURE_NEXT_STDERR" >&2
exit "${CAPTURE_NEXT_EXIT:-0}"
`
	if err := os.WriteFile(filepath.Join(filepath.Dir(argsPath), config.BinSandboxCtl), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	o := testOrchCfg(t, checkpointOrchestratorConfig(t, mode))
	return o, &types.Sandbox{ID: "capture-test", BaseDir: t.TempDir()}, trace
}

func assertCaptureAttempts(t testing.TB, trace string, want int) {
	t.Helper()
	raw, err := os.ReadFile(trace + ".count")
	if err != nil || strings.TrimSpace(string(raw)) != strconv.Itoa(want) {
		t.Fatalf("capture attempts = %q, %v; want %d", raw, err, want)
	}
}

func captureTestReport(kind types.CaptureKind) (string, types.ResumeSource) {
	if kind == types.CaptureSnapshot {
		return fmt.Sprintf(`{"snapshotRef":%q,"sandboxRef":%q,"removedRefs":[]}`, checkpointSnapshotRef, checkpointSandboxRef), types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: checkpointSnapshotRef, SandboxRef: checkpointSandboxRef}
	}
	return fmt.Sprintf(`{"sandboxRef":%q,"removedRefs":[]}`, checkpointSandboxRef), types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: checkpointSandboxRef}
}
