package orch

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

// A real ctl socket exercises framing, complete-list validation, chunking and
// ambiguous connection failures rather than substituting the client API.
type diskCheckpointPeer struct {
	mu                            sync.Mutex
	refs                          []ctl.DiffCheckpointRef
	lists                         int
	deletes                       [][]ctl.DiffCheckpointRef
	truncateList, loseDeleteReply bool
}

func newDiskCheckpointFixture(t *testing.T, sid string) sandboxFinalizerFixture {
	f := newSandboxFinalizerFixture(t, sid)
	f.o.cfg.Paths.RunRoot = shortOrchestratorTestDir(t)
	f.sb.RunDir = nodepath.SandboxRunDir(f.o.cfg.Paths.RunRoot, sid)
	if err := os.MkdirAll(f.sb.RunDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := f.o.st.Put(context.Background(), f.sb); err != nil {
		t.Fatal(err)
	}
	return f
}

func (p *diskCheckpointPeer) setup(refs []ctl.DiffCheckpointRef, truncate, lose bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.refs, p.truncateList, p.loseDeleteReply = refs, truncate, lose
}

func serveDiskCheckpointPeer(t *testing.T, sb *types.Sandbox) *diskCheckpointPeer {
	t.Helper()
	ln, err := net.Listen("unix", filepath.Join(sb.RunDir, "ctl.sock"))
	if err != nil {
		t.Fatal(err)
	}
	p := &diskCheckpointPeer{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			func() {
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(2 * time.Second))
				var req ctl.Request
				if err := ctl.ReadMessage(c, &req); err != nil {
					return
				}
				p.mu.Lock()
				defer p.mu.Unlock()
				switch req.Type {
				case ctl.TypeCheckpointListRequest:
					p.lists++
					if p.truncateList {
						_ = ctl.WriteMessage(c, ctl.Response{Type: ctl.TypeCheckpointListChunk, Checkpoints: p.refs})
						return
					}
					_ = ctl.WriteCheckpointList(c, p.refs)
				case ctl.TypeCheckpointDeleteRequest:
					if req.CheckpointOwner != "" {
						t.Error("online release supplied an offline owner")
					}
					p.deletes = append(p.deletes, append([]ctl.DiffCheckpointRef(nil), req.CheckpointRefs...))
					var remaining []ctl.DiffCheckpointRef
					for _, ref := range p.refs {
						removed := false
						for _, selected := range req.CheckpointRefs {
							removed = removed || ref == selected
						}
						if !removed {
							remaining = append(remaining, ref)
						}
					}
					p.refs = remaining
					if p.loseDeleteReply {
						p.loseDeleteReply = false
						return
					}
					_ = ctl.WriteMessage(c, ctl.Response{Type: ctl.TypeCheckpointDeleteDone})
				default:
					t.Errorf("unexpected ctl request %q", req.Type)
				}
			}()
		}
	}()
	t.Cleanup(func() { ln.Close(); <-done })
	return p
}

func TestDiskCheckpointReleaseBothCompletionOrders(t *testing.T) {
	for _, exportFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "export-first", false: "running-first"}[exportFirst], func(t *testing.T) {
			f := newDiskCheckpointFixture(t, "disk-event-order")
			ctx, cancel := context.WithCancel(context.Background())
			f.o.SetLifecycleContext(ctx)
			t.Cleanup(func() {
				cancel()
				drain, stop := context.WithTimeout(context.Background(), 10*time.Second)
				defer stop()
				if err := f.o.acceptedOps.Drain(drain); err != nil {
					t.Error(err)
				}
			})
			p := serveDiskCheckpointPeer(t, f.sb)
			p.setup([]ctl.DiffCheckpointRef{{DiffID: "root", ID: "old"}}, false, false)
			release := holdTerminalSource(t, f.o, f.sb)
			_, cancelUpload := context.WithCancelCause(context.Background())
			defer cancelUpload(nil)
			attempt, ok := f.o.exports.Begin(f.sb.ID, f.sb.ResumeSource, true, cancelUpload)
			if !ok {
				t.Fatal("export admission")
			}
			defer f.o.exports.Finish(attempt)
			f.sb.State, f.sb.LaunchMode = types.StateStarting, types.LaunchCold
			f.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
			if err := f.o.st.Put(ctx, f.sb); err != nil {
				t.Fatal(err)
			}
			finishExport := func() { release(); f.o.exports.Finish(attempt); f.o.requestDiskCheckpointRelease(f.sb.ID) }
			finishRunning := func() {
				f.sb.State, f.sb.LaunchMode = types.StateRunning, ""
				if err := f.o.st.Put(ctx, f.sb); err != nil {
					t.Fatal(err)
				}
				f.o.requestDiskCheckpointRelease(f.sb.ID)
			}
			if exportFirst {
				finishExport()
			} else {
				finishRunning()
			}
			// The first event alone never admits runtime deletion. The other
			// event must trigger or wake the coalesced release worker.
			time.Sleep(50 * time.Millisecond)
			p.mu.Lock()
			early := p.lists
			p.mu.Unlock()
			if early != 0 {
				t.Fatal("released before both completion boundaries")
			}
			if exportFirst {
				finishRunning()
			} else {
				finishExport()
			}
			deadline := time.Now().Add(3 * time.Second)
			for {
				p.mu.Lock()
				released := len(p.deletes) > 0 && len(p.refs) == 0
				p.mu.Unlock()
				if released {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("completion event did not release versions")
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}

func TestDiskCheckpointReleaseRetriesOnlySelectedVersions(t *testing.T) {
	f := newDiskCheckpointFixture(t, "disk-release-fixed")
	p := serveDiskCheckpointPeer(t, f.sb)
	old := []ctl.DiffCheckpointRef{{DiffID: "root", ID: "old-root"}, {DiffID: "data", ID: "old-data"}}
	p.setup(old, false, true)
	selection := &diskCheckpointSelection{}
	if err := f.o.releaseDiskCheckpointsOnce(context.Background(), f.sb.ID, selection); err == nil {
		t.Fatal("lost Delete reply was accepted")
	}
	p.mu.Lock()
	p.refs = append(append([]ctl.DiffCheckpointRef(nil), old...), ctl.DiffCheckpointRef{DiffID: "root", ID: "new-version"})
	p.mu.Unlock()
	if err := f.o.releaseDiskCheckpointsOnce(context.Background(), f.sb.ID, selection); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lists != 1 || len(p.deletes) != 2 || !reflect.DeepEqual(p.deletes[0], old) || !reflect.DeepEqual(p.deletes[1], old) {
		t.Fatalf("retry widened selection: lists=%d deletes=%+v", p.lists, p.deletes)
	}
}

func TestDiskCheckpointReleaseIncompleteListNeverDeletes(t *testing.T) {
	f := newDiskCheckpointFixture(t, "disk-release-partial")
	p := serveDiskCheckpointPeer(t, f.sb)
	p.setup([]ctl.DiffCheckpointRef{{DiffID: "root", ID: "old"}}, true, false)
	selection := &diskCheckpointSelection{}
	if err := f.o.releaseDiskCheckpointsOnce(context.Background(), f.sb.ID, selection); err == nil {
		t.Fatal("partial List accepted")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if selection.listed || len(p.deletes) != 0 {
		t.Fatal("partial List selected or deleted versions")
	}
}

func TestDiskCheckpointReleaseRechecksStateAndExportOwnership(t *testing.T) {
	f := newDiskCheckpointFixture(t, "disk-release-state")
	p := serveDiskCheckpointPeer(t, f.sb)
	p.setup([]ctl.DiffCheckpointRef{{DiffID: "root", ID: "old"}}, false, false)
	release := holdTerminalSource(t, f.o, f.sb)
	if err := f.o.releaseDiskCheckpointsOnce(context.Background(), f.sb.ID, &diskCheckpointSelection{}); !errors.Is(err, errSourceOperationBusy) {
		t.Fatalf("live child = %v", err)
	}
	release()
	for _, state := range []types.State{types.StateStarting, types.StatePaused, types.StateDeleting, types.StateDead} {
		f.sb.State = state
		f.sb.LaunchMode = ""
		if state == types.StateStarting {
			f.sb.LaunchMode = types.LaunchImage
		}
		if state == types.StatePaused {
			f.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
		}
		row := cloneSandbox(f.sb)
		if state == types.StateDead {
			row.RunID, row.RunDir, row.BaseDir, row.VswitchPort, row.FloatingIP, row.InnerIP, row.PortMAC = "", "", "", "", "", "", ""
			row.ResumeSource = types.ResumeSource{}
		}
		if err := f.o.st.Put(context.Background(), row); err != nil {
			t.Fatal(err)
		}
		if err := f.o.releaseDiskCheckpointsOnce(context.Background(), f.sb.ID, &diskCheckpointSelection{}); err != nil {
			t.Fatal(err)
		}
	}
	p.mu.Lock()
	if p.lists != 0 || len(p.deletes) != 0 {
		t.Error("non-running state issued runtime commands")
	}
	p.mu.Unlock()
	f.sb.State = types.StateRunning
	// A retained ResumeSource does not make versions live once running without
	// an active publisher. This is the normal successful restore state.
	f.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	if err := f.o.st.Put(context.Background(), f.sb); err != nil {
		t.Fatal(err)
	}
	if err := f.o.releaseDiskCheckpointsOnce(context.Background(), f.sb.ID, &diskCheckpointSelection{}); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lists != 1 || len(p.deletes) != 1 {
		t.Fatal("retained ResumeSource blocked release")
	}
}

func TestDiskCheckpointReleaseRetryStopsAfterNewPauseOrRun(t *testing.T) {
	for _, successor := range []string{"paused", "new-run", "accepted-result"} {
		t.Run(successor, func(t *testing.T) {
			f := newDiskCheckpointFixture(t, "disk-release-recheck")
			p := serveDiskCheckpointPeer(t, f.sb)
			p.setup([]ctl.DiffCheckpointRef{{DiffID: "root", ID: "old"}}, false, true)
			selection := &diskCheckpointSelection{}
			if err := f.o.releaseDiskCheckpointsOnce(context.Background(), f.sb.ID, selection); err == nil {
				t.Fatal("expected uncertain delete")
			}
			switch successor {
			case "paused":
				f.sb.State = types.StatePaused
				f.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
			case "new-run":
				f.sb.RunID = "sr-00000000-0000-7000-8000-000000000999"
			case "accepted-result":
				addRunEndResult(t, f, f.sb.ID, f.sb.RunID)
			}
			if successor != "accepted-result" {
				if err := f.o.st.Put(context.Background(), f.sb); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.o.releaseDiskCheckpointsOnce(context.Background(), f.sb.ID, selection); err != nil {
				t.Fatal(err)
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.lists != 1 || len(p.deletes) != 1 {
				t.Fatal("stale retry crossed successor state")
			}
		})
	}
}
