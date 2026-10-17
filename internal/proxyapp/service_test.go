package proxyapp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/proxycompletion"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"golang.org/x/sys/unix"
)

type statusLogHandler struct {
	slog.Handler
	service *Service
}

func TestCompletionBeforeSuccessorInitialization(t *testing.T) {
	s := NewService()
	cfg := testProxyConfig(t)
	cfg.StatsSocket = filepath.Join(t.TempDir(), "stats.sock")
	cfg.ConfigSocket = filepath.Join(t.TempDir(), "offline.sock")
	var err error
	s.baseline, err = FreezeConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.ownershipRestored = true
	identity := routesync.ProxyInstance{ServiceID: s.serviceID, BootID: s.serviceID, Slot: "coordinator", Generation: 1, CoordinatorPID: os.Getpid()}
	s.completion, s.completionKey, err = proxycompletion.Create(cfg.StatsSocket, identity)
	if err != nil {
		t.Fatal(err)
	}
	identity.Completion = s.completion
	s.child = &serviceChild{done: make(chan struct{})}
	if err := s.finishServiceRegistration(); err == nil {
		t.Fatal("signed completion before candidate reaping")
	}
	if done, _ := proxycompletion.Verify(identity); done {
		t.Fatal("live candidate was signed complete")
	}
	close(s.child.done)
	if err := s.finishServiceRegistration(); err != nil {
		t.Fatal(err)
	}
	if done, err := proxycompletion.Verify(identity); err != nil || !done {
		t.Fatalf("uninitialized A-prime lost completed B responsibility: %v %v", done, err)
	}
	s.operation, s.target = &RestartStatus{ID: "test"}, &restartTarget{}
	candidate := s.envelope("candidate", newBootID(), "proxy-b", &retainedResources{})
	successor := s.envelope("resume", newBootID(), "proxy", &retainedResources{})
	if len(candidate.CompletionKey) != 0 || !bytes.Equal(successor.CompletionKey, s.completionKey) {
		t.Fatal("completion signing authority crossed the wrong process boundary")
	}
}

func (h statusLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h statusLogHandler) Handle(context.Context, slog.Record) error {
	_ = h.service.Status()
	return nil
}

func TestOperationLoggerMayInspectStatus(t *testing.T) {
	service := NewService()
	service.operation = &RestartStatus{ID: "reentrant-status"}
	service.logger = slog.New(statusLogHandler{service: service})
	done := make(chan struct{})
	go func() {
		service.phase("preparing_b", nil)
		service.operationComplete("failed", errors.New("candidate failed"))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("logger deadlocked while inspecting public status")
	}
}

func TestShutdownBeforeRunPreventsInitialization(t *testing.T) {
	service := NewService()
	if err := service.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	prepare := func(context.Context, ServiceInput) (*Startup, error) { t.Fatal("prepared after stop"); return nil, nil }
	if err := service.Run(context.Background(), ServiceInput{}, nil, prepare, RunMaster); err != nil {
		t.Fatal(err)
	}
	if err := service.WaitReady(context.Background()); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
	if _, err := service.InplaceRestart(context.Background()); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
	if err := service.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFinishEntryWakesReadyWaiter(t *testing.T) {
	service := NewService()
	failure := errors.New("bootstrap failure")
	service.FinishEntry(failure)
	if err := service.WaitReady(context.Background()); !errors.Is(err, failure) {
		t.Fatal(err)
	}
}

func TestShutdownCancelsLocalPreparation(t *testing.T) {
	for _, resumed := range []bool{false, true} {
		t.Run(fmt.Sprint(resumed), func(t *testing.T) {
			service := NewService()
			entered := make(chan struct{})
			prepare := func(ctx context.Context, _ ServiceInput) (*Startup, error) {
				close(entered)
				<-ctx.Done()
				return nil, ctx.Err()
			}
			done := make(chan error, 1)
			if resumed {
				// An A-prime attempt has a live service and a separate preparation
				// timeout. Complete Shutdown must cancel it immediately as well.
				service.started = true
				service.prepare = prepare
				service.stopRecord, _ = newStopRecord()
				defer service.stopRecord.file.Close()
				go func() {
					err := service.startLocal(context.Background(), "resumed", "proxy", nil)
					done <- err
					service.finished.Do(func() { close(service.done) })
				}()
			} else {
				go func() { done <- service.Run(context.Background(), ServiceInput{}, nil, prepare, RunMaster) }()
			}
			<-entered
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := service.Shutdown(ctx); err != nil {
				t.Fatalf("Shutdown did not cancel preparation: %v", err)
			}
			err := <-done
			if resumed && !errors.Is(err, context.Canceled) || !resumed && err != nil {
				t.Fatalf("unexpected preparation result: %v", err)
			}
		})
	}
}

func TestShutdownRetainsPreviouslyConsumedDrainResult(t *testing.T) {
	service := NewService()
	service.started = true
	service.stopRecord, _ = newStopRecord()
	defer service.stopRecord.file.Close()
	service.local = NewInstance("proxy", "boot", "", 0)
	service.localDone = make(chan error, 1)
	service.localDone <- ErrForcedClose
	service.local.status.Data = "stopped"
	if err := service.requestShutdown(); err != nil {
		t.Fatal(err)
	}
	service.local.mu.Lock()
	service.local.status.Data = "stopped"
	service.local.notifyLocked()
	service.local.mu.Unlock()
	if err := service.drainLocal(time.Now()); !errors.Is(err, ErrForcedClose) {
		t.Fatal(err)
	}
	if err := service.shutdownAll(); !errors.Is(err, ErrForcedClose) {
		t.Fatalf("consumed instance result was lost: %v", err)
	}
}

func TestCandidateStatusOwnsSnapshot(t *testing.T) {
	service := NewService()
	service.candidate = true
	next := time.Now()
	service.knownService = &Status{
		InstanceStatus: InstanceStatus{Reasons: []string{"original"}},
		Instances:      []InstanceStatus{{BootID: "peer", Reasons: []string{"original"}}},
		InplaceRestart: &RestartStatus{Phase: "recovering", NextAttempt: &next},
	}
	status := service.Status()
	status.Instances[0].Reasons[0] = "modified"
	status.InplaceRestart.Phase = "modified"
	*status.InplaceRestart.NextAttempt = time.Time{}
	again := service.Status()
	if again.Instances[0].Reasons[0] != "original" || again.InplaceRestart.Phase != "recovering" || again.InplaceRestart.NextAttempt.IsZero() {
		t.Fatalf("caller mutated retained state: %+v", again)
	}
}

func TestStopRecordKeepsFirstDeadlineAndSurvivesDuplicate(t *testing.T) {
	record, err := newStopRecord()
	if err != nil {
		t.Fatal(err)
	}
	defer record.file.Close()
	first := time.Now().Add(time.Second).Round(0)
	if got, err := record.request(first); err != nil || !got.Equal(first) {
		t.Fatalf("first=%v %v", got, err)
	}
	if got, err := record.request(first.Add(time.Minute)); err != nil || !got.Equal(first) {
		t.Fatalf("extended=%v %v", got, err)
	}
	fd, err := unix.FcntlInt(record.file.Fd(), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		t.Fatal(err)
	}
	file := os.NewFile(uintptr(fd), "continued-stop")
	defer file.Close()
	continued, err := receiveStopRecord(file)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := continued.read(); err != nil || !got.Equal(first) {
		t.Fatalf("lost stop=%v %v", got, err)
	}
}

func TestShutdownPersistsBeforeExecAndCancelsOnlyCallerWait(t *testing.T) {
	service := NewService()
	service.started = true
	var err error
	service.stopRecord, err = newStopRecord()
	if err != nil {
		t.Fatal(err)
	}
	defer service.stopRecord.file.Close()
	// Hold exactly the critical section used by StartProcess and exec. A
	// stop intent must survive exec even while its old Go caller is blocked.
	service.actionMu.Lock()
	locked := true
	defer func() {
		if locked {
			service.actionMu.Unlock()
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- service.Shutdown(ctx) }()
	select {
	case <-service.stop:
	case <-time.After(time.Second):
		t.Error("stop was not published while exec held the action mutex")
	}
	first, err := service.stopRecord.read()
	if err != nil || first.IsZero() {
		t.Errorf("stop intent was not durable before exec: %v %v", first, err)
	}
	cancel()
	returned := false
	select {
	case err := <-result:
		returned = true
		if !errors.Is(err, context.Canceled) {
			t.Errorf("caller cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Error("caller cancellation waited for the engine critical section")
	}
	service.actionMu.Unlock()
	locked = false
	if !returned {
		select {
		case <-result:
		case <-time.After(time.Second):
			t.Fatal("Shutdown caller did not return after releasing the mutex")
		}
	}
	if err := service.shutdownAll(); err != nil {
		t.Fatal(err)
	}
	second, err := service.stopRecord.read()
	if err != nil || (!first.IsZero() && !first.Equal(second)) {
		t.Fatalf("stop deadline changed after caller cancellation: %v %v", second, err)
	}
}

func TestShutdownBeforeStopRecordPublicationKeepsDeadline(t *testing.T) {
	service := NewService()
	service.started = true
	if err := service.requestShutdown(); err != nil {
		t.Fatal(err)
	}
	first := service.stopDeadline
	if first.IsZero() {
		t.Fatal("stop before record allocation lost its deadline")
	}
	record, err := newStopRecord()
	if err != nil {
		t.Fatal(err)
	}
	defer record.file.Close()
	service.mu.Lock()
	err = service.installStopRecordLocked(record)
	service.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := service.requestShutdown(); err != nil {
		t.Fatal(err)
	}
	if got, err := record.read(); err != nil || !got.Equal(first) {
		t.Fatalf("record publication reset the stop budget: %v, %v", got, err)
	}
}

func TestCandidateResourceBoundaries(t *testing.T) {
	for _, test := range []struct {
		base, want string
		invalid    bool
	}{{"", "", false}, {"127.0.0.1:0", "127.0.0.1:0", false}, {"[::1]:9090", "[::1]:9091", false}, {":65534", ":65535", false}, {":65535", "", true}, {"host:invalid", "", true}} {
		got, err := candidateMetricsAddress(test.base)
		if (err != nil) != test.invalid || got != test.want {
			t.Fatalf("%s => %q %v", test.base, got, err)
		}
	}
	dir := t.TempDir()
	base := filepath.Join(dir, "explicit.sock")
	boot := newBootID()
	path, err := candidatePath(base, boot, true)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(path) != dir {
		t.Fatal(path)
	}
	if err := os.WriteFile(path, []byte("owned"), 0600); err != nil {
		t.Fatal(err)
	}
	identity, err := identifyPath(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := candidatePath(base, boot, true); err == nil {
		t.Fatal("accepted conflicting path")
	}
	replacement := filepath.Join(dir, "replacement")
	if err := os.WriteFile(replacement, []byte("other owner"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	if err := removeOwnedPath(path, identity); err == nil {
		t.Fatal("removed another owner's path")
	}
	if body, err := os.ReadFile(path); err != nil || string(body) != "other owner" {
		t.Fatalf("foreign path changed: %q %v", body, err)
	}
}
