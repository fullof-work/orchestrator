package proxyapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

type statusLogHandler struct {
	slog.Handler
	service *Service
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
