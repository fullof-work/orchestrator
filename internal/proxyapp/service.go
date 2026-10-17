package proxyapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// Startup transfers process-local runtime ownership to the service. Release is
// called after the instance and its workers have ended, including failed starts.
type Startup struct {
	Effective *EffectiveConfig
	Runtime   *Runtime
	Release   func()
}

type PrepareService func(context.Context, ServiceInput) (*Startup, error)
type MasterRunner func(context.Context, *EffectiveConfig, *Runtime) error

// Service coordinates one original-PID instance and at most one child. All
// public actions, management responses and private control use this same state.
type Service struct {
	mu sync.Mutex
	// actionMu orders stop acceptance against process creation and exec. No
	// application callback or drain runs while it is held.
	actionMu       sync.Mutex
	input          ServiceInput
	baseline       *EffectiveConfig
	prepare        PrepareService
	runner         MasterRunner
	local          *Instance
	localDone      chan error
	localExtension any
	resources      *retainedResources
	child          *serviceChild
	operation      *RestartStatus
	target         *restartTarget
	stopRecord     *stopRecord
	stop           chan struct{}
	stopOnce       sync.Once
	stopMMDS       chan struct{}
	stopReleased   sync.Once
	done           chan struct{}
	changed        chan struct{}
	requests       chan struct{}
	preparing      bool
	running        bool
	started        bool
	closing        bool
	finished       sync.Once
	knownService   *Status
	candidate      bool
	parent         *serviceWire
	err            error
	timeout        time.Duration
	logger         *slog.Logger
	coordinatorPID int
}

// NewService performs no I/O. Runtime resources are acquired by Run.
func NewService() *Service {
	return &Service{stop: make(chan struct{}), done: make(chan struct{}), changed: make(chan struct{}), requests: make(chan struct{}, 1), timeout: 30 * time.Second, coordinatorPID: os.Getpid()}
}

func (s *Service) notifyLocked() { close(s.changed); s.changed = make(chan struct{}) }

func (s *Service) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := Status{InstanceStatus: InstanceStatus{Phase: "starting", Reasons: []string{"not_started"}}, CoordinatorPID: s.coordinatorPID, Instances: []InstanceStatus{}}
	if s.local != nil {
		result.InstanceStatus = s.local.Snapshot()
		result.Instances = append(result.Instances, result.InstanceStatus)
	}
	if s.child != nil {
		result.Instances = append(result.Instances, s.child.snapshot())
	}
	for _, instance := range result.Instances {
		result.ServiceReady = result.ServiceReady || instance.Ready
	}
	if s.stopping() {
		result.ServiceReady = false
	}
	if s.candidate && s.knownService != nil {
		local := result.InstanceStatus
		result = copyStatus(*s.knownService)
		result.InstanceStatus = local
		for index := range result.Instances {
			if result.Instances[index].BootID == local.BootID {
				result.Instances[index] = local
			}
		}
		ready := false
		for _, instance := range result.Instances {
			ready = ready || instance.Ready
		}
		result.ServiceReady = result.ServiceReady && ready && !s.stopping()
		return result
	}
	if s.operation != nil {
		operation := *s.operation
		if operation.NextAttempt != nil {
			stamp := *operation.NextAttempt
			operation.NextAttempt = &stamp
		}
		result.InplaceRestart = &operation
	}
	return result
}

// WaitReady waits for this process's business readiness. Cancellation affects
// only this wait. It never sends synthetic requests or starts a stopped service.
func (s *Service) WaitReady(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("proxy wait context is nil")
	}
	for {
		s.mu.Lock()
		instance, changed := s.local, s.changed
		s.mu.Unlock()
		if instance != nil {
			return instance.WaitReady(ctx)
		}
		select {
		case <-changed:
		case <-s.done:
			return errors.Join(ErrStopped, s.result())
		case <-s.stop:
			return ErrStopped
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Service) result() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }

// Shutdown requests one bounded stop of the entire service, including B. The
// first request fixes the deadline; caller cancellation only ends this wait.
// Handlers should request shutdown asynchronously rather than wait for themselves.
func (s *Service) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("proxy shutdown context is nil")
	}
	select {
	case <-s.done:
		return s.result()
	default:
	}
	if err := s.requestShutdown(); err != nil {
		return err
	}
	select {
	case <-s.done:
		return s.result()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// FinishEntry wakes lifecycle waiters when an entry fails before Run can take
// ownership, for example when receiving a component bootstrap fails.
func (s *Service) FinishEntry(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return
	}
	s.started = true
	s.err = err
	s.finished.Do(func() { close(s.done) })
	s.notifyLocked()
}

func (s *Service) requestShutdown() error {
	s.actionMu.Lock()
	defer s.actionMu.Unlock()
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	if !s.started {
		s.stopOnce.Do(func() { close(s.stop) })
		s.finished.Do(func() { close(s.done) })
		s.notifyLocked()
		s.mu.Unlock()
		return nil
	}
	record, timeout, parent, candidate := s.stopRecord, s.timeout, s.parent, s.candidate
	s.mu.Unlock()
	if candidate {
		if parent == nil {
			return ErrNotReady
		}
		return parent.send(serviceMessage{Kind: "shutdown"})
	}
	if record == nil {
		s.stopOnce.Do(func() { close(s.stop) })
		return nil
	}
	deadline, err := record.request(time.Now().Add(timeout))
	if err != nil {
		return err
	}
	s.stopOnce.Do(func() { close(s.stop) })
	s.beginShutdown(deadline)
	return nil
}

// Stop accepting immediately, even when the coordinator is waiting for an
// earlier drain or cleanup. MMDS stays available behind the shared data barrier.
func (s *Service) beginShutdown(deadline time.Time) chan struct{} {
	s.mu.Lock()
	if s.stopMMDS != nil {
		gate := s.stopMMDS
		s.mu.Unlock()
		return gate
	}
	s.stopMMDS = make(chan struct{})
	gate, instance, child, done := s.stopMMDS, s.local, s.child, s.localDone
	s.mu.Unlock()
	if instance != nil && done != nil {
		instance.mu.Lock()
		instance.mmdsGate = gate
		instance.mu.Unlock()
		instance.requestStop(deadline)
	}
	if child != nil {
		_ = child.wire.send(serviceMessage{Kind: "drain", Deadline: deadline, HoldMMDS: true})
	}
	go s.releaseShutdownMMDS(deadline, instance, done, child, gate)
	return gate
}

func (s *Service) stopping() bool {
	select {
	case <-s.stop:
		return true
	default:
		return false
	}
}

// InplaceRestart returns after acceptance, not after exec. Cancellation before
// acceptance aborts preparation. Afterwards the engine owns the operation and
// reports its outcome through Status; the original Go stack cannot survive exec.
func (s *Service) InplaceRestart(ctx context.Context) (RestartID, error) {
	if ctx == nil {
		return "", fmt.Errorf("proxy restart context is nil")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	if s.stopping() {
		s.mu.Unlock()
		return "", ErrStopped
	}
	if s.candidate {
		s.mu.Unlock()
		return "", fmt.Errorf("%w: candidate cannot start a nested operation", ErrIncompatible)
	}
	if s.preparing || (s.operation != nil && s.operation.Result == "") {
		s.mu.Unlock()
		return "", ErrRestartInProgress
	}
	if !s.running || s.local == nil || s.resources == nil || !s.local.Snapshot().Ready {
		s.mu.Unlock()
		return "", ErrNotReady
	}
	local, extension, input := s.local, s.localExtension, s.input
	local.mu.Lock()
	supported := slices.Contains(local.capabilities, routesync.InplaceRestartV1)
	local.mu.Unlock()
	if !supported || !local.extensionCompatible() {
		s.mu.Unlock()
		return "", fmt.Errorf("%w: conductor or worker completion contract", ErrIncompatible)
	}
	if extension != nil {
		if _, ok := extension.(proxyextension.Shutdown); !ok {
			s.mu.Unlock()
			return "", fmt.Errorf("%w: master extension has no Shutdown", ErrIncompatible)
		}
	}
	if _, err := candidateMetricsAddress(s.baseline.config.MetricsListen); err != nil {
		s.mu.Unlock()
		return "", err
	}
	s.preparing = true
	s.mu.Unlock()
	target, err := openRestartTarget(ctx, input.Executable)
	s.mu.Lock()
	var logOperation func()
	defer func() {
		s.mu.Unlock()
		if logOperation != nil {
			logOperation()
		}
	}()
	s.preparing = false
	if err != nil {
		return "", err
	}
	if err = ctx.Err(); err != nil {
		target.file.Close()
		return "", err
	}
	if s.stopping() {
		target.file.Close()
		return "", ErrStopped
	}
	if !local.Snapshot().Ready {
		target.file.Close()
		return "", ErrNotReady
	}
	id := RestartID(newBootID())
	s.target = target
	s.operation = &RestartStatus{ID: id, Phase: "preparing_b", Target: target.description()}
	logOperation = s.operationLogLocked()
	s.notifyLocked()
	s.requests <- struct{}{}
	return id, nil
}

// Run consumes a continuation before invoking any application preparation. In
// resume mode child supervision and the persisted stop deadline are restored
// before Configure, TLS resolution, extension startup or listener acquisition.
func (s *Service) Run(ctx context.Context, input ServiceInput, continuation *Continuation, prepare PrepareService, runner MasterRunner) (returnErr error) {
	if ctx == nil || prepare == nil || runner == nil {
		return fmt.Errorf("proxy service has unresolved startup")
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("proxy service already running")
	}
	if s.stopping() {
		s.mu.Unlock()
		return nil
	}
	s.started = true
	s.running = true
	s.input = input
	s.prepare = prepare
	s.runner = runner
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.err = returnErr
		s.running = false
		s.notifyLocked()
		s.finished.Do(func() { close(s.done) })
		s.mu.Unlock()
	}()
	defer s.releaseService()
	if continuation != nil {
		defer continuation.Close()
		if err := s.restore(continuation); err != nil {
			// A malformed inherited contract is fatal, unlike a retryable
			// application initialization error. Even then, supervise and finish
			// any already-restored child instead of abandoning it.
			return errors.Join(err, s.reapChild(s.child, time.Now().Add(s.timeout), false))
		}
	} else {
		record, err := newStopRecord()
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.stopRecord = record
		s.mu.Unlock()
	}
	if s.candidate {
		return s.runCandidate(ctx)
	}
	deadline, err := s.stopRecord.read()
	if err != nil {
		return err
	}
	if !deadline.IsZero() {
		s.stopOnce.Do(func() { close(s.stop) })
	}
	if s.stopping() {
		return s.shutdownAll()
	}
	watchDone := make(chan struct{})
	defer close(watchDone)
	go s.publishService(watchDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = s.requestShutdown()
		case <-watchDone:
		}
	}()
	if continuation != nil {
		if err := s.resumeOperation(); err != nil {
			return err
		}
	} else if err := s.startLocal(ctx, "", routesync.ProxyPluginID, nil); err != nil {
		return err
	}
	for {
		if s.stopping() {
			return s.shutdownAll()
		}
		s.mu.Lock()
		localDone := s.localDone
		s.mu.Unlock()
		select {
		case <-s.stop:
			return s.shutdownAll()
		case <-s.requests:
			if err := s.restart(); err != nil {
				return err
			}
		case err := <-localDone:
			return err
		}
	}
}

func (s *Service) startLocal(ctx context.Context, boot, id string, resources *retainedResources) error {
	if s.stopping() {
		return ErrStopped
	}
	input := s.input
	input.Config = append(json.RawMessage(nil), input.Config...)
	startup, err := s.prepare(ctx, input)
	if err != nil {
		s.mu.Lock()
		if s.baseline != nil {
			operation := RestartID("")
			if s.operation != nil {
				operation = s.operation.ID
			}
			instance := NewInstance(id, boot, operation, s.baseline.config.Workers)
			instance.finish(err)
			s.local = instance
			s.localDone = nil
			s.notifyLocked()
		}
		s.mu.Unlock()
		return err
	}
	if startup == nil || startup.Effective == nil || startup.Runtime == nil {
		if startup != nil && startup.Release != nil {
			startup.Release()
		}
		return fmt.Errorf("proxy startup is incomplete")
	}
	release := startup.Release
	failed := true
	defer func() {
		if failed && release != nil {
			release()
		}
	}()
	s.mu.Lock()
	if s.stopping() {
		s.mu.Unlock()
		return ErrStopped
	}
	if s.baseline == nil {
		s.baseline = startup.Effective
	} else if !bytes.Equal(s.baseline.raw, startup.Effective.raw) {
		s.mu.Unlock()
		return fmt.Errorf("%w: Configure changed the accepted declarative configuration", ErrIncompatible)
	}
	config := startup.Effective.Config()
	if s.candidate {
		config.StatsSocket = resources.StatsPath
		config.ShmPath = resources.ShmPath
		config.MetricsListen = resources.MetricsExpression
	}
	effective, err := FreezeConfig(config)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	operation := RestartID("")
	if s.operation != nil {
		operation = s.operation.ID
	}
	instance := NewInstance(id, boot, operation, config.Workers)
	instance.statusProvider = s.Status
	startup.Runtime.Instance = instance
	startup.Runtime.inherited = resources
	startup.Runtime.retain = func(retained *retainedResources) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.resources == nil {
			s.resources = retained
		} else {
			retained.close()
		}
		return nil
	}
	s.local = instance
	s.timeout = config.ShutdownTimeoutDur()
	s.localExtension = startup.Runtime.MasterExtension
	s.logger = startup.Runtime.Logger
	done := make(chan error, 1)
	s.localDone = done
	s.notifyLocked()
	s.mu.Unlock()
	failed = false
	go func() {
		err := s.runner(context.WithoutCancel(ctx), effective, startup.Runtime)
		if release != nil {
			release()
		}
		done <- err
	}()
	return nil
}

func (s *Service) phase(phase string, err error) {
	s.mu.Lock()
	s.operation.Phase = phase
	s.operation.NextAttempt = nil
	if err != nil {
		s.operation.LastError = err.Error()
	}
	logOperation := s.operationLogLocked()
	s.notifyLocked()
	s.mu.Unlock()
	logOperation()
}

func (s *Service) operationComplete(result string, err error) {
	s.phase("completed", err)
	s.mu.Lock()
	s.operation.Result = result
	logOperation := s.operationLogLocked()
	target := s.target
	s.target = nil
	s.notifyLocked()
	s.mu.Unlock()
	logOperation()
	if target != nil {
		target.file.Close()
	}
}

func (s *Service) releaseService() {
	s.actionMu.Lock()
	defer s.actionMu.Unlock()
	s.mu.Lock()
	s.closing = true
	resources, target, record := s.resources, s.target, s.stopRecord
	s.mu.Unlock()
	if resources != nil {
		resources.close()
		if !s.candidate {
			_ = resources.removePaths()
		}
	}
	if target != nil {
		target.file.Close()
	}
	if record != nil {
		record.file.Close()
	}
	if s.parent != nil {
		s.parent.conn.Close()
	}
}

// The retry delay is independent of preparation and drain budgets.
func (s *Service) backoff(attempt int, err error) bool {
	delays := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second}
	index := attempt - 1
	if index < 0 {
		index = 0
	}
	if index >= len(delays) {
		index = len(delays) - 1
	}
	next := time.Now().Add(delays[index])
	s.mu.Lock()
	s.operation.Phase = "recovering"
	s.operation.LastError = err.Error()
	s.operation.NextAttempt = &next
	logOperation := s.operationLogLocked()
	s.notifyLocked()
	s.mu.Unlock()
	logOperation()
	timer := time.NewTimer(time.Until(next))
	defer timer.Stop()
	select {
	case <-timer.C:
		return !s.stopping()
	case <-s.stop:
		return false
	}
}

// Copy under the state lock, then invoke the application-supplied logger after
// unlocking. A logger may itself inspect the public status snapshot.
func (s *Service) operationLogLocked() func() {
	logger := s.logger
	if logger == nil {
		logger = slog.Default()
	}
	op := *s.operation
	return func() {
		logger.Info("proxy InplaceRestart", "id", op.ID, "phase", op.Phase,
			"target", op.Target, "attempts", op.Attempts, "last_error", op.LastError,
			"next_attempt", op.NextAttempt, "result", op.Result,
			"forced_close_count", op.ForcedCloseCount)
	}
}
