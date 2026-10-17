package proxyapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"golang.org/x/sys/unix"
)

var fixtureBuild = "initial"

// The conductor fixture records actual retirement requests. The product has
// no fault switch, shortened clock, or alternate retry implementation.
type fixtureRetirementFailure struct {
	mu       sync.Mutex
	blocked  bool
	requests []time.Time
}

func (f *fixtureRetirementFailure) reject() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, time.Now())
	return f.blocked
}

func (f *fixtureRetirementFailure) release() {
	f.mu.Lock()
	f.blocked = false
	f.mu.Unlock()
}

type serviceFixtureExtension struct{ s *Service }

// Cancel only after the target really has an open process-local descriptor.
// This context is part of the test executable, not a production fault hook.
type fixtureCancelOnTargetOpen struct {
	context.Context
	target   os.FileInfo
	cancel   context.CancelFunc
	observed bool
}

func fixtureTargetOpen(target os.FileInfo) bool {
	entries, _ := os.ReadDir("/proc/self/fd")
	for _, entry := range entries {
		if info, err := os.Stat("/proc/self/fd/" + entry.Name()); err == nil && os.SameFile(info, target) {
			return true
		}
	}
	return false
}

func (c *fixtureCancelOnTargetOpen) Err() error {
	if !c.observed && fixtureTargetOpen(c.target) {
		c.observed = true
		c.cancel()
	}
	return c.Context.Err()
}

func (*serviceFixtureExtension) Start(context.Context, proxyextension.MasterHost) error { return nil }
func (*serviceFixtureExtension) Shutdown(context.Context) error                         { return nil }
func (e *serviceFixtureExtension) WrapManagement(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fixture/cancel-opening-restart":
			e.s.mu.Lock()
			path := e.s.input.Executable
			e.s.mu.Unlock()
			info, err := os.Stat(path)
			if err != nil || fixtureTargetOpen(info) {
				http.Error(w, "target unavailable or already open", 500)
				return
			}
			ctx, cancel := context.WithCancel(r.Context())
			defer cancel()
			opening := &fixtureCancelOnTargetOpen{Context: ctx, target: info, cancel: cancel}
			id, err := e.s.InplaceRestart(opening)
			if !opening.observed || id != "" || !errors.Is(err, context.Canceled) || fixtureTargetOpen(info) {
				http.Error(w, fmt.Sprintf("target cancellation: observed=%v id=%s err=%v", opening.observed, id, err), 500)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case "/fixture/concurrent-restart":
			type result struct {
				id  RestartID
				err error
			}
			results := make(chan result, 8)
			start := make(chan struct{})
			for range 8 {
				go func() {
					ctx, cancel := context.WithCancel(r.Context())
					<-start
					id, err := e.s.InplaceRestart(ctx)
					cancel() // An accepted operation outlives this caller.
					results <- result{id, err}
				}()
			}
			close(start)
			var accepted RestartID
			var failure error
			for range 8 {
				result := <-results
				if result.err == nil && result.id != "" && accepted == "" {
					accepted = result.id
				} else if result.id != "" || !errors.Is(result.err, ErrRestartInProgress) {
					failure = errors.Join(failure, fmt.Errorf("unexpected concurrent result: %s %v", result.id, result.err))
				}
			}
			if failure != nil || accepted == "" {
				http.Error(w, fmt.Sprintf("concurrent acceptance: id=%s err=%v", accepted, failure), 500)
				return
			}
			fmt.Fprint(w, accepted)
		case "/fixture/cancelled-restart":
			ctx, cancel := context.WithCancel(r.Context())
			cancel()
			id, err := e.s.InplaceRestart(ctx)
			if id != "" || !errors.Is(err, context.Canceled) {
				http.Error(w, fmt.Sprintf("cancelled request: id=%s err=%v", id, err), 500)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case "/fixture/restart":
			id, err := e.s.InplaceRestart(r.Context())
			if err != nil {
				http.Error(w, err.Error(), 409)
				return
			}
			fmt.Fprint(w, id)
		case "/fixture/stop":
			go e.s.Shutdown(context.Background())
			w.WriteHeader(202)
		case "/fixture/fail-local":
			w.WriteHeader(202)
			w.(http.Flusher).Flush()
			e.s.mu.Lock()
			instance := e.s.local
			e.s.mu.Unlock()
			// Test-only recoverable instance termination. The coordinator and
			// its control/reaping responsibilities remain alive.
			instance.requestStop(time.Now().Add(e.s.timeout))
		default:
			next.ServeHTTP(w, r)
		}
	})
}

func runFixtureService() error {
	service := NewService()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	var continuation *Continuation
	var input ServiceInput
	if ContinuationPresent() {
		var err error
		continuation, err = ReceiveContinuation()
		if err != nil {
			return err
		}
		defer continuation.Close()
		input = continuation.Input()
	} else {
		if len(os.Args) != 4 {
			return fmt.Errorf("fixture input missing")
		}
		raw, err := os.ReadFile(os.Args[2])
		if err != nil {
			return err
		}
		input = ServiceInput{Config: raw, Executable: os.Args[3], Native: true, Signals: true}
	}
	if continuation != nil {
		if err := fixtureStopBeforeRun(service, input, continuation.envelope.Mode); err != nil {
			service.FinishEntry(err)
			return err
		}
	}
	prepare := func(ctx context.Context, input ServiceInput) (*Startup, error) {
		var cfg publicconfig.Proxy
		if err := json.Unmarshal(input.Config, &cfg); err != nil {
			return nil, err
		}
		if continuation != nil {
			mode := continuation.envelope.Mode
			if _, err := os.Stat(filepath.Join(filepath.Dir(cfg.StatsSocket), "stop-before-run-"+mode)); err == nil {
				_ = os.WriteFile(filepath.Join(filepath.Dir(cfg.StatsSocket), "entry-prepared-"+mode), nil, 0600)
			}
		}
		// This fault hook exists only in the test executable, after restored
		// supervision and before application preparation can fail.
		if continuation != nil && continuation.envelope.Mode == "resume" {
			if _, err := os.Stat(filepath.Join(filepath.Dir(cfg.StatsSocket), "fail-resume")); err == nil {
				return nil, fmt.Errorf("fixture A-prime initialization failure")
			}
		}
		effective, err := FreezeConfig(&cfg)
		if err != nil {
			return nil, err
		}
		return &Startup{Effective: effective, Runtime: &Runtime{Logger: testMasterLogger(), MasterExtension: &serviceFixtureExtension{s: service}}}, nil
	}
	err := RunEntry(ctx, service, input, continuation, prepare, RunMaster)
	if continuation != nil {
		var cfg publicconfig.Proxy
		if json.Unmarshal(input.Config, &cfg) == nil {
			service.mu.Lock()
			body, _ := json.Marshal(struct {
				Deadline time.Time
				Started  bool
			}{service.stopDeadline, service.local != nil})
			service.mu.Unlock()
			_ = os.WriteFile(filepath.Join(filepath.Dir(cfg.StatsSocket), "entry-result-"+continuation.envelope.Mode+".json"), body, 0600)
		}
	}
	var config publicconfig.Proxy
	if json.Unmarshal(input.Config, &config) == nil {
		if continuation == nil || continuation.envelope.Mode == "resume" {
			service.mu.Lock()
			state, _ := json.Marshal(struct{ Deadline time.Time }{service.stopDeadline})
			service.mu.Unlock()
			_ = os.WriteFile(filepath.Join(filepath.Dir(config.StatsSocket), "coordinator-stop.json"), state, 0600)
		}
		result, _ := json.Marshal(map[string]bool{"forced": errors.Is(service.Shutdown(context.Background()), ErrForcedClose)})
		_ = os.WriteFile(filepath.Join(filepath.Dir(config.StatsSocket), "service-result.json"), result, 0600)
	}
	return err
}

// This injection belongs only to the test executable. It models a programmatic
// caller racing startup before any continuation descriptor has been restored by
// Service.Run, and allows the parent test to observe the actual process first.
func fixtureStopBeforeRun(service *Service, input ServiceInput, mode string) error {
	var cfg publicconfig.Proxy
	if err := json.Unmarshal(input.Config, &cfg); err != nil {
		return err
	}
	dir := filepath.Dir(cfg.StatsSocket)
	if _, err := os.Stat(filepath.Join(dir, "stop-before-run-"+mode)); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- service.Shutdown(ctx) }()
	var accepted time.Time
	for {
		var changed <-chan struct{}
		service.mu.Lock()
		accepted, changed = service.earlyStopAt, service.changed
		service.mu.Unlock()
		if !accepted.IsZero() {
			break
		}
		select {
		case <-changed:
		case err := <-done:
			return fmt.Errorf("pre-Run stop returned before restoration: %v", err)
		case <-time.After(time.Second):
			return fmt.Errorf("pre-Run stop was not recorded")
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		return fmt.Errorf("pre-Run caller cancellation: %v", err)
	}
	body, _ := json.Marshal(struct {
		PID      int
		Accepted time.Time
	}{os.Getpid(), accepted})
	if err := os.WriteFile(filepath.Join(dir, "entry-stop-"+mode+".json"), body, 0600); err != nil {
		return err
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(dir, "continue-entry")); err == nil {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return fmt.Errorf("fixture entry was not released")
}

func TestServiceRealImageReplacement(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{concurrent: true})
}

func TestServiceRealSignalReplacement(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{signals: true})
}
func TestServiceRealAutomaticRecovery(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{recovery: true})
}
func TestServiceRealShutdownDuringRecovery(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{recovery: true, stopRecovery: true})
}

func TestServiceRealExecFailureRecovery(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{execFailure: true, metrics: "ipv4"})
}

func TestServiceRealMetricsResources(t *testing.T) {
	for _, mode := range []string{"ipv4", "ipv6", "dynamic", "occupied", "boundary"} {
		t.Run(mode, func(t *testing.T) {
			testServiceRealImageReplacement(t, serviceScenario{metrics: mode})
		})
	}
}

func TestServiceRealLegacyConductor(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{legacy: true})
}

func TestServiceRealRetirementFailureRecovery(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{retirementFailure: true})
}

func TestServiceRealCandidateFailureDuringDrain(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{candidateFailure: true})
}

func TestServiceRealShutdownDuringOriginalDrain(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{stopDrain: true})
}

func TestServiceRealForcedShutdownDuringOriginalDrain(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{stopDrain: true, forceStopDrain: true})
}

func TestServiceRealSuccessorFailureDuringCandidateDrain(t *testing.T) {
	testServiceRealImageReplacement(t, serviceScenario{successorFailure: true})
}

type serviceScenario struct {
	retirementFailure                                                                                           bool
	concurrent                                                                                                  bool
	signals, recovery, stopRecovery, execFailure, candidateFailure, stopDrain, forceStopDrain, successorFailure bool
	legacy                                                                                                      bool
	metrics                                                                                                     string
}

func testServiceRealImageReplacement(t *testing.T, scenario serviceScenario) {
	signals, recovery, stopRecovery := scenario.signals, scenario.recovery, scenario.stopRecovery
	if testing.Short() {
		t.Skip("real compatible target build and process lifecycle")
	}
	effective, _ := testMasterEffective(t)
	cfg := effective.Config()
	// Keep real Unix endpoints below the kernel limit independently of Go's
	// full test-name-based temporary directory. Product length checks stay on.
	shortRoot, err := os.MkdirTemp("", "proxy-lifecycle-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(shortRoot) })
	cfg.ConfigSocket = filepath.Join(shortRoot, "config.sock")
	cfg.StatsSocket = filepath.Join(shortRoot, "stats.sock")
	cfg.ShmPath = filepath.Join(shortRoot, "routes.shm")
	cfg.Workers = 2
	cfg.ShutdownTimeout = "3s"
	if scenario.successorFailure {
		cfg.ShutdownTimeout = "8s"
	}
	cfg.InplaceRestart = signals
	var occupiedMetrics net.Listener
	if scenario.metrics != "" {
		cfg.MetricsListen, occupiedMetrics = fixtureMetricsAddress(t, scenario.metrics)
		if occupiedMetrics != nil {
			defer occupiedMetrics.Close()
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.DataListen = listener.Addr().String()
	listener.Close()
	var retirement *fixtureRetirementFailure
	if scenario.retirementFailure {
		retirement = &fixtureRetirementFailure{blocked: true}
		defer retirement.release()
	}
	consumeCompletions := serveFixtureRoutes(t, cfg.ConfigSocket, false, routeFixtureOptions{legacy: scenario.legacy, retirement: retirement})
	dir := filepath.Dir(cfg.StatsSocket)
	initial, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	deployment := filepath.Join(dir, "deployed-proxy")
	copyExecutable(t, initial, deployment)
	configPath := filepath.Join(dir, "input.json")
	raw, _ := json.Marshal(cfg)
	if err := os.WriteFile(configPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "target-proxy")
	buildArgs := []string{"test", "-c", "-o", target, "-ldflags", "-X github.com/kuasar-sandbox/orchestrator/internal/proxyapp.fixtureBuild=target"}
	if fixtureRace {
		buildArgs = append(buildArgs, "-race")
	}
	buildArgs = append(buildArgs, ".")
	build := exec.Command("go", buildArgs...)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("target build: %v\n%s", err, output)
	}
	logfile, err := os.Create(filepath.Join(dir, "service.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logfile.Close()
	command := exec.Command(deployment, "fixture-service", configPath, deployment)
	command.Stdout = logfile
	command.Stderr = logfile
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	t.Cleanup(func() {
		_ = command.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(6 * time.Second):
			_ = command.Process.Kill()
			<-done
		}
		data, _ := os.ReadFile(logfile.Name())
		if t.Failed() {
			t.Logf("service log:\n%s", data)
		}
	})
	pid := command.Process.Pid
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 5 * time.Second}
	management := &http.Client{Transport: &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", cfg.StatsSocket)
	}}, Timeout: time.Second}
	status := waitServiceStatus(t, management, func(status Status) bool { return status.Ready })
	if status.PID != pid || status.CoordinatorPID != pid || !status.ServiceReady {
		t.Fatalf("initial identity: %+v", status)
	}
	initialBoot, initialMetrics := status.BootID, status.MetricsAddress
	if scenario.metrics != "" {
		if scenario.metrics != "dynamic" && initialMetrics != cfg.MetricsListen {
			t.Fatalf("A changed configured metrics address: %s want=%s", initialMetrics, cfg.MetricsListen)
		}
		checkFixtureMetrics(t, initialMetrics)
	}
	cancelPaths := []string{"/fixture/cancelled-restart"}
	if scenario.concurrent {
		cancelPaths = append(cancelPaths, "/fixture/cancel-opening-restart")
	}
	for _, path := range cancelPaths {
		response, err := management.Post("http://proxy"+path, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != http.StatusNoContent {
			t.Fatalf("pre-acceptance cancellation: %s %d %s %v", path, response.StatusCode, body, err)
		}
	}
	status = waitServiceStatus(t, management, func(status Status) bool { return status.Ready })
	if status.InplaceRestart != nil || len(status.Instances) != 1 || status.BootID != initialBoot {
		t.Fatalf("cancelled request changed the running service: %+v", status)
	}
	if err := os.Rename(target, deployment); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var businessErrors []error
	var expectedLosses []error
	var faultWindow atomic.Bool
	var builds = map[string]int{}
	loadCtx, stopLoad := context.WithCancel(context.Background())
	loadDone := make(chan struct{})
	go func() {
		defer close(loadDone)
		for loadCtx.Err() == nil {
			response, err := client.Get("http://" + cfg.DataListen + "/fixture/id")
			if err == nil {
				body, readErr := io.ReadAll(response.Body)
				response.Body.Close()
				if readErr != nil {
					err = readErr
				} else if response.StatusCode != 200 {
					err = fmt.Errorf("business %d %q", response.StatusCode, body)
				} else {
					mu.Lock()
					builds[response.Header.Get("X-Fixture-Build")]++
					mu.Unlock()
				}
			}
			if err != nil {
				mu.Lock()
				if faultWindow.Load() && (errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET)) {
					expectedLosses = append(expectedLosses, err)
				} else {
					t.Logf("unexpected connection result: during_injected_failure=%v type=%T error=%v", faultWindow.Load(), err, err)
					businessErrors = append(businessErrors, err)
				}
				mu.Unlock()
			}
		}
	}()
	defer func() { stopLoad(); <-loadDone }()
	var previous RestartID
	stopFromCandidate := false
	var stoppedCandidate int
	var cancelledAttempt time.Time
	rejectRestart := scenario.legacy || scenario.metrics == "boundary"
	for iteration := 0; iteration < 2; iteration++ {
		var finalRetirementRetry time.Time
		var releaseHeld func()
		if (scenario.execFailure || scenario.candidateFailure || scenario.stopDrain || scenario.successorFailure) && iteration == 0 || scenario.concurrent || scenario.metrics != "" && !rejectRestart && !(scenario.metrics == "occupied" && iteration == 0) {
			release := make(chan struct{})
			var once sync.Once
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-release:
					fmt.Fprint(w, "complete\n")
				case <-r.Context().Done():
				}
			}))
			defer upstream.Close()
			defer once.Do(func() { close(release) })
			request, _ := http.NewRequest("GET", "http://"+cfg.DataListen+"/fixture/hold", nil)
			request.Header.Set("X-Fixture-Target", upstream.URL)
			// The original request predates B preparation. Its client deadline
			// must include that separate 30s budget, not just the drain budget.
			// The server's shutdown_timeout and full-body assertion stay fixed.
			heldClient := &http.Client{Transport: client.Transport, Timeout: candidatePrepareTimeout + cfg.ShutdownTimeoutDur() + time.Second}
			heldAt := time.Now()
			response, err := heldClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			releaseHeld = func() {
				t.Logf("release finite response: iteration=%d held_for=%s", iteration, time.Since(heldAt))
				if !scenario.forceStopDrain {
					once.Do(func() { close(release) })
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if scenario.forceStopDrain {
					if err == nil || string(body) != "begin\n" {
						t.Fatalf("forced original request did not expose truncation: %q %v", body, err)
					}
					return
				}
				if err != nil || string(body) != "begin\ncomplete\n" {
					t.Fatalf("held request truncated: %q %v", body, err)
				}
			}
		}
		if recovery && iteration == 0 {
			if err := os.WriteFile(filepath.Join(dir, "fail-resume"), nil, 0600); err != nil {
				t.Fatal(err)
			}
		}
		var id RestartID
		if signals {
			for repeat := 0; repeat < 4; repeat++ {
				if err := command.Process.Signal(syscall.SIGUSR2); err != nil {
					t.Fatal(err)
				}
			}
			status = waitServiceStatus(t, management, func(status Status) bool { return status.InplaceRestart != nil && status.InplaceRestart.ID != previous })
			id = status.InplaceRestart.ID
		} else {
			path := "/fixture/restart"
			if scenario.concurrent {
				path = "/fixture/concurrent-restart"
			}
			response, err := management.Post("http://proxy"+path, "", nil)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			if rejectRestart {
				if response.StatusCode != http.StatusConflict {
					t.Fatalf("incompatible operation accepted: %d %s", response.StatusCode, body)
				}
				status = waitServiceStatus(t, management, func(status Status) bool { return status.Ready })
				if status.InplaceRestart != nil || len(status.Instances) != 1 || status.BootID != initialBoot || status.Data != "serving" {
					t.Fatalf("rejected operation changed A: %+v", status)
				}
				t.Logf("rejected before acceptance while A serves: %s", body)
				continue
			}
			if response.StatusCode != 200 {
				t.Fatalf("accept: %d %s", response.StatusCode, body)
			}
			id = RestartID(body)
		}
		if id == "" || id == previous {
			t.Fatalf("operation ID %q", id)
		}
		previous = id
		if scenario.concurrent {
			draining := waitServiceStatus(t, management, func(status Status) bool {
				return status.InplaceRestart != nil && status.InplaceRestart.Phase == "draining_a" && len(status.Instances) == 2
			})
			checkFixtureRestartRejected(t, fixtureManagement(draining.Instances[1].StatsSocket), "nested")
			checkFixtureRestartRejected(t, management, ErrRestartInProgress.Error())
			releaseHeld()
		}
		if scenario.metrics == "occupied" && iteration == 0 {
			status = waitServiceStatus(t, management, func(status Status) bool {
				return status.InplaceRestart != nil && status.InplaceRestart.Result != ""
			})
			if status.InplaceRestart.Result != "failed" || !strings.Contains(status.InplaceRestart.LastError, "address already in use") || !status.Ready || status.BootID != initialBoot || len(status.Instances) != 1 {
				t.Fatalf("occupied metrics did not roll back before A drain: %+v", status)
			}
			occupiedMetrics.Close()
			continue
		}
		var candidateMetrics string
		if scenario.metrics != "" {
			draining := waitServiceStatus(t, management, func(status Status) bool {
				return status.InplaceRestart != nil && status.InplaceRestart.Phase == "draining_a" && len(status.Instances) == 2
			})
			candidate := draining.Instances[1]
			candidateMetrics = candidate.MetricsAddress
			if !candidate.Ready || candidateMetrics == initialMetrics {
				t.Fatalf("candidate did not acquire distinct metrics: %+v", draining)
			}
			if scenario.metrics != "dynamic" {
				want, _ := candidateMetricsAddress(cfg.MetricsListen)
				if candidateMetrics != want {
					t.Fatalf("candidate metrics=%s want=%s", candidateMetrics, want)
				}
			}
			checkFixtureMetrics(t, candidateMetrics)
			checkFixturePortHeld(t, initialMetrics)
			candidateClient := fixtureManagement(candidate.StatsSocket)
			checkFixtureRestartRejected(t, candidateClient, "nested")
			checkFixtureRestartRejected(t, management, ErrRestartInProgress.Error())
			if !scenario.execFailure || iteration != 0 {
				releaseHeld()
			}
		}
		if scenario.successorFailure && iteration == 0 {
			originalDraining := waitServiceStatus(t, management, func(status Status) bool {
				return status.InplaceRestart != nil && status.InplaceRestart.Phase == "draining_a" && status.Workers.Ready == 0
			})
			// A no longer accepts, so this finite request belongs to B.
			releaseCandidate := holdServiceRequest(t, client, cfg.DataListen, originalDraining.Instances[1].PID)
			releaseHeld()
			draining := waitServiceStatus(t, management, func(status Status) bool {
				return status.InplaceRestart != nil && status.InplaceRestart.Phase == "draining_b" && status.Ready
			})
			response, err := management.Post("http://proxy/fixture/fail-local", "", nil)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			recovered := waitServiceStatus(t, management, func(status Status) bool {
				return status.Ready && status.BootID != draining.BootID
			})
			if recovered.InplaceRestart.ID != id || len(recovered.Instances) != 2 || recovered.Instances[1].PID != draining.Instances[1].PID || recovered.Instances[1].Data != "draining" {
				t.Fatalf("successor recovery did not preserve B drain: %+v", recovered)
			}
			t.Logf("A-prime recovered during B drain: main_pid=%d old_boot=%s new_boot=%s candidate_pid=%d attempts=%d", pid, draining.BootID, recovered.BootID, recovered.Instances[1].PID, recovered.InplaceRestart.Attempts)
			releaseCandidate()
		}
		if scenario.stopDrain && iteration == 0 {
			draining := waitServiceStatus(t, management, func(status Status) bool {
				return status.InplaceRestart != nil && status.InplaceRestart.Phase == "draining_a" && len(status.Instances) == 2
			})
			candidate := draining.Instances[1]
			candidateClient := &http.Client{Transport: &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", candidate.StatsSocket)
			}}, Timeout: time.Second}
			stopLoad()
			<-loadDone
			response, err := management.Post("http://proxy/fixture/stop", "", nil)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			// B must stop data while A still holds its finite response. Waiting
			// for A's complete drain before telling B would deadlock this check.
			stopping := waitServiceStatus(t, candidateClient, func(status Status) bool { return status.Data == "stopped" })
			if stopping.ServiceReady || stopping.Ready {
				t.Fatalf("stopping service advertised readiness: %+v", stopping)
			}
			releaseHeld()
			stopFromCandidate, stoppedCandidate = true, candidate.PID
			break
		}
		if scenario.candidateFailure && iteration == 0 {
			draining := waitServiceStatus(t, management, func(status Status) bool {
				return status.InplaceRestart != nil && status.InplaceRestart.Phase == "draining_a" && len(status.Instances) == 2
			})
			failedPID := draining.Instances[1].PID
			faultWindow.Store(true)
			if err := unix.Kill(failedPID, syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			replaced := waitServiceStatus(t, management, func(status Status) bool {
				return len(status.Instances) == 2 && status.Instances[1].PID != failedPID && status.Instances[1].Ready
			})
			faultWindow.Store(false)
			if replaced.InplaceRestart.ID != id || replaced.Data != "draining" {
				t.Fatalf("B recovery did not preserve A drain: %+v", replaced)
			}
			if err := unix.Kill(failedPID, 0); err != unix.ESRCH {
				t.Fatalf("failed candidate not reaped: %v", err)
			}
			releaseHeld()
		}
		if scenario.execFailure && iteration == 0 {
			waitServiceStatus(t, management, func(status Status) bool {
				return status.InplaceRestart != nil && status.InplaceRestart.Phase == "draining_a"
			})
			// Change only this task-owned executable's mode after B is serving.
			// execve now returns real EACCES; the running B remains unaffected.
			if err := os.Chmod(deployment, 0600); err != nil {
				t.Fatal(err)
			}
			releaseHeld()
		}
		if (recovery || scenario.execFailure) && iteration == 0 {
			var candidatePath string
			until := time.Now().Add(5 * time.Second)
			for time.Now().Before(until) {
				paths, _ := filepath.Glob(filepath.Join(dir, ".proxy-*.sock"))
				if len(paths) == 1 {
					candidatePath = paths[0]
					break
				}
				time.Sleep(time.Millisecond)
			}
			if candidatePath == "" {
				t.Fatal("no candidate management socket")
			}
			candidateClient := &http.Client{Transport: &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", candidatePath)
			}}, Timeout: time.Second}
			recovering := waitServiceStatus(t, candidateClient, func(status Status) bool {
				return status.InplaceRestart != nil && status.InplaceRestart.Attempts >= 4 && status.InplaceRestart.NextAttempt != nil
			})
			if !recovering.Ready || !recovering.ServiceReady || recovering.CoordinatorPID != pid || recovering.InplaceRestart.ID != id || recovering.InplaceRestart.LastError == "" {
				t.Fatalf("missing supervised recovery: %+v", recovering)
			}
			t.Logf("recovery: main_pid=%d candidate_pid=%d operation=%s target=%s attempts=%d last_error=%q next_attempt=%v", pid, recovering.PID, id, recovering.InplaceRestart.Target, recovering.InplaceRestart.Attempts, recovering.InplaceRestart.LastError, recovering.InplaceRestart.NextAttempt)
			if scenario.metrics != "" {
				if recovering.MetricsAddress != candidateMetrics {
					t.Fatalf("retry replaced candidate metrics: %+v", recovering)
				}
				checkFixtureMetrics(t, candidateMetrics)
				checkFixturePortHeld(t, initialMetrics)
			}
			if stopRecovery {
				stopLoad()
				<-loadDone
				response, err := candidateClient.Post("http://proxy/fixture/stop", "", nil)
				if err != nil {
					t.Fatal(err)
				}
				response.Body.Close()
				stopFromCandidate = true
				stoppedCandidate = recovering.PID
				cancelledAttempt = *recovering.InplaceRestart.NextAttempt
				break
			}
			if scenario.execFailure {
				if err := os.Chmod(deployment, 0700); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Remove(filepath.Join(dir, "fail-resume")); err != nil {
					t.Fatal(err)
				}
			}
		}
		if scenario.retirementFailure && iteration == 0 {
			var previousNext time.Time
			var firstAttempts int
			var candidateBoot string
			for retry, delay := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second} {
				retrying := waitServiceStatus(t, management, func(status Status) bool {
					return status.InplaceRestart != nil && status.InplaceRestart.NextAttempt != nil && status.InplaceRestart.NextAttempt.After(previousNext)
				})
				if !retrying.Ready || !retrying.ServiceReady || retrying.InplaceRestart.ID != id || retrying.InplaceRestart.Result != "" || !strings.Contains(retrying.InplaceRestart.LastError, "HTTP 503") || len(retrying.Instances) != 2 {
					t.Fatalf("retirement failure lost service or ownership: %+v", retrying)
				}
				candidate := retrying.Instances[1]
				if retry == 0 {
					firstAttempts, candidateBoot = retrying.InplaceRestart.Attempts, candidate.BootID
					checkFixtureRestartRejected(t, management, ErrRestartInProgress.Error())
				} else if candidate.BootID != candidateBoot {
					t.Fatalf("reclamation started another candidate: %+v", retrying)
				}
				if err := unix.Kill(candidate.PID, 0); err != unix.ESRCH {
					t.Fatalf("candidate process not reaped before retirement: %d %v", candidate.PID, err)
				}
				retirement.mu.Lock()
				requests := append([]time.Time(nil), retirement.requests...)
				retirement.mu.Unlock()
				if len(requests) != retry+1 {
					t.Fatalf("retirement attempts=%d want=%d", len(requests), retry+1)
				}
				at, next := requests[retry], *retrying.InplaceRestart.NextAttempt
				if (!previousNext.IsZero() && at.Before(previousNext)) || next.Sub(at) < delay || next.Sub(at) > delay+time.Second {
					t.Fatalf("retirement backoff: request=%s previous_next=%s next=%s want_delay=%s", at, previousNext, next, delay)
				}
				if retrying.InplaceRestart.Attempts != firstAttempts+retry {
					t.Errorf("retirement retry absent from status counter: retry=%d first=%d attempts=%d", retry, firstAttempts, retrying.InplaceRestart.Attempts)
				}
				t.Logf("retirement recovery: retry=%d attempts=%d request=%s next=%s main_pid=%d candidate_boot=%s", retry, retrying.InplaceRestart.Attempts, at.Format(time.RFC3339Nano), next.Format(time.RFC3339Nano), pid, candidateBoot)
				previousNext = next
			}
			retirement.release()
			finalRetirementRetry = previousNext
		}
		status = waitServiceStatus(t, management, func(status Status) bool {
			return status.InplaceRestart != nil && status.InplaceRestart.ID == id && status.InplaceRestart.Result != ""
		})
		wantResult := "completed"
		if scenario.candidateFailure && iteration == 0 {
			wantResult = "completed_with_forced_close"
		}
		if status.InplaceRestart.Result != wantResult || !status.Ready || len(status.Instances) != 1 || status.PID != pid {
			t.Fatalf("completion: %+v", status)
		}
		if !finalRetirementRetry.IsZero() {
			retirement.mu.Lock()
			requests := append([]time.Time(nil), retirement.requests...)
			retirement.mu.Unlock()
			if len(requests) != 7 || requests[6].Before(finalRetirementRetry) {
				t.Fatalf("successful retirement did not respect final capped backoff: requests=%v next=%v", requests, finalRetirementRetry)
			}
			t.Logf("retirement recovery completed: request=%s previous_next=%s", requests[6].Format(time.RFC3339Nano), finalRetirementRetry.Format(time.RFC3339Nano))
		}
		t.Logf("complete: iteration=%d main_pid=%d operation=%s target=%s attempts=%d result=%s forced=%d", iteration, pid, id, status.InplaceRestart.Target, status.InplaceRestart.Attempts, status.InplaceRestart.Result, status.InplaceRestart.ForcedCloseCount)
		if scenario.metrics != "" {
			if status.MetricsAddress != initialMetrics {
				t.Fatalf("A-prime changed metrics address: %s want=%s", status.MetricsAddress, initialMetrics)
			}
			checkFixtureMetrics(t, initialMetrics)
			released, err := net.Listen("tcp", candidateMetrics)
			if err != nil {
				t.Fatalf("candidate metrics resource leaked: %v", err)
			}
			released.Close()
		}
		actual, err := os.Stat(fmt.Sprintf("/proc/%d/exe", pid))
		if err != nil {
			t.Fatal(err)
		}
		want, err := os.Stat(deployment)
		if err != nil {
			t.Fatal(err)
		}
		if !os.SameFile(actual, want) {
			t.Fatal("main PID did not execute the selected target")
		}
	}
	stopLoad()
	<-loadDone
	mu.Lock()
	defer mu.Unlock()
	if len(businessErrors) != 0 {
		t.Fatalf("business failures without retries: %v", businessErrors)
	}
	if !rejectRestart && builds["target"] == 0 {
		t.Fatalf("no target business response: %v", builds)
	}
	if rejectRestart && builds["initial"] == 0 {
		t.Fatalf("no ordinary business under compatibility rejection: %v", builds)
	}
	t.Logf("business results: builds=%v unexpected_failures=%d expected_injected_connection_losses=%v", builds, len(businessErrors), expectedLosses)
	if !stopFromCandidate {
		response, err := management.Post("http://proxy/fixture/stop", "", nil)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
	}
	select {
	case err := <-done:
		done <- err
		if scenario.forceStopDrain {
			body, readErr := os.ReadFile(filepath.Join(dir, "service-result.json"))
			var result struct {
				Forced bool `json:"forced"`
			}
			if err == nil || readErr != nil || json.Unmarshal(body, &result) != nil || !result.Forced {
				t.Fatalf("public Shutdown lost the original forced result: exit=%v read=%v body=%s", err, readErr, body)
			}
			t.Logf("complete service Shutdown preserved forced close of A; B and workers reaped")
		} else if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("whole service did not stop")
	}
	if stoppedCandidate != 0 {
		if err := unix.Kill(stoppedCandidate, 0); err != unix.ESRCH {
			t.Fatalf("candidate was not reaped: %v", err)
		}
		// Cross the already-advertised retry deadline before checking cleanup.
		if remaining := time.Until(cancelledAttempt.Add(100 * time.Millisecond)); remaining > 0 {
			time.Sleep(remaining)
		}
		if err := unix.Kill(stoppedCandidate, 0); err != unix.ESRCH {
			t.Fatalf("candidate survived stop: %v", err)
		}
	}
	for _, path := range []string{cfg.StatsSocket, cfg.ShmPath} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("resource remains: %s: %v", path, err)
		}
	}
	// A forced stop can exhaust the service's one deadline before an RPC ACK.
	// The authority consumes its admitted, signed completion on its next read.
	consumeCompletions()
	paths, _ := filepath.Glob(filepath.Join(dir, ".proxy-*"))
	if len(paths) != 0 {
		t.Fatalf("candidate resources remain: %v", paths)
	}
}

func fixtureManagement(socket string) *http.Client {
	return &http.Client{Transport: &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}, Timeout: time.Second}
}

func checkFixtureRestartRejected(t *testing.T, client *http.Client, reason string) {
	t.Helper()
	response, err := client.Post("http://proxy/fixture/restart", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusConflict || !strings.Contains(string(body), reason) {
		t.Fatalf("restart rejection: status=%d body=%q err=%v", response.StatusCode, body, err)
	}
}

// Selecting a free adjacent pair belongs to test setup. Production must bind
// exactly the configured p and p+1, and the assertions below check those binds.
func fixtureMetricsAddress(t *testing.T, mode string) (string, net.Listener) {
	t.Helper()
	if mode == "dynamic" {
		return "127.0.0.1:0", nil
	}
	if mode == "boundary" {
		return "127.0.0.1:65535", nil
	}
	host := "127.0.0.1"
	if mode == "ipv6" {
		host = "::1"
	}
	for attempt := 0; attempt < 50; attempt++ {
		first, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
		if err != nil {
			t.Fatal(err)
		}
		address := first.Addr().String()
		_, port, _ := net.SplitHostPort(address)
		number, _ := strconv.Atoi(port)
		second, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(number+1)))
		first.Close()
		if err != nil {
			continue
		}
		if mode == "occupied" {
			return address, second
		}
		second.Close()
		return address, nil
	}
	t.Fatal("cannot allocate an adjacent metrics pair for the fixture")
	return "", nil
}

func checkFixturePortHeld(t *testing.T, address string) {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if listener != nil {
		listener.Close()
	}
	if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("metrics listener was not continuously held: %s: %v", address, err)
	}
}

func checkFixtureMetrics(t *testing.T, address string) {
	t.Helper()
	checkFixturePortHeld(t, address)
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: time.Second}
	response, err := client.Get("http://" + address + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	// The fixture sends only an initial Bookmark, so its master counter
	// registry can legitimately be empty. Check the actual exposition route.
	if err != nil || response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/plain; version=0.0.4" {
		t.Fatalf("metrics unavailable: status=%d bytes=%d err=%v", response.StatusCode, len(body), err)
	}
}

func holdServiceRequest(t *testing.T, client *http.Client, address string, parent int) func() {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
			fmt.Fprint(w, "complete\n")
		case <-r.Context().Done():
		}
	}))
	request, _ := http.NewRequest("GET", "http://"+address+"/fixture/hold", nil)
	request.Header.Set("X-Fixture-Target", upstream.URL)
	response, err := client.Do(request)
	if err != nil {
		upstream.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		response.Body.Close()
		upstream.Close()
	})
	worker, err := strconv.Atoi(response.Header.Get("X-Fixture-PID"))
	if err != nil {
		t.Fatal(err)
	}
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", worker))
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(stat[strings.LastIndexByte(string(stat), ')')+1:]))
	if len(fields) < 2 || fields[1] != strconv.Itoa(parent) {
		t.Fatalf("held request belongs to worker %d with stat %q, expected B parent %d", worker, stat, parent)
	}
	return func() {
		once.Do(func() { close(release) })
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || string(body) != "begin\ncomplete\n" {
			t.Fatalf("finite candidate response truncated: %q %v", body, err)
		}
	}
}

func copyExecutable(t *testing.T, source, dest string) {
	t.Helper()
	input, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	output, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(output, input)
	closeErr := output.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("copy binary: %v %v", err, closeErr)
	}
}

func waitServiceStatus(t *testing.T, client *http.Client, condition func(Status) bool) Status {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var last Status
	var lastErr error
	for time.Now().Before(deadline) {
		response, err := client.Get("http://proxy/status")
		if err == nil {
			err = json.NewDecoder(response.Body).Decode(&last)
			response.Body.Close()
			if err == nil && condition(last) {
				return last
			}
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("status did not satisfy condition: %+v operation=%+v err=%v", last, last.InplaceRestart, lastErr)
	return last
}
