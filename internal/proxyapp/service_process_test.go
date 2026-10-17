package proxyapp

import (
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
)

var fixtureBuild = "initial"

type serviceFixtureExtension struct{ s *Service }

func (*serviceFixtureExtension) Start(context.Context, proxyextension.MasterHost) error { return nil }
func (*serviceFixtureExtension) Shutdown(context.Context) error                         { return nil }
func (e *serviceFixtureExtension) WrapManagement(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
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
		default:
			next.ServeHTTP(w, r)
		}
	})
}

func runFixtureService() error {
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
	service := NewService()
	prepare := func(ctx context.Context, input ServiceInput) (*Startup, error) {
		var cfg publicconfig.Proxy
		if err := json.Unmarshal(input.Config, &cfg); err != nil {
			return nil, err
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
	return RunEntry(ctx, service, input, continuation, prepare, RunMaster)
}

func TestServiceRealImageReplacement(t *testing.T) {
	testServiceRealImageReplacement(t, false, false, false)
}

func TestServiceRealSignalReplacement(t *testing.T) {
	testServiceRealImageReplacement(t, true, false, false)
}
func TestServiceRealAutomaticRecovery(t *testing.T) {
	testServiceRealImageReplacement(t, false, true, false)
}
func TestServiceRealShutdownDuringRecovery(t *testing.T) {
	testServiceRealImageReplacement(t, false, true, true)
}

func testServiceRealImageReplacement(t *testing.T, signals, recovery, stopRecovery bool) {
	if testing.Short() {
		t.Skip("real compatible target build and process lifecycle")
	}
	effective, _ := testMasterEffective(t)
	cfg := effective.Config()
	cfg.Workers = 2
	cfg.ShutdownTimeout = "3s"
	cfg.InplaceRestart = signals
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.DataListen = listener.Addr().String()
	listener.Close()
	serveFixtureRoutes(t, cfg.ConfigSocket, false)
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
	if status.PID != pid || !status.ServiceReady {
		t.Fatalf("initial identity: %+v", status)
	}
	if err := os.Rename(target, deployment); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var businessErrors []error
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
				businessErrors = append(businessErrors, err)
				mu.Unlock()
			}
		}
	}()
	defer func() { stopLoad(); <-loadDone }()
	var previous RestartID
	stopFromCandidate := false
	var stoppedCandidate int
	var cancelledAttempt time.Time
	for iteration := 0; iteration < 2; iteration++ {
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
			response, err := management.Post("http://proxy/fixture/restart", "", nil)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != 200 {
				t.Fatalf("accept: %d %s", response.StatusCode, body)
			}
			id = RestartID(body)
		}
		if id == "" || id == previous {
			t.Fatalf("operation ID %q", id)
		}
		previous = id
		if recovery && iteration == 0 {
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
			if !recovering.Ready || !recovering.ServiceReady || recovering.InplaceRestart.ID != id || recovering.InplaceRestart.LastError == "" {
				t.Fatalf("missing supervised recovery: %+v", recovering)
			}
			t.Logf("recovery: main_pid=%d candidate_pid=%d operation=%s target=%s attempts=%d last_error=%q next_attempt=%v", pid, recovering.PID, id, recovering.InplaceRestart.Target, recovering.InplaceRestart.Attempts, recovering.InplaceRestart.LastError, recovering.InplaceRestart.NextAttempt)
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
			if err := os.Remove(filepath.Join(dir, "fail-resume")); err != nil {
				t.Fatal(err)
			}
		}
		status = waitServiceStatus(t, management, func(status Status) bool {
			return status.InplaceRestart != nil && status.InplaceRestart.ID == id && status.InplaceRestart.Result != ""
		})
		if status.InplaceRestart.Result != "completed" || !status.Ready || len(status.Instances) != 1 || status.PID != pid {
			t.Fatalf("completion: %+v", status)
		}
		t.Logf("complete: iteration=%d main_pid=%d operation=%s target=%s attempts=%d result=%s forced=%d", iteration, pid, id, status.InplaceRestart.Target, status.InplaceRestart.Attempts, status.InplaceRestart.Result, status.InplaceRestart.ForcedCloseCount)
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
	if builds["target"] == 0 {
		t.Fatalf("no target business response: %v", builds)
	}
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
		if err != nil {
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
	paths, _ := filepath.Glob(filepath.Join(dir, ".proxy-*"))
	if len(paths) != 0 {
		t.Fatalf("candidate resources remain: %v", paths)
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
