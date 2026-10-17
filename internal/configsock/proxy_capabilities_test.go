package configsock

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/proxycompletion"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

type proxyCapabilitySink struct {
	*nativeStatsSink
	mu           sync.Mutex
	capabilities []string
}

func (s *proxyCapabilitySink) SetProxyCapabilities(capabilities []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.capabilities = append([]string(nil), capabilities...)
}

func TestProxyPIDAdmissionOtherProcess(t *testing.T) {
	socket := os.Getenv("KUASAR_PROXY_PID_ADMISSION_TEST_SOCKET")
	if socket == "" {
		return
	}
	var body bytes.Buffer
	caps := proxyCaps()
	caps.Mmds = true
	if err := routesync.WriteMsg(&body, &routesync.Msg{Type: routesync.TypeRegister, Register: &caps}); err != nil {
		t.Fatal(err)
	}
	client := HTTPClient(socket)
	client.Timeout = 3 * time.Second
	request, err := http.NewRequest(http.MethodPut, "http://conductor/internal/plugin/proxy-candidate/register", &body)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("unlisted child PID admitted: %d", response.StatusCode)
	}
}

func TestProxyCapabilitiesPreservePIDAdmissionAndCompletion(t *testing.T) {
	for _, mode := range []string{"socket", "pidfile", "pidfile-offline"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
			if err != nil {
				t.Fatal(err)
			}
			database, err := store.Open(filepath.Join(t.TempDir(), "state.db"), box)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			identity := routesync.ProxyInstance{ServiceID: strings.Repeat("a", 32), BootID: strings.Repeat("b", 32), Slot: "coordinator", Generation: 1, CoordinatorPID: os.Getpid()}
			dir, err := os.MkdirTemp("", "proxy-admission-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			stats := filepath.Join(dir, "stats.sock")
			binding, private, err := proxycompletion.Create(stats, identity)
			if err != nil {
				t.Fatal(err)
			}
			identity.Completion = binding
			registry, err := NewPersistentRegistry(ctx, database, binding.KernelBoot)
			if err != nil {
				t.Fatal(err)
			}
			pidfile := ""
			if mode != "socket" {
				pidfile = filepath.Join(dir, "plugins.pid")
				mustWrite(t, pidfile, fmt.Sprint(os.Getpid()))
			}
			socket, client := startTestServer(t, Deps{Plugins: registry, RouteSource: nativeStatsSource{}, PluginPidfile: pidfile})
			streamCtx, cancel := context.WithCancel(ctx)
			sink := &proxyCapabilitySink{nativeStatsSink: &nativeStatsSink{ready: make(chan struct{})}}
			caps := proxyCaps()
			caps.Mmds = true
			caps.Proxy.Instance = &identity
			caps.Proxy.StatsSocket = &routesync.Socket{Path: stats}
			subscriber := routesync.NewSubscriber(func(ctx context.Context) (net.Conn, error) { return (&net.Dialer{}).DialContext(ctx, "unix", socket) }, "proxy", caps, sink, nil, discardLogger())
			done := make(chan struct{})
			go func() { subscriber.Run(streamCtx); close(done) }()
			defer func() { cancel(); <-done }()
			select {
			case <-sink.ready:
			case <-time.After(3 * time.Second):
				t.Fatal("allowlisted master did not apply initial Bookmark")
			}
			sink.mu.Lock()
			capabilities := append([]string(nil), sink.capabilities...)
			sink.mu.Unlock()
			if !slices.Contains(capabilities, routesync.ProxyCompletionV1) || slices.Contains(capabilities, routesync.InplaceRestartV1) != (pidfile == "") {
				t.Fatalf("incorrect admission capabilities: %v", capabilities)
			}
			if pidfile != "" {
				command := exec.Command(os.Args[0], "-test.run=^TestProxyPIDAdmissionOtherProcess$")
				command.Env = append(os.Environ(), "KUASAR_PROXY_PID_ADMISSION_TEST_SOCKET="+socket)
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("PID gate regression: %v\n%s", err, output)
				}
			}
			cancel()
			<-done
			if mode != "pidfile-offline" {
				if code, body := rawPost(t, client, routesync.ProxyRetirePath, identity); code != http.StatusNoContent {
					t.Fatalf("instance retirement: %d %s", code, body)
				}
			}
			if err := proxycompletion.Complete(identity, private); err != nil {
				t.Fatal(err)
			}
			if mode != "pidfile-offline" {
				if code, body := rawPost(t, client, routesync.ProxyRetirePath, identity); code != http.StatusNoContent {
					t.Fatalf("service completion: %d %s", code, body)
				}
			}
			// Offline mode deliberately delivers no retirement/completion RPC.
			// Reload actual SQLite obligations as on conductor startup.
			restored, err := NewPersistentRegistry(ctx, database, binding.KernelBoot)
			if err != nil || len(restored.obligations) != 0 {
				t.Fatalf("completed single instance retained obligations: %v, %v", restored, err)
			}
			records, err := database.LoadProxyStats(ctx, binding.KernelBoot)
			if err != nil || len(records) != 0 {
				t.Fatalf("retained durable rows: %v, %v", records, err)
			}
			if _, err := os.Stat(binding.Path); !os.IsNotExist(err) {
				t.Fatalf("completion receipt retained: %v", err)
			}
		})
	}
}
