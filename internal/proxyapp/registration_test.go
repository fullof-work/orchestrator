package proxyapp

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/proxycompletion"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestSingleInstanceCompletionContracts(t *testing.T) {
	for _, capability := range []string{"legacy", routesync.InplaceRestartV1, routesync.ProxyCompletionV1} {
		t.Run(capability, func(t *testing.T) {
			s := NewService()
			cfg := testProxyConfig(t)
			dir, err := os.MkdirTemp("", "proxy-completion-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			cfg.StatsSocket = filepath.Join(dir, "stats.sock")
			cfg.ConfigSocket = filepath.Join(dir, "offline.sock")
			s.baseline, err = FreezeConfig(cfg)
			if err != nil {
				t.Fatal(err)
			}
			s.ownershipRestored = true
			s.local = NewInstance("proxy", "", "", 0)
			s.local.capabilitiesKnown = true
			if capability != "legacy" {
				s.local.capabilities = []string{capability}
			}
			identity := routesync.ProxyInstance{ServiceID: s.serviceID, BootID: s.serviceID, Slot: "coordinator", Generation: 1, CoordinatorPID: os.Getpid()}
			s.completion, s.completionKey, err = proxycompletion.Create(cfg.StatsSocket, identity)
			if err != nil {
				t.Fatal(err)
			}
			identity.Completion = s.completion
			if err := s.finishServiceRegistration(); err != nil {
				t.Fatal(err)
			}
			if capability == "legacy" {
				if _, err := os.Stat(identity.Completion.Path); !os.IsNotExist(err) {
					t.Fatalf("legacy completion retained: %v", err)
				}
				return
			}
			if complete, err := proxycompletion.Verify(identity); err != nil || !complete {
				t.Fatalf("offline conductor lost signed completion: complete=%v err=%v", complete, err)
			}
		})
	}
}

func TestCompletionOnlyRetirementDoesNotIgnoreAdmissionFailure(t *testing.T) {
	dir, err := os.MkdirTemp("", "proxy-retire-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "config.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) })}
	go server.Serve(listener)
	defer server.Close()
	s := NewService()
	cfg := testProxyConfig(t)
	cfg.ConfigSocket = socket
	s.baseline, err = FreezeConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.local = NewInstance("proxy", "", "", 0)
	s.local.capabilities = []string{routesync.ProxyCompletionV1}
	s.localDone, s.localEnded = make(chan error), true
	s.localIdentity = &routesync.ProxyInstance{ServiceID: s.serviceID, BootID: s.serviceID, Slot: "coordinator", Generation: 1, CoordinatorPID: os.Getpid()}
	if err := s.finishLocalRegistration(); err == nil || s.localDone == nil {
		t.Fatalf("completion-only conductor authorization failure ignored: %v", err)
	}
	if err := retireProxyInstanceContext(context.Background(), socket, *s.localIdentity, false); err != nil {
		t.Fatalf("legacy generic API fallback changed: %v", err)
	}
}
