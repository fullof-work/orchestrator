package proxycompletion

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestCompletionRequiresExactSignedOwnerAndFile(t *testing.T) {
	identity := routesync.ProxyInstance{ServiceID: strings.Repeat("a", 32), BootID: strings.Repeat("b", 32), Slot: "coordinator", Generation: 1, CoordinatorPID: os.Getpid()}
	binding, key, err := Create(filepath.Join(t.TempDir(), "stats.sock"), identity)
	if err != nil {
		t.Fatal(err)
	}
	identity.Completion = binding
	if err := ValidatePending(identity); err != nil {
		t.Fatal(err)
	}
	if complete, err := Verify(identity); err != nil || complete {
		t.Fatalf("pending became complete: %v %v", complete, err)
	}
	if err := Complete(identity, key); err != nil {
		t.Fatal(err)
	}
	if complete, err := Verify(identity); err != nil || !complete {
		t.Fatalf("completed receipt missing: %v %v", complete, err)
	}
	if err := ValidatePending(identity); err == nil {
		t.Fatal("completed service admitted")
	}
	other := identity
	other.ServiceID = strings.Repeat("c", 32)
	if complete, _ := Verify(other); complete {
		t.Fatal("receipt retired another service")
	}
	if len(Pending(filepath.Join(filepath.Dir(binding.Path), "stats.sock"))) != 1 {
		t.Fatal("could not recover completed receipt")
	}
	raw, err := os.ReadFile(binding.Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(binding.Path, raw[:len(raw)/2], 0600); err != nil {
		t.Fatal(err)
	}
	if complete, _ := Verify(identity); complete {
		t.Fatal("partial receipt accepted")
	}
	if err := Complete(identity, key); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(binding.Path, binding.Path+".original"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(binding.Path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if complete, _ := Verify(identity); complete {
		t.Fatal("substituted inode accepted")
	}
	if err := Remove(identity); err == nil {
		t.Fatal("removed foreign replacement")
	}
}
