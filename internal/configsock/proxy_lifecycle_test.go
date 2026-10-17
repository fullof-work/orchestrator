package configsock

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/proxycompletion"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

func TestPersistentProxyStatsObligations(t *testing.T) {
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
	registry, err := NewPersistentRegistry(ctx, database, "kernel-boot")
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command("sleep", "60")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Process.Signal(syscall.SIGTERM); _ = child.Wait() })
	makePlugin := func(service, boot, id, slot string, generation uint64, pid int) *Plugin {
		caps := proxyCaps()
		caps.Proxy.StatsSocket = &routesync.Socket{Path: "/run/" + id + ".sock"}
		caps.Proxy.Instance = &routesync.ProxyInstance{ServiceID: service, BootID: boot, Slot: slot, Generation: generation, CoordinatorPID: os.Getpid()}
		return &Plugin{ID: id, Caps: caps, peerPID: pid, cancel: func() {}}
	}
	service := strings.Repeat("a", 32)
	a := makePlugin(service, strings.Repeat("1", 32), "proxy", "coordinator", 1, os.Getpid())
	b := makePlugin(service, strings.Repeat("2", 32), "proxy-b", "candidate", 1, child.Process.Pid)
	other := makePlugin(strings.Repeat("b", 32), strings.Repeat("3", 32), "another-service", "coordinator", 1, os.Getpid())
	for _, plugin := range []*Plugin{a, b, other} {
		if err := registry.Add(plugin); err != nil {
			t.Fatal(err)
		}
	}
	if targets, ok := registry.ProxyStatsTargets(); !ok || len(targets) != 3 {
		t.Fatalf("missing members: %v %v", targets, ok)
	}
	registry.Remove(a)
	if _, ok := registry.ProxyStatsTargets(); ok {
		t.Fatal("disconnected A was counted as absent/idle")
	}
	// Reconstruct the conductor's registry from its real SQLite state. Even if
	// an entirely different service reconnects first, A and B remain necessary.
	restored, err := NewPersistentRegistry(ctx, database, "kernel-boot")
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Add(other); err != nil {
		t.Fatal(err)
	}
	if _, ok := restored.ProxyStatsTargets(); ok {
		t.Fatal("lost an entire service across conductor restart")
	}
	if err := restored.Add(b); err != nil {
		t.Fatal(err)
	}
	if _, ok := restored.ProxyStatsTargets(); ok {
		t.Fatal("B reconnect hid the disconnected A")
	}
	a2 := makePlugin(service, a.Caps.Proxy.Instance.BootID, "proxy", "coordinator", 1, os.Getpid())
	if err := restored.Add(a2); err != nil {
		t.Fatal(err)
	}
	restored.Remove(a) // a prior lease must not remove the reconnected incarnation.
	if targets, ok := restored.ProxyStatsTargets(); !ok || len(targets) != 3 {
		t.Fatalf("reconnect did not restore full set: %v %v", targets, ok)
	}
	next := makePlugin(service, strings.Repeat("4", 32), "proxy", "coordinator", 2, os.Getpid())
	if err := restored.Add(next); err == nil {
		t.Fatal("replaced an unretired business lifetime")
	}
	if err := restored.retireProxy(*a.Caps.Proxy.Instance, child.Process.Pid); err == nil {
		t.Fatal("candidate retired its coordinator")
	}
	if err := restored.retireProxy(*a.Caps.Proxy.Instance, os.Getpid()); err != nil {
		t.Fatal(err)
	}
	if err := restored.Add(next); err != nil {
		t.Fatal(err)
	}
	if err := restored.retireProxy(*a.Caps.Proxy.Instance, os.Getpid()); err != nil {
		t.Fatal(err)
	}
	if err := restored.Add(a2); err == nil {
		t.Fatal("delayed retired registration revived")
	}
	// Keep the durable state bounded by ownership slots, not number of restarts.
	for generation := uint64(3); generation < 100; generation++ {
		if err := restored.retireProxy(*next.Caps.Proxy.Instance, os.Getpid()); err != nil {
			t.Fatal(err)
		}
		next = makePlugin(service, fmt.Sprintf("%032x", generation+100), "proxy", "coordinator", generation, os.Getpid())
		if err := restored.Add(next); err != nil {
			t.Fatal(err)
		}
	}
	records, err := database.LoadProxyStats(ctx, "kernel-boot")
	if err != nil || len(records) != 3 {
		t.Fatalf("unbounded or lost records: %d %v", len(records), err)
	}
	newBoot, err := NewPersistentRegistry(ctx, database, "new-kernel-boot")
	if err != nil || len(newBoot.obligations) != 0 {
		t.Fatalf("kernel restart retained nonexistent processes: %v", err)
	}
}

func TestRetirementBeforeRegistrationRejectsDelayedFrame(t *testing.T) {
	r := NewRegistry()
	identity := routesync.ProxyInstance{ServiceID: strings.Repeat("c", 32), BootID: strings.Repeat("d", 32), Slot: "coordinator", Generation: 1, CoordinatorPID: os.Getpid()}
	if err := r.retireProxy(identity, os.Getpid()); err != nil {
		t.Fatal(err)
	}
	caps := proxyCaps()
	caps.Proxy.Instance = &identity
	caps.Proxy.StatsSocket = &routesync.Socket{Path: "/run/late.sock"}
	if err := r.Add(&Plugin{ID: "proxy", Caps: caps, peerPID: os.Getpid()}); err == nil {
		t.Fatal("admitted a frame after acknowledged retirement")
	}
	identity.Generation++
	identity.BootID = strings.Repeat("e", 32)
	if err := r.retireProxy(identity, os.Getpid()); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(&Plugin{ID: "proxy", Caps: caps, peerPID: os.Getpid()}); err == nil {
		t.Fatal("admitted a newer failed attempt after retirement")
	}
}

func TestRetirementOfUnknownSlotPreservesServiceOwner(t *testing.T) {
	r := NewRegistry()
	owner := routesync.ProxyInstance{ServiceID: strings.Repeat("a", 32), BootID: strings.Repeat("b", 32), Slot: "coordinator", Generation: 1, CoordinatorPID: os.Getpid()}
	if err := r.retireProxy(owner, os.Getpid()); err != nil {
		t.Fatal(err)
	}
	other := exec.Command("sleep", "60")
	if err := other.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Process.Kill(); _ = other.Wait() }()
	candidate := owner
	candidate.Slot, candidate.BootID = "candidate", strings.Repeat("c", 32)
	candidate.CoordinatorPID = other.Process.Pid
	if err := r.retireProxy(candidate, other.Process.Pid); err == nil {
		t.Fatal("another admitted process claimed the unknown slot of an existing service")
	}
	if len(r.obligations) != 1 {
		t.Fatal("rejected retirement changed obligations")
	}
	candidate.CoordinatorPID = os.Getpid()
	if err := r.retireProxy(candidate, os.Getpid()); err != nil {
		t.Fatal(err)
	}
}

func TestCompletedServiceRecoveredAfterConductorOutage(t *testing.T) {
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
	binding, private, err := proxycompletion.Create(filepath.Join(t.TempDir(), "stats.sock"), identity)
	if err != nil {
		t.Fatal(err)
	}
	identity.Completion = binding
	// Use the actual conductor entry input, including the procfs trailing
	// newline. The stored identity must match the completion binding as well
	// as a registry reconstructed with its already-normalized value below.
	kernelBoot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewPersistentRegistry(ctx, database, string(kernelBoot))
	if err != nil {
		t.Fatal(err)
	}
	caps := proxyCaps()
	caps.Proxy.Instance = &identity
	caps.Proxy.StatsSocket = &routesync.Socket{Path: "/run/a.sock"}
	p := &Plugin{ID: "proxy", Caps: caps, peerPID: os.Getpid()}
	if err := r.Add(p); err != nil {
		t.Fatal(err)
	}
	r.Remove(p)
	if _, ok := r.ProxyStatsTargets(); ok {
		t.Fatal("lost lease became idle")
	}
	// No retirement RPC is delivered. Completion is written after the service
	// ends; conductor's next startup must consume the durable signed receipt.
	if err := proxycompletion.Complete(identity, private); err != nil {
		t.Fatal(err)
	}
	restored, err := NewPersistentRegistry(ctx, database, binding.KernelBoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(restored.obligations) != 0 {
		t.Fatal("conductor restart did not consume completed-service evidence")
	}
	if _, err := os.Stat(binding.Path); !os.IsNotExist(err) {
		t.Fatalf("completion object not reclaimed: %v", err)
	}
	if err := restored.Add(p); err == nil {
		t.Fatal("late completed registration revived")
	}
	pruned, err := NewPersistentRegistry(ctx, database, binding.KernelBoot)
	if err != nil || len(pruned.obligations) != 0 {
		t.Fatalf("completed service retained durable rows: %v %v", pruned, err)
	}
}

func TestCompletedUnacknowledgedRegistrationCanBeRelayed(t *testing.T) {
	identity := routesync.ProxyInstance{ServiceID: strings.Repeat("a", 32), BootID: strings.Repeat("b", 32), Slot: "coordinator", Generation: 1, CoordinatorPID: os.Getpid()}
	path := filepath.Join(t.TempDir(), "stats.sock")
	binding, private, err := proxycompletion.Create(path, identity)
	if err != nil {
		t.Fatal(err)
	}
	identity.Completion = binding
	if err := proxycompletion.Complete(identity, private); err != nil {
		t.Fatal(err)
	}
	queued := proxycompletion.Pending(path)
	if len(queued) != 1 {
		t.Fatalf("missing outbox: %v", queued)
	}
	r := NewRegistry()
	// Identity authorization comes from the admitted service's key. The relay
	// does not impersonate the exited coordinator or obtain its private key.
	if err := r.retireProxy(queued[0], os.Getpid()+10000); err != nil {
		t.Fatal(err)
	}
	if len(r.obligations) != 0 {
		t.Fatal("self-declared completion key claimed unregistered service ownership")
	}
	if err := r.retireProxy(queued[0], os.Getpid()+10000); err != nil || len(r.obligations) != 0 {
		t.Fatalf("lost completion ACK recreated ownership: %v", err)
	}
	if _, err := os.Stat(binding.Path); !os.IsNotExist(err) {
		t.Fatalf("unacknowledged startup receipt retained: %v", err)
	}
}

func TestCompletedServicesDoNotAccumulateWithoutConductorRestart(t *testing.T) {
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
	root := t.TempDir()
	var registry *Registry
	for index := 1; index <= 20; index++ {
		identity := routesync.ProxyInstance{ServiceID: fmt.Sprintf("%032x", index), BootID: strings.Repeat("b", 32), Slot: "coordinator", Generation: 1, CoordinatorPID: os.Getpid()}
		binding, key, err := proxycompletion.Create(filepath.Join(root, "stats.sock"), identity)
		if err != nil {
			t.Fatal(err)
		}
		identity.Completion = binding
		if registry == nil {
			registry, err = NewPersistentRegistry(ctx, database, binding.KernelBoot)
			if err != nil {
				t.Fatal(err)
			}
		}
		caps := proxyCaps()
		caps.Proxy.Instance = &identity
		caps.Proxy.StatsSocket = &routesync.Socket{Path: filepath.Join(root, "stats.sock")}
		if err := registry.Add(&Plugin{ID: "proxy", Caps: caps, peerPID: os.Getpid()}); err != nil {
			t.Fatal(err)
		}
		if err := proxycompletion.Complete(identity, key); err != nil {
			t.Fatal(err)
		}
		registry.ProxyStatsTargets()
		if len(registry.obligations) != 0 {
			t.Fatal("completed service accumulated in memory")
		}
		records, err := database.LoadProxyStats(ctx, binding.KernelBoot)
		if err != nil || len(records) != 0 {
			t.Fatalf("completed service accumulated in SQLite: %v %v", records, err)
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("completion files accumulated: %v %v", entries, err)
	}
}
