package configsock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/proxycompletion"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type ProxyStatsStore interface {
	LoadProxyStats(context.Context, string) ([]routesync.ProxyStatsRecord, error)
	SaveProxyStats(context.Context, string, routesync.ProxyStatsRecord) error
	DeleteCompletedProxyStats(context.Context, string) error
}

func NewPersistentRegistry(ctx context.Context, storage ProxyStatsStore, kernelBoot string) (*Registry, error) {
	// procfs includes a trailing newline; normalize before both durable lookup
	// and comparison with the Proxy's completion-file binding.
	kernelBoot = strings.TrimSpace(kernelBoot)
	records, err := storage.LoadProxyStats(ctx, kernelBoot)
	if err != nil {
		return nil, err
	}
	r := NewRegistry()
	r.storage, r.kernelBoot = storage, kernelBoot
	for _, record := range records {
		r.obligations[record.Identity.Key()] = record
	}
	r.consumeCompletionsLocked()
	return r, nil
}

func (r *Registry) saveObligationLocked(record routesync.ProxyStatsRecord) error {
	if r.storage != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.storage.SaveProxyStats(ctx, r.kernelBoot, record); err != nil {
			return err
		}
	}
	r.obligations[record.Identity.Key()] = record
	return nil
}

func (r *Registry) admitObligationLocked(p *Plugin) error {
	if p.Caps.Proxy == nil || p.Caps.Proxy.Instance == nil {
		return nil
	}
	identity := *p.Caps.Proxy.Instance
	if err := identity.Validate(); err != nil {
		return err
	}
	if p.Caps.Proxy.StatsSocket == nil || !validLocalSocket(p.Caps.Proxy.StatsSocket.Path) || p.peerPID <= 1 {
		return fmt.Errorf("invalid proxy stats obligation")
	}
	if identity.Slot == "coordinator" && identity.CoordinatorPID != p.peerPID {
		return fmt.Errorf("proxy coordinator peer mismatch")
	}
	record := routesync.ProxyStatsRecord{Identity: identity, InstanceID: p.ID, PeerPID: p.peerPID, Socket: p.Caps.Proxy.StatsSocket.Path}
	parent, start, err := proxyProcessIdentity(p.peerPID)
	if err != nil {
		return err
	}
	if identity.Slot == "candidate" && parent != identity.CoordinatorPID {
		return fmt.Errorf("proxy candidate parent mismatch")
	}
	_, ownerStart, err := proxyProcessIdentity(identity.CoordinatorPID)
	if err != nil {
		return err
	}
	record.PeerStart, record.CoordinatorStart = start, ownerStart
	if identity.Completion.Path != "" && (identity.Completion.OwnerStart != ownerStart || (r.kernelBoot != "" && identity.Completion.KernelBoot != r.kernelBoot)) {
		return fmt.Errorf("proxy completion owner mismatch")
	}
	if err := proxycompletion.ValidatePending(identity); err != nil {
		return err
	}
	for _, existing := range r.obligations {
		if existing.Identity.ServiceID == identity.ServiceID && (existing.Identity.CoordinatorPID != identity.CoordinatorPID || existing.CoordinatorStart != ownerStart || existing.Identity.Completion != identity.Completion) {
			return fmt.Errorf("proxy service owner changed")
		}
	}
	prior, found := r.obligations[identity.Key()]
	if found {
		if prior == record {
			return nil
		}
		if !prior.Retired || prior.Identity.Generation >= identity.Generation || prior.Identity.CoordinatorPID != identity.CoordinatorPID || prior.CoordinatorStart != ownerStart {
			return fmt.Errorf("proxy lifetime conflicts with an existing obligation")
		}
	}
	return r.saveObligationLocked(record)
}

func (r *Registry) retireProxy(identity routesync.ProxyInstance, peer int) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// A completed service may have exited while conductor was offline. Any
	// admitted peer can relay its signed receipt, but cannot change its owner.
	if complete, err := proxycompletion.Verify(identity); err == nil && complete {
		known := false
		for _, existing := range r.obligations {
			if existing.Identity.ServiceID == identity.ServiceID {
				known = true
				if existing.Identity.CoordinatorPID != identity.CoordinatorPID || existing.Identity.Completion != identity.Completion {
					return fmt.Errorf("completed proxy service owner mismatch")
				}
			}
		}
		if !known {
			// There was no admitted obligation. Do not assign ownership using
			// a self-declared key; remove only this exact complete object. A
			// late registration cannot pass ValidatePending after its removal.
			return proxycompletion.Remove(identity)
		}
		r.consumeCompletionsLocked()
		for _, existing := range r.obligations {
			if existing.Identity.ServiceID == identity.ServiceID && !existing.ServiceCompleted {
				return fmt.Errorf("proxy completion persistence failed")
			}
		}
		return nil
	}
	record, found := r.obligations[identity.Key()]
	if !found {
		if identity.Completion.Path != "" && os.IsNotExist(proxycompletion.ValidatePending(identity)) {
			// A completed object and its durable rows may already have been
			// consumed before the caller received its ACK. Nothing remains to
			// retire, and a delayed registration cannot recreate this lifetime.
			return nil
		}
		if peer != identity.CoordinatorPID {
			return fmt.Errorf("unknown proxy retirement owner")
		}
		_, start, err := proxyProcessIdentity(peer)
		if err != nil {
			return err
		}
		for _, existing := range r.obligations {
			if existing.Identity.ServiceID == identity.ServiceID && (existing.Identity.CoordinatorPID != peer || existing.CoordinatorStart != start || existing.Identity.Completion != identity.Completion) {
				return fmt.Errorf("proxy retirement service owner changed")
			}
		}
		// Failed startup may end before its registration arrives. Persist the
		// retired generation floor as well, so that delayed frame cannot revive it.
		return r.saveObligationLocked(routesync.ProxyStatsRecord{Identity: identity, PeerPID: peer, PeerStart: start, CoordinatorStart: start, Retired: true})
	}
	if peer != record.PeerPID && peer != record.Identity.CoordinatorPID {
		return fmt.Errorf("proxy retirement owner mismatch")
	}
	_, start, err := proxyProcessIdentity(peer)
	if err != nil || !((peer == record.PeerPID && start == record.PeerStart) || (peer == record.Identity.CoordinatorPID && start == record.CoordinatorStart)) {
		return fmt.Errorf("proxy retirement process identity changed")
	}
	if record.Identity.CoordinatorPID != identity.CoordinatorPID {
		return fmt.Errorf("proxy retirement coordinator mismatch")
	}
	if record.Identity.Generation > identity.Generation {
		return nil
	} // already retired, never remove its successor.
	if record.Retired && record.Identity.Generation < identity.Generation {
		record.Identity = identity
		return r.saveObligationLocked(record)
	}
	if record.Identity != identity {
		return fmt.Errorf("proxy retirement lifetime mismatch")
	}
	if record.Retired {
		return nil
	}
	record.Retired = true
	if err := r.saveObligationLocked(record); err != nil {
		return err
	}
	if current := r.m[record.InstanceID]; current != nil && current.Caps.Proxy != nil && current.Caps.Proxy.Instance != nil && *current.Caps.Proxy.Instance == identity {
		r.failBarriersLocked(current, ErrProxyRouteDisconnected)
		current.ready = false
		if current.revoke != nil {
			current.revoke()
		}
		if current.cancel != nil {
			current.cancel()
		}
	}
	return nil
}

// The admitted public key authorizes completion even after the coordinator
// exited. A missing or corrupt receipt preserves unknown business obligations.
func (r *Registry) consumeCompletionsLocked() {
	seen := map[string]bool{}
	for _, record := range r.obligations {
		service := record.Identity.ServiceID
		if seen[service] {
			continue
		}
		seen[service] = true
		complete, err := proxycompletion.Verify(record.Identity)
		persisted := true
		for _, member := range r.obligations {
			if member.Identity.ServiceID == service {
				persisted = persisted && member.ServiceCompleted
			}
		}
		if !persisted && (err != nil || !complete) {
			continue
		}
		saved := true
		for _, member := range r.obligations {
			if member.Identity.ServiceID != service {
				continue
			}
			if member.Identity.Completion != record.Identity.Completion || member.Identity.CoordinatorPID != record.Identity.CoordinatorPID {
				saved = false
				break
			}
			member.Retired, member.ServiceCompleted = true, true
			if r.saveObligationLocked(member) != nil {
				saved = false
				break
			}
		}
		if saved && proxycompletion.Remove(record.Identity) == nil {
			if r.storage != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := r.storage.DeleteCompletedProxyStats(ctx, service)
				cancel()
				if err != nil {
					continue
				}
			}
			for key, member := range r.obligations {
				if member.Identity.ServiceID == service {
					delete(r.obligations, key)
				}
			}
			for id, plugin := range r.m {
				if plugin.Caps.Proxy == nil || plugin.Caps.Proxy.Instance == nil || plugin.Caps.Proxy.Instance.ServiceID != service {
					continue
				}
				r.failBarriersLocked(plugin, ErrProxyRouteDisconnected)
				if plugin.revoke != nil {
					plugin.revoke()
				}
				if plugin.cancel != nil {
					plugin.cancel()
				}
				delete(r.m, id)
			}
		}
	}
}

// SO_PEERCRED authenticates the connection; starttime prevents a later process
// reusing that numeric PID from inheriting an earlier service's responsibility.
func proxyProcessIdentity(pid int) (parent int, start uint64, err error) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, 0, err
	}
	end := strings.LastIndexByte(string(raw), ')')
	if end < 0 {
		return 0, 0, fmt.Errorf("invalid process identity")
	}
	fields := strings.Fields(string(raw[end+1:]))
	if len(fields) < 20 {
		return 0, 0, fmt.Errorf("incomplete process identity")
	}
	parent, err = strconv.Atoi(fields[1])
	if err != nil {
		return 0, 0, err
	}
	start, err = strconv.ParseUint(fields[19], 10, 64)
	return parent, start, err
}

func (s *Server) handleProxyRetire(w http.ResponseWriter, request *http.Request) {
	peer, ok := peerFrom(request.Context())
	if !ok || !s.pluginAuthed(peer) {
		http.Error(w, "not authorized (plugin)", http.StatusForbidden)
		return
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, request.Body, 4096))
	decoder.DisallowUnknownFields()
	var identity routesync.ProxyInstance
	var extra any
	if decoder.Decode(&identity) != nil || !errors.Is(decoder.Decode(&extra), io.EOF) || identity.Validate() != nil {
		http.Error(w, "invalid proxy retirement", http.StatusBadRequest)
		return
	}
	if err := s.deps.Plugins.retireProxy(identity, peer); err != nil {
		http.Error(w, "proxy retirement could not be confirmed", http.StatusConflict)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}
