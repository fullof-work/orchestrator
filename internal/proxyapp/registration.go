package proxyapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"slices"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/proxycompletion"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// This runs before service resources are released and before done is closed.
// Failed ACKs leave a signed durable receipt, consumed by conductor recovery or
// a later admitted service. A live or unjoined process never produces a receipt.
func (s *Service) finishServiceRegistration() error {
	s.mu.Lock()
	if s.candidate || s.completion.Path == "" {
		s.mu.Unlock()
		return nil
	}
	if !s.ownershipRestored || s.baseline == nil {
		s.mu.Unlock()
		return fmt.Errorf("service completion withheld: ownership restoration incomplete")
	}
	identity := routesync.ProxyInstance{ServiceID: s.serviceID, BootID: s.serviceID, Slot: "coordinator", Generation: max(s.localGeneration, 1), CoordinatorPID: s.coordinatorPID, Completion: s.completion}
	private := s.completionKey
	instance, operation := s.local, s.operation
	ended, child, socket := s.localDone == nil || s.localEnded, s.child, s.baseline.config.ConfigSocket
	s.mu.Unlock()
	if child != nil {
		select {
		case <-child.done:
		default:
			ended = false
		}
	}
	if !ended {
		return fmt.Errorf("service completion withheld: business processes have not ended")
	}
	legacy := false
	if instance != nil {
		instance.mu.Lock()
		legacy = operation == nil && instance.capabilitiesKnown && !slices.Contains(instance.capabilities, routesync.InplaceRestartV1)
		instance.mu.Unlock()
	}
	if legacy {
		return proxycompletion.Remove(identity)
	}
	if err := proxycompletion.Complete(identity, private); err != nil {
		return fmt.Errorf("persist service completion: %w", err)
	}
	// Delivery failure is recoverable from the receipt. It is not a false
	// business drain failure, and cannot keep a completed service alive.
	_ = s.retireInstance(socket, identity, true)
	return nil
}

// Retirement follows RunMaster's complete return, including joining the route
// subscriber and reaping workers. A failed ACK retains localDone and its cached
// result; later cleanup retries it without waiting on an already-consumed channel.
func (s *Service) finishLocalRegistration() error {
	s.mu.Lock()
	identity, instance, done, ended := s.localIdentity, s.local, s.localDone, s.localEnded
	candidate, baseline := s.candidate, s.baseline
	s.mu.Unlock()
	if done == nil {
		return nil
	}
	if !ended {
		return fmt.Errorf("local instance has not ended")
	}
	if identity != nil && !candidate {
		instance.mu.Lock()
		supported := slices.Contains(instance.capabilities, routesync.InplaceRestartV1)
		instance.mu.Unlock()
		if err := s.retireInstance(baseline.config.ConfigSocket, *identity, supported); err != nil {
			return err
		}
	}
	s.mu.Lock()
	if s.localDone == done {
		s.localDone = nil
	}
	s.mu.Unlock()
	return nil
}

// This is bookkeeping after business has ended, not a new business drain budget.
// An unavailable conductor cannot cause a completed generation to be forgotten
// or counted as idle. The owning coordinator retries during recovery.
func retireProxyInstance(socket string, identity routesync.ProxyInstance, supported bool) error {
	return retireProxyInstanceContext(context.Background(), socket, identity, supported)
}

func (s *Service) retireInstance(socket string, identity routesync.ProxyInstance, supported bool) error {
	s.mu.Lock()
	record := s.stopRecord
	s.mu.Unlock()
	if record != nil {
		deadline, err := record.read()
		if err != nil {
			return err
		}
		if !deadline.IsZero() {
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			return retireProxyInstanceContext(ctx, socket, identity, supported)
		}
	}
	return retireProxyInstance(socket, identity, supported)
}

func retireProxyInstanceContext(parent context.Context, socket string, identity routesync.ProxyInstance, supported bool) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	payload, err := json.Marshal(identity)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	defer transport.CloseIdleConnections()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://conductor"+routesync.ProxyRetirePath, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Transport: transport}).Do(request)
	if err != nil {
		return fmt.Errorf("proxy retirement acknowledgement: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNoContent {
		return nil
	}
	// A legacy conductor never stored these optional registration fields. Its
	// generic API fallback may require API credentials or report no such route.
	if !supported && (response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden) {
		return nil
	}
	return fmt.Errorf("proxy retirement acknowledgement: HTTP %d", response.StatusCode)
}
