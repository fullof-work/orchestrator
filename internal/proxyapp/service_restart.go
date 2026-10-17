package proxyapp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const candidatePrepareTimeout = 30 * time.Second

func (s *Service) drainLocal(deadline time.Time) error {
	s.mu.Lock()
	instance, done := s.local, s.localDone
	s.mu.Unlock()
	if instance == nil || done == nil {
		return nil
	}
	instance.requestStop(deadline)
	err := <-done
	s.mu.Lock()
	if s.localDone == done {
		s.localDone = nil
	}
	s.mu.Unlock()
	if errors.Is(err, ErrForcedClose) {
		s.recordForced()
	}
	return err
}

func (s *Service) recordForced() {
	s.mu.Lock()
	if s.operation != nil {
		s.operation.ForcedCloseCount++
	}
	s.mu.Unlock()
}

func (s *Service) prepareChild() (*serviceChild, error) {
	child, err := s.startChild()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), candidatePrepareTimeout)
	defer cancel()
	return child, s.waitChildReady(ctx, child)
}

func (s *Service) restart() error {
	child, err := s.prepareChild()
	if err != nil {
		cleanup := s.reapChild(child, time.Now().Add(s.timeout), false)
		if s.stopping() {
			return s.shutdownAll()
		}
		s.operationComplete("failed", errors.Join(err, cleanup))
		return nil
	}
	if s.stopping() {
		return s.shutdownAll()
	}
	// This transition is the rollback boundary. B's readiness comes exclusively
	// from its dedicated control connection, never the shared business listener.
	if !child.snapshot().Ready || child.failed() != nil {
		err = s.reapChild(child, time.Now().Add(s.timeout), false)
		s.operationComplete("failed", errors.Join(ErrNotReady, err))
		return nil
	}
	s.phase("draining_a", nil)
	err = s.drainLocal(time.Now().Add(s.timeout))
	if err != nil {
		s.phase("recovering", err)
	}
	if s.stopping() {
		return s.shutdownAll()
	}
	for attempt := 1; !s.stopping(); attempt++ {
		if err := s.ensureChild(); err != nil {
			if !s.backoff(attempt, err) {
				break
			}
			continue
		}
		s.phase("exec", nil)
		err = s.execTarget()
		if s.stopping() {
			break
		}
		if err == nil {
			err = fmt.Errorf("proxy exec returned without replacing the image")
		}
		s.mu.Lock()
		s.operation.Attempts++
		s.mu.Unlock()
		if !s.backoff(attempt, err) {
			break
		}
	}
	return s.shutdownAll()
}

// ensureChild never creates a replacement before the previous process group
// has been reaped. Every successful invocation leaves exactly one healthy B.
func (s *Service) ensureChild() error {
	s.mu.Lock()
	child := s.child
	s.mu.Unlock()
	if child != nil {
		if child.failed() == nil && child.snapshot().Ready {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), candidatePrepareTimeout)
		err := s.waitChildReady(ctx, child)
		cancel()
		if err == nil {
			return nil
		}
		if err := s.reapChild(child, time.Now().Add(s.timeout), false); err != nil {
			s.phase("recovering", err)
			s.mu.Lock()
			remaining := s.child != nil
			s.mu.Unlock()
			if remaining {
				return err
			}
		}
	}
	if s.stopping() {
		return ErrStopped
	}
	child, err := s.prepareChild()
	if err != nil {
		cleanup := s.reapChild(child, time.Now().Add(s.timeout), false)
		return errors.Join(err, cleanup)
	}
	return nil
}

func (s *Service) waitLocalReady(ctx context.Context) error {
	s.mu.Lock()
	instance, child := s.local, s.child
	s.mu.Unlock()
	for {
		instance.mu.Lock()
		status, changed, runErr := instance.status, instance.changed, instance.err
		instance.mu.Unlock()
		if status.Ready {
			return nil
		}
		if status.Phase == "failed" || status.Phase == "stopped" {
			return errors.Join(ErrNotReady, runErr)
		}
		var childChanged <-chan struct{}
		if child != nil {
			child.mu.Lock()
			childChanged = child.changed
			err := child.err
			child.mu.Unlock()
			if err != nil {
				return err
			}
		}
		select {
		case <-changed:
		case <-childChanged:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stop:
			return ErrStopped
		}
	}
}

func (s *Service) resumeOperation() error {
	for attempt := 1; !s.stopping(); attempt++ {
		if err := s.ensureChild(); err != nil {
			if !s.backoff(attempt, err) {
				break
			}
			continue
		}
		s.phase("preparing_a_prime", nil)
		s.mu.Lock()
		s.operation.Attempts++
		s.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), candidatePrepareTimeout)
		err := s.startLocal(ctx, newBootID(), routesync.ProxyPluginID, s.resources)
		if err == nil {
			err = s.waitLocalReady(ctx)
		}
		cancel()
		if err != nil {
			cleanup := s.drainLocal(time.Now().Add(s.timeout))
			if !s.backoff(attempt, errors.Join(err, cleanup)) {
				break
			}
			continue
		}
		if s.stopping() {
			break
		}
		s.phase("draining_b", nil)
		s.mu.Lock()
		child, instance := s.child, s.local
		s.mu.Unlock()
		reaped := make(chan error, 1)
		go func() { reaped <- s.reapChild(child, time.Now().Add(s.timeout), false) }()
		failed := false
		for !failed {
			instance.mu.Lock()
			changed := instance.changed
			ready := instance.status.Ready
			instance.mu.Unlock()
			if !ready {
				failed = true
				break
			}
			select {
			case <-changed:
			case <-s.stop:
				// A single public stop budget replaces the remaining service work.
				deadline, _ := s.stopRecord.read()
				instance.requestStop(deadline)
			case err = <-reaped:
				if errors.Is(err, ErrForcedClose) {
					s.recordForced()
				}
				s.mu.Lock()
				reapedCompletely := s.child == nil
				s.mu.Unlock()
				if instance.Snapshot().Ready && reapedCompletely {
					s.mu.Lock()
					forced := s.operation.ForcedCloseCount
					s.mu.Unlock()
					result := "completed"
					if forced != 0 {
						result = "completed_with_forced_close"
					}
					s.operationComplete(result, nil)
					return nil
				}
				if instance.Snapshot().Ready && !s.stopping() {
					// Keep the healthy successor serving while retrying cleanup of
					// the same B. Reclamation failure never starts a third instance.
					if !s.backoff(attempt, err) {
						reaped <- err
						failed = true
						continue
					}
					attempt++
					go func() { reaped <- s.reapChild(child, time.Now().Add(s.timeout), false) }()
					continue
				}
				// A reaping error remains observable; no third generation is started.
				reaped <- err
				failed = true
			}
		}
		err = <-reaped
		cleanup := s.drainLocal(time.Now().Add(s.timeout))
		if s.stopping() {
			break
		}
		if !s.backoff(attempt, errors.Join(fmt.Errorf("A-prime lost readiness while B was draining"), err, cleanup)) {
			break
		}
	}
	return s.shutdownAll()
}

func (s *Service) shutdownAll() error {
	deadline, err := s.stopRecord.read()
	if err != nil {
		return err
	}
	if deadline.IsZero() {
		deadline, err = s.stopRecord.request(time.Now().Add(s.timeout))
		if err != nil {
			return err
		}
	}
	s.mu.Lock()
	instance, child, localDone := s.local, s.child, s.localDone
	s.mu.Unlock()
	gate := make(chan struct{})
	if instance != nil && localDone != nil {
		instance.mu.Lock()
		if !instance.stopping {
			instance.mmdsGate = gate
		}
		instance.mu.Unlock()
		instance.requestStop(deadline)
	}
	if child != nil {
		_ = child.wire.send(serviceMessage{Kind: "drain", Deadline: deadline, HoldMMDS: true})
	}
	timer := time.NewTimer(max(time.Until(deadline), 0))
	defer timer.Stop()
	for {
		dataDone := true
		var localChanged, childChanged <-chan struct{}
		if instance != nil && localDone != nil {
			instance.mu.Lock()
			localChanged = instance.changed
			dataDone = instance.status.Data == "stopped"
			instance.mu.Unlock()
		}
		if child != nil {
			child.mu.Lock()
			childChanged = child.changed
			dataDone = dataDone && child.status.Data == "stopped"
			child.mu.Unlock()
		}
		if dataDone {
			break
		}
		select {
		case <-localChanged:
		case <-childChanged:
		case <-timer.C:
			goto release
		}
	}
release:
	close(gate)
	if child != nil {
		_ = child.wire.send(serviceMessage{Kind: "release_mmds", Deadline: deadline})
	}
	results := make(chan error, 2)
	go func() { results <- s.drainLocal(deadline) }()
	go func() { results <- s.reapChild(child, deadline, false) }()
	err = errors.Join(<-results, <-results)
	s.mu.Lock()
	active := s.operation != nil && s.operation.Result == ""
	s.mu.Unlock()
	if active {
		s.operationComplete("stopped", err)
	}
	return err
}
