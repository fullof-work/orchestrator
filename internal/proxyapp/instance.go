package proxyapp

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (i *Instance) attachWorker(index int, epoch uint64, peer *controlPeer) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.stopping {
		return false
	}
	i.workers[index] = workerState{epoch: epoch, peer: peer, alive: true}
	i.recalculateLocked()
	i.notifyLocked()
	return true
}

func (i *Instance) workerExited(index int, epoch uint64) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.workers[index].epoch != epoch {
		return
	}
	if i.stopping && i.workers[index].alive && !i.workers[index].mmdsDone {
		i.err = errors.Join(i.err, ErrForcedClose)
	}
	i.workers[index].alive = false
	i.workers[index].ready = false
	i.recalculateLocked()
	i.notifyLocked()
}

func (i *Instance) workerFault(index int, epoch uint64) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.workers[index].epoch != epoch {
		return
	}
	i.workers[index].ready = false
	i.recalculateLocked()
	i.notifyLocked()
}

func (i *Instance) workerEvent(index int, epoch uint64, message controlMessage) error {
	i.mu.Lock()
	w := &i.workers[index]
	if w.epoch != epoch || !w.alive {
		i.mu.Unlock()
		return nil
	}
	var start bool
	switch message.Kind {
	case "prepared":
		if !w.prepared {
			w.prepared = true
			w.canShutdown = message.CanShutdown
			start = !i.stopping
		}
	case "serving":
		if !w.prepared {
			i.mu.Unlock()
			return fmt.Errorf("proxy worker serving before prepared")
		}
		if !i.stopping {
			w.ready = true
		}
	case "data_drained":
		if !i.stopping {
			i.mu.Unlock()
			return fmt.Errorf("proxy worker drained without a stop request")
		}
		w.dataDone = true
		w.ready = false
	case "mmds_drained":
		if !w.dataDone {
			i.mu.Unlock()
			return fmt.Errorf("proxy worker MMDS drained before data")
		}
		w.mmdsDone = true
	case "stopped":
		if !w.mmdsDone {
			i.mu.Unlock()
			return fmt.Errorf("proxy worker stopped before MMDS drained")
		}
	default:
		i.mu.Unlock()
		return fmt.Errorf("proxy worker invalid lifecycle event %q", message.Kind)
	}
	if message.Forced {
		i.err = errors.Join(i.err, ErrForcedClose)
	}
	if message.Error != "" {
		i.err = errors.Join(i.err, fmt.Errorf("worker %d: %s", index, message.Error))
	}
	peer := w.peer
	i.recalculateLocked()
	i.notifyLocked()
	i.mu.Unlock()
	if start {
		return peer.send(controlMessage{Kind: "start"})
	}
	return nil
}

func (i *Instance) workerFinishing(index int, epoch uint64) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	w := i.workers[index]
	return w.epoch == epoch && i.stopping && w.mmdsDone
}

// drainWorkers enforces a global data barrier before any MMDS listener stops.
// Dependencies remain live until the caller reaps every worker. Any unresponsive
// control connection consumes only the original absolute deadline.
func (i *Instance) drainWorkers() error {
	i.mu.Lock()
	deadline := i.deadline
	i.mu.Unlock()
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	for _, phase := range []string{"drain_data", "drain_mmds", "stop"} {
		i.mu.Lock()
		peers := make([]*controlPeer, 0, len(i.workers))
		for _, w := range i.workers {
			if w.alive {
				peers = append(peers, w.peer)
			}
		}
		if phase == "drain_mmds" {
			i.status.Data = "stopped"
			if i.status.MMDS != "disabled" {
				i.status.MMDS = "draining"
			}
			i.notifyLocked()
		}
		i.mu.Unlock()
		// Send independently: an unresponsive worker cannot delay another worker's
		// stop-accepting command or consume a fresh per-worker budget.
		sent := make(chan error, len(peers))
		for _, peer := range peers {
			go func() { sent <- peer.send(controlMessage{Kind: phase, Deadline: deadline}) }()
		}
		for range peers {
			select {
			case err := <-sent:
				if err != nil {
					return errors.Join(ErrForcedClose, err)
				}
			case <-ctx.Done():
				return fmt.Errorf("proxy %s: %w", phase, errors.Join(ErrForcedClose, ctx.Err()))
			}
		}
		for {
			i.mu.Lock()
			complete := true
			for _, w := range i.workers {
				if w.alive && ((phase == "drain_data" && !w.dataDone) || (phase == "drain_mmds" && !w.mmdsDone) || phase == "stop") {
					complete = false
				}
			}
			changed := i.changed
			i.mu.Unlock()
			if complete {
				break
			}
			select {
			case <-changed:
			case <-ctx.Done():
				return fmt.Errorf("proxy %s: %w", phase, errors.Join(ErrForcedClose, ctx.Err()))
			}
		}
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.err
}

func (i *Instance) stopped() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.stopping
}

func (i *Instance) extensionCompatible() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, w := range i.workers {
		if !w.alive || !w.ready || !w.canShutdown {
			return false
		}
	}
	return true
}

func (i *Instance) remainingContext() (context.Context, context.CancelFunc) {
	i.mu.Lock()
	deadline := i.deadline
	i.mu.Unlock()
	if deadline.IsZero() {
		deadline = time.Now()
	}
	return context.WithDeadline(context.Background(), deadline)
}
