package proxyapp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/listenresource"
	"golang.org/x/sys/unix"
)

type serviceChild struct {
	mu          sync.Mutex
	pid         int
	boot        string
	wire        *serviceWire
	resources   *retainedResources
	status      InstanceStatus
	canShutdown bool
	draining    bool
	err         error
	done        chan struct{}
	changed     chan struct{}
	detached    chan struct{}
	readerDone  chan struct{}
}

func (c *serviceChild) snapshot() InstanceStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	status := c.status
	status.Reasons = append([]string{}, status.Reasons...)
	return status
}
func (c *serviceChild) notifyLocked() { close(c.changed); c.changed = make(chan struct{}) }
func (c *serviceChild) failed() error { c.mu.Lock(); defer c.mu.Unlock(); return c.err }

func (s *Service) watchChild(c *serviceChild) {
	go func() {
		var status unix.WaitStatus
		var err error
		for {
			_, err = unix.Wait4(c.pid, &status, 0, nil)
			if err != unix.EINTR {
				break
			}
		}
		if err == nil && (!status.Exited() || status.ExitStatus() != 0) {
			err = fmt.Errorf("proxy candidate process exited: %v", status)
		}
		// Subreaping is scoped by the child's own process group. A dead master
		// cannot leave business workers alive while a new candidate starts.
		if killErr := unix.Kill(-c.pid, syscall.SIGKILL); killErr == nil {
			err = errors.Join(err, ErrForcedClose)
		} else if killErr != unix.ESRCH {
			err = errors.Join(err, killErr)
		}
		for {
			_, waitErr := unix.Wait4(-c.pid, nil, 0, nil)
			if waitErr == unix.EINTR {
				continue
			}
			if waitErr == unix.ECHILD {
				break
			}
			if waitErr != nil {
				err = errors.Join(err, waitErr)
				break
			}
		}
		c.mu.Lock()
		c.err = errors.Join(c.err, err)
		c.status.Ready = false
		c.status.Phase = "stopped"
		c.status.Data = "stopped"
		c.status.MMDS = "stopped"
		c.notifyLocked()
		close(c.done)
		c.mu.Unlock()
		_ = c.wire.conn.Close()
	}()
	s.readChild(c)
}

func (s *Service) readChild(c *serviceChild) {
	c.readerDone = make(chan struct{})
	go func() {
		defer close(c.readerDone)
		for {
			message, err := c.wire.read()
			if err != nil {
				c.mu.Lock()
				if !c.draining {
					c.err = errors.Join(c.err, err)
				}
				c.status.Ready = false
				if !c.draining {
					c.status.Phase = "unknown"
					c.status.Reasons = []string{"control_unavailable"}
				}
				c.notifyLocked()
				c.mu.Unlock()
				return
			}
			switch message.Kind {
			case "snapshot":
				if message.Status == nil || message.Status.BootID != c.boot || message.Status.PID != c.pid {
					c.mu.Lock()
					c.err = fmt.Errorf("proxy candidate snapshot identity mismatch")
					c.status.Ready = false
					c.notifyLocked()
					c.mu.Unlock()
					return
				}
				c.mu.Lock()
				c.status = *message.Status
				c.canShutdown = message.CanShutdown
				c.notifyLocked()
				c.mu.Unlock()
			case "error":
				c.mu.Lock()
				c.err = errors.New(message.Error)
				c.status.Ready = false
				c.notifyLocked()
				c.mu.Unlock()
			case "shutdown":
				_ = s.requestShutdown()
			case "detached":
				close(c.detached)
				return
			default:
				c.mu.Lock()
				c.err = fmt.Errorf("unexpected candidate message %q", message.Kind)
				c.status.Ready = false
				c.notifyLocked()
				c.mu.Unlock()
				return
			}
		}
	}()
}

func (s *Service) waitChildReady(ctx context.Context, c *serviceChild) error {
	for {
		c.mu.Lock()
		ready, compatible, err, changed := c.status.Ready, c.canShutdown, c.err, c.changed
		c.mu.Unlock()
		if err != nil {
			return err
		}
		if ready {
			if !compatible {
				return fmt.Errorf("%w: candidate extension completion", ErrIncompatible)
			}
			return nil
		}
		select {
		case <-changed:
		case <-c.done:
			return errors.Join(ErrNotReady, c.failed())
		case <-s.stop:
			return ErrStopped
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Service) candidateResources(boot string) (result *retainedResources, returnErr error) {
	s.mu.Lock()
	base, source := s.baseline.config, s.resources
	s.mu.Unlock()
	stats, err := candidatePath(base.StatsSocket, boot, true)
	if err != nil {
		return nil, err
	}
	shm, err := candidatePath(base.ShmPath, boot, false)
	if err != nil {
		return nil, err
	}
	metrics, err := candidateMetricsAddress(base.MetricsListen)
	if err != nil {
		return nil, err
	}
	r := &retainedResources{StatsPath: stats, ShmPath: shm, MetricsExpression: metrics, DataExpression: source.DataExpression, MMDSExpression: source.MMDSExpression}
	defer func() {
		if returnErr != nil {
			r.close()
			_ = r.removePaths()
		}
	}()
	r.Shm, err = os.OpenFile(shm, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	r.ShmIdentity, err = identifyPath(shm)
	if err != nil {
		return nil, err
	}
	listener, err := listenUnixMode(stats, true)
	if err != nil {
		return nil, err
	}
	defer listener.Close()
	listener.(*net.UnixListener).SetUnlinkOnClose(false)
	r.StatsIdentity, err = identifyPath(stats)
	if err != nil {
		return nil, err
	}
	r.Stats, err = listenerFile(listener)
	if err != nil {
		return nil, err
	}
	r.Data, err = listenresource.Dup(source.Data, "proxy-candidate-data")
	if err != nil {
		return nil, err
	}
	if source.MMDS != nil {
		r.MMDS, err = listenresource.Dup(source.MMDS, "proxy-candidate-mmds")
		if err != nil {
			return nil, err
		}
	}
	if metrics != "" {
		listener, err := net.Listen("tcp", metrics)
		if err != nil {
			return nil, err
		}
		r.Metrics, err = listenerFile(listener)
		listener.Close()
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func cleanContinuationEnvironment() []string {
	result := []string{}
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if key == continuationEnvironment || strings.HasPrefix(key, "KUASAR_INTERNAL_") || key == "LISTEN_PID" || key == "LISTEN_FDS" || key == "LISTEN_FDNAMES" {
			continue
		}
		result = append(result, entry)
	}
	return result
}

func (s *Service) envelope(mode, boot, id string, r *retainedResources) continuationEnvelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	return continuationEnvelope{Mode: mode, Input: s.input, Baseline: append([]byte(nil), s.baseline.raw...), BootID: boot, InstanceID: id, Operation: *s.operation, TargetIdentity: s.target.identity, TargetDigest: s.target.digest, DataExpression: r.DataExpression, MMDSExpression: r.MMDSExpression, StatsPath: r.StatsPath, ShmPath: r.ShmPath, StatsIdentity: r.StatsIdentity, ShmIdentity: r.ShmIdentity, MetricsExpression: r.MetricsExpression}
}

func (s *Service) startChild() (child *serviceChild, returnErr error) {
	if s.stopping() {
		return nil, ErrStopped
	}
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		return nil, err
	}
	boot := newBootID()
	r, err := s.candidateResources(boot)
	if err != nil {
		return nil, err
	}
	defer func() {
		if returnErr != nil {
			r.close()
			_ = r.removePaths()
		}
	}()
	parent, remote, err := newServicePair()
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	defer remote.Close()
	s.mu.Lock()
	target, record := s.target, s.stopRecord
	s.operation.Attempts++
	s.operation.Phase = "preparing_b"
	s.operation.NextAttempt = nil
	s.notifyLocked()
	s.mu.Unlock()
	envelope := s.envelope("candidate", boot, "proxy-"+boot, r)
	files := []*os.File{os.Stdin, os.Stdout, os.Stderr}
	add := func(file *os.File) int {
		if file == nil {
			return -1
		}
		fd := len(files)
		files = append(files, file)
		return fd
	}
	envelope.Files = continuationFiles{Data: add(r.Data), MMDS: add(r.MMDS), Stats: add(r.Stats), Shm: add(r.Shm), Metrics: add(r.Metrics), CandidateMetrics: -1, Stop: add(record.file), Target: add(target.file), Control: add(remote)}
	bootstrap, err := encodeContinuation(envelope)
	if err != nil {
		return nil, err
	}
	defer bootstrap.Close()
	bootstrapFD := add(bootstrap)
	wire, err := serviceConnection(parent, boot, envelope.Operation.ID)
	if err != nil {
		return nil, err
	}
	defer func() {
		if returnErr != nil {
			wire.conn.Close()
		}
	}()
	// The child path resolves after descriptor remapping and names exactly the
	// already-opened target, even if the deployment path changes meanwhile.
	process, err := os.StartProcess("/proc/self/fd/"+strconv.Itoa(envelope.Files.Target), []string{s.input.Executable}, &os.ProcAttr{Files: files, Env: append(cleanContinuationEnvironment(), continuationEnvironment+"="+strconv.Itoa(bootstrapFD)), Sys: &syscall.SysProcAttr{Setpgid: true}})
	if err != nil {
		return nil, err
	}
	child = &serviceChild{pid: process.Pid, boot: boot, wire: wire, resources: r, status: InstanceStatus{InstanceID: envelope.InstanceID, BootID: boot, PID: process.Pid, Phase: "starting"}, done: make(chan struct{}), changed: make(chan struct{}), detached: make(chan struct{})}
	_ = process.Release()
	s.mu.Lock()
	s.child = child
	s.notifyLocked()
	s.mu.Unlock()
	s.watchChild(child)
	return child, nil
}

func (s *Service) reapChild(c *serviceChild, deadline time.Time, hold bool) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	c.draining = true
	c.mu.Unlock()
	_ = c.wire.send(serviceMessage{Kind: "drain", Deadline: deadline, HoldMMDS: hold})
	timer := time.NewTimer(max(time.Until(deadline), 0))
	defer timer.Stop()
	forced := false
	select {
	case <-c.done:
	case <-timer.C:
		forced = true
		if err := unix.Kill(-c.pid, syscall.SIGKILL); err != nil && err != unix.ESRCH {
			return errors.Join(ErrForcedClose, err)
		}
		<-c.done
	}
	_ = c.wire.conn.Close()
	<-c.readerDone
	err := c.failed()
	if forced {
		err = errors.Join(err, ErrForcedClose)
	}
	if err != nil {
		err = errors.Join(ErrForcedClose, err)
	}
	if c.resources != nil {
		c.resources.close()
		if cleanup := c.resources.removePaths(); cleanup != nil {
			return errors.Join(err, cleanup)
		}
	}
	s.mu.Lock()
	if s.child == c {
		s.child = nil
		s.notifyLocked()
	}
	s.mu.Unlock()
	return err
}
