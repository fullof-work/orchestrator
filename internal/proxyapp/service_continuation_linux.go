package proxyapp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/componentexec"
	"github.com/kuasar-sandbox/orchestrator/internal/listenresource"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"golang.org/x/sys/unix"
)

func (s *Service) restore(c *Continuation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := c.envelope
	s.input = e.Input
	s.operation = &e.Operation
	baseline, err := effectiveConfigFromRaw(e.Baseline, sha256.Sum256(e.Baseline))
	if err != nil {
		return err
	}
	s.baseline = baseline
	s.timeout = baseline.config.ShutdownTimeoutDur()
	s.candidate = e.Mode == "candidate"
	control := c.take(e.Files.Control)
	if control == nil {
		return fmt.Errorf("proxy continuation control missing")
	}
	defer control.Close()
	boot := e.BootID
	if !s.candidate {
		boot = e.CandidateBootID
	}
	wire, err := serviceConnection(control, boot, e.Operation.ID)
	if err != nil {
		return err
	}
	if s.candidate {
		s.parent = wire
	} else {
		if e.CandidatePID <= 1 || e.CandidatePID == os.Getpid() {
			wire.conn.Close()
			return fmt.Errorf("proxy continuation child identity invalid")
		}
		child := &serviceChild{pid: e.CandidatePID, boot: boot, wire: wire, resources: &retainedResources{StatsPath: e.CandidateStatsPath, ShmPath: e.CandidateShmPath, StatsIdentity: e.CandidateStatsIdentity, ShmIdentity: e.CandidateShmIdentity, Metrics: c.take(e.Files.CandidateMetrics), MetricsExpression: e.CandidateMetricsExpression}, status: InstanceStatus{PID: e.CandidatePID, BootID: boot, Phase: "starting"}, done: make(chan struct{}), changed: make(chan struct{}), detached: make(chan struct{})}
		s.child = child
		// Restore supervision before validating or preparing business resources.
		s.watchChild(child)
		if err := wire.send(serviceMessage{Kind: "resume"}); err != nil {
			child.mu.Lock()
			child.err = err
			child.mu.Unlock()
		}
	}
	recordFile := c.take(e.Files.Stop)
	if recordFile == nil {
		return fmt.Errorf("proxy continuation stop record missing")
	}
	record, err := receiveStopRecord(recordFile)
	if err != nil {
		recordFile.Close()
		return err
	}
	s.stopRecord = record
	s.resources = &retainedResources{Data: c.take(e.Files.Data), MMDS: c.take(e.Files.MMDS), Stats: c.take(e.Files.Stats), Shm: c.take(e.Files.Shm), Metrics: c.take(e.Files.Metrics), DataExpression: e.DataExpression, MMDSExpression: e.MMDSExpression, StatsPath: e.StatsPath, ShmPath: e.ShmPath, StatsIdentity: e.StatsIdentity, ShmIdentity: e.ShmIdentity, MetricsExpression: e.MetricsExpression}
	target := c.take(e.Files.Target)
	if target == nil {
		return fmt.Errorf("proxy continuation target missing")
	}
	s.target = &restartTarget{file: target, path: e.Input.Executable, identity: e.TargetIdentity, digest: e.TargetDigest}
	if err := componentexec.VerifyCurrentExecutable(e.TargetIdentity); err != nil {
		return err
	}
	stat, err := target.Stat()
	if err != nil {
		return err
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, io.NewSectionReader(target, 0, stat.Size())); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != e.TargetDigest {
		return fmt.Errorf("%w: target contents changed after acceptance", ErrIncompatible)
	}
	if _, err := listenresource.TCP(s.resources.Data); err != nil {
		return err
	}
	if s.resources.MMDS != nil {
		if _, err := listenresource.TCP(s.resources.MMDS); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) runCandidate(ctx context.Context) error {
	defer s.parent.conn.Close()
	deadline, err := s.stopRecord.read()
	if err != nil {
		return err
	}
	if !deadline.IsZero() {
		return nil
	}
	boot := s.parent.boot
	if err := s.startLocal(ctx, boot, "proxy-"+boot, s.resources); err != nil {
		_ = s.parent.send(serviceMessage{Kind: "error", Error: err.Error()})
		return err
	}
	messages := make(chan serviceMessage, 1)
	connectionError := make(chan error, 1)
	readerCtx, cancelReader := context.WithCancel(context.Background())
	defer cancelReader()
	go func() {
		for {
			message, err := s.parent.read()
			if err != nil {
				connectionError <- err
				return
			}
			select {
			case messages <- message:
			case <-readerCtx.Done():
				return
			}
		}
	}()
	var gate chan struct{}
	paused := false
	sendSnapshot := func() error {
		status := s.local.Snapshot()
		compatible := s.local.extensionCompatible()
		if s.localExtension != nil {
			_, ok := s.localExtension.(proxyextension.Shutdown)
			compatible = compatible && ok
		}
		return s.parent.send(serviceMessage{Kind: "snapshot", Status: &status, CanShutdown: compatible})
	}
	ctxDone := ctx.Done()
	for {
		s.local.mu.Lock()
		changed := s.local.changed
		s.local.mu.Unlock()
		if paused {
			changed = nil
		} else if err := sendSnapshot(); err != nil {
			s.local.requestStop(time.Now().Add(s.timeout))
		}
		select {
		case <-changed:
		case err := <-s.localDone:
			_ = sendSnapshot()
			if err != nil {
				_ = s.parent.send(serviceMessage{Kind: "error", Error: err.Error()})
			}
			return err
		case err := <-connectionError:
			s.local.requestStop(time.Now().Add(s.timeout))
			if gate != nil {
				close(gate)
				gate = nil
			}
			return errors.Join(err, <-s.localDone)
		case <-ctxDone:
			ctxDone = nil
			_ = s.requestShutdown()
		case message := <-messages:
			switch message.Kind {
			case "drain":
				if message.Deadline.IsZero() {
					return fmt.Errorf("candidate drain deadline missing")
				}
				if message.HoldMMDS {
					s.stopOnce.Do(func() { close(s.stop) })
				}
				if message.HoldMMDS && gate == nil {
					gate = make(chan struct{})
					s.local.mu.Lock()
					s.local.mmdsGate = gate
					s.local.mu.Unlock()
				}
				s.local.requestStop(message.Deadline)
			case "release_mmds":
				if gate != nil {
					close(gate)
					gate = nil
				}
			case "detach":
				paused = true
				if err := s.parent.send(serviceMessage{Kind: "detached"}); err != nil {
					s.local.requestStop(time.Now().Add(s.timeout))
				}
			case "resume":
				paused = false
				if err := sendSnapshot(); err != nil {
					s.local.requestStop(time.Now().Add(s.timeout))
				}
			case "service": // Coordinator updates preserve whole-service /status on B.
				if message.Service != nil {
					snapshot := *message.Service
					s.mu.Lock()
					s.knownService = &snapshot
					s.mu.Unlock()
				}
			default:
				s.local.requestStop(time.Now().Add(s.timeout))
			}
		}
	}
}

func (s *Service) execTarget() error {
	s.mu.Lock()
	child, r, target, record := s.child, s.resources, s.target, s.stopRecord
	s.mu.Unlock()
	if child == nil {
		return fmt.Errorf("proxy exec requires a supervised replacement")
	}
	if s.stopping() {
		return ErrStopped
	}
	child.detached = make(chan struct{})
	if err := child.wire.send(serviceMessage{Kind: "detach"}); err != nil {
		return err
	}
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-child.detached:
		<-child.readerDone
	case <-child.done:
		return errors.Join(ErrNotReady, child.failed())
	case <-s.stop:
		return ErrStopped
	case <-timer.C:
		return fmt.Errorf("proxy candidate detach timed out")
	}
	// From here B sends no notifications until the new image restores control.
	defer func() { s.readChild(child); _ = child.wire.send(serviceMessage{Kind: "resume"}) }()
	envelope := s.envelope("resume", newBootID(), routesync.ProxyPluginID, r)
	envelope.CandidatePID = child.pid
	envelope.CandidateBootID = child.boot
	envelope.CandidateStatsPath = child.resources.StatsPath
	envelope.CandidateShmPath = child.resources.ShmPath
	envelope.CandidateStatsIdentity = child.resources.StatsIdentity
	envelope.CandidateShmIdentity = child.resources.ShmIdentity
	envelope.CandidateMetricsExpression = child.resources.MetricsExpression
	control, err := listenresource.Dup(child.wire.conn, "proxy-service-control")
	if err != nil {
		return err
	}
	defer control.Close()
	// Keep current descriptor numbers and toggle CLOEXEC on private copies only;
	// no bootstrap, log descriptor or other runtime-owned file is overwritten.
	var inherited []*os.File
	defer func() { closeFiles(inherited) }()
	var duplicateErr error
	add := func(file *os.File) int {
		if file == nil {
			return -1
		}
		copy, err := listenresource.Dup(file, file.Name())
		if err != nil {
			duplicateErr = errors.Join(duplicateErr, err)
			return -1
		}
		inherited = append(inherited, copy)
		return int(copy.Fd())
	}
	envelope.Files = continuationFiles{Data: add(r.Data), MMDS: add(r.MMDS), Stats: add(r.Stats), Shm: add(r.Shm), Metrics: add(r.Metrics), Stop: add(record.file), Target: add(target.file), Control: add(control), CandidateMetrics: add(child.resources.Metrics)}
	if duplicateErr != nil {
		return duplicateErr
	}
	bootstrap, err := encodeContinuation(envelope)
	if err != nil {
		return err
	}
	inherited = append(inherited, bootstrap)
	for _, file := range inherited {
		if _, err := unix.FcntlInt(file.Fd(), unix.F_SETFD, 0); err != nil {
			return err
		}
	}
	s.actionMu.Lock()
	defer s.actionMu.Unlock()
	deadline, err := record.read()
	if err != nil {
		return err
	}
	if !deadline.IsZero() {
		return ErrStopped
	}
	releaseSignal, err := holdRestartSignal(s.input.Signals && s.baseline.config.InplaceRestart)
	if err != nil {
		return err
	}
	defer releaseSignal()
	err = unix.Exec("/proc/self/fd/"+strconv.Itoa(envelope.Files.Target), []string{s.input.Executable}, append(cleanContinuationEnvironment(), continuationEnvironment+"="+strconv.Itoa(int(bootstrap.Fd()))))
	// exec errors keep the old coordinator responsible for B and for retrying.
	return err
}
