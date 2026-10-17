// Package proxydrain owns one Proxy ingress server and its transport lifetime.
package proxydrain

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// ErrForcedClose identifies a drain that exhausted its original deadline.
var ErrForcedClose = errors.New("proxy drain required forced connection close")

// Server tracks physical connections even after HTTP hijacking. Serve returning
// is distinct from Drain completing: h2c, WebSocket and CONNECT may outlive Serve.
// It must not be copied or reused after Drain. The caller owns upstream resources.
type Server struct {
	http        *http.Server
	tls         bool
	mu          sync.Mutex
	connections map[*trackedConn]struct{}
	changed     chan struct{}
	draining    bool
	handlers    int
	ready       func()
	acceptOnce  sync.Once
	drainOnce   sync.Once
	done        chan struct{}
	drainErr    error
}

// New constructs a server without starting I/O. ready is called once when Serve
// enters its accept loop. It reports this listener only, not route or App ready.
// ctx belongs to the instance's dependencies and stays live during normal drain.
func New(ctx context.Context, handler http.Handler, tlsConfig *tls.Config, ready func()) (*Server, error) {
	s := &Server{connections: make(map[*trackedConn]struct{}), changed: make(chan struct{}), done: make(chan struct{}), ready: ready, tls: tlsConfig != nil}
	s.http = &http.Server{ReadHeaderTimeout: 10 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	if tlsConfig != nil {
		s.http.TLSConfig = tlsConfig.Clone()
	}
	// ConfigureServer registers the same HTTP/2 connection set for TLS and h2c.
	// Its shutdown hook sends GOAWAY to both; net/http alone does not track h2c.
	h2 := &http2.Server{}
	if err := http2.ConfigureServer(s.http, h2); err != nil {
		return nil, err
	}
	guard := s.guard(handler)
	s.http.Handler = guard
	if tlsConfig == nil {
		s.http.Handler = h2c.NewHandler(guard, h2)
	}
	return s, nil
}

// NewHTTP1 preserves the supplied HTTP/1 server limits and protocols. Ownership
// of server transfers to the returned drain controller before Serve is called.
func NewHTTP1(server *http.Server, ready func()) *Server {
	s := &Server{http: server, connections: make(map[*trackedConn]struct{}), changed: make(chan struct{}), done: make(chan struct{}), ready: ready}
	server.Handler = s.guard(server.Handler)
	return s
}

func (s *Server) guard(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		if s.draining {
			s.mu.Unlock()
			w.Header().Set("Connection", "close")
			http.Error(w, "proxy is draining", http.StatusServiceUnavailable)
			return
		}
		s.handlers++
		s.mu.Unlock()
		defer func() { s.mu.Lock(); s.handlers--; s.notifyLocked(); s.mu.Unlock() }()
		handler.ServeHTTP(w, r)
	})
}

// Serve consumes this instance's listener copy. It never calls socket shutdown.
func (s *Server) Serve(listener net.Listener) error {
	tracked := &trackedListener{Listener: listener, server: s}
	var err error
	if s.tls {
		err = s.http.ServeTLS(tracked, "", "")
	} else {
		err = s.http.Serve(tracked)
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Drain refuses new business, including requests on existing keep-alive/h2
// connections, and waits for responses and hijacked connections. The first call
// fixes the work deadline. Later contexts only bound their caller's wait; they
// never cancel or extend the underlying operation. It is unsafe for a handler to
// synchronously wait for its own server's Drain; initiate service Shutdown from
// a separate goroutine or use a bounded caller wait instead.
func (s *Server) Drain(ctx context.Context) error {
	if ctx == nil {
		return errors.New("proxy drain context is nil")
	}
	s.drainOnce.Do(func() {
		s.mu.Lock()
		s.draining = true
		s.notifyLocked()
		s.mu.Unlock()
		// Preserve the first deadline without inheriting caller cancellation.
		work := context.Background()
		cancel := func() {}
		if deadline, ok := ctx.Deadline(); ok {
			work, cancel = context.WithDeadline(work, deadline)
		}
		go func() {
			defer cancel()
			defer close(s.done)
			err := s.http.Shutdown(work)
			for err == nil {
				s.mu.Lock()
				complete := len(s.connections) == 0 && s.handlers == 0
				changed := s.changed
				s.mu.Unlock()
				if complete {
					break
				}
				select {
				case <-changed:
				case <-work.Done():
					err = work.Err()
				}
			}
			if err != nil {
				s.ForceClose()
				s.drainErr = errors.Join(ErrForcedClose, err)
			}
		}()
	})
	select {
	case <-s.done:
		return s.drainErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ForceClose closes only this server's accepted connections, including hijacks.
// The owning worker still must be reaped before releasing its runtime mappings.
func (s *Server) ForceClose() {
	s.mu.Lock()
	s.draining = true
	s.notifyLocked()
	s.mu.Unlock()
	_ = s.http.Close()
	s.mu.Lock()
	connections := make([]*trackedConn, 0, len(s.connections))
	for conn := range s.connections {
		connections = append(connections, conn)
	}
	s.mu.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
	}
}

func (s *Server) notifyLocked() { close(s.changed); s.changed = make(chan struct{}) }

type trackedListener struct {
	net.Listener
	server *Server
}

func (l *trackedListener) Accept() (net.Conn, error) {
	l.server.acceptOnce.Do(func() {
		if l.server.ready != nil {
			l.server.ready()
		}
	})
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	wrapped := &trackedConn{Conn: conn, server: l.server}
	l.server.mu.Lock()
	if l.server.draining {
		l.server.mu.Unlock()
		_ = conn.Close()
		return nil, net.ErrClosed
	}
	l.server.connections[wrapped] = struct{}{}
	l.server.notifyLocked()
	l.server.mu.Unlock()
	return wrapped, nil
}

type trackedConn struct {
	net.Conn
	server *Server
	once   sync.Once
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() {
		c.server.mu.Lock()
		delete(c.server.connections, c)
		c.server.notifyLocked()
		c.server.mu.Unlock()
	})
	return err
}

// Preserve TCP half-close for CONNECT; closing writes does not end the tunnel.
func (c *trackedConn) CloseWrite() error {
	if conn, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return conn.CloseWrite()
	}
	return nil
}
