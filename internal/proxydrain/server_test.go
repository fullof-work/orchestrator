package proxydrain

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/http2"
)

func testServer(t *testing.T, handler http.Handler) (*Server, string, <-chan error) {
	t.Helper()
	ready := make(chan struct{})
	server, err := New(context.Background(), handler, nil, func() { close(ready) })
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	t.Cleanup(func() { server.ForceClose() })
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("accept loop not entered")
	}
	return server, "http://" + listener.Addr().String(), serveDone
}

func waitDraining(t *testing.T, s *Server) {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		s.mu.Lock()
		draining, changed := s.draining, s.changed
		s.mu.Unlock()
		if draining {
			return
		}
		select {
		case <-changed:
		case <-timer.C:
			t.Fatal("drain did not start")
		}
	}
}

func TestDrainWaitsForCompleteStreamingResponse(t *testing.T) {
	for _, protocol := range []string{"http1", "h2c"} {
		t.Run(protocol, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			payload := strings.Repeat("complete-response-", 4096)
			server, address, serveDone := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.WriteString(w, "start:")
				http.NewResponseController(w).Flush()
				close(entered)
				<-release
				io.WriteString(w, payload)
			}))
			var transport http.RoundTripper
			if protocol == "h2c" {
				transport = &http2.Transport{AllowHTTP: true, DialTLSContext: func(ctx context.Context, network, address string, _ *tls.Config) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, network, address)
				}}
			} else {
				transport = &http.Transport{DisableKeepAlives: true}
			}
			client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
			response, err := client.Get(address)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			<-entered
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			drained := make(chan error, 1)
			go func() { drained <- server.Drain(ctx) }()
			waitDraining(t, server)
			select {
			case err := <-serveDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("Serve did not stop accepting")
			}
			select {
			case err := <-drained:
				t.Fatalf("drain finished before response: %v", err)
			default:
			}
			close(release)
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if string(body) != "start:"+payload {
				t.Fatalf("response truncated: %d bytes", len(body))
			}
			if err := <-drained; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDrainWaitsForHijackedTunnelAndPreservesSharedListener(t *testing.T) {
	entered := make(chan struct{})
	server, err := New(context.Background(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, rw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		io.WriteString(rw, "HTTP/1.1 200 Connection Established\r\n\r\n")
		rw.Flush()
		close(entered)
		io.Copy(conn, conn)
	}), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	file, err := owner.File()
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	copy, err := net.FileListener(file)
	if err != nil {
		t.Fatal(err)
	}
	go server.Serve(copy)
	defer server.ForceClose()
	conn, err := net.Dial("tcp", owner.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	io.WriteString(conn, "CONNECT example:80 HTTP/1.1\r\nHost: example\r\n\r\n")
	reader := bufio.NewReader(conn)
	if _, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect}); err != nil {
		t.Fatal(err)
	}
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Drain(ctx) }()
	waitDraining(t, server)
	io.WriteString(conn, "still-alive")
	buffer := make([]byte, len("still-alive"))
	if _, err := io.ReadFull(reader, buffer); err != nil {
		t.Fatal(err)
	}
	if string(buffer) != "still-alive" {
		t.Fatal("tunnel corrupted")
	}
	select {
	case err := <-done:
		t.Fatalf("hijack escaped drain: %v", err)
	default:
	}
	conn.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// Closing a process-local copy must not shutdown the shared socket.
	client, err := net.Dial("tcp", owner.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	owner.SetDeadline(time.Now().Add(time.Second))
	accepted, err := owner.Accept()
	if err != nil {
		t.Fatal(err)
	}
	accepted.Close()
}

func TestForcedCloseAndCallerCancellationDoNotResetDrain(t *testing.T) {
	entered := make(chan struct{})
	server, address, _ := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done() }))
	requestDone := make(chan error, 1)
	go func() {
		response, err := (&http.Client{Timeout: 3 * time.Second}).Get(address)
		if response != nil {
			response.Body.Close()
		}
		requestDone <- err
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	first := make(chan error, 1)
	go func() { first <- server.Drain(ctx) }()
	waitDraining(t, server)
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("caller result=%v", err)
	}
	start := time.Now()
	if err := server.Drain(context.Background()); !errors.Is(err, ErrForcedClose) {
		t.Fatalf("final result=%v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("drain deadline was extended")
	}
	if err := <-requestDone; err == nil {
		t.Fatal("unanswered request unexpectedly succeeded")
	}
}
