package proxydrain

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// Hold Serve between Accept and publishing the connection to net/http. This
// reproduces the boundary that previously discarded an accepted first request.
func TestDrainCompletesAlreadyAcceptedFirstRequest(t *testing.T) {
	server, err := New(context.Background(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "complete") }), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	previous := server.http.ConnContext
	server.http.ConnContext = func(ctx context.Context, conn net.Conn) context.Context {
		close(entered)
		<-release
		return previous(ctx, conn)
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 2 * time.Second}
	result := make(chan error, 1)
	go func() {
		response, err := client.Get("http://" + listener.Addr().String())
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != 200 || string(body) != "complete" {
				err = io.ErrUnexpectedEOF
			} else {
				err = readErr
			}
		}
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("no accepted connection")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	drained := make(chan error, 1)
	go func() { drained <- server.Drain(ctx) }()
	for {
		server.mu.Lock()
		stopping, changed := server.draining, server.changed
		server.mu.Unlock()
		if stopping {
			break
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatalf("accepted request lost: %v", err)
	}
	if err := <-drained; err != nil {
		t.Fatal(err)
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
}
