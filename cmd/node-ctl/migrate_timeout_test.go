package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExportSandboxRequestTimeout(t *testing.T) {
	t.Setenv("E2B_API_KEY", "test-owner")
	for _, value := range []string{"0", "-1s", "invalid"} {
		t.Run("invalid_"+value, func(t *testing.T) {
			err := exportSandboxCmd([]string{"source", "--timeout", value}, nil)
			if err == nil || !strings.Contains(err.Error(), "timeout") {
				t.Fatalf("invalid timeout: %v", err)
			}
		})
	}
	t.Run("deadline_cancels_request", func(t *testing.T) {
		socket := filepath.Join(t.TempDir(), "ctl.sock")
		listener, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		canceled := make(chan struct{})
		server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.Copy(io.Discard, r.Body)
			<-r.Context().Done()
			close(canceled)
		})}
		go server.Serve(listener)
		t.Cleanup(func() { server.Close() })
		err = exportSandboxCmd([]string{"source", "--socket", socket, "--timeout", "100ms"}, nil)
		if err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
			t.Fatalf("deadline did not stop export: %v", err)
		}
		select {
		case <-canceled:
		case <-time.After(3 * time.Second):
			t.Fatal("deadline did not cancel the server request")
		}
	})
	t.Run("longer_budget_allows_response", func(t *testing.T) {
		socket := filepath.Join(t.TempDir(), "ctl.sock")
		listener, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(150 * time.Millisecond):
			}
			io.WriteString(w, fmt.Sprintf(`{"result":"kmt1.test","sandboxRef":"manifest://%s","removedRefs":[]}`, strings.Repeat("b", 64)))
		})}
		go server.Serve(listener)
		t.Cleanup(func() { server.Close() })
		if err := exportSandboxCmd([]string{"source", "--socket", socket, "--timeout", "3s"}, nil); err != nil {
			t.Fatal(err)
		}
	})
}
