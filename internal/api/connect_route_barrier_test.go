package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestStandaloneConnectMapsProxyUnavailableWithAndWithoutMigrationToken(t *testing.T) {
	for _, test := range []struct {
		name  string
		token string
	}{
		{name: "existing target"},
		{name: "migration token", token: "kmt1.opaque"},
	} {
		t.Run(test.name, func(t *testing.T) {
			called := false
			core := &connectMMDSCoreStub{
				connectMMDS: func(_ context.Context, id, _ string, token string, _ ConnectOptions, _ map[string]string, _ *string) (*types.Sandbox, error) {
					called = true
					if id != "s1" || token != test.token {
						t.Fatalf("ConnectWithMMDS id=%q token=%q", id, token)
					}
					return nil, errors.Join(ErrProxyUnavailable, errors.New("private route session detail"))
				},
			}
			handler, apiKey := newSandboxContractHandler(t, core, Resources{})
			request := httptest.NewRequest(http.MethodPost, "/sandboxes/s1/connect", strings.NewReader(`{}`))
			request.Header.Set("X-API-KEY", apiKey)
			if test.token != "" {
				request.Header.Set(MigrationTokenHeader, test.token)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if !called {
				t.Fatal("ConnectWithMMDS was not called")
			}
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("status=%d, want 503; body=%s", response.Code, response.Body.String())
			}
			if got := response.Body.String(); got != "{\"message\":\"proxy temporarily unavailable\"}\n" {
				t.Fatalf("public response=%q", got)
			}
		})
	}
}
