package taskartifact

import (
	"context"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	storepb "github.com/kuasar-sandbox/accelerator/pkg/store/pb"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestSamePrepareRecoversRealManifestRPCs(t *testing.T) {
	for _, mode := range []types.LaunchMode{types.LaunchCold, types.LaunchMemory} {
		for _, existing := range []bool{false, true} {
			name := string(mode) + "/first-open"
			if existing {
				name = string(mode) + "/existing-connection-closed"
			}
			t.Run(name, func(t *testing.T) {
				var armed atomic.Bool
				var gets, failures atomic.Int32
				var connections sync.Map
				var firstPeer string
				var peerMu sync.Mutex
				intercept := func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
					if armed.Load() && info.FullMethod == "/cac.store.v1.Store/Get" {
						n := gets.Add(1)
						p, _ := peer.FromContext(stream.Context())
						if existing && n <= 2 {
							peerMu.Lock()
							if n == 1 {
								firstPeer = p.Addr.String()
							} else if firstPeer != p.Addr.String() {
								t.Error("second Get did not reuse the established connection")
							}
							peerMu.Unlock()
						}
						start := int32(1)
						if existing {
							start = 2
						}
						if n >= start && n < start+3 {
							failures.Add(1)
							// An actual gRPC stream sends a partial object before
							// an Unavailable stream status; consumer recovery owns replay.
							if err := stream.SendMsg(&storepb.GetResponse{Data: []byte("partial")}); err != nil {
								return err
							}
							if existing {
								conn, ok := connections.Load(p.Addr.String())
								if !ok {
									t.Error("active connection identity missing")
								} else {
									_ = conn.(net.Conn).Close()
								}
							}
							return status.Error(codes.Unavailable, "test-owned dependency interruption")
						}
					}
					return handler(srv, stream)
				}
				e, s, cfg := writeTaskManifestStoreArtifactsWithListener(t, "resources:\n  capacity: {cpu: 2, memory: 256MiB}\nboot: {}\n", func(l net.Listener) net.Listener {
					return &observedStoreListener{Listener: l, connections: &connections}
				}, grpc.StreamInterceptor(intercept))
				body, err := os.ReadFile(cfg)
				if err != nil {
					t.Fatal(err)
				}
				// Force one client connection so the successful Get and precise
				// disconnect cannot be serviced by two different pool members.
				if err := os.WriteFile(cfg, []byte(strings.Replace(string(body), "pool: 2", "pool: 1", 1)), 0600); err != nil {
					t.Fatal(err)
				}
				ref, kind := e, types.ResumeSourceSandbox
				if mode == types.LaunchMemory {
					ref, kind = s, types.ResumeSourceSnapshot
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				armed.Store(true)
				got, err := Prepare(ctx, configsock.ArtifactPrepareSpec{
					RootSourceKind: string(kind), LaunchMode: string(mode), RootRef: ref, ManifestConfig: cfg, MaxRefs: 16,
				})
				if err != nil {
					t.Fatal(err)
				}
				if failures.Load() != 3 || got.PreparedSource.Ref != ref || got.Summary.Capacity.CPU != 2 {
					t.Fatalf("failures=%d gets=%d result=%+v", failures.Load(), gets.Load(), got)
				}
			})
		}
	}
}

type observedStoreListener struct {
	net.Listener
	connections *sync.Map
}

func (l *observedStoreListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	wrapped := &observedStoreConn{Conn: c, connections: l.connections}
	l.connections.Store(c.RemoteAddr().String(), wrapped)
	return wrapped, nil
}

type observedStoreConn struct {
	net.Conn
	connections *sync.Map
}

func (c *observedStoreConn) Close() error {
	c.connections.Delete(c.RemoteAddr().String())
	return c.Conn.Close()
}
