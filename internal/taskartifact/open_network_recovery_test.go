package taskartifact

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	storepb "github.com/kuasar-sandbox/accelerator/pkg/store/pb"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSamePrepareRecoversRealManifestRPCs(t *testing.T) {
	for _, mode := range []types.LaunchMode{types.LaunchCold, types.LaunchMemory} {
		for _, existing := range []bool{false, true} {
			name := string(mode) + "/first-open"
			if existing {
				name = string(mode) + "/existing-connection"
			}
			t.Run(name, func(t *testing.T) {
				var armed atomic.Bool
				var gets, failures atomic.Int32
				intercept := func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
					if armed.Load() && info.FullMethod == "/cac.store.v1.Store/Get" {
						n := gets.Add(1)
						start := int32(1)
						if existing {
							start = 2
						}
						if n >= start && n < start+3 {
							failures.Add(1)
							// An actual gRPC stream sends a partial object before
							// its transport fails; consumer recovery owns replay.
							if err := stream.SendMsg(&storepb.GetResponse{Data: []byte("partial")}); err != nil {
								return err
							}
							return status.Error(codes.Unavailable, "test-owned dependency interruption")
						}
					}
					return handler(srv, stream)
				}
				e, s, cfg := writeTaskManifestStoreArtifacts(t, "resources:\n  capacity: {cpu: 2, memory: 256MiB}\nboot: {}\n", grpc.StreamInterceptor(intercept))
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
