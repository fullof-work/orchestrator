package proxystats

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
)

func TestAggregateInstancesRetainsOldWorkAndConservativeIdle(t *testing.T) {
	old := time.Unix(100, 0)
	recent := time.Unix(200, 0)
	batch := func(n uint64, idle *time.Time) []BatchResult {
		return []BatchResult{{SandboxID: "sandbox", Stats: &api.TrafficStats{
			State: "running", Inflight: api.TrafficInflight{Connected: n}, IdleSince: idle,
			Services: map[string]api.ServiceTrafficStats{"exec": {Connected: n, IdleSince: idle}},
		}}}
	}
	first, second := batch(2, nil), batch(3, nil)
	got, err := AggregateBatches([][]BatchResult{first, second})
	if err != nil || got[0].Stats.Inflight.Connected != 5 || got[0].Stats.Services["exec"].Connected != 5 || got[0].Stats.IdleSince != nil {
		t.Fatalf("result=%+v err=%v", got, err)
	}
	if first[0].Stats.Services["exec"].Connected != 2 {
		t.Fatal("aggregation mutated instance snapshot")
	}
	got, err = AggregateBatches([][]BatchResult{batch(0, &old), batch(0, &recent)})
	if err != nil || got[0].Stats.IdleSince == nil || !got[0].Stats.IdleSince.Equal(recent) {
		t.Fatalf("idle=%+v err=%v", got, err)
	}
	got, err = AggregateBatches([][]BatchResult{batch(0, &old), batch(0, nil)})
	if err != nil || got[0].Stats.IdleSince != nil {
		t.Fatal("unknown idle was converted into known idle")
	}
	if _, err := AggregateBatches([][]BatchResult{batch(math.MaxUint64, nil), batch(1, nil)}); !errors.Is(err, api.ErrStatsUnavailable) {
		t.Fatalf("overflow=%v", err)
	}
	second = batch(0, &recent)
	second[0].Stats.State = "paused"
	if _, err := AggregateBatches([][]BatchResult{batch(0, &old), second}); !errors.Is(err, api.ErrStatsConflict) {
		t.Fatalf("conflict=%v", err)
	}
	if _, err := AggregateBatches([][]BatchResult{batch(0, &old), {{SandboxID: "sandbox"}}}); !errors.Is(err, api.ErrStatsUnavailable) {
		t.Fatalf("missing=%v", err)
	}
}
