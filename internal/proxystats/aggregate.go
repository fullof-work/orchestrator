package proxystats

import (
	"maps"
	"math"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
)

// AggregateBatches combines exact-instance observations of one ordered query.
// Every necessary source must be present. Missing/contradictory state is never
// treated as idle, and an idle boundary is the latest proven boundary across
// every generation. Connector counters are collected separately by conductor.
func AggregateBatches(batches [][]BatchResult) ([]BatchResult, error) {
	if len(batches) == 0 || len(batches[0]) == 0 {
		return nil, api.ErrStatsUnavailable
	}
	result := make([]BatchResult, len(batches[0]))
	for index, item := range batches[0] {
		if item.Stats == nil {
			return nil, api.ErrStatsUnavailable
		}
		copy := *item.Stats
		copy.Services = maps.Clone(item.Stats.Services)
		result[index] = BatchResult{SandboxID: item.SandboxID, Stats: &copy}
	}
	for _, batch := range batches[1:] {
		if len(batch) != len(result) {
			return nil, api.ErrStatsUnavailable
		}
		for index, item := range batch {
			a, b := result[index].Stats, item.Stats
			if b == nil {
				return nil, api.ErrStatsUnavailable
			}
			if item.SandboxID != result[index].SandboxID || a.State != b.State || a.MaxInflight != b.MaxInflight {
				return nil, api.ErrStatsConflict
			}
			if len(a.Services) != len(b.Services) {
				return nil, api.ErrStatsUnavailable
			}
			if !addInflight(&a.Inflight.Parking, b.Inflight.Parking) || !addInflight(&a.Inflight.Connected, b.Inflight.Connected) {
				return nil, api.ErrStatsUnavailable
			}
			a.IdleSince = conservativeIdle(a.IdleSince, b.IdleSince, a.Inflight.Parking == 0 && a.Inflight.Connected == 0)
			for name, service := range a.Services {
				other, ok := b.Services[name]
				if !ok {
					return nil, api.ErrStatsUnavailable
				}
				if !addInflight(&service.Parking, other.Parking) || !addInflight(&service.Connected, other.Connected) {
					return nil, api.ErrStatsUnavailable
				}
				service.IdleSince = conservativeIdle(service.IdleSince, other.IdleSince, service.Parking == 0 && service.Connected == 0)
				a.Services[name] = service
			}
		}
	}
	return result, nil
}

func addInflight(total *uint64, value uint64) bool {
	if value > math.MaxUint64-*total {
		return false
	}
	*total += value
	return true
}

func conservativeIdle(a, b *time.Time, idle bool) *time.Time {
	if !idle || a == nil || b == nil {
		return nil
	}
	if b.After(*a) {
		copy := *b
		return &copy
	}
	copy := *a
	return &copy
}
