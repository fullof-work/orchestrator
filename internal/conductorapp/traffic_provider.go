package conductorapp

import (
	"context"
	"slices"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// ExternalTrafficProvider reads traffic snapshots from the currently
// registered independent proxy instances, including generations draining data.
type ExternalTrafficProvider struct {
	Plugins *configsock.Registry
}

func (p *ExternalTrafficProvider) SandboxTrafficStats(ctx context.Context, sandboxID, runID string, profile types.Profile, state types.State) (*api.TrafficStats, error) {
	results, err := p.SandboxTrafficStatsBatch(ctx, []proxystats.TrafficQuery{{
		SandboxID: sandboxID, RunID: runID, Profile: profile, State: state,
	}})
	if err != nil {
		return nil, err
	}
	return results[0].Stats, nil
}

func (p *ExternalTrafficProvider) SandboxTrafficStatsBatch(ctx context.Context, queries []proxystats.TrafficQuery) ([]proxystats.BatchResult, error) {
	if p == nil || p.Plugins == nil {
		return nil, api.ErrStatsUnavailable
	}
	targets, found := p.Plugins.ProxyStatsTargets()
	if !found {
		return nil, api.ErrStatsUnavailable
	}
	batches := make([][]proxystats.BatchResult, len(targets))
	for index, target := range targets {
		var err error
		batches[index], err = proxystats.QueryInstanceSocket(ctx, target.Socket, target.BootID, queries)
		if err != nil {
			return nil, err
		}
	}
	current, ok := p.Plugins.ProxyStatsTargets()
	if !ok || !slices.Equal(current, targets) {
		return nil, api.ErrStatsUnavailable
	}
	return proxystats.AggregateBatches(batches)
}
