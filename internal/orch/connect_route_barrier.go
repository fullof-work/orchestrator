package orch

import (
	"context"
	"errors"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
)

// waitConnectRouteBarrier keeps a standalone Connect response behind the route
// updates its admission already published. The proxy can then park the first
// request on Starting instead of dialing the previous running incarnation.
// Accepted launch work remains independent of this request and its barrier.
// Cluster command rejection has ownership rollback semantics and does not use it.
func (o *Orchestrator) waitConnectRouteBarrier(ctx context.Context, sid string) (retErr error) {
	started := time.Now()
	defer func() { o.logProxyRouteBarrier(sid, retErr, time.Since(started)) }()
	if o.routeBarriers == nil {
		return errors.Join(api.ErrProxyUnavailable, configsock.ErrProxyRouteUnavailable)
	}
	barrier, err := o.routeBarriers.BeginProxyRouteBarrier()
	if err != nil {
		return errors.Join(api.ErrProxyUnavailable, err)
	}
	defer barrier.Cancel()

	lifecycleCtx := o.launchContext()
	barrierCtx, cancel := context.WithTimeout(ctx, o.cfg.ParkTimeoutDur())
	stopLifecycle := context.AfterFunc(lifecycleCtx, cancel)
	defer func() {
		stopLifecycle()
		cancel()
	}()
	if err := barrierCtx.Err(); err != nil {
		return errors.Join(api.ErrProxyUnavailable, err)
	}
	if err := lifecycleCtx.Err(); err != nil {
		return errors.Join(api.ErrProxyUnavailable, err)
	}
	o.publishRouteBarrier(barrier.ID())
	if err = barrier.Wait(barrierCtx); err == nil {
		err = barrier.Commit()
	}
	if err == nil {
		err = barrierCtx.Err()
	}
	if err == nil {
		err = lifecycleCtx.Err()
	}
	if err != nil {
		return errors.Join(api.ErrProxyUnavailable, err)
	}
	return nil
}
