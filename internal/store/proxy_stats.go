package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/proxycompletion"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// LoadProxyStats restores unfinished business obligations across conductor
// restarts. A kernel boot change proves that none of the old processes survive.
// Fully retired services can be discarded on conductor startup: old registration
// streams have ended and their owners have already joined/reaped every process.
func (s *Store) LoadProxyStats(ctx context.Context, kernelBoot string) ([]routesync.ProxyStatsRecord, error) {
	if kernelBoot == "" {
		return nil, fmt.Errorf("proxy stats kernel boot identity is empty")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `DELETE FROM proxy_stats_instances WHERE kernel_boot != ?`, kernelBoot); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT record_json FROM proxy_stats_instances ORDER BY service_id, slot`)
	if err != nil {
		return nil, err
	}
	var result []routesync.ProxyStatsRecord
	for rows.Next() {
		var raw string
		var record routesync.ProxyStatsRecord
		if err = rows.Scan(&raw); err != nil {
			rows.Close()
			return nil, err
		}
		if err = json.Unmarshal([]byte(raw), &record); err != nil {
			rows.Close()
			return nil, err
		}
		if err = record.Identity.Validate(); err != nil {
			rows.Close()
			return nil, err
		}
		result = append(result, record)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	finished := map[string]bool{}
	for _, record := range result {
		service := record.Identity.ServiceID
		if _, found := finished[service]; !found {
			finished[service] = true
		}
		finished[service] = finished[service] && record.Retired && record.ServiceCompleted
	}
	kept := result[:0]
	for _, record := range result {
		if finished[record.Identity.ServiceID] && proxycompletion.Remove(record.Identity) == nil {
			if _, err = tx.ExecContext(ctx, `DELETE FROM proxy_stats_instances WHERE service_id = ? AND slot = ?`, record.Identity.ServiceID, record.Identity.Slot); err != nil {
				return nil, err
			}
		} else {
			kept = append(kept, record)
		}
	}
	result = kept
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

// SaveProxyStats is called under the Registry's ownership lock, before a lease
// is admitted or a retirement is acknowledged. There are at most two rows per
// service, independent of the number of successful or failed restart attempts.
func (s *Store) SaveProxyStats(ctx context.Context, kernelBoot string, record routesync.ProxyStatsRecord) error {
	if err := record.Identity.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO proxy_stats_instances (service_id, slot, kernel_boot, retired, record_json) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(service_id, slot) DO UPDATE SET kernel_boot=excluded.kernel_boot, retired=excluded.retired, record_json=excluded.record_json`, record.Identity.ServiceID, record.Identity.Slot, kernelBoot, record.Retired, string(raw))
	return err
}

// Called only after every slot has durable completion and the exact receipt
// object was removed. Late registrations must then fail their file validation.
func (s *Store) DeleteCompletedProxyStats(ctx context.Context, service string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM proxy_stats_instances WHERE service_id = ?`, service)
	return err
}
