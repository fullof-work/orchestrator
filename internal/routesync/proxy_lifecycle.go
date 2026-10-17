package routesync

import (
	"encoding/hex"
	"fmt"
	"math"
)

const ProxyRetirePath = "/internal/plugin/proxy-instance/retire"

// ProxyInstance binds a business lifetime independently of its reconnecting
// registration lease. Generations are assigned by the service, never operators.
// The two slots describe ownership, not plugin admission or routing roles.
type ProxyInstance struct {
	ServiceID      string          `json:"service_id"`
	BootID         string          `json:"boot_id"`
	Slot           string          `json:"slot"`
	Generation     uint64          `json:"generation"`
	CoordinatorPID int             `json:"coordinator_pid"`
	Completion     ProxyCompletion `json:"completion"`
}

// ProxyCompletion binds the final whole-service receipt to the admitted owner.
// The private signing key is never part of a registration or a B bootstrap.
type ProxyCompletion struct {
	Path       string `json:"path"`
	PublicKey  string `json:"public_key"`
	KernelBoot string `json:"kernel_boot"`
	OwnerStart uint64 `json:"owner_start"`
	Device     uint64 `json:"device"`
	Inode      uint64 `json:"inode"`
}

func (p ProxyInstance) Key() string { return p.ServiceID + "/" + p.Slot }

func (p ProxyInstance) Validate() error {
	for _, id := range []string{p.ServiceID, p.BootID} {
		decoded, err := hex.DecodeString(id)
		if err != nil || len(decoded) != 16 || hex.EncodeToString(decoded) != id {
			return fmt.Errorf("invalid proxy lifetime identity")
		}
	}
	if (p.Slot != "coordinator" && p.Slot != "candidate") || p.Generation == 0 || p.Generation > math.MaxInt64 || p.CoordinatorPID <= 1 {
		return fmt.Errorf("invalid proxy lifetime ownership")
	}
	return nil
}

// ProxyStatsRecord retains an obligation until the owner proves that all of its
// business processes have ended. Retired generation floors reject delayed old
// registrations without retaining a row for every InplaceRestart operation.
type ProxyStatsRecord struct {
	Identity         ProxyInstance `json:"identity"`
	InstanceID       string        `json:"instance_id"`
	PeerPID          int           `json:"peer_pid"`
	PeerStart        uint64        `json:"peer_start"`
	CoordinatorStart uint64        `json:"coordinator_start"`
	Socket           string        `json:"socket"`
	Retired          bool          `json:"retired"`
	ServiceCompleted bool          `json:"service_completed"`
}
