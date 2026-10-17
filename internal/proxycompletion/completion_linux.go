// Package proxycompletion preserves proof of a completed service when its
// conductor is temporarily unavailable. It does not infer completion from a
// missing PID, lost registration or an inaccessible statistics socket.
package proxycompletion

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"golang.org/x/sys/unix"
)

const prefix = ".proxy-service-completion-"

type Receipt struct {
	Identity  routesync.ProxyInstance `json:"identity"`
	Signature []byte                  `json:"signature"`
}

func payload(identity routesync.ProxyInstance) []byte {
	// Completion covers the whole service, including both slots and every
	// generation. It is signed only after all business processes were joined.
	identity.BootID, identity.Slot, identity.Generation = "", "", 0
	raw, _ := json.Marshal(identity)
	return append([]byte("kuasar-proxy-service-completed-v1\x00"), raw...)
}

func Create(statsPath string, identity routesync.ProxyInstance) (routesync.ProxyCompletion, ed25519.PrivateKey, error) {
	var result routesync.ProxyCompletion
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return result, nil, err
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return result, nil, err
	}
	stat, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return result, nil, err
	}
	fields := strings.Fields(string(stat[strings.LastIndexByte(string(stat), ')')+1:]))
	if len(fields) < 20 {
		return result, nil, fmt.Errorf("incomplete coordinator identity")
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return result, nil, err
	}
	parent := filepath.Dir(statsPath)
	if err := os.MkdirAll(parent, 0755); err != nil {
		return result, nil, err
	}
	path := filepath.Join(parent, prefix+identity.ServiceID)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return result, nil, err
	}
	defer file.Close()
	var info unix.Stat_t
	if err = unix.Fstat(int(file.Fd()), &info); err == nil {
		err = file.Sync()
	}
	if err == nil {
		directory, openErr := os.Open(parent)
		if openErr != nil {
			err = openErr
		} else {
			err = directory.Sync()
			directory.Close()
		}
	}
	if err != nil {
		os.Remove(path)
		return result, nil, err
	}
	result = routesync.ProxyCompletion{Path: path, PublicKey: hex.EncodeToString(public), KernelBoot: strings.TrimSpace(string(boot)), OwnerStart: start, Device: uint64(info.Dev), Inode: info.Ino}
	return result, private, nil
}

func open(binding routesync.ProxyCompletion, flags int) (*os.File, error) {
	if !filepath.IsAbs(binding.Path) || filepath.Base(binding.Path) == "." || binding.Inode == 0 {
		return nil, fmt.Errorf("invalid completion object")
	}
	fd, err := unix.Open(binding.Path, flags|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), binding.Path)
	var info unix.Stat_t
	if err = unix.Fstat(fd, &info); err != nil || info.Mode&unix.S_IFMT != unix.S_IFREG || info.Mode&0777 != 0600 || uint64(info.Dev) != binding.Device || info.Ino != binding.Inode || info.Nlink != 1 {
		file.Close()
		return nil, fmt.Errorf("completion object identity changed")
	}
	return file, nil
}

// ValidatePending is called before admitting the registration, so a late
// registration cannot revive a service whose completed object was removed.
func ValidatePending(identity routesync.ProxyInstance) error {
	if identity.Completion.Path == "" {
		return nil
	} // older optional identity
	binding := identity.Completion
	key, err := hex.DecodeString(binding.PublicKey)
	if err != nil || len(key) != ed25519.PublicKeySize || binding.OwnerStart == 0 || binding.KernelBoot == "" || filepath.Base(binding.Path) != prefix+identity.ServiceID {
		return fmt.Errorf("invalid completion binding")
	}
	file, err := open(binding, unix.O_RDONLY)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() != 0 {
		return fmt.Errorf("service already completed or receipt unavailable")
	}
	return nil
}

func Complete(identity routesync.ProxyInstance, private ed25519.PrivateKey) error {
	if identity.Completion.Path == "" {
		return nil
	}
	if len(private) != ed25519.PrivateKeySize || hex.EncodeToString(private.Public().(ed25519.PublicKey)) != identity.Completion.PublicKey {
		return fmt.Errorf("completion signing identity mismatch")
	}
	raw, err := json.Marshal(Receipt{Identity: identity, Signature: ed25519.Sign(private, payload(identity))})
	if err != nil {
		return err
	}
	file, err := open(identity.Completion, unix.O_WRONLY)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err = file.WriteAt(raw, 0); err != nil {
		return err
	}
	if err = file.Truncate(int64(len(raw))); err != nil {
		return err
	}
	return file.Sync()
}

func Verify(identity routesync.ProxyInstance) (bool, error) {
	if identity.Completion.Path == "" {
		return false, nil
	}
	file, err := open(identity.Completion, unix.O_RDONLY)
	if err != nil {
		return false, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 8193))
	if err != nil {
		return false, err
	}
	if len(raw) == 0 {
		return false, nil
	}
	var receipt Receipt
	if len(raw) > 8192 || json.Unmarshal(raw, &receipt) != nil || string(payload(receipt.Identity)) != string(payload(identity)) {
		return false, fmt.Errorf("completion receipt identity mismatch")
	}
	key, err := hex.DecodeString(identity.Completion.PublicKey)
	if err != nil || len(key) != ed25519.PublicKeySize || !ed25519.Verify(key, payload(identity), receipt.Signature) {
		return false, fmt.Errorf("completion signature mismatch")
	}
	return true, nil
}

// Remove is used only after durable retirement. File names include the random
// ServiceID and are never reused by a later service.
func Remove(identity routesync.ProxyInstance) error {
	if identity.Completion.Path == "" {
		return nil
	}
	file, err := open(identity.Completion, unix.O_RDONLY)
	if os.IsNotExist(err) || err == unix.ENOENT {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	return os.Remove(identity.Completion.Path)
}

// Pending returns only complete, signed receipts in this service's configured
// stats directory. An incomplete record remains untouched. A later admitted
// process can relay completion without possessing the old private signing key.
func Pending(statsPath string) []routesync.ProxyInstance {
	paths, _ := filepath.Glob(filepath.Join(filepath.Dir(statsPath), prefix+"*"))
	var result []routesync.ProxyInstance
	for _, path := range paths {
		fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
		if err != nil {
			continue
		}
		file := os.NewFile(uintptr(fd), path)
		var info unix.Stat_t
		if unix.Fstat(fd, &info) != nil || info.Mode&unix.S_IFMT != unix.S_IFREG || info.Size > 8192 {
			file.Close()
			continue
		}
		raw, err := io.ReadAll(io.LimitReader(file, 8193))
		file.Close()
		var receipt Receipt
		if err != nil || json.Unmarshal(raw, &receipt) != nil || receipt.Identity.Completion.Path != path || receipt.Identity.Validate() != nil {
			continue
		}
		if complete, err := Verify(receipt.Identity); err == nil && complete {
			result = append(result, receipt.Identity)
		}
	}
	return result
}
