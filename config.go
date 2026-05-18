package jaccl

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/tmc/gojaccl/internal/ipc"
	"github.com/tmc/gojaccl/internal/topology"
)

const (
	// BackendAuto selects the working backend for the current implementation.
	BackendAuto = "auto"

	// BackendDirect selects the in-process RDMA backend.
	BackendDirect = "direct"

	// BackendDaemon selects the jaccld IPC backend.
	BackendDaemon = "daemon"
)

// Config describes the local rank and RDMA connectivity for a group.
type Config struct {
	// Rank is this process's zero-based rank.
	Rank int
	// Size is the number of ranks. It is required for the daemon backend when
	// Devices is empty.
	Size int
	// Coordinator is the rank-zero TCP side-channel address, host:port.
	Coordinator string
	// Devices is indexed as [src][dst][wire] and names RDMA device paths.
	Devices [][][]string
	// PreferRing asks NewGroup to choose ring when the matrix is valid for it.
	PreferRing bool
	// Backend selects "auto", "direct", or "daemon". Empty means "auto".
	Backend string
	// DaemonSocket is the Unix-domain socket path for BackendDaemon.
	DaemonSocket string
}

// ConfigFromEnv reads the JACCL configuration environment, using MLX fallbacks.
func ConfigFromEnv() (Config, error) {
	var cfg Config

	rankText, ok := getenv("JACCL_RANK", "MLX_RANK")
	if !ok {
		return Config{}, fmt.Errorf("rank: missing JACCL_RANK or MLX_RANK")
	}
	rank, err := strconv.Atoi(rankText)
	if err != nil {
		return Config{}, fmt.Errorf("rank: parse %q: %w", rankText, err)
	}
	cfg.Rank = rank

	if backend, ok := os.LookupEnv("JACCL_BACKEND"); ok {
		cfg.Backend = backend
	}
	if socket, ok := os.LookupEnv("JACCL_DAEMON_SOCKET"); ok {
		cfg.DaemonSocket = socket
	}
	if cfg.DaemonSocket == "" && cfg.backendMode() == BackendDaemon {
		cfg.DaemonSocket = ipc.DefaultSocket
	}
	if sizeText, ok := getenvAny("JACCL_SIZE", "MLX_WORLD_SIZE", "MLX_SIZE"); ok {
		size, err := strconv.Atoi(sizeText)
		if err != nil {
			return Config{}, fmt.Errorf("size: parse %q: %w", sizeText, err)
		}
		cfg.Size = size
	}
	if coord, ok := getenv("JACCL_COORDINATOR", "MLX_JACCL_COORDINATOR"); ok {
		cfg.Coordinator = coord
	}
	if path, ok := getenv("JACCL_IBV_DEVICES", "MLX_IBV_DEVICES"); ok {
		devices, err := readDeviceMatrix(path)
		if err != nil {
			return Config{}, err
		}
		cfg.Devices = devices
	}
	if ring, ok := getenv("JACCL_RING", "MLX_JACCL_RING"); ok {
		prefer, err := parseBool(ring)
		if err != nil {
			return Config{}, fmt.Errorf("ring: parse %q: %w", ring, err)
		}
		cfg.PreferRing = prefer
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate() error {
	if c.Rank < 0 {
		return fmt.Errorf("rank %d out of range", c.Rank)
	}
	switch c.backendMode() {
	case BackendAuto, BackendDirect:
		return c.validateDirect()
	case BackendDaemon:
		return c.validateDaemon()
	default:
		return fmt.Errorf("backend %q is invalid", c.Backend)
	}
}

func (c Config) validateDirect() error {
	if strings.TrimSpace(c.Coordinator) == "" {
		return fmt.Errorf("coordinator is empty")
	}
	if err := topology.ValidateDeviceMatrix(c.Devices); err != nil {
		return err
	}
	if c.Size > 0 && c.Size != len(c.Devices) {
		return fmt.Errorf("size %d does not match device matrix size %d", c.Size, len(c.Devices))
	}
	if c.Rank >= len(c.Devices) {
		return fmt.Errorf("rank %d out of range for size %d", c.Rank, len(c.Devices))
	}
	if _, err := topology.Choose(c.Devices, c.PreferRing); err != nil {
		return err
	}
	return nil
}

func (c Config) validateDaemon() error {
	size, err := c.groupSize()
	if err != nil {
		return err
	}
	if c.Rank >= size {
		return fmt.Errorf("rank %d out of range for size %d", c.Rank, size)
	}
	return nil
}

func (c Config) groupSize() (int, error) {
	if len(c.Devices) > 0 {
		if err := topology.ValidateDeviceMatrix(c.Devices); err != nil {
			return 0, err
		}
		if c.Size > 0 && c.Size != len(c.Devices) {
			return 0, fmt.Errorf("size %d does not match device matrix size %d", c.Size, len(c.Devices))
		}
		return len(c.Devices), nil
	}
	if c.Size <= 0 {
		return 0, fmt.Errorf("size %d is not positive", c.Size)
	}
	return c.Size, nil
}

func (c Config) backendMode() string {
	switch strings.ToLower(strings.TrimSpace(c.Backend)) {
	case "", BackendAuto:
		return BackendAuto
	case BackendDirect:
		return BackendDirect
	case BackendDaemon:
		return BackendDaemon
	default:
		return strings.ToLower(strings.TrimSpace(c.Backend))
	}
}

func (c Config) daemonSocket() string {
	if socket := strings.TrimSpace(c.DaemonSocket); socket != "" {
		return socket
	}
	return ipc.DefaultSocket
}

func readDeviceMatrix(path string) ([][][]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("devices: read %s: %w", path, err)
	}
	var matrix [][][]string
	if err := json.Unmarshal(data, &matrix); err != nil {
		return nil, fmt.Errorf("devices: parse %s: %w", path, err)
	}
	return matrix, nil
}

func getenv(primary, fallback string) (string, bool) {
	if v, ok := os.LookupEnv(primary); ok {
		return v, true
	}
	return os.LookupEnv(fallback)
}

func getenvAny(names ...string) (string, bool) {
	for _, name := range names {
		if v, ok := os.LookupEnv(name); ok {
			return v, true
		}
	}
	return "", false
}

func parseBool(s string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "t", "true", "y", "yes", "on":
		return true, nil
	case "0", "f", "false", "n", "no", "off", "":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean")
	}
}
