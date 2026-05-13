package jaccl

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/tmc/gojaccl/internal/topology"
)

// Config describes the local rank and RDMA connectivity for a group.
type Config struct {
	Rank        int
	Coordinator string
	Devices     [][][]string
	PreferRing  bool
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

	coord, ok := getenv("JACCL_COORDINATOR", "MLX_JACCL_COORDINATOR")
	if !ok {
		return Config{}, fmt.Errorf("coordinator: missing JACCL_COORDINATOR or MLX_JACCL_COORDINATOR")
	}
	cfg.Coordinator = coord

	path, ok := getenv("JACCL_IBV_DEVICES", "MLX_IBV_DEVICES")
	if !ok {
		return Config{}, fmt.Errorf("devices: missing JACCL_IBV_DEVICES or MLX_IBV_DEVICES")
	}
	devices, err := readDeviceMatrix(path)
	if err != nil {
		return Config{}, err
	}
	cfg.Devices = devices

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
	if strings.TrimSpace(c.Coordinator) == "" {
		return fmt.Errorf("coordinator is empty")
	}
	if err := topology.ValidateDeviceMatrix(c.Devices); err != nil {
		return err
	}
	if c.Rank >= len(c.Devices) {
		return fmt.Errorf("rank %d out of range for size %d", c.Rank, len(c.Devices))
	}
	if _, err := topology.Choose(c.Devices, c.PreferRing); err != nil {
		return err
	}
	return nil
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
