package jaccl

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tmc/gojaccl/internal/ipc"
)

func TestConfigFromEnv(t *testing.T) {
	t.Run("JACCLValues", func(t *testing.T) {
		path := writeDevices(t, fakeConfig(0, 2).Devices)
		t.Setenv("JACCL_RANK", "1")
		t.Setenv("JACCL_COORDINATOR", "host:1234")
		t.Setenv("JACCL_IBV_DEVICES", path)
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Rank != 1 || cfg.Coordinator != "host:1234" || len(cfg.Devices) != 2 {
			t.Fatalf("ConfigFromEnv = %+v", cfg)
		}
	})
	t.Run("MLXFallbackValues", func(t *testing.T) {
		path := writeDevices(t, fakeConfig(0, 2).Devices)
		t.Setenv("MLX_RANK", "0")
		t.Setenv("MLX_JACCL_COORDINATOR", "mlx:1234")
		t.Setenv("MLX_IBV_DEVICES", path)
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Rank != 0 || cfg.Coordinator != "mlx:1234" {
			t.Fatalf("ConfigFromEnv = %+v", cfg)
		}
	})
	t.Run("JACCLOverridesMLXFallback", func(t *testing.T) {
		path := writeDevices(t, fakeConfig(0, 2).Devices)
		t.Setenv("JACCL_RANK", "1")
		t.Setenv("MLX_RANK", "0")
		t.Setenv("JACCL_COORDINATOR", "jaccl:1")
		t.Setenv("MLX_JACCL_COORDINATOR", "mlx:1")
		t.Setenv("JACCL_IBV_DEVICES", path)
		t.Setenv("MLX_IBV_DEVICES", "/bad")
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Rank != 1 || cfg.Coordinator != "jaccl:1" {
			t.Fatalf("ConfigFromEnv = %+v", cfg)
		}
	})
	t.Run("MissingRank", func(t *testing.T) {
		t.Setenv("JACCL_COORDINATOR", "host:1")
		t.Setenv("JACCL_IBV_DEVICES", writeDevices(t, fakeConfig(0, 2).Devices))
		if _, err := ConfigFromEnv(); err == nil {
			t.Fatal("ConfigFromEnv missing rank = nil")
		}
	})
	t.Run("InvalidRank", func(t *testing.T) {
		t.Setenv("JACCL_RANK", "x")
		t.Setenv("JACCL_COORDINATOR", "host:1")
		t.Setenv("JACCL_IBV_DEVICES", writeDevices(t, fakeConfig(0, 2).Devices))
		if _, err := ConfigFromEnv(); err == nil {
			t.Fatal("ConfigFromEnv invalid rank = nil")
		}
	})
	t.Run("MissingCoordinator", func(t *testing.T) {
		t.Setenv("JACCL_RANK", "0")
		t.Setenv("JACCL_IBV_DEVICES", writeDevices(t, fakeConfig(0, 2).Devices))
		if _, err := ConfigFromEnv(); err == nil {
			t.Fatal("ConfigFromEnv missing coordinator = nil")
		}
	})
	t.Run("MissingDevices", func(t *testing.T) {
		t.Setenv("JACCL_RANK", "0")
		t.Setenv("JACCL_COORDINATOR", "host:1")
		if _, err := ConfigFromEnv(); err == nil {
			t.Fatal("ConfigFromEnv missing devices = nil")
		}
	})
	t.Run("InvalidDevicesJSON", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "devices.json")
		if err := os.WriteFile(path, []byte("{"), 0o666); err != nil {
			t.Fatal(err)
		}
		t.Setenv("JACCL_RANK", "0")
		t.Setenv("JACCL_COORDINATOR", "host:1")
		t.Setenv("JACCL_IBV_DEVICES", path)
		if _, err := ConfigFromEnv(); err == nil {
			t.Fatal("ConfigFromEnv invalid JSON = nil")
		}
	})
	t.Run("PreferRingTrue", func(t *testing.T) {
		path := writeDevices(t, fakeConfig(0, 2).Devices)
		t.Setenv("JACCL_RANK", "0")
		t.Setenv("JACCL_COORDINATOR", "host:1")
		t.Setenv("JACCL_IBV_DEVICES", path)
		t.Setenv("JACCL_RING", "yes")
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if !cfg.PreferRing {
			t.Fatal("PreferRing = false, want true")
		}
	})
	t.Run("PreferRingFalse", func(t *testing.T) {
		path := writeDevices(t, fakeConfig(0, 2).Devices)
		t.Setenv("JACCL_RANK", "0")
		t.Setenv("JACCL_COORDINATOR", "host:1")
		t.Setenv("JACCL_IBV_DEVICES", path)
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.PreferRing {
			t.Fatal("PreferRing = true, want false")
		}
	})
	t.Run("BackendAndDaemonSocket", func(t *testing.T) {
		path := writeDevices(t, fakeConfig(0, 2).Devices)
		t.Setenv("JACCL_RANK", "0")
		t.Setenv("JACCL_COORDINATOR", "host:1")
		t.Setenv("JACCL_IBV_DEVICES", path)
		t.Setenv("JACCL_BACKEND", BackendDaemon)
		t.Setenv("JACCL_DAEMON_SOCKET", "/tmp/custom-jaccld.sock")
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Backend != BackendDaemon || cfg.DaemonSocket != "/tmp/custom-jaccld.sock" {
			t.Fatalf("backend/socket = %q/%q", cfg.Backend, cfg.DaemonSocket)
		}
	})
	t.Run("DaemonSocketDefault", func(t *testing.T) {
		path := writeDevices(t, fakeConfig(0, 2).Devices)
		t.Setenv("JACCL_RANK", "0")
		t.Setenv("JACCL_COORDINATOR", "host:1")
		t.Setenv("JACCL_IBV_DEVICES", path)
		t.Setenv("JACCL_BACKEND", BackendDaemon)
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.DaemonSocket != ipc.DefaultSocket {
			t.Fatalf("DaemonSocket = %q, want default", cfg.DaemonSocket)
		}
	})
	t.Run("DaemonBackendWithoutDirectTopology", func(t *testing.T) {
		t.Setenv("JACCL_RANK", "1")
		t.Setenv("JACCL_SIZE", "2")
		t.Setenv("JACCL_BACKEND", BackendDaemon)
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Rank != 1 || cfg.Size != 2 || len(cfg.Devices) != 0 || cfg.Coordinator != "" {
			t.Fatalf("daemon config = %+v", cfg)
		}
		if cfg.DaemonSocket != ipc.DefaultSocket {
			t.Fatalf("DaemonSocket = %q, want default", cfg.DaemonSocket)
		}
	})
	t.Run("DaemonBackendMLXSizeFallback", func(t *testing.T) {
		t.Setenv("JACCL_RANK", "0")
		t.Setenv("MLX_WORLD_SIZE", "3")
		t.Setenv("JACCL_BACKEND", BackendDaemon)
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Size != 3 {
			t.Fatalf("Size = %d, want 3", cfg.Size)
		}
	})
	t.Run("NoMLXBackendFallback", func(t *testing.T) {
		path := writeDevices(t, fakeConfig(0, 2).Devices)
		t.Setenv("MLX_RANK", "0")
		t.Setenv("MLX_JACCL_COORDINATOR", "mlx:1234")
		t.Setenv("MLX_IBV_DEVICES", path)
		t.Setenv("MLX_BACKEND", BackendDaemon)
		t.Setenv("MLX_DAEMON_SOCKET", "/tmp/mlx.sock")
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Backend != "" || cfg.DaemonSocket != "" {
			t.Fatalf("MLX backend fallback set backend/socket = %q/%q", cfg.Backend, cfg.DaemonSocket)
		}
	})
}

func TestConfigValidate(t *testing.T) {
	t.Run("TwoRankMesh", func(t *testing.T) {
		if err := fakeConfig(0, 2).validate(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("TwoRankRing", func(t *testing.T) {
		cfg := fakeConfig(0, 2)
		cfg.PreferRing = true
		if err := cfg.validate(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("ThreeRankMesh", func(t *testing.T) {
		if err := fakeConfig(2, 3).validate(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("ThreeRankLine", func(t *testing.T) {
		cfg := fakeConfig(1, 3)
		cfg.Devices = lineDeviceMatrix("left", "right")
		cfg.PreferRing = true
		if err := cfg.validate(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("FourRankConnectedPartial", func(t *testing.T) {
		cfg := fakeConfig(3, 4)
		cfg.Devices = fakePartialMatrix()
		cfg.PreferRing = true
		if err := cfg.validate(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("NegativeRank", func(t *testing.T) {
		cfg := fakeConfig(0, 2)
		cfg.Rank = -1
		if err := cfg.validate(); err == nil {
			t.Fatal("validate negative rank = nil")
		}
	})
	t.Run("RankOutOfBounds", func(t *testing.T) {
		cfg := fakeConfig(0, 2)
		cfg.Rank = 2
		if err := cfg.validate(); err == nil {
			t.Fatal("validate rank out of bounds = nil")
		}
	})
	t.Run("EmptyCoordinator", func(t *testing.T) {
		cfg := fakeConfig(0, 2)
		cfg.Coordinator = ""
		if err := cfg.validate(); err == nil {
			t.Fatal("validate empty coordinator = nil")
		}
	})
	t.Run("EmptyDeviceMatrix", func(t *testing.T) {
		cfg := fakeConfig(0, 2)
		cfg.Devices = nil
		if err := cfg.validate(); err == nil {
			t.Fatal("validate empty device matrix = nil")
		}
	})
	t.Run("NonSquareDeviceMatrix", func(t *testing.T) {
		cfg := fakeConfig(0, 2)
		cfg.Devices = append(cfg.Devices, [][]string{{}, {}, {}})
		if err := cfg.validate(); err == nil {
			t.Fatal("validate non-square = nil")
		}
	})
	t.Run("RaggedDeviceMatrix", func(t *testing.T) {
		cfg := fakeConfig(0, 3)
		cfg.Devices[1] = cfg.Devices[1][:2]
		if err := cfg.validate(); err == nil {
			t.Fatal("validate ragged = nil")
		}
	})
	t.Run("NoUsableTopology", func(t *testing.T) {
		cfg := fakeConfig(0, 2)
		cfg.Devices[0][1] = []string{}
		cfg.Devices[1][0] = []string{}
		if err := cfg.validate(); err == nil {
			t.Fatal("validate no topology = nil")
		}
	})
	t.Run("InvalidBackend", func(t *testing.T) {
		cfg := fakeConfig(0, 2)
		cfg.Backend = "magic"
		if err := cfg.validate(); err == nil {
			t.Fatal("validate invalid backend = nil")
		}
	})
	t.Run("DaemonSizeOnly", func(t *testing.T) {
		cfg := Config{Rank: 1, Size: 2, Backend: BackendDaemon}
		if err := cfg.validate(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("DaemonSizeRequiredWithoutDevices", func(t *testing.T) {
		cfg := Config{Rank: 0, Backend: BackendDaemon}
		if err := cfg.validate(); err == nil {
			t.Fatal("validate daemon without size = nil")
		}
	})
	t.Run("DaemonRankOutOfBoundsForSize", func(t *testing.T) {
		cfg := Config{Rank: 2, Size: 2, Backend: BackendDaemon}
		if err := cfg.validate(); err == nil {
			t.Fatal("validate daemon rank out of bounds = nil")
		}
	})
	t.Run("SizeMismatch", func(t *testing.T) {
		cfg := fakeConfig(0, 2)
		cfg.Size = 3
		if err := cfg.validate(); err == nil {
			t.Fatal("validate size mismatch = nil")
		}
	})
	t.Run("BackendModes", func(t *testing.T) {
		for _, backend := range []string{"", BackendAuto, BackendDirect, BackendDaemon, " DAEMON "} {
			cfg := fakeConfig(0, 2)
			cfg.Backend = backend
			if err := cfg.validate(); err != nil {
				t.Fatalf("validate backend %q: %v", backend, err)
			}
		}
	})
}
