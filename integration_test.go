package jaccl

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tmc/gojaccl/internal/rdma"
	"github.com/tmc/gojaccl/internal/topology"
)

func requireRDMA(t *testing.T) string {
	t.Helper()
	if os.Getenv("JACCL_TEST_RDMA") != "1" {
		t.Skip("set JACCL_TEST_RDMA=1 to run RDMA integration tests")
	}
	if !Available() {
		t.Skip("RDMA backend is unavailable")
	}
	if name := os.Getenv("JACCL_TEST_RDMA_DEVICE"); name != "" {
		return name
	}
	names, err := rdma.DeviceNames()
	if err != nil {
		t.Fatalf("list RDMA devices: %v", err)
	}
	if len(names) == 0 {
		t.Skip("no RDMA devices")
	}
	return names[len(names)-1]
}

func TestIntegrationChild(t *testing.T) {
	if os.Getenv("JACCL_TEST_RDMA_CHILD") != "1" {
		t.Skip("integration child helper")
	}
	requireRTRAllowed(t)
	rank := mustAtoi(t, os.Getenv("JACCL_TEST_RANK"))
	size := mustAtoi(t, os.Getenv("JACCL_TEST_SIZE"))
	device := os.Getenv("JACCL_TEST_RDMA_DEVICE")
	coordinator := os.Getenv("JACCL_TEST_COORDINATOR")
	preferRing := os.Getenv("JACCL_TEST_PREFER_RING") == "1"
	op := os.Getenv("JACCL_TEST_OP")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	backend := os.Getenv("JACCL_BACKEND")
	socket := os.Getenv("JACCL_DAEMON_SOCKET")
	cfg, err := integrationConfig(rank, size, device, coordinator, preferRing)
	if err != nil {
		t.Fatal(err)
	}
	summary, err := topology.Summarize(cfg.Devices, cfg.PreferRing)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(os.Stderr, "rank %d: new group op=%s device=%s devices=%s coordinator=%s backend=%s socket=%s\n", rank, op, device, integrationDeviceMatrixPath(), coordinator, backend, socket)
	fmt.Fprintf(os.Stderr, "rank %d: topology=%s ranks=%d directed_edges=%d empty_edges=%d total_wires=%d max_wires=%d matrix_sha256=%s\n", rank, summary.Topology, summary.Ranks, summary.DirectedEdges, summary.EmptyEdges, summary.TotalWires, summary.MaxWires, summary.MatrixSHA256)
	g, err := NewGroup(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	fmt.Fprintf(os.Stderr, "rank %d: group ready\n", rank)

	switch op {
	case "barrier-sum":
		if err := g.Barrier(ctx); err != nil {
			t.Fatal(err)
		}
		dst := []int32{0}
		if err := AllSum(ctx, g, dst, []int32{int32(rank + 1)}); err != nil {
			t.Fatal(err)
		}
		want := int32(size * (size + 1) / 2)
		if dst[0] != want {
			t.Fatalf("allsum = %d, want %d", dst[0], want)
		}
	case "allmax":
		dst := []int32{0}
		if err := AllMax(ctx, g, dst, []int32{int32(rank + 1)}); err != nil {
			t.Fatal(err)
		}
		if dst[0] != int32(size) {
			t.Fatalf("allmax = %d, want %d", dst[0], size)
		}
	case "allmin":
		dst := []int32{0}
		if err := AllMin(ctx, g, dst, []int32{int32(rank + 1)}); err != nil {
			t.Fatal(err)
		}
		if dst[0] != 1 {
			t.Fatalf("allmin = %d, want 1", dst[0])
		}
	case "allgather":
		dst := make([]int32, size)
		if err := AllGather(ctx, g, dst, []int32{int32(rank + 1)}); err != nil {
			t.Fatal(err)
		}
		for i, v := range dst {
			if v != int32(i+1) {
				t.Fatalf("allgather[%d] = %d, want %d", i, v, i+1)
			}
		}
	case "sendrecv":
		if rank == 0 {
			if err := g.Send(ctx, 1, []byte("hello")); err != nil {
				t.Fatal(err)
			}
			if err := g.Recv(ctx, 1, nil); err != nil {
				t.Fatal(err)
			}
			return
		}
		if rank == 1 {
			buf := make([]byte, 5)
			if err := g.Recv(ctx, 0, buf); err != nil {
				t.Fatal(err)
			}
			if string(buf) != "hello" {
				t.Fatalf("recv = %q, want hello", buf)
			}
			if err := g.Send(ctx, 0, nil); err != nil {
				t.Fatal(err)
			}
			return
		}
		t.Fatalf("sendrecv supports two ranks, got rank %d", rank)
	case "barrier":
		if err := g.Barrier(ctx); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown integration op %q", op)
	}
	fmt.Fprintf(os.Stderr, "rank %d: %s done\n", rank, op)
}

func TestIntegrationTwoRankBarrierAndAllSum(t *testing.T) {
	device := requireRDMA(t)
	requireRDMAReadyToRun(t)
	runIntegrationCase(t, 2, device, false, "barrier-sum")
}

func TestIntegrationTwoRankAllMax(t *testing.T) {
	device := requireRDMA(t)
	requireRDMAReadyToRun(t)
	runIntegrationCase(t, 2, device, false, "allmax")
}

func TestIntegrationTwoRankAllMin(t *testing.T) {
	device := requireRDMA(t)
	requireRDMAReadyToRun(t)
	runIntegrationCase(t, 2, device, false, "allmin")
}

func TestIntegrationTwoRankAllGather(t *testing.T) {
	device := requireRDMA(t)
	requireRDMAReadyToRun(t)
	runIntegrationCase(t, 2, device, false, "allgather")
}

func TestIntegrationTwoRankSendRecv(t *testing.T) {
	device := requireRDMA(t)
	requireRDMAReadyToRun(t)
	runIntegrationCase(t, 2, device, false, "sendrecv")
}

func TestIntegrationPeerFailure(t *testing.T) {
	device := requireRDMA(t)
	t.Run("PropagatesDialContextDeadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		cfg, err := integrationConfig(1, 2, device, "127.0.0.1:1", false)
		if err != nil {
			t.Fatal(err)
		}
		_, err = NewGroup(ctx, cfg)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("NewGroup dial = %v, want context deadline", err)
		}
	})
}

func TestIntegrationRingPreferredWhenValid(t *testing.T) {
	device := requireRDMA(t)
	requireRDMAReadyToRun(t)
	runIntegrationCase(t, 2, device, true, "barrier")
}

func TestIntegrationMeshFallbackWhenRingInvalid(t *testing.T) {
	device := requireRDMA(t)
	requireRDMAReadyToRun(t)
	runIntegrationCase(t, 4, device, true, "barrier")
}

func TestIntegrationConfigDaemonBackendEnv(t *testing.T) {
	t.Setenv("JACCL_BACKEND", BackendDaemon)
	t.Setenv("JACCL_DAEMON_SOCKET", "/tmp/jaccld-test.sock")
	cfg, err := integrationConfig(1, 2, "rdma_en1", "127.0.0.1:1", false)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Backend != BackendDaemon || cfg.DaemonSocket != "/tmp/jaccld-test.sock" {
		t.Fatalf("backend/socket = %q/%q", cfg.Backend, cfg.DaemonSocket)
	}
}

func TestIntegrationConfigDeviceMatrixEnv(t *testing.T) {
	devices := lineDeviceMatrix("left", "right")
	path := writeDevices(t, devices)
	t.Setenv("JACCL_TEST_RDMA_DEVICES", path)
	cfg, err := integrationConfig(1, 3, "", "127.0.0.1:1", true)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Rank != 1 || len(cfg.Devices) != 3 || cfg.Devices[1][0][0] != "left" || cfg.Devices[1][2][0] != "right" {
		t.Fatalf("integrationConfig devices = %+v", cfg.Devices)
	}
}

func TestIntegrationConfigSingleDeviceExpandsCompleteMatrix(t *testing.T) {
	cfg, err := integrationConfig(0, 2, "rdma_en1", "127.0.0.1:1", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Devices) != 2 || cfg.Devices[0][1][0] != "rdma_en1" {
		t.Fatalf("integrationConfig devices = %+v", cfg.Devices)
	}
}

func TestIntegrationConfigRejectsLargeSingleDeviceShorthand(t *testing.T) {
	_, err := integrationConfig(0, 3, "rdma_en1", "127.0.0.1:1", true)
	if err == nil || !strings.Contains(err.Error(), "single-device RDMA shorthand expands to a complete mesh") {
		t.Fatalf("integrationConfig = %v, want single-device shorthand error", err)
	}
}

func requireRDMAReadyToRun(t *testing.T) {
	t.Helper()
	requireRTRAllowed(t)
	if os.Getenv("JACCL_TEST_RDMA_ALLOW_LOCAL_LOOPBACK") != "1" {
		t.Skip("local RTR loopback is unsafe for Apple Thunderbolt RDMA; run TestIntegrationChild once per physical host or set JACCL_TEST_RDMA_ALLOW_LOCAL_LOOPBACK=1 for an explicit loopback experiment")
	}
}

func requireRTRAllowed(t *testing.T) {
	t.Helper()
	if os.Getenv("JACCL_TEST_RDMA_ALLOW_RTR") != "1" {
		t.Skip("set JACCL_TEST_RDMA_ALLOW_RTR=1 to run tests that transition queue pairs to RTR")
	}
}

func runIntegrationCase(t *testing.T, size int, device string, preferRing bool, op string) {
	t.Helper()
	addr := unusedIntegrationAddr(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	type result struct {
		rank int
		out  []byte
		err  error
	}
	ch := make(chan result, size)
	for rank := 0; rank < size; rank++ {
		rank := rank
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run", "^TestIntegrationChild$", "-test.v")
		cmd.Env = append(os.Environ(),
			"JACCL_TEST_RDMA_CHILD=1",
			"JACCL_TEST_RDMA=1",
			"JACCL_TEST_RANK="+strconv.Itoa(rank),
			"JACCL_TEST_SIZE="+strconv.Itoa(size),
			"JACCL_TEST_RDMA_DEVICE="+device,
			"JACCL_TEST_COORDINATOR="+addr,
			"JACCL_TEST_OP="+op,
		)
		if preferRing {
			cmd.Env = append(cmd.Env, "JACCL_TEST_PREFER_RING=1")
		}
		go func() {
			cmd.WaitDelay = 2 * time.Second
			out, err := cmd.CombinedOutput()
			ch <- result{rank: rank, out: out, err: err}
		}()
	}

	var failures []string
	for i := 0; i < size; i++ {
		res := <-ch
		if res.err != nil {
			failures = append(failures, fmt.Sprintf("rank %d: %v\n%s", res.rank, res.err, res.out))
		}
	}
	if len(failures) > 0 {
		t.Fatalf("integration %s failed:\n%s", op, strings.Join(failures, "\n"))
	}
}

func integrationConfig(rank, size int, device, coordinator string, preferRing bool) (Config, error) {
	if path := integrationDeviceMatrixPath(); path != "" {
		devices, err := readDeviceMatrix(path)
		if err != nil {
			return Config{}, err
		}
		if size != len(devices) {
			return Config{}, fmt.Errorf("test size %d does not match device matrix size %d", size, len(devices))
		}
		return Config{
			Rank:         rank,
			Coordinator:  coordinator,
			Devices:      devices,
			PreferRing:   preferRing,
			Backend:      os.Getenv("JACCL_BACKEND"),
			DaemonSocket: os.Getenv("JACCL_DAEMON_SOCKET"),
		}, nil
	}
	if size > 2 && device != "" {
		return Config{}, fmt.Errorf("single-device RDMA shorthand expands to a complete mesh for size %d; use JACCL_TEST_RDMA_DEVICES for explicit multi-rank topology", size)
	}
	devices := make([][][]string, size)
	for i := range devices {
		devices[i] = make([][]string, size)
		for j := range devices[i] {
			devices[i][j] = []string{}
			if i != j {
				devices[i][j] = []string{device}
			}
		}
	}
	return Config{
		Rank:         rank,
		Coordinator:  coordinator,
		Devices:      devices,
		PreferRing:   preferRing,
		Backend:      os.Getenv("JACCL_BACKEND"),
		DaemonSocket: os.Getenv("JACCL_DAEMON_SOCKET"),
	}, nil
}

func integrationDeviceMatrixPath() string {
	for _, name := range []string{"JACCL_TEST_RDMA_DEVICES", "JACCL_IBV_DEVICES", "MLX_IBV_DEVICES"} {
		if path := os.Getenv(name); path != "" {
			return path
		}
	}
	return ""
}

func lineDeviceMatrix(left, right string) [][][]string {
	return [][][]string{
		{{}, {left}, {}},
		{{left}, {}, {right}},
		{{}, {right}, {}},
	}
}

func unusedIntegrationAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("parse int %q: %v", s, err)
	}
	return n
}
