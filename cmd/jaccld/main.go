// Command jaccld owns local Apple RDMA resources for jaccl clients.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/tmc/gojaccl/internal/allocator"
	"github.com/tmc/gojaccl/internal/ipc"
	"github.com/tmc/gojaccl/internal/jaccld/resource"
	"github.com/tmc/gojaccl/internal/keepalive"
	"github.com/tmc/gojaccl/internal/rdma"
	"github.com/tmc/gojaccl/internal/tcpchan"
)

func main() {
	var cfg config
	flag.StringVar(&cfg.socket, "socket", ipc.DefaultSocket, "Unix-domain socket path")
	flag.StringVar(&cfg.device, "device", "", "RDMA device name; empty selects the first device")
	flag.IntVar(&cfg.rank, "rank", -1, "daemon rank")
	flag.IntVar(&cfg.size, "size", 0, "number of daemon ranks")
	flag.StringVar(&cfg.coordinator, "coordinator", "", "rank-zero TCP side-channel address")
	flag.Int64Var(&cfg.slabSize, "slab-size", 1<<30, "shared slab size in bytes")
	flag.IntVar(&cfg.maxSessions, "max-sessions", 128, "maximum local resource sessions")
	flag.DurationVar(&cfg.controlPlaneLiveness, "control-plane-liveness", time.Minute, "provider-free session liveness pulse interval; zero disables")
	flag.DurationVar(&cfg.heartbeat, "heartbeat", time.Minute, "experimental RDMA heartbeat interval")
	flag.DurationVar(&cfg.heartbeatTimeout, "heartbeat-timeout", time.Second, "maximum experimental RDMA heartbeat completion wait")
	flag.DurationVar(&cfg.heartbeatLeaseTTL, "heartbeat-lease-ttl", defaultHeartbeatLeaseTTL, "lifetime of exchanged heartbeat memory leases")
	flag.BoolVar(&cfg.experimentalRDMAHeartbeat, "experimental-rdma-heartbeat", false, "post experimental RDMA-write heartbeats")
	flag.BoolVar(&cfg.noRDMA, "no-rdma", false, "start IPC without opening RDMA hardware")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

type config struct {
	socket                    string
	device                    string
	rank                      int
	size                      int
	coordinator               string
	slabSize                  int64
	maxSessions               int
	heartbeat                 time.Duration
	heartbeatTimeout          time.Duration
	heartbeatLeaseTTL         time.Duration
	controlPlaneLiveness      time.Duration
	experimentalRDMAHeartbeat bool
	noRDMA                    bool
}

const defaultHeartbeatLeaseTTL = 24 * time.Hour

func maintenanceBytes(size int) int64 {
	if size <= 1 {
		return 2
	}
	if int64(size) > maxInt64/2 {
		return maxInt64
	}
	return int64(2 * size)
}

func run(ctx context.Context, cfg config) error {
	if cfg.controlPlaneLiveness < 0 {
		return fmt.Errorf("control-plane liveness interval %s must be non-negative", cfg.controlPlaneLiveness)
	}
	if !cfg.noRDMA {
		if err := cfg.validateRDMA(); err != nil {
			return err
		}
	}
	var side *tcpchan.Channel
	if !cfg.noRDMA {
		var err error
		log.Printf("jaccld phase=side_channel start rank=%d size=%d coordinator=%s", cfg.rank, cfg.size, cfg.coordinator)
		side, err = tcpchan.New(ctx, cfg.rank, cfg.size, cfg.coordinator)
		if err != nil {
			return fmt.Errorf("side channel: %w", err)
		}
		log.Printf("jaccld phase=side_channel done rank=%d size=%d", cfg.rank, cfg.size)
		defer side.Close()
	}
	log.Printf("jaccld phase=slab start bytes=%d", cfg.slabSize)
	slab, err := allocator.NewSlab("", cfg.slabSize)
	if err != nil {
		return err
	}
	log.Printf("jaccld phase=slab done bytes=%d", len(slab.Bytes()))
	defer slab.Close()
	resources, err := newResourceStore(slab, cfg.maxSessions)
	if err != nil {
		return err
	}
	log.Printf("jaccld phase=resource_store done max_sessions=%d", cfg.maxSessions)
	if cfg.controlPlaneLiveness > 0 {
		go func() {
			if err := resources.RunControlPlaneLiveness(ctx, cfg.controlPlaneLiveness); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("jaccld control-plane liveness stopped: %v", err)
			}
		}()
	}

	var heartbeat allocator.Lease
	if !cfg.noRDMA {
		heartbeat, err = slab.Alloc(maintenanceBytes(cfg.size))
		if err != nil {
			return fmt.Errorf("reserve heartbeat bytes: %w", err)
		}
		log.Printf("jaccld phase=maintenance_lease done length=%d", heartbeat.Length)
	}

	var hw *hardware
	if !cfg.noRDMA {
		hw, err = openHardware(cfg.device, slab)
		if err != nil {
			return err
		}
		defer hw.Close()
	}

	var tracker *keepalive.Tracker
	if hw != nil && cfg.experimentalRDMAHeartbeat {
		tracker, err = keepalive.New(cfg.heartbeat)
		if err != nil {
			return err
		}
		go tracker.Run(ctx)
	}

	var transport ipc.Transport
	var rdmaTransport *daemonTransport
	if hw != nil {
		log.Printf("jaccld phase=daemon_transport start")
		rdmaTransport, err = openDaemonTransport(ctx, cfg, side, slab, hw, tracker, heartbeat)
		if err != nil {
			return err
		}
		log.Printf("jaccld phase=daemon_transport done")
		defer rdmaTransport.Close()
		transport = rdmaTransport
	}

	server, err := ipc.NewServerWithResources(slab, transport, resources)
	if err != nil {
		return err
	}
	log.Printf("jaccld phase=ipc_listen start socket=%s", cfg.socket)
	return server.ListenAndServe(ctx, cfg.socket)
}

func newResourceStore(slab *allocator.Slab, maxSessions int) (*resource.Store, error) {
	if maxSessions <= 0 {
		return nil, fmt.Errorf("max sessions %d must be positive", maxSessions)
	}
	mr, err := resource.NewSlabMRPool(slab)
	if err != nil {
		return nil, err
	}
	// TODO(hardware): Replace static handles with hardware-backed pools in a
	// later hardware-gated slice.
	qpHandles := make([]resource.QueuePairHandle, maxSessions)
	cqHandles := make([]resource.CompletionQueueHandle, maxSessions)
	for i := 0; i < maxSessions; i++ {
		qpHandles[i] = resource.QueuePairHandle(fmt.Sprintf("qp-%d", i))
		cqHandles[i] = resource.CompletionQueueHandle(fmt.Sprintf("cq-%d", i))
	}
	qp, err := resource.NewStaticQueuePairPool(qpHandles)
	if err != nil {
		return nil, err
	}
	cq, err := resource.NewStaticCompletionQueuePool(cqHandles)
	if err != nil {
		return nil, err
	}
	store, err := resource.NewStore(mr, qp, cq)
	if err != nil {
		return nil, err
	}
	if err := store.SetState(resource.StateReady); err != nil {
		return nil, err
	}
	return store, nil
}

func (cfg config) validateRDMA() error {
	if cfg.rank < 0 {
		return fmt.Errorf("rank %d out of range", cfg.rank)
	}
	if cfg.size <= 0 {
		return fmt.Errorf("size %d must be positive", cfg.size)
	}
	if cfg.rank >= cfg.size {
		return fmt.Errorf("rank %d out of range for size %d", cfg.rank, cfg.size)
	}
	if strings.TrimSpace(cfg.coordinator) == "" {
		return fmt.Errorf("coordinator is empty")
	}
	if cfg.experimentalRDMAHeartbeat && cfg.heartbeat <= 0 {
		return fmt.Errorf("heartbeat interval %s must be positive", cfg.heartbeat)
	}
	if cfg.experimentalRDMAHeartbeat && cfg.heartbeatTimeout <= 0 {
		return fmt.Errorf("heartbeat timeout %s must be positive", cfg.heartbeatTimeout)
	}
	if cfg.experimentalRDMAHeartbeat && cfg.heartbeatLeaseTTL <= 0 {
		return fmt.Errorf("heartbeat lease ttl %s must be positive", cfg.heartbeatLeaseTTL)
	}
	return nil
}

type hardware struct {
	dev *rdma.Device
	pd  *rdma.ProtectionDomain
	mr  *rdma.MemoryRegion
}

func openHardware(device string, slab *allocator.Slab) (*hardware, error) {
	log.Printf("jaccld phase=hardware_open start device=%q", device)
	dev, err := rdma.OpenDevice(device)
	if err != nil {
		return nil, err
	}
	log.Printf("jaccld phase=hardware_open device_done name=%q", dev.Name())
	hw := &hardware{dev: dev}
	defer func() {
		if err != nil {
			_ = hw.Close()
		}
	}()
	log.Printf("jaccld phase=pd_alloc start")
	if hw.pd, err = rdma.NewProtectionDomain(dev); err != nil {
		return nil, err
	}
	log.Printf("jaccld phase=pd_alloc done")
	log.Printf("jaccld phase=mr_register start length=%d", len(slab.Bytes()))
	if hw.mr, err = rdma.RegisterMemory(hw.pd, slab.Bytes()); err != nil {
		return nil, err
	}
	log.Printf("jaccld phase=mr_register done addr_nonzero=%t lkey_nonzero=%t rkey_nonzero=%t length=%d",
		hw.mr.Addr() != 0,
		hw.mr.LKey() != 0,
		hw.mr.RKey() != 0,
		len(hw.mr.Buffer()),
	)
	return hw, nil
}

func (h *hardware) Close() error {
	var first error
	if h == nil {
		return nil
	}
	if h.mr != nil {
		if err := h.mr.Close(); err != nil && first == nil {
			first = err
		}
	}
	if h.pd != nil {
		if err := h.pd.Close(); err != nil && first == nil {
			first = err
		}
	}
	if h.dev != nil {
		if err := h.dev.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
