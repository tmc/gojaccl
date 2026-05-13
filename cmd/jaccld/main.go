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
	flag.DurationVar(&cfg.heartbeat, "heartbeat", time.Minute, "idle queue-pair heartbeat interval")
	flag.BoolVar(&cfg.noRDMA, "no-rdma", false, "start IPC without opening RDMA hardware")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

type config struct {
	socket      string
	device      string
	rank        int
	size        int
	coordinator string
	slabSize    int64
	maxSessions int
	heartbeat   time.Duration
	noRDMA      bool
}

func run(ctx context.Context, cfg config) error {
	if !cfg.noRDMA {
		if err := cfg.validateRDMA(); err != nil {
			return err
		}
	}
	slab, err := allocator.NewSlab("", cfg.slabSize)
	if err != nil {
		return err
	}
	defer slab.Close()
	resources, err := newResourceStore(slab, cfg.maxSessions)
	if err != nil {
		return err
	}

	var heartbeat allocator.Lease
	if !cfg.noRDMA {
		heartbeat, err = slab.Alloc(1)
		if err != nil {
			return fmt.Errorf("reserve heartbeat byte: %w", err)
		}
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
	if hw != nil {
		tracker, err = keepalive.New(cfg.heartbeat)
		if err != nil {
			return err
		}
		go tracker.Run(ctx)
	}

	var transport ipc.Transport
	var rdmaTransport *daemonTransport
	if hw != nil {
		rdmaTransport, err = openDaemonTransport(ctx, cfg, hw, tracker, heartbeat.Offset)
		if err != nil {
			return err
		}
		defer rdmaTransport.Close()
		transport = rdmaTransport
	}

	server, err := ipc.NewServerWithResources(slab, transport, resources)
	if err != nil {
		return err
	}
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
	if cfg.heartbeat <= 0 {
		return fmt.Errorf("heartbeat interval %s must be positive", cfg.heartbeat)
	}
	return nil
}

type hardware struct {
	dev *rdma.Device
	pd  *rdma.ProtectionDomain
	mr  *rdma.MemoryRegion
}

func openHardware(device string, slab *allocator.Slab) (*hardware, error) {
	dev, err := rdma.OpenDevice(device)
	if err != nil {
		return nil, err
	}
	hw := &hardware{dev: dev}
	defer func() {
		if err != nil {
			_ = hw.Close()
		}
	}()
	if hw.pd, err = rdma.NewProtectionDomain(dev); err != nil {
		return nil, err
	}
	if hw.mr, err = rdma.RegisterMemory(hw.pd, slab.Bytes()); err != nil {
		return nil, err
	}
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
