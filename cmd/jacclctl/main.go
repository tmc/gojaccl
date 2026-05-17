// Command jacclctl sends operator control requests to jaccld.
//
// The stats subcommand reports daemon resource leases and jaccld-observed
// scarce provider slot counters. The maintain subcommand asks a daemon to run
// explicit same-data-QP maintenance. The rdma-metadata and tcp-diagnostic
// subcommands are bounded operator preflights.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/netip"
	"os"
	"time"

	"github.com/tmc/gojaccl/internal/ipc"
	"github.com/tmc/gojaccl/internal/jaccld/resource"
	"github.com/tmc/gojaccl/internal/rdma"
	"github.com/tmc/gojaccl/internal/tcpchan"
)

const (
	defaultTCPDiagnosticPayload = "gojaccl tcp diagnostic"
	tcpDiagnosticAck            = "gojaccl tcp diagnostic ack"
	defaultRDMAMetadataMaxGIDs  = 64
)

func main() {
	log.SetFlags(0)
	var socket string
	flag.StringVar(&socket, "socket", ipc.DefaultSocket, "jaccld Unix-domain socket path")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "usage: jacclctl [flags] maintain [-timeout duration]\n")
		fmt.Fprintf(flag.CommandLine.Output(), "       jacclctl [flags] stats [-timeout duration] [-json]\n")
		fmt.Fprintf(flag.CommandLine.Output(), "       jacclctl [flags] rdma-metadata -device name\n")
		fmt.Fprintf(flag.CommandLine.Output(), "       jacclctl [flags] tcp-diagnostic (-listen addr | -dial addr)\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(2)
	}

	ctx := context.Background()
	switch flag.Arg(0) {
	case "maintain":
		if err := runMaintainCommand(ctx, socket, flag.Args()[1:]); err != nil {
			log.Fatal(err)
		}
	case "stats":
		if err := runStatsCommand(ctx, socket, flag.Args()[1:], os.Stdout); err != nil {
			log.Fatal(err)
		}
	case "tcp-diagnostic":
		if err := runTCPDiagnosticCommand(ctx, flag.Args()[1:], os.Stdout); err != nil {
			log.Fatal(err)
		}
	case "rdma-metadata":
		if err := runRDMAMetadataCommand(ctx, flag.Args()[1:], os.Stdout); err != nil {
			log.Fatal(err)
		}
	default:
		flag.Usage()
		os.Exit(2)
	}
}

func runStatsCommand(ctx context.Context, socket string, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	fs.SetOutput(out)
	timeout := fs.Duration("timeout", 5*time.Second, "stats request timeout")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected stats arguments")
	}
	if *timeout <= 0 {
		return fmt.Errorf("timeout %s must be positive", *timeout)
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	client, err := ipc.Dial(ctx, socket)
	if err != nil {
		return err
	}
	defer client.Close()
	stats, err := client.ResourceStats(ctx)
	if err != nil {
		return err
	}
	if *jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(stats)
	}
	formatResourceStats(out, stats)
	return nil
}

func formatResourceStats(out io.Writer, stats resource.Stats) {
	fmt.Fprintf(out, "jaccld resource state=%s leases=%d\n", stats.State, stats.Leases)
	fmt.Fprintf(out, "pool memory_regions in_use=%d available=%d bytes_in_use=%d bytes_available=%d\n",
		stats.MemoryRegions.InUse, stats.MemoryRegions.Available, stats.MemoryRegions.BytesInUse, stats.MemoryRegions.BytesAvailable)
	fmt.Fprintf(out, "pool queue_pairs in_use=%d available=%d\n", stats.QueuePairs.InUse, stats.QueuePairs.Available)
	fmt.Fprintf(out, "pool completion_queues in_use=%d available=%d\n", stats.CompletionQueues.InUse, stats.CompletionQueues.Available)
	slots := stats.Slots
	fmt.Fprintf(out, "slot_ledger boot_id=%q source=%q external_use_unknown=%t state_path=%q\n",
		slots.BootID, slots.Source, slots.ExternalUseUnknown, slots.StatePath)
	formatSlotCounter(out, resource.SlotProtectionDomain, slots.ProtectionDomains)
	formatSlotCounter(out, resource.SlotMemoryRegion, slots.MemoryRegions)
	formatSlotCounter(out, resource.SlotQueuePair, slots.QueuePairs)
	formatSlotCounter(out, resource.SlotCompletionQueue, slots.CompletionQueues)
}

func formatSlotCounter(out io.Writer, kind resource.SlotKind, c resource.SlotCounter) {
	fmt.Fprintf(out, "slot kind=%s opened=%d closed=%d outstanding=%d live=%d failed=%d\n",
		kind, c.Opened, c.Closed, c.Outstanding, c.Live, c.Failed)
}

func runMaintainCommand(ctx context.Context, socket string, args []string) error {
	fs := flag.NewFlagSet("maintain", flag.ContinueOnError)
	timeout := fs.Duration("timeout", 5*time.Second, "maintenance request timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected maintain arguments")
	}
	if *timeout <= 0 {
		return fmt.Errorf("timeout %s must be positive", *timeout)
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	client, err := ipc.Dial(ctx, socket)
	if err != nil {
		return err
	}
	defer client.Close()
	if err := client.Maintain(ctx); err != nil {
		return err
	}
	return nil
}

func runRDMAMetadataCommand(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("rdma-metadata", flag.ContinueOnError)
	fs.SetOutput(out)
	device := fs.String("device", "", "RDMA device name")
	maxGIDs := fs.Int("max-gids", defaultRDMAMetadataMaxGIDs, "maximum GID table entries to query")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected rdma-metadata arguments")
	}
	if *device == "" {
		return fmt.Errorf("device is required")
	}
	if *maxGIDs <= 0 {
		return fmt.Errorf("max-gids %d must be positive", *maxGIDs)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	dev, err := rdma.OpenDevice(*device)
	if err != nil {
		return err
	}
	defer dev.Close()
	info, err := rdma.QueryPort(dev, *maxGIDs)
	if err != nil {
		return err
	}
	formatRDMAPortInfo(out, info)
	return nil
}

func formatRDMAPortInfo(out io.Writer, info rdma.PortInfo) {
	fmt.Fprintf(out, "rdma metadata device=%s port=%d lid=%d gid_tbl_len=%d gid_scan_limit=%d selected_gid_index=%d\n",
		info.Device, info.PortNum, info.LID, info.GIDTableLength, info.GIDScanLimit, info.SelectedGIDIndex)
	for _, entry := range info.GIDs {
		fmt.Fprintf(out, "gid index=%d value=%s ipv4_mapped=%t zero=%t",
			entry.Index, formatGID(entry.GID), entry.IPv4Mapped, entry.Zero)
		if ip, ok := ipv4MappedGID(entry.GID); ok {
			fmt.Fprintf(out, " ipv4=%s", ip)
		}
		fmt.Fprintln(out)
	}
}

func formatGID(gid [16]byte) string {
	return netip.AddrFrom16(gid).String()
}

func ipv4MappedGID(gid [16]byte) (netip.Addr, bool) {
	addr := netip.AddrFrom16(gid)
	if !addr.Is4In6() {
		return netip.Addr{}, false
	}
	return addr.Unmap(), true
}

func runTCPDiagnosticCommand(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("tcp-diagnostic", flag.ContinueOnError)
	fs.SetOutput(out)
	listen := fs.String("listen", "", "listen address for one diagnostic connection")
	dial := fs.String("dial", "", "dial address for one diagnostic connection")
	timeout := fs.Duration("timeout", 5*time.Second, "diagnostic timeout")
	payload := fs.String("payload", defaultTCPDiagnosticPayload, "diagnostic payload")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected tcp-diagnostic arguments")
	}
	if (*listen == "") == (*dial == "") {
		return fmt.Errorf("set exactly one of -listen or -dial")
	}
	if *timeout <= 0 {
		return fmt.Errorf("timeout %s must be positive", *timeout)
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	if *listen != "" {
		return tcpDiagnosticListen(ctx, *listen, []byte(*payload), out)
	}
	return tcpDiagnosticDial(ctx, *dial, []byte(*payload), out)
}

func tcpDiagnosticListen(ctx context.Context, addr string, payload []byte, out io.Writer) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("tcp diagnostic listen %s: %w", addr, err)
	}
	defer ln.Close()
	return serveTCPDiagnostic(ctx, ln, payload, out)
}

func serveTCPDiagnostic(ctx context.Context, ln net.Listener, payload []byte, out io.Writer) error {
	fmt.Fprintf(out, "tcp diagnostic listening addr=%s\n", ln.Addr())
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = ln.Close()
		case <-done:
		}
	}()
	conn, err := ln.Accept()
	close(done)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("tcp diagnostic accept: %w", err)
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	got, err := tcpchan.ReadFrame(conn)
	if err != nil {
		return fmt.Errorf("tcp diagnostic read: %w", err)
	}
	if string(got) != string(payload) {
		return fmt.Errorf("tcp diagnostic payload mismatch")
	}
	if err := tcpchan.WriteFrame(conn, []byte(tcpDiagnosticAck)); err != nil {
		return fmt.Errorf("tcp diagnostic write ack: %w", err)
	}
	fmt.Fprintf(out, "tcp diagnostic listen ok addr=%s bytes=%d\n", ln.Addr(), len(got))
	return nil
}

func tcpDiagnosticDial(ctx context.Context, addr string, payload []byte, out io.Writer) error {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("tcp diagnostic dial %s: %w", addr, err)
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if err := tcpchan.WriteFrame(conn, payload); err != nil {
		return fmt.Errorf("tcp diagnostic write: %w", err)
	}
	ack, err := tcpchan.ReadFrame(conn)
	if err != nil {
		return fmt.Errorf("tcp diagnostic read ack: %w", err)
	}
	if string(ack) != tcpDiagnosticAck {
		return fmt.Errorf("tcp diagnostic ack mismatch")
	}
	fmt.Fprintf(out, "tcp diagnostic dial ok addr=%s bytes=%d\n", addr, len(payload))
	return nil
}
