// Command jacclctl sends operator control requests to jaccld.
//
// The stats subcommand reports daemon resource leases and jaccld-observed
// scarce provider slot counters. The maintain subcommand asks a daemon to run
// explicit same-data-QP maintenance. The rdma-metadata, rdma-alloc, rdma-init,
// and tcp-diagnostic subcommands are bounded operator preflights.
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
	"path/filepath"
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
		fmt.Fprintf(flag.CommandLine.Output(), "       jacclctl [flags] rdma-alloc -device name\n")
		fmt.Fprintf(flag.CommandLine.Output(), "       jacclctl [flags] rdma-init -device name\n")
		fmt.Fprintf(flag.CommandLine.Output(), "       jacclctl [flags] rdma-rtr-diagnostic -device name -artifact dir -peer-destination file -allow-rtr\n")
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
	case "rdma-alloc":
		if err := runRDMAAllocCommand(ctx, flag.Args()[1:], os.Stdout); err != nil {
			log.Fatal(err)
		}
	case "rdma-init":
		if err := runRDMAInitCommand(ctx, flag.Args()[1:], os.Stdout); err != nil {
			log.Fatal(err)
		}
	case "rdma-rtr-diagnostic":
		if err := runRDMARTRDiagnosticCommand(ctx, flag.Args()[1:], os.Stdout); err != nil {
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

func runRDMAAllocCommand(ctx context.Context, args []string, out io.Writer) (err error) {
	return runRDMAResourceCommand(ctx, "rdma-alloc", args, out, false)
}

func runRDMAInitCommand(ctx context.Context, args []string, out io.Writer) (err error) {
	return runRDMAResourceCommand(ctx, "rdma-init", args, out, true)
}

func runRDMAResourceCommand(ctx context.Context, name string, args []string, out io.Writer, initQP bool) (err error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(out)
	device := fs.String("device", "", "RDMA device name")
	cqCapacity := fs.Int("cq-capacity", 4, "completion queue capacity")
	mrBytes := fs.Int("mr-bytes", 4096, "mmap-backed memory region size")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected %s arguments", name)
	}
	if *device == "" {
		return fmt.Errorf("device is required")
	}
	if *cqCapacity <= 0 {
		return fmt.Errorf("cq-capacity %d must be positive", *cqCapacity)
	}
	if *mrBytes <= 0 {
		return fmt.Errorf("mr-bytes %d must be positive", *mrBytes)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	type cleanup struct {
		name string
		fn   func() error
	}
	var cleanups []cleanup
	defer func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			if closeErr := cleanups[i].fn(); closeErr != nil && err == nil {
				err = fmt.Errorf("close %s: %w", cleanups[i].name, closeErr)
			}
		}
	}()

	dev, err := rdma.OpenDevice(*device)
	if err != nil {
		return err
	}
	cleanups = append(cleanups, cleanup{"device", dev.Close})
	pd, err := rdma.NewProtectionDomain(dev)
	if err != nil {
		return err
	}
	cleanups = append(cleanups, cleanup{"protection domain", pd.Close})
	cq, err := rdma.NewCompletionQueue(dev, *cqCapacity)
	if err != nil {
		return err
	}
	cleanups = append(cleanups, cleanup{"completion queue", cq.Close})
	qp, err := rdma.NewQueuePair(pd, cq)
	if err != nil {
		return err
	}
	cleanups = append(cleanups, cleanup{"queue pair", qp.Close})
	mr, err := rdma.NewMemoryRegion(pd, *mrBytes)
	if err != nil {
		return err
	}
	cleanups = append(cleanups, cleanup{"memory region", mr.Close})
	if initQP {
		if err := rdma.InitQueuePair(qp); err != nil {
			return err
		}
	}

	formatRDMAResource(out, name, *device, *cqCapacity, *mrBytes, initQP, qp, mr)
	return nil
}

func formatRDMAResource(out io.Writer, command, device string, cqCapacity, mrBytes int, initQP bool, qp *rdma.QueuePair, mr *rdma.MemoryRegion) {
	qpn := qp.Number()
	fmt.Fprintf(out, "rdma resource command=%s device=%s cq_capacity=%d mr_bytes=%d qpn=%d qpn_nonzero=%t addr_nonzero=%t lkey_nonzero=%t rkey_nonzero=%t init=%t rtr=false work_requests=false\n",
		command, device, cqCapacity, mrBytes, qpn, qpn != 0, mr.Addr() != 0, mr.LKey() != 0, mr.RKey() != 0, initQP)
	fmt.Fprintln(out, "resource protection_domain=allocated")
	fmt.Fprintln(out, "resource completion_queue=allocated")
	fmt.Fprintln(out, "resource queue_pair=allocated")
	fmt.Fprintln(out, "resource memory_region=allocated")
}

const rtrDiagnosticGate = "JACCLCTL_RDMA_RTR_DIAGNOSTIC_ONE_SHOT"

type rtrDiagnosticReport struct {
	Command              string               `json:"command"`
	Device               string               `json:"device"`
	RouteInterface       string               `json:"route_interface,omitempty"`
	PeerRouteInterface   string               `json:"peer_route_interface,omitempty"`
	CQCapacity           int                  `json:"cq_capacity"`
	AllowRTR             bool                 `json:"allow_rtr"`
	Gate                 string               `json:"gate"`
	GeneratedAt          time.Time            `json:"generated_at"`
	Local                rtrDestinationRecord `json:"local"`
	Remote               rtrDestinationRecord `json:"remote"`
	Transition           rtrTransitionReport  `json:"transition"`
	State                string               `json:"state"`
	Error                string               `json:"error,omitempty"`
	WorkRequests         bool                 `json:"work_requests"`
	ReadyToSend          bool                 `json:"ready_to_send"`
	DatapathClaim        bool                 `json:"datapath_claim"`
	IndexZeroRoute       bool                 `json:"index_zero_route"`
	IPv4MappedRoute      bool                 `json:"ipv4_mapped_route"`
	LocalDestinationFile string               `json:"local_destination_file"`
	PeerDestinationFile  string               `json:"peer_destination_file"`
}

type rtrTransitionReport struct {
	From      string   `json:"from"`
	To        string   `json:"to"`
	Mask      int      `json:"mask"`
	MaskHex   string   `json:"mask_hex"`
	MaskNames []string `json:"mask_names"`
}

type rtrDestinationRecord struct {
	Device         string    `json:"device,omitempty"`
	RouteInterface string    `json:"route_interface,omitempty"`
	LID            uint16    `json:"lid"`
	QPN            uint32    `json:"qpn"`
	PSN            uint32    `json:"psn"`
	GIDIndex       int       `json:"gid_index"`
	GID            string    `json:"gid"`
	GeneratedAt    time.Time `json:"generated_at,omitempty"`
}

func runRDMARTRDiagnosticCommand(ctx context.Context, args []string, out io.Writer) (err error) {
	fs := flag.NewFlagSet("rdma-rtr-diagnostic", flag.ContinueOnError)
	fs.SetOutput(out)
	device := fs.String("device", "", "RDMA device name")
	artifact := fs.String("artifact", "", "directory for local destination and final report")
	localDestination := fs.String("local-destination", "", "path to write local destination JSON")
	peerDestination := fs.String("peer-destination", "", "path to read peer destination JSON")
	routeInterface := fs.String("route-interface", "", "local route interface name recorded in the report")
	peerRouteInterface := fs.String("peer-route-interface", "", "peer route interface name recorded in the report")
	cqCapacity := fs.Int("cq-capacity", 4, "completion queue capacity")
	waitPeer := fs.Duration("wait-peer", 2*time.Minute, "maximum time to wait for peer destination JSON")
	allowRTR := fs.Bool("allow-rtr", false, "allow one RTR transition diagnostic")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected rdma-rtr-diagnostic arguments")
	}
	if *device == "" {
		return fmt.Errorf("device is required")
	}
	if *artifact == "" {
		return fmt.Errorf("artifact is required")
	}
	if *cqCapacity <= 0 {
		return fmt.Errorf("cq-capacity %d must be positive", *cqCapacity)
	}
	if *waitPeer <= 0 {
		return fmt.Errorf("wait-peer %s must be positive", *waitPeer)
	}
	if *peerDestination == "" {
		return fmt.Errorf("peer-destination is required")
	}
	if !*allowRTR || os.Getenv(rtrDiagnosticGate) != "one-shot-rtr" {
		return fmt.Errorf("rdma-rtr-diagnostic refuses to run without -allow-rtr and %s=one-shot-rtr", rtrDiagnosticGate)
	}
	if err := os.MkdirAll(*artifact, 0777); err != nil {
		return fmt.Errorf("create artifact: %w", err)
	}
	if *localDestination == "" {
		*localDestination = filepath.Join(*artifact, "local-destination.json")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	report := rtrDiagnosticReport{
		Command:              "rdma-rtr-diagnostic",
		Device:               *device,
		RouteInterface:       *routeInterface,
		PeerRouteInterface:   *peerRouteInterface,
		CQCapacity:           *cqCapacity,
		AllowRTR:             true,
		Gate:                 rtrDiagnosticGate,
		GeneratedAt:          time.Now().UTC(),
		Transition:           rtrTransition(),
		State:                "started",
		LocalDestinationFile: *localDestination,
		PeerDestinationFile:  *peerDestination,
	}
	defer func() {
		report.GeneratedAt = time.Now().UTC()
		if writeErr := writeRTRReport(*artifact, report); writeErr != nil && err == nil {
			err = writeErr
		}
	}()

	type cleanup struct {
		name string
		fn   func() error
	}
	var cleanups []cleanup
	defer func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			if closeErr := cleanups[i].fn(); closeErr != nil && err == nil {
				err = fmt.Errorf("close %s: %w", cleanups[i].name, closeErr)
			}
		}
	}()

	dev, err := rdma.OpenDevice(*device)
	if err != nil {
		report.State = "open_failed"
		report.Error = err.Error()
		return err
	}
	cleanups = append(cleanups, cleanup{"device", dev.Close})
	pd, err := rdma.NewProtectionDomain(dev)
	if err != nil {
		report.State = "pd_failed"
		report.Error = err.Error()
		return err
	}
	cleanups = append(cleanups, cleanup{"protection domain", pd.Close})
	cq, err := rdma.NewCompletionQueue(dev, *cqCapacity)
	if err != nil {
		report.State = "cq_failed"
		report.Error = err.Error()
		return err
	}
	cleanups = append(cleanups, cleanup{"completion queue", cq.Close})
	qp, err := rdma.NewQueuePair(pd, cq)
	if err != nil {
		report.State = "qp_failed"
		report.Error = err.Error()
		return err
	}
	cleanups = append(cleanups, cleanup{"queue pair", qp.Close})
	if err := rdma.InitQueuePair(qp); err != nil {
		report.State = "init_failed"
		report.Error = err.Error()
		return err
	}
	local, err := rdma.LocalDestination(qp)
	if err != nil {
		report.State = "local_destination_failed"
		report.Error = err.Error()
		return err
	}
	report.Local = newRTRDestinationRecord(*device, *routeInterface, local)
	report.IndexZeroRoute = local.GIDIndex == 0 && local.GID != ([16]byte{})
	report.IPv4MappedRoute = netip.AddrFrom16(local.GID).Is4In6()
	if err := writeRTRDestination(*localDestination, report.Local); err != nil {
		report.State = "write_local_destination_failed"
		report.Error = err.Error()
		return err
	}
	remoteRecord, remote, err := waitRTRDestination(ctx, *peerDestination, *waitPeer)
	if err != nil {
		report.State = "peer_destination_failed"
		report.Error = err.Error()
		return err
	}
	if remoteRecord.RouteInterface == "" {
		remoteRecord.RouteInterface = *peerRouteInterface
	}
	report.Remote = remoteRecord
	if err := rdma.ReadyToReceive(qp, local, remote); err != nil {
		report.State = "rtr_failed"
		report.Error = err.Error()
		formatRTRDiagnostic(out, report)
		return err
	}
	report.State = "rtr_ready"
	formatRTRDiagnostic(out, report)
	return nil
}

func rtrTransition() rtrTransitionReport {
	mask := rdma.ReadyToReceiveMask()
	return rtrTransitionReport{
		From:    "INIT",
		To:      "RTR",
		Mask:    mask,
		MaskHex: fmt.Sprintf("0x%x", mask),
		MaskNames: []string{
			"IBV_QP_STATE",
			"IBV_QP_AV",
			"IBV_QP_PATH_MTU",
			"IBV_QP_DEST_QPN",
			"IBV_QP_RQ_PSN",
		},
	}
}

func writeRTRReport(dir string, report rtrDiagnosticReport) error {
	return writeJSONFile(filepath.Join(dir, "rtr-diagnostic-report.json"), report)
}

func writeRTRDestination(path string, dst rtrDestinationRecord) error {
	return writeJSONFile(path, dst)
}

func writeJSONFile(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func waitRTRDestination(ctx context.Context, path string, timeout time.Duration) (rtrDestinationRecord, rdma.Destination, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		record, dst, err := readRTRDestination(path)
		if err == nil {
			return record, dst, nil
		}
		if !errorsIsNotExist(err) {
			return rtrDestinationRecord{}, rdma.Destination{}, err
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return rtrDestinationRecord{}, rdma.Destination{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func errorsIsNotExist(err error) bool {
	return os.IsNotExist(err)
}

func readRTRDestination(path string) (rtrDestinationRecord, rdma.Destination, error) {
	f, err := os.Open(path)
	if err != nil {
		return rtrDestinationRecord{}, rdma.Destination{}, err
	}
	defer f.Close()
	var record rtrDestinationRecord
	if err := json.NewDecoder(f).Decode(&record); err != nil {
		return rtrDestinationRecord{}, rdma.Destination{}, fmt.Errorf("read peer destination: %w", err)
	}
	dst, err := record.destination()
	if err != nil {
		return rtrDestinationRecord{}, rdma.Destination{}, err
	}
	return record, dst, nil
}

func newRTRDestinationRecord(device, routeInterface string, dst rdma.Destination) rtrDestinationRecord {
	return rtrDestinationRecord{
		Device:         device,
		RouteInterface: routeInterface,
		LID:            dst.LID,
		QPN:            dst.QPN,
		PSN:            dst.PSN,
		GIDIndex:       dst.GIDIndex,
		GID:            formatGID(dst.GID),
		GeneratedAt:    time.Now().UTC(),
	}
}

func (r rtrDestinationRecord) destination() (rdma.Destination, error) {
	if r.QPN == 0 {
		return rdma.Destination{}, fmt.Errorf("peer destination qpn is zero")
	}
	addr, err := netip.ParseAddr(r.GID)
	if err != nil {
		return rdma.Destination{}, fmt.Errorf("parse peer gid: %w", err)
	}
	return rdma.Destination{
		LID:      r.LID,
		QPN:      r.QPN,
		PSN:      r.PSN,
		GIDIndex: r.GIDIndex,
		GID:      addr.As16(),
	}, nil
}

func formatRTRDiagnostic(out io.Writer, report rtrDiagnosticReport) {
	fmt.Fprintf(out, "rdma rtr diagnostic state=%s device=%s route_interface=%s peer_route_interface=%s mask=%s local_lid=%d local_qpn=%d local_psn=%d local_gid_index=%d local_gid=%s remote_lid=%d remote_qpn=%d remote_psn=%d remote_gid_index=%d remote_gid=%s work_requests=false ready_to_send=false datapath_claim=false\n",
		report.State, report.Device, report.RouteInterface, report.PeerRouteInterface,
		report.Transition.MaskHex, report.Local.LID, report.Local.QPN, report.Local.PSN,
		report.Local.GIDIndex, report.Local.GID, report.Remote.LID, report.Remote.QPN,
		report.Remote.PSN, report.Remote.GIDIndex, report.Remote.GID)
	if report.Error != "" {
		fmt.Fprintf(out, "rdma rtr diagnostic error=%q\n", report.Error)
	}
	if report.IndexZeroRoute {
		fmt.Fprintln(out, "rdma rtr diagnostic warning=local_selected_gid_index_zero")
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
