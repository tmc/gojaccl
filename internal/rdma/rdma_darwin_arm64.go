//go:build darwin && arm64

package rdma

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"syscall"
	"time"
	"unsafe"

	applerdma "github.com/tmc/apple/rdma"
	xrdma "github.com/tmc/apple/x/rdma"
)

func Available() bool {
	return applerdma.Available()
}

func DeviceNames() ([]string, error) {
	if !Available() {
		return nil, ErrUnavailable
	}
	devices, err := applerdma.Devices()
	if err != nil {
		return nil, fmt.Errorf("list devices: %w", err)
	}
	names := make([]string, 0, len(devices))
	for _, dev := range devices {
		names = append(names, dev.Name)
	}
	return names, nil
}

func OpenDevice(path string) (*Device, error) {
	if !Available() {
		return nil, ErrUnavailable
	}
	devices, err := applerdma.Devices()
	if err != nil {
		return nil, fmt.Errorf("list devices: %w", err)
	}
	for _, dev := range devices {
		if path != "" && dev.Name != path {
			continue
		}
		ctx, err := applerdma.Ibv_open_device(dev.Handle)
		if err != nil {
			return nil, fmt.Errorf("open device %q: %w", dev.Name, err)
		}
		if ctx == 0 {
			return nil, fmt.Errorf("open device %q: returned nil context", dev.Name)
		}
		return &Device{handle: uintptr(ctx), name: dev.Name}, nil
	}
	if path == "" {
		return nil, fmt.Errorf("open device: no RDMA devices")
	}
	return nil, fmt.Errorf("open device %q: not found", path)
}

func (d *Device) Close() error {
	if d == nil || d.handle == 0 {
		return nil
	}
	var err error
	d.once.Do(func() {
		rc, e := applerdma.Ibv_close_device(applerdma.RDMAContext(d.handle))
		if e != nil {
			err = fmt.Errorf("close device %q: %w", d.name, e)
			return
		}
		if rc != 0 {
			err = fmt.Errorf("close device %q: errno %d", d.name, rc)
			return
		}
		d.handle = 0
	})
	return err
}

func NewProtectionDomain(dev *Device) (*ProtectionDomain, error) {
	if dev == nil || dev.handle == 0 {
		return nil, fmt.Errorf("alloc protection domain: nil device")
	}
	pd, err := applerdma.Ibv_alloc_pd(applerdma.RDMAContext(dev.handle))
	if err != nil {
		return nil, fmt.Errorf("alloc protection domain: %w", err)
	}
	if pd == 0 {
		return nil, fmt.Errorf("alloc protection domain: returned nil handle")
	}
	return &ProtectionDomain{dev: dev, handle: uintptr(pd)}, nil
}

func (p *ProtectionDomain) Close() error {
	if p == nil || p.handle == 0 {
		return nil
	}
	var err error
	p.once.Do(func() {
		rc, e := applerdma.Ibv_dealloc_pd(applerdma.RDMAPD(p.handle))
		if e != nil {
			err = fmt.Errorf("dealloc protection domain: %w", e)
			return
		}
		if rc != 0 {
			err = fmt.Errorf("dealloc protection domain: errno %d", rc)
			return
		}
		p.handle = 0
	})
	return err
}

func NewCompletionQueue(dev *Device, capacity int) (*CompletionQueue, error) {
	if dev == nil || dev.handle == 0 {
		return nil, fmt.Errorf("create completion queue: nil device")
	}
	if capacity <= 0 {
		return nil, fmt.Errorf("create completion queue: capacity %d must be positive", capacity)
	}
	cq, err := applerdma.Ibv_create_cq(applerdma.RDMAContext(dev.handle), capacity, 0, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("create completion queue: %w", err)
	}
	if cq == 0 {
		return nil, fmt.Errorf("create completion queue: returned nil handle")
	}
	return &CompletionQueue{dev: dev, handle: uintptr(cq)}, nil
}

func (c *CompletionQueue) Close() error {
	if c == nil || c.handle == 0 {
		return nil
	}
	var err error
	c.once.Do(func() {
		rc, e := applerdma.Ibv_destroy_cq(applerdma.RDMACQ(c.handle))
		if e != nil {
			err = fmt.Errorf("destroy completion queue: %w", e)
			return
		}
		if rc != 0 {
			err = fmt.Errorf("destroy completion queue: errno %d", rc)
			return
		}
		c.handle = 0
	})
	return err
}

func NewQueuePair(pd *ProtectionDomain, cq *CompletionQueue) (*QueuePair, error) {
	if pd == nil || pd.handle == 0 {
		return nil, fmt.Errorf("create queue pair: nil protection domain")
	}
	if cq == nil || cq.handle == 0 {
		return nil, fmt.Errorf("create queue pair: nil completion queue")
	}
	attr := applerdma.IbvQPInitAttr{
		SendCQ: applerdma.RDMACQ(cq.handle),
		RecvCQ: applerdma.RDMACQ(cq.handle),
		Cap: applerdma.IbvQPCap{
			MaxSendWR:  64,
			MaxRecvWR:  64,
			MaxSendSGE: 1,
			MaxRecvSGE: 1,
		},
		QPType:   applerdma.IBV_QPT_UC,
		SQSigAll: 1,
	}
	qp, err := applerdma.Ibv_create_qp(applerdma.RDMAPD(pd.handle), uintptr(unsafe.Pointer(&attr)))
	if err != nil {
		return nil, fmt.Errorf("create queue pair: %w", err)
	}
	if qp == 0 {
		return nil, fmt.Errorf("create queue pair: returned nil handle")
	}
	poster, err := applerdma.NewIbvQPPoster(qp)
	if err != nil {
		_, _ = applerdma.Ibv_destroy_qp(qp)
		return nil, fmt.Errorf("create queue pair poster: %w", err)
	}
	return &QueuePair{pd: pd, cq: cq, handle: uintptr(qp), poster: poster}, nil
}

func (q *QueuePair) Close() error {
	if q == nil || q.handle == 0 {
		return nil
	}
	var err error
	q.once.Do(func() {
		rc, e := applerdma.Ibv_destroy_qp(applerdma.RDMAQP(q.handle))
		if e != nil {
			err = fmt.Errorf("destroy queue pair: %w", e)
			return
		}
		if rc != 0 {
			err = fmt.Errorf("destroy queue pair: errno %d", rc)
			return
		}
		q.handle = 0
	})
	return err
}

func (q *QueuePair) number() uint32 {
	if q == nil || q.handle == 0 {
		return 0
	}
	return applerdma.Ibv_qp_num(applerdma.RDMAQP(q.handle))
}

func LocalDestination(qp *QueuePair) (Destination, error) {
	port, gid, gidIndex, err := localPortGID(qp)
	if err != nil {
		return Destination{}, err
	}
	return Destination{
		LID:      port.LID,
		QPN:      qp.Number(),
		PSN:      7,
		GIDIndex: gidIndex,
		GID:      [16]byte(gid),
	}, nil
}

func QueryPort(dev *Device, maxGIDs int) (PortInfo, error) {
	if dev == nil || dev.handle == 0 {
		return PortInfo{}, fmt.Errorf("query port: nil device")
	}
	if maxGIDs <= 0 {
		return PortInfo{}, fmt.Errorf("query port: max gids %d must be positive", maxGIDs)
	}
	port, gids, selected, err := queryPortGIDs(applerdma.RDMAContext(dev.handle), maxGIDs)
	if err != nil {
		return PortInfo{}, err
	}
	info := PortInfo{
		Device:           dev.name,
		PortNum:          1,
		LID:              port.LID,
		GIDTableLength:   int(port.GIDTblLen),
		GIDScanLimit:     maxGIDs,
		SelectedGIDIndex: selected,
		GIDs:             make([]GIDEntry, 0, len(gids)),
	}
	for _, entry := range gids {
		gid := [16]byte(entry.gid)
		info.GIDs = append(info.GIDs, GIDEntry{
			Index:      entry.index,
			GID:        gid,
			IPv4Mapped: isIPv4MappedGID(entry.gid),
			Zero:       gid == ([16]byte{}),
		})
	}
	return info, nil
}

func InitQueuePair(qp *QueuePair) error {
	if qp == nil || qp.handle == 0 {
		return fmt.Errorf("change queue pair to INIT: nil queue pair")
	}
	attr := applerdma.IbvQPAttr{
		QPState:       applerdma.IBV_QPS_INIT,
		PortNum:       1,
		PKeyIndex:     0,
		QPAccessFlags: applerdma.IBV_ACCESS_LOCAL_WRITE | applerdma.IBV_ACCESS_REMOTE_READ | applerdma.IBV_ACCESS_REMOTE_WRITE,
	}
	mask := applerdma.IBV_QP_STATE | applerdma.IBV_QP_PKEY_INDEX | applerdma.IBV_QP_PORT | applerdma.IBV_QP_ACCESS_FLAGS
	return modifyQueuePair(qp, &attr, mask, "INIT")
}

func ReadyToReceive(qp *QueuePair, local, remote Destination) error {
	if qp == nil || qp.handle == 0 {
		return fmt.Errorf("change queue pair to RTR: nil queue pair")
	}
	attr := applerdma.IbvQPAttr{
		QPState:   applerdma.IBV_QPS_RTR,
		PathMTU:   applerdma.IBV_MTU_1024,
		RQPSN:     remote.PSN,
		DestQPNum: remote.QPN,
		AHAttr: applerdma.IbvAHAttr{
			DLID:    remote.LID,
			PortNum: 1,
		},
	}
	if remote.GID != ([16]byte{}) {
		gidIndex := local.GIDIndex
		if gidIndex < 0 || gidIndex > 255 {
			return fmt.Errorf("local gid index %d out of uint8 range", gidIndex)
		}
		attr.AHAttr.IsGlobal = 1
		attr.AHAttr.GRH.HopLimit = 1
		attr.AHAttr.GRH.DGID = applerdma.IbvGID(remote.GID)
		attr.AHAttr.GRH.SGIDIndex = uint8(gidIndex)
	}
	mask := ReadyToReceiveMask()
	return modifyQueuePair(qp, &attr, mask, "RTR")
}

func ReadyToSend(qp *QueuePair, psn uint32) error {
	if qp == nil || qp.handle == 0 {
		return fmt.Errorf("change queue pair to RTS: nil queue pair")
	}
	attr := applerdma.IbvQPAttr{
		QPState: applerdma.IBV_QPS_RTS,
		SQPSN:   psn,
	}
	mask := applerdma.IBV_QP_STATE | applerdma.IBV_QP_SQ_PSN
	return modifyQueuePair(qp, &attr, mask, "RTS")
}

func modifyQueuePair(qp *QueuePair, attr *applerdma.IbvQPAttr, mask int, state string) error {
	rc, err := applerdma.Ibv_modify_qp(applerdma.RDMAQP(qp.handle), uintptr(unsafe.Pointer(attr)), mask)
	if err != nil {
		return fmt.Errorf("change queue pair to %s: %w mask=0x%x", state, err, mask)
	}
	if rc != 0 {
		return fmt.Errorf("change queue pair to %s: %s mask=0x%x", state, xrdma.ErrnoText(rc), mask)
	}
	return nil
}

func ReadyToReceiveMask() int {
	return applerdma.IBV_QP_STATE | applerdma.IBV_QP_AV | applerdma.IBV_QP_PATH_MTU | applerdma.IBV_QP_DEST_QPN | applerdma.IBV_QP_RQ_PSN
}

func localPortGID(qp *QueuePair) (applerdma.IbvPortAttr, applerdma.IbvGID, int, error) {
	if qp == nil || qp.handle == 0 || qp.pd == nil || qp.pd.dev == nil {
		return applerdma.IbvPortAttr{}, applerdma.IbvGID{}, 0, fmt.Errorf("local destination: nil queue pair")
	}
	port, gids, selected, err := queryPortGIDs(applerdma.RDMAContext(qp.pd.dev.handle), 0)
	if err != nil {
		return applerdma.IbvPortAttr{}, applerdma.IbvGID{}, 0, err
	}
	for _, entry := range gids {
		if entry.index == selected {
			return port, entry.gid, selected, nil
		}
	}
	return port, applerdma.IbvGID{}, selected, nil
}

type portGIDEntry struct {
	index int
	gid   applerdma.IbvGID
}

func queryPortGIDs(ctx applerdma.RDMAContext, maxGIDs int) (applerdma.IbvPortAttr, []portGIDEntry, int, error) {
	var port applerdma.IbvPortAttr
	if rc, err := applerdma.Ibv_query_port(ctx, 1, uintptr(unsafe.Pointer(&port))); err != nil {
		return applerdma.IbvPortAttr{}, nil, 0, fmt.Errorf("query port: %w", err)
	} else if rc != 0 {
		return applerdma.IbvPortAttr{}, nil, 0, fmt.Errorf("query port: errno %d", rc)
	}

	n := int(port.GIDTblLen)
	if maxGIDs > 0 && maxGIDs < n {
		n = maxGIDs
	}
	var gids []portGIDEntry
	selected := 0
	haveSelected := false
	selectedIPv4 := false
	for i := 0; i < n; i++ {
		var candidate applerdma.IbvGID
		rc, err := applerdma.Ibv_query_gid(ctx, 1, i, uintptr(unsafe.Pointer(&candidate)))
		if err != nil || rc != 0 {
			continue
		}
		gids = append(gids, portGIDEntry{index: i, gid: candidate})
		if !haveSelected {
			selected = i
			haveSelected = true
		}
		if isIPv4MappedGID(candidate) && !selectedIPv4 {
			selected = i
			selectedIPv4 = true
		}
	}
	return port, gids, selected, nil
}

func isIPv4MappedGID(gid applerdma.IbvGID) bool {
	for i := 0; i < 10; i++ {
		if gid[i] != 0 {
			return false
		}
	}
	return gid[10] == 0xff && gid[11] == 0xff
}

func RegisterMemory(pd *ProtectionDomain, buf []byte) (*MemoryRegion, error) {
	if pd == nil || pd.handle == 0 {
		return nil, fmt.Errorf("register memory: nil protection domain")
	}
	if len(buf) == 0 {
		return nil, fmt.Errorf("register memory: empty buffer")
	}
	aligned := pageAligned(buf)
	mr, err := applerdma.Ibv_reg_mr(applerdma.RDMAPD(pd.handle), uintptr(unsafe.Pointer(&aligned[0])), uintptr(len(aligned)), applerdma.IBV_ACCESS_LOCAL_WRITE|applerdma.IBV_ACCESS_REMOTE_WRITE|applerdma.IBV_ACCESS_REMOTE_READ)
	if err != nil {
		return nil, fmt.Errorf("register memory: %w", err)
	}
	if mr == 0 {
		return nil, fmt.Errorf("register memory: returned nil handle")
	}
	return &MemoryRegion{
		pd:     pd,
		handle: uintptr(mr),
		buf:    aligned,
		lkey:   applerdma.Ibv_mr_lkey(mr),
		rkey:   applerdma.Ibv_mr_rkey(mr),
	}, nil
}

func NewMemoryRegion(pd *ProtectionDomain, size int) (*MemoryRegion, error) {
	if pd == nil || pd.handle == 0 {
		return nil, fmt.Errorf("alloc memory region: nil protection domain")
	}
	if size <= 0 {
		return nil, fmt.Errorf("alloc memory region: size %d must be positive", size)
	}
	buf, err := syscall.Mmap(-1, 0, roundPage(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		return nil, fmt.Errorf("alloc memory region: mmap: %w", err)
	}
	mr, err := registerMappedMemory(pd, buf)
	if err != nil {
		_ = syscall.Munmap(buf)
		return nil, err
	}
	mr.mapped = true
	return mr, nil
}

func registerMappedMemory(pd *ProtectionDomain, buf []byte) (*MemoryRegion, error) {
	mr, err := applerdma.Ibv_reg_mr(applerdma.RDMAPD(pd.handle), uintptr(unsafe.Pointer(&buf[0])), uintptr(len(buf)), applerdma.IBV_ACCESS_LOCAL_WRITE|applerdma.IBV_ACCESS_REMOTE_WRITE|applerdma.IBV_ACCESS_REMOTE_READ)
	if err != nil {
		return nil, fmt.Errorf("register memory: %w", err)
	}
	if mr == 0 {
		return nil, fmt.Errorf("register memory: returned nil handle")
	}
	return &MemoryRegion{
		pd:     pd,
		handle: uintptr(mr),
		buf:    buf,
		lkey:   applerdma.Ibv_mr_lkey(mr),
		rkey:   applerdma.Ibv_mr_rkey(mr),
	}, nil
}

func (m *MemoryRegion) Close() error {
	if m == nil || m.handle == 0 {
		return nil
	}
	var err error
	m.once.Do(func() {
		rc, e := applerdma.Ibv_dereg_mr(applerdma.RDMAMR(m.handle))
		if e != nil {
			err = fmt.Errorf("dereg memory: %w", e)
			return
		}
		if rc != 0 {
			err = fmt.Errorf("dereg memory: errno %d", rc)
			return
		}
		if m.mapped {
			if e := syscall.Munmap(m.buf); e != nil && err == nil {
				err = fmt.Errorf("unmap memory: %w", e)
				return
			}
		}
		m.handle = 0
		m.buf = nil
	})
	return err
}

func PostSend(qp *QueuePair, mr *MemoryRegion, offset, length int, id uint64) error {
	return post(qp, mr, offset, length, id, true)
}

func PostRecv(qp *QueuePair, mr *MemoryRegion, offset, length int, id uint64) error {
	return post(qp, mr, offset, length, id, false)
}

// PostWrite posts an RDMA write from mr[offset:offset+length] to remoteAddr.
func PostWrite(qp *QueuePair, mr *MemoryRegion, offset, length int, remoteAddr uint64, rkey uint32, id uint64) error {
	if qp == nil || qp.handle == 0 {
		return fmt.Errorf("post write: nil queue pair")
	}
	if mr == nil || mr.handle == 0 {
		return fmt.Errorf("post write: nil memory region")
	}
	if offset < 0 || length < 0 || offset+length > len(mr.buf) {
		return fmt.Errorf("post write: range [%d,%d) outside buffer length %d", offset, offset+length, len(mr.buf))
	}
	if length == 0 {
		return nil
	}
	sge := applerdma.IbvSGE{
		Addr:   uint64(uintptr(unsafe.Pointer(&mr.buf[offset]))),
		Length: uint32(length),
		LKey:   mr.lkey,
	}
	wr := applerdma.IbvSendWR{
		WRID:      id,
		SGList:    &sge,
		NumSGE:    1,
		Opcode:    applerdma.IBV_WR_RDMA_WRITE,
		SendFlags: applerdma.IBV_SEND_SIGNALED,
	}
	binary.LittleEndian.PutUint64(wr.WR[0:8], remoteAddr)
	binary.LittleEndian.PutUint32(wr.WR[8:12], rkey)
	var bad *applerdma.IbvSendWR
	poster := qp.poster.(applerdma.IbvQPPoster)
	if rc := poster.PostSend(&wr, &bad); rc != 0 {
		return fmt.Errorf("post write: errno %d", rc)
	}
	return nil
}

func post(qp *QueuePair, mr *MemoryRegion, offset, length int, id uint64, send bool) error {
	if qp == nil || qp.handle == 0 {
		return fmt.Errorf("post work request: nil queue pair")
	}
	if mr == nil || mr.handle == 0 {
		return fmt.Errorf("post work request: nil memory region")
	}
	if offset < 0 || length < 0 || offset+length > len(mr.buf) {
		return fmt.Errorf("post work request: range [%d,%d) outside buffer length %d", offset, offset+length, len(mr.buf))
	}
	if length == 0 {
		return nil
	}
	sge := applerdma.IbvSGE{
		Addr:   uint64(uintptr(unsafe.Pointer(&mr.buf[offset]))),
		Length: uint32(length),
		LKey:   mr.lkey,
	}
	poster := qp.poster.(applerdma.IbvQPPoster)
	if send {
		wr := applerdma.IbvSendWR{
			WRID:      id,
			SGList:    &sge,
			NumSGE:    1,
			Opcode:    applerdma.IBV_WR_SEND,
			SendFlags: applerdma.IBV_SEND_SIGNALED,
		}
		var bad *applerdma.IbvSendWR
		if rc := poster.PostSend(&wr, &bad); rc != 0 {
			return fmt.Errorf("post send: errno %d", rc)
		}
		return nil
	}
	wr := applerdma.IbvRecvWR{
		WRID:   id,
		SGList: &sge,
		NumSGE: 1,
	}
	var bad *applerdma.IbvRecvWR
	if rc := poster.PostRecv(&wr, &bad); rc != 0 {
		return fmt.Errorf("post recv: errno %d", rc)
	}
	return nil
}

func PollCompletion(ctx context.Context, cq *CompletionQueue) ([]WorkRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cq == nil || cq.handle == 0 {
		return nil, fmt.Errorf("poll completion queue: nil completion queue")
	}
	var wc applerdma.IbvWC
	spins := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, err := applerdma.Ibv_poll_cq(applerdma.RDMACQ(cq.handle), 1, &wc)
		if err != nil {
			return nil, fmt.Errorf("poll completion queue: %w", err)
		}
		if n < 0 {
			return nil, fmt.Errorf("poll completion queue: errno %d", n)
		}
		if n > 0 {
			if wc.Status != applerdma.IBV_WC_SUCCESS {
				return nil, fmt.Errorf("work completion opcode %d status %d", wc.Opcode, wc.Status)
			}
			return []WorkRequest{{
				ID:     wc.WRID,
				Opcode: int(wc.Opcode),
				Bytes:  int(wc.ByteLen),
				Status: int(wc.Status),
			}}, nil
		}
		spins++
		if spins < 64 {
			runtime.Gosched()
			continue
		}
		time.Sleep(50 * time.Microsecond)
	}
}

func pageAligned(buf []byte) []byte {
	page := uintptr(osPageSize())
	addr := uintptr(unsafe.Pointer(&buf[0]))
	if addr%page == 0 {
		return buf
	}
	raw := make([]byte, len(buf)+int(page)-1)
	base := uintptr(unsafe.Pointer(&raw[0]))
	off := int((page - base%page) % page)
	aligned := raw[off : off+len(buf)]
	copy(aligned, buf)
	return aligned
}

func osPageSize() int {
	return 16 * 1024
}

func roundPage(n int) int {
	page := osPageSize()
	if n%page == 0 {
		return n
	}
	return n + page - n%page
}
