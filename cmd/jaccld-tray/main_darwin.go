//go:build darwin

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ebitengine/purego"
	"github.com/tmc/apple/objc"
	"github.com/tmc/gojaccl/internal/ipc"
)

const (
	nsApplicationActivationPolicyAccessory = 1
	nsVariableStatusItemLength             = -1.0
	nsDefaultRunLoopMode                   = "NSDefaultRunLoopMode"
	nsEventTrackingRunLoopMode             = "NSEventTrackingRunLoopMode"
)

var (
	delegateSerial uint64
	appDelegate    objc.ID
	cfRunLoopWake  func(uintptr)
)

type trayApp struct {
	app        objc.ID
	statusBar  objc.ID
	statusItem objc.ID
	menu       objc.ID
	delegateID objc.ID
	menuOpen   bool
	menuLines  []objc.ID

	socket   string
	interval time.Duration

	mu       sync.Mutex
	snap     snapshot
	closed   chan struct{}
	closeMux sync.Once
}

func main() {
	runtime.LockOSThread()

	socket := flag.String("socket", ipc.DefaultSocket, "jaccld Unix-domain socket path")
	interval := flag.Duration("interval", 5*time.Second, "poll interval")
	flag.Parse()

	if err := loadAppKit(); err != nil {
		log.Fatal(err)
	}

	app := objc.Send[objc.ID](objc.ID(objc.GetClass("NSApplication")), objc.Sel("sharedApplication"))
	objc.Send[struct{}](app, objc.Sel("setActivationPolicy:"), nsApplicationActivationPolicyAccessory)

	tray, err := newTrayApp(app, *socket, *interval)
	if err != nil {
		log.Fatal(err)
	}
	appDelegate, err = newApplicationDelegate(tray)
	if err != nil {
		log.Fatal(err)
	}
	objc.Send[struct{}](app, objc.Sel("setDelegate:"), appDelegate)
	tray.start()
	objc.Send[struct{}](app, objc.Sel("run"))
}

func loadAppKit() error {
	loadCoreFoundation()
	for _, path := range []string{
		"/System/Library/Frameworks/AppKit.framework/AppKit",
		"/usr/lib/libAppKit.dylib",
	} {
		if _, err := purego.Dlopen(path, purego.RTLD_LAZY|purego.RTLD_GLOBAL); err == nil {
			return nil
		}
	}
	return fmt.Errorf("load AppKit framework")
}

func loadCoreFoundation() {
	for _, path := range []string{
		"/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation",
		"/usr/lib/libCoreFoundation.dylib",
	} {
		h, err := purego.Dlopen(path, purego.RTLD_LAZY|purego.RTLD_GLOBAL)
		if err != nil {
			continue
		}
		sym, err := purego.Dlsym(h, "CFRunLoopWakeUp")
		if err == nil && sym != 0 {
			purego.RegisterFunc(&cfRunLoopWake, sym)
		}
		return
	}
}

func newApplicationDelegate(tray *trayApp) (objc.ID, error) {
	className := fmt.Sprintf("JACCLDTrayApplicationDelegate_%d", atomic.AddUint64(&delegateSerial, 1))
	cls, err := objc.RegisterClass(
		className,
		objc.GetClass("NSObject"),
		nil, nil,
		[]objc.MethodDef{
			{Cmd: objc.RegisterName("applicationWillTerminate:"), Fn: func(_ objc.ID, _ objc.SEL, _ objc.ID) {
				tray.close()
			}},
		},
	)
	if err != nil {
		return 0, fmt.Errorf("register application delegate: %w", err)
	}
	return objc.Send[objc.ID](objc.Send[objc.ID](objc.ID(cls), objc.Sel("alloc")), objc.Sel("init")), nil
}

func newTrayApp(app objc.ID, socket string, interval time.Duration) (*trayApp, error) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	t := &trayApp{
		app:      app,
		socket:   socket,
		interval: interval,
		closed:   make(chan struct{}),
		snap: snapshot{
			Socket:    socket,
			CheckedAt: time.Now(),
		},
	}
	if err := t.registerDelegate(); err != nil {
		return nil, err
	}
	t.statusBar = objc.Send[objc.ID](objc.ID(objc.GetClass("NSStatusBar")), objc.Sel("systemStatusBar"))
	t.statusItem = objc.Send[objc.ID](t.statusBar, objc.Sel("statusItemWithLength:"), float64(nsVariableStatusItemLength))
	objc.Send[struct{}](t.statusItem, objc.Sel("setAutosaveName:"), objc.String("com.tmc.gojaccl.jaccld-tray"))
	objc.Send[struct{}](t.statusItem, objc.Sel("setVisible:"), true)

	t.menu = objc.Send[objc.ID](objc.Send[objc.ID](objc.ID(objc.GetClass("NSMenu")), objc.Sel("alloc")), objc.Sel("initWithTitle:"), objc.String("jaccld"))
	objc.Send[struct{}](t.menu, objc.Sel("setDelegate:"), t.delegateID)
	objc.Send[struct{}](t.statusItem, objc.Sel("setMenu:"), t.menu)
	t.refreshStatusItem()
	return t, nil
}

func (t *trayApp) registerDelegate() error {
	className := fmt.Sprintf("JACCLDTrayDelegate_%d", atomic.AddUint64(&delegateSerial, 1))
	cls, err := objc.RegisterClass(
		className,
		objc.GetClass("NSObject"),
		nil, nil,
		[]objc.MethodDef{
			{Cmd: objc.RegisterName("applyRefresh:"), Fn: t.handleApplyRefresh},
			{Cmd: objc.RegisterName("menuNeedsUpdate:"), Fn: t.handleMenuNeedsUpdate},
			{Cmd: objc.RegisterName("menuWillOpen:"), Fn: t.handleMenuWillOpen},
			{Cmd: objc.RegisterName("menuDidClose:"), Fn: t.handleMenuDidClose},
			{Cmd: objc.RegisterName("refreshNow:"), Fn: t.handleRefreshNow},
			{Cmd: objc.RegisterName("quit:"), Fn: t.handleQuit},
		},
	)
	if err != nil {
		return fmt.Errorf("register status item delegate: %w", err)
	}
	t.delegateID = objc.Send[objc.ID](objc.ID(cls), objc.Sel("alloc"))
	t.delegateID = objc.Send[objc.ID](t.delegateID, objc.Sel("init"))
	return nil
}

func (t *trayApp) start() {
	t.refresh()
	go func() {
		ticker := time.NewTicker(t.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				t.refresh()
			case <-t.closed:
				return
			}
		}
	}()
}

func (t *trayApp) close() {
	t.closeMux.Do(func() {
		close(t.closed)
		if t.statusItem != 0 && t.statusBar != 0 {
			objc.Send[struct{}](t.statusBar, objc.Sel("removeStatusItem:"), t.statusItem)
		}
	})
}

func (t *trayApp) refresh() {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	s := fetchSnapshot(ctx, t.socket)
	cancel()

	t.mu.Lock()
	t.snap = s
	t.mu.Unlock()

	t.onMain()
}

func (t *trayApp) onMain() {
	modes := nsStringArray(nsDefaultRunLoopMode, nsEventTrackingRunLoopMode)
	objc.Send[struct{}](
		t.delegateID,
		objc.Sel("performSelectorOnMainThread:withObject:waitUntilDone:modes:"),
		objc.Sel("applyRefresh:"),
		objc.ID(0),
		false,
		modes,
	)
	runLoop := objc.Send[objc.ID](objc.ID(objc.GetClass("NSRunLoop")), objc.Sel("mainRunLoop"))
	if cfRunLoopWake != nil {
		rl := objc.Send[objc.ID](runLoop, objc.Sel("getCFRunLoop"))
		cfRunLoopWake(uintptr(rl))
	}
}

func nsStringArray(values ...string) objc.ID {
	array := objc.Send[objc.ID](objc.ID(objc.GetClass("NSMutableArray")), objc.Sel("arrayWithCapacity:"), uint(len(values)))
	for _, value := range values {
		objc.Send[struct{}](array, objc.Sel("addObject:"), objc.String(value))
	}
	return array
}

func (t *trayApp) currentSnapshot() snapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.snap
}

func (t *trayApp) refreshStatusItem() {
	if t.statusItem == 0 {
		return
	}
	s := t.currentSnapshot()
	button := objc.Send[objc.ID](t.statusItem, objc.Sel("button"))
	objc.Send[struct{}](button, objc.Sel("setTitle:"), objc.String(s.title()))
	objc.Send[struct{}](button, objc.Sel("setToolTip:"), objc.String(s.tooltip()))
	objc.Send[struct{}](t.menu, objc.Sel("setTitle:"), objc.String("jaccld"))
}

func (t *trayApp) handleApplyRefresh(_ objc.ID, _ objc.SEL, _ objc.ID) {
	t.refreshStatusItem()
	if t.menuOpen {
		t.refreshOpenMenu()
	}
}

func (t *trayApp) handleMenuNeedsUpdate(_ objc.ID, _ objc.SEL, menuID objc.ID) {
	t.rebuildMenu(menuID)
}

func (t *trayApp) rebuildMenu(menu objc.ID) {
	objc.Send[struct{}](menu, objc.Sel("removeAllItems"))
	t.menuLines = t.menuLines[:0]

	addDisabledItem(menu, "jaccld")
	addDisabledItem(menu, "Poll: "+t.interval.String())
	addSeparator(menu)
	for _, line := range t.currentSnapshot().menuLines() {
		t.menuLines = append(t.menuLines, addDisabledItem(menu, line))
	}
	addSeparator(menu)
	addActionItem(menu, "Refresh Now", "refreshNow:", t.delegateID)
	addActionItem(menu, "Quit", "quit:", t.delegateID)
}

func (t *trayApp) refreshOpenMenu() {
	lines := t.currentSnapshot().menuLines()
	if len(lines) != len(t.menuLines) {
		t.rebuildMenu(t.menu)
		objc.Send[struct{}](t.menu, objc.Sel("update"))
		return
	}
	for i, line := range lines {
		item := t.menuLines[i]
		objc.Send[struct{}](item, objc.Sel("setTitle:"), objc.String(line))
		objc.Send[struct{}](t.menu, objc.Sel("itemChanged:"), item)
	}
	objc.Send[struct{}](t.menu, objc.Sel("update"))
}

func (t *trayApp) handleMenuWillOpen(_ objc.ID, _ objc.SEL, _ objc.ID) {
	t.menuOpen = true
	go t.refresh()
}

func (t *trayApp) handleMenuDidClose(_ objc.ID, _ objc.SEL, _ objc.ID) {
	t.menuOpen = false
}

func (t *trayApp) handleRefreshNow(_ objc.ID, _ objc.SEL, _ objc.ID) {
	go t.refresh()
}

func (t *trayApp) handleQuit(_ objc.ID, _ objc.SEL, _ objc.ID) {
	t.close()
	objc.Send[struct{}](t.app, objc.Sel("terminate:"), objc.ID(0))
}

func addDisabledItem(menu objc.ID, title string) objc.ID {
	item := newMenuItem(title, 0)
	objc.Send[struct{}](item, objc.Sel("setEnabled:"), false)
	objc.Send[struct{}](menu, objc.Sel("addItem:"), item)
	return item
}

func addActionItem(menu objc.ID, title, selector string, target objc.ID) {
	item := newMenuItem(title, objc.Sel(selector))
	objc.Send[struct{}](item, objc.Sel("setTarget:"), target)
	objc.Send[struct{}](menu, objc.Sel("addItem:"), item)
}

func addSeparator(menu objc.ID) {
	item := objc.Send[objc.ID](objc.ID(objc.GetClass("NSMenuItem")), objc.Sel("separatorItem"))
	objc.Send[struct{}](menu, objc.Sel("addItem:"), item)
}

func newMenuItem(title string, action objc.SEL) objc.ID {
	item := objc.Send[objc.ID](objc.ID(objc.GetClass("NSMenuItem")), objc.Sel("alloc"))
	return objc.Send[objc.ID](item, objc.Sel("initWithTitle:action:keyEquivalent:"), objc.String(title), action, objc.String(""))
}
