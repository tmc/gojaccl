package rdma

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAvailable(t *testing.T) {
	t.Run("NeverPanics", func(t *testing.T) {
		_ = Available()
	})
	t.Run("FalseWhenBackendUnavailable", func(t *testing.T) {
		if !Available() {
			return
		}
		if _, err := OpenDevice("definitely-not-an-rdma-device"); err == nil {
			t.Fatal("OpenDevice(missing) = nil")
		}
	})
	t.Run("AllRequiredSymbolsPresent", func(t *testing.T) {
		if Available() {
			return
		}
		if _, err := OpenDevice(""); err == nil {
			t.Fatal("OpenDevice without available backend = nil")
		}
	})
}

func TestLoader(t *testing.T) {
	t.Run("OpenFailureIncludesDevice", func(t *testing.T) {
		_, err := OpenDevice("missing-device")
		if err == nil || !strings.Contains(err.Error(), "missing-device") {
			t.Fatalf("OpenDevice error = %v, want device name", err)
		}
	})
	t.Run("CloseIdempotent", func(t *testing.T) {
		var d Device
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestDevice(t *testing.T) {
	t.Run("OpenByPath", func(t *testing.T) {
		if !Available() {
			if _, err := OpenDevice(""); err == nil {
				t.Fatal("OpenDevice unavailable = nil")
			}
			return
		}
		if _, err := OpenDevice("missing-device"); err == nil {
			t.Fatal("OpenDevice(missing) = nil")
		}
	})
	t.Run("CloseReleasesContext", func(t *testing.T) {
		var d Device
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestProtectionDomain(t *testing.T) {
	t.Run("AllocFailure", func(t *testing.T) {
		if _, err := NewProtectionDomain(nil); err == nil {
			t.Fatal("NewProtectionDomain(nil) = nil")
		}
	})
	t.Run("Close", func(t *testing.T) {
		var pd ProtectionDomain
		if err := pd.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestCompletionQueue(t *testing.T) {
	t.Run("CreateFailure", func(t *testing.T) {
		if _, err := NewCompletionQueue(nil, 1); err == nil {
			t.Fatal("NewCompletionQueue(nil) = nil")
		}
	})
	t.Run("Close", func(t *testing.T) {
		var cq CompletionQueue
		if err := cq.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestQueuePair(t *testing.T) {
	t.Run("CreateFailure", func(t *testing.T) {
		if _, err := NewQueuePair(nil, nil); err == nil {
			t.Fatal("NewQueuePair(nil) = nil")
		}
	})
	t.Run("Close", func(t *testing.T) {
		var qp QueuePair
		if err := qp.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMemoryRegistration(t *testing.T) {
	t.Run("RegisterFailure", func(t *testing.T) {
		if _, err := RegisterMemory(nil, make([]byte, 16)); err == nil {
			t.Fatal("RegisterMemory(nil) = nil")
		}
	})
	t.Run("DeregisterOnClose", func(t *testing.T) {
		var mr MemoryRegion
		if err := mr.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestWorkRequests(t *testing.T) {
	t.Run("PostSendFailure", func(t *testing.T) {
		if err := PostSend(nil, nil, 0, 1, 1); err == nil {
			t.Fatal("PostSend(nil) = nil")
		}
	})
	t.Run("PostRecvFailure", func(t *testing.T) {
		if err := PostRecv(nil, nil, 0, 1, 1); err == nil {
			t.Fatal("PostRecv(nil) = nil")
		}
	})
}

func TestPollCompletion(t *testing.T) {
	t.Run("ContextCancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := PollCompletion(ctx, &CompletionQueue{}); !errors.Is(err, context.Canceled) {
			t.Fatalf("PollCompletion canceled = %v, want context.Canceled", err)
		}
	})
	t.Run("PollErrorIncludesCQ", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		if _, err := PollCompletion(ctx, nil); err == nil || !strings.Contains(err.Error(), "completion queue") {
			t.Fatalf("PollCompletion nil = %v, want completion queue error", err)
		}
	})
}

func TestPureGoBoundary(t *testing.T) {
	t.Run("DoesNotImportC", func(t *testing.T) {
		err := filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if strings.Contains(string(data), `import "C"`) {
				t.Fatalf("%s imports C", path)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("UsesAppleRDMABindingsBehindInternalBoundary", func(t *testing.T) {
		data, err := os.ReadFile("rdma_darwin_arm64.go")
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), "github.com/tmc/apple/rdma") {
			t.Fatal("darwin wrapper does not import apple rdma bindings")
		}
	})
}
