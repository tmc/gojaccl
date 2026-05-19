package jaccl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCollectiveLengthValidation(t *testing.T) {
	t.Run("AllSumSameLength", func(t *testing.T) {
		g := newFakeGroup(0, 1, newFakeNetwork(1))
		dst := []int32{0, 0}
		if err := AllSum(context.Background(), g, dst, []int32{1, 2}); err != nil {
			t.Fatal(err)
		}
		if want := []int32{1, 2}; !reflect.DeepEqual(dst, want) {
			t.Fatalf("dst = %v, want %v", dst, want)
		}
	})
	t.Run("AllSumRejectsMismatchBeforeBackend", func(t *testing.T) {
		g := newFakeGroup(0, 1, newFakeNetwork(1))
		if err := AllSum(context.Background(), g, []int32{0}, []int32{1, 2}); err == nil {
			t.Fatal("AllSum mismatch = nil")
		}
	})
	t.Run("AllMaxRejectsMismatchBeforeBackend", func(t *testing.T) {
		g := newFakeGroup(0, 1, newFakeNetwork(1))
		if err := AllMax(context.Background(), g, []int32{0}, []int32{1, 2}); err == nil {
			t.Fatal("AllMax mismatch = nil")
		}
	})
	t.Run("AllMinRejectsMismatchBeforeBackend", func(t *testing.T) {
		g := newFakeGroup(0, 1, newFakeNetwork(1))
		if err := AllMin(context.Background(), g, []int32{0}, []int32{1, 2}); err == nil {
			t.Fatal("AllMin mismatch = nil")
		}
	})
	t.Run("AllGatherRequiresSizeTimesInput", func(t *testing.T) {
		net := newFakeNetwork(2)
		g0 := newFakeGroup(0, 2, net)
		g1 := newFakeGroup(1, 2, net)
		dst0 := make([]int32, 2)
		dst1 := make([]int32, 2)
		errc := make(chan error, 2)
		go func() { errc <- AllGather(context.Background(), g0, dst0, []int32{1}) }()
		go func() { errc <- AllGather(context.Background(), g1, dst1, []int32{2}) }()
		for i := 0; i < 2; i++ {
			if err := <-errc; err != nil {
				t.Fatal(err)
			}
		}
		if want := []int32{1, 2}; !reflect.DeepEqual(dst0, want) || !reflect.DeepEqual(dst1, want) {
			t.Fatalf("allgather dst0=%v dst1=%v, want %v", dst0, dst1, want)
		}
	})
	t.Run("AllGatherRejectsMismatchBeforeBackend", func(t *testing.T) {
		g := newFakeGroup(0, 2, newFakeNetwork(2))
		if err := AllGather(context.Background(), g, []int32{0}, []int32{1}); err == nil {
			t.Fatal("AllGather mismatch = nil")
		}
	})
	t.Run("AllGatherRejectsAliasedSlices", func(t *testing.T) {
		g := newFakeGroup(0, 2, newFakeNetwork(2))
		buf := []int32{1, 2}
		if err := AllGather(context.Background(), g, buf, buf[:1]); err == nil {
			t.Fatal("AllGather alias = nil")
		}
	})
	t.Run("NilSlicesAllowedWhenLengthsMatch", func(t *testing.T) {
		g := newFakeGroup(0, 1, newFakeNetwork(1))
		if err := AllSum[int32](context.Background(), g, nil, nil); err != nil {
			t.Fatal(err)
		}
		if err := AllGather[int32](context.Background(), g, nil, nil); err != nil {
			t.Fatal(err)
		}
	})
}

func TestCollectiveSimulatedLineTopology(t *testing.T) {
	t.Run("AllSumThreeRankLine", func(t *testing.T) {
		net := newFakeNetwork(3)
		devices := lineDeviceMatrix("left", "right")
		g0 := newFakeTopologyGroup(0, devices, net, true)
		g1 := newFakeTopologyGroup(1, devices, net, true)
		g2 := newFakeTopologyGroup(2, devices, net, true)
		dst := [][]int32{{0}, {0}, {0}}
		errc := make(chan error, 3)
		go func() { errc <- AllSum(context.Background(), g0, dst[0], []int32{1}) }()
		go func() { errc <- AllSum(context.Background(), g1, dst[1], []int32{2}) }()
		go func() { errc <- AllSum(context.Background(), g2, dst[2], []int32{3}) }()
		for i := 0; i < 3; i++ {
			if err := <-errc; err != nil {
				t.Fatal(err)
			}
		}
		for rank, got := range dst {
			if want := []int32{6}; !reflect.DeepEqual(got, want) {
				t.Fatalf("rank %d AllSum = %v, want %v", rank, got, want)
			}
		}
	})
	t.Run("AllGatherThreeRankLine", func(t *testing.T) {
		net := newFakeNetwork(3)
		devices := lineDeviceMatrix("left", "right")
		g0 := newFakeTopologyGroup(0, devices, net, true)
		g1 := newFakeTopologyGroup(1, devices, net, true)
		g2 := newFakeTopologyGroup(2, devices, net, true)
		dst := [][]int32{make([]int32, 3), make([]int32, 3), make([]int32, 3)}
		errc := make(chan error, 3)
		go func() { errc <- AllGather(context.Background(), g0, dst[0], []int32{1}) }()
		go func() { errc <- AllGather(context.Background(), g1, dst[1], []int32{2}) }()
		go func() { errc <- AllGather(context.Background(), g2, dst[2], []int32{3}) }()
		for i := 0; i < 3; i++ {
			if err := <-errc; err != nil {
				t.Fatal(err)
			}
		}
		for rank, got := range dst {
			if want := []int32{1, 2, 3}; !reflect.DeepEqual(got, want) {
				t.Fatalf("rank %d AllGather = %v, want %v", rank, got, want)
			}
		}
	})
	t.Run("InjectedEdgeFailure", func(t *testing.T) {
		net := newFakeNetwork(3)
		net.failBidirectional(1, 2, errors.New("simulated link down"))
		devices := lineDeviceMatrix("left", "right")
		g0 := newFakeTopologyGroup(0, devices, net, true)
		dst := []int32{0}
		err := AllSum(context.Background(), g0, dst, []int32{1})
		if err == nil || !strings.Contains(err.Error(), "simulated link down") {
			t.Fatalf("AllSum edge failure = %v, want simulated link down", err)
		}
	})
}

func TestCollectiveSimulatedConnectedTopology(t *testing.T) {
	net := newFakeNetwork(4)
	devices := fakePartialMatrix()
	groups := []*Group{
		newFakeTopologyGroup(0, devices, net, true),
		newFakeTopologyGroup(1, devices, net, true),
		newFakeTopologyGroup(2, devices, net, true),
		newFakeTopologyGroup(3, devices, net, true),
	}
	dst := [][]int32{{0}, {0}, {0}, {0}}
	errc := make(chan error, len(groups))
	for rank, g := range groups {
		rank, g := rank, g
		go func() {
			errc <- AllSum(context.Background(), g, dst[rank], []int32{int32(rank + 1)})
		}()
	}
	for range groups {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
	for rank, got := range dst {
		if want := []int32{10}; !reflect.DeepEqual(got, want) {
			t.Fatalf("rank %d AllSum = %v, want %v", rank, got, want)
		}
	}
}

func TestCollectiveContextCancellation(t *testing.T) {
	tests := []struct {
		name string
		call func(context.Context, *Group) error
	}{
		{"AllSum", func(ctx context.Context, g *Group) error { return AllSum(ctx, g, []int32{0}, []int32{1}) }},
		{"AllMax", func(ctx context.Context, g *Group) error { return AllMax(ctx, g, []int32{0}, []int32{1}) }},
		{"AllMin", func(ctx context.Context, g *Group) error { return AllMin(ctx, g, []int32{0}, []int32{1}) }},
		{"AllGather", func(ctx context.Context, g *Group) error { return AllGather(ctx, g, make([]int32, 2), []int32{1}) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := newFakeGroup(0, 2, newFakeNetwork(2))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			if err := tt.call(ctx, g); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("%s = %v, want deadline", tt.name, err)
			}
		})
	}
}

func TestCollectiveZeroLength(t *testing.T) {
	g := newFakeGroup(0, 1, newFakeNetwork(1))
	t.Run("AllSum", func(t *testing.T) {
		if err := AllSum[int32](context.Background(), g, nil, nil); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("AllMax", func(t *testing.T) {
		if err := AllMax[int32](context.Background(), g, nil, nil); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("AllMin", func(t *testing.T) {
		if err := AllMin[int32](context.Background(), g, nil, nil); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("AllGather", func(t *testing.T) {
		if err := AllGather[int32](context.Background(), g, nil, nil); err != nil {
			t.Fatal(err)
		}
	})
}

func TestCollectiveZeroLengthUsesGroupState(t *testing.T) {
	t.Run("ClosedGroup", func(t *testing.T) {
		g := newFakeGroup(0, 2, newFakeNetwork(2))
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if err := AllSum[int32](context.Background(), g, nil, nil); !errors.Is(err, ErrClosed) {
			t.Fatalf("AllSum closed = %v, want ErrClosed", err)
		}
		if err := AllGather[int32](context.Background(), g, nil, nil); !errors.Is(err, ErrClosed) {
			t.Fatalf("AllGather closed = %v, want ErrClosed", err)
		}
	})
	t.Run("CanceledContext", func(t *testing.T) {
		g := newFakeGroup(0, 2, newFakeNetwork(2))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := AllSum[int32](ctx, g, nil, nil); !errors.Is(err, context.Canceled) {
			t.Fatalf("AllSum canceled = %v, want context.Canceled", err)
		}
		if err := AllGather[int32](ctx, g, nil, nil); !errors.Is(err, context.Canceled) {
			t.Fatalf("AllGather canceled = %v, want context.Canceled", err)
		}
	})
}

func TestCollectiveInPlaceReductions(t *testing.T) {
	t.Run("AllSum", func(t *testing.T) {
		g := newFakeGroup(0, 1, newFakeNetwork(1))
		v := []int32{1, 2}
		if err := AllSum(context.Background(), g, v, v); err != nil {
			t.Fatal(err)
		}
		if want := []int32{1, 2}; !reflect.DeepEqual(v, want) {
			t.Fatalf("in-place sum = %v, want %v", v, want)
		}
	})
	t.Run("AllMax", func(t *testing.T) {
		g := newFakeGroup(0, 1, newFakeNetwork(1))
		v := []int32{1, 2}
		if err := AllMax(context.Background(), g, v, v); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("AllMin", func(t *testing.T) {
		g := newFakeGroup(0, 1, newFakeNetwork(1))
		v := []int32{1, 2}
		if err := AllMin(context.Background(), g, v, v); err != nil {
			t.Fatal(err)
		}
	})
}

func TestCollectiveElementDispatch(t *testing.T) {
	t.Run("Bool", func(t *testing.T) { checkAllSum(t, []bool{false}, []bool{true}, []bool{true}) })
	t.Run("Int8", func(t *testing.T) { checkAllSum(t, []int8{1}, []int8{2}, []int8{2}) })
	t.Run("Int16", func(t *testing.T) { checkAllSum(t, []int16{1}, []int16{2}, []int16{2}) })
	t.Run("Int32", func(t *testing.T) { checkAllSum(t, []int32{1}, []int32{2}, []int32{2}) })
	t.Run("Int64", func(t *testing.T) { checkAllSum(t, []int64{1}, []int64{2}, []int64{2}) })
	t.Run("Uint8", func(t *testing.T) { checkAllSum(t, []uint8{1}, []uint8{2}, []uint8{2}) })
	t.Run("Uint16", func(t *testing.T) { checkAllSum(t, []uint16{1}, []uint16{2}, []uint16{2}) })
	t.Run("Uint32", func(t *testing.T) { checkAllSum(t, []uint32{1}, []uint32{2}, []uint32{2}) })
	t.Run("Uint64", func(t *testing.T) { checkAllSum(t, []uint64{1}, []uint64{2}, []uint64{2}) })
	t.Run("Float32", func(t *testing.T) { checkAllSum(t, []float32{1}, []float32{2}, []float32{2}) })
	t.Run("Float64", func(t *testing.T) { checkAllSum(t, []float64{1}, []float64{2}, []float64{2}) })
	t.Run("Complex64", func(t *testing.T) { checkAllSum(t, []complex64{1}, []complex64{2}, []complex64{2}) })
}

func TestElementConstraint(t *testing.T) {
	t.Run("AcceptsSupportedTypes", func(t *testing.T) {
		acceptElement[bool]()
		acceptElement[int8]()
		acceptElement[int16]()
		acceptElement[int32]()
		acceptElement[int64]()
		acceptElement[uint8]()
		acceptElement[uint16]()
		acceptElement[uint32]()
		acceptElement[uint64]()
		acceptElement[float32]()
		acceptElement[float64]()
		acceptElement[complex64]()
	})
	rejects := map[string]string{
		"RejectsString":    `jaccl.AllSum(context.Background(), (*jaccl.Group)(nil), []string{}, []string{})`,
		"RejectsSlice":     `jaccl.AllSum(context.Background(), (*jaccl.Group)(nil), [][]byte{}, [][]byte{})`,
		"RejectsMap":       `jaccl.AllSum(context.Background(), (*jaccl.Group)(nil), []map[string]int{}, []map[string]int{})`,
		"RejectsPointer":   `jaccl.AllSum(context.Background(), (*jaccl.Group)(nil), []*int{}, []*int{})`,
		"RejectsInterface": `jaccl.AllSum(context.Background(), (*jaccl.Group)(nil), []any{}, []any{})`,
		"RejectsFloat16AliasUntilAdded": `type Float16 uint16
		jaccl.AllSum(context.Background(), (*jaccl.Group)(nil), []Float16{}, []Float16{})`,
		"RejectsBFloat16AliasUntilAdded": `type BFloat16 uint16
		jaccl.AllSum(context.Background(), (*jaccl.Group)(nil), []BFloat16{}, []BFloat16{})`,
	}
	for name, expr := range rejects {
		t.Run(name, func(t *testing.T) {
			assertDoesNotCompile(t, expr)
		})
	}
}

func checkAllSum[T Element](t *testing.T, dst, src, want []T) {
	t.Helper()
	g := newFakeGroup(0, 1, newFakeNetwork(1))
	if err := AllSum(context.Background(), g, dst, src); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(dst, want) {
		t.Fatalf("dst = %v, want %v", dst, want)
	}
}

func acceptElement[T Element]() {}

func assertDoesNotCompile(t *testing.T, body string) {
	t.Helper()
	dir := t.TempDir()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	appleVersion, appleReplace := appleModuleForCompileTest(t, wd)
	mod := fmt.Sprintf("module tmp\n\ngo 1.24\n\nrequire (\n\tgithub.com/tmc/gojaccl v0.0.0\n\tgithub.com/tmc/apple %s\n)\n\nreplace github.com/tmc/gojaccl => %s\n", appleVersion, filepath.ToSlash(wd))
	if appleReplace != "" {
		mod += "replace github.com/tmc/apple => " + filepath.ToSlash(appleReplace) + "\n"
	}
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(mod), 0o666); err != nil {
		t.Fatal(err)
	}
	src := `package main

import (
	"context"
	"github.com/tmc/gojaccl"
)

func main() {
	` + body + `
}
`
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(src), 0o666); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-mod=mod", ".")
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("snippet compiled unexpectedly:\n%s", out)
	}
	if !strings.Contains(string(out), "does not satisfy") && !strings.Contains(string(out), "not in") {
		t.Fatalf("compile failed for unexpected reason:\n%s", out)
	}
}

func appleModuleForCompileTest(t *testing.T, wd string) (version, replace string) {
	t.Helper()
	if dir := os.Getenv("JACCL_TEST_APPLE_REPLACE"); dir != "" {
		return "v0.0.0", dir
	}
	cmd := exec.Command("go", "list", "-m", "-json", "github.com/tmc/apple")
	cmd.Dir = wd
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list github.com/tmc/apple: %v", err)
	}
	var info struct {
		Version string
		Replace *struct {
			Dir string
		}
	}
	if err := json.Unmarshal(out, &info); err != nil {
		t.Fatalf("parse go list github.com/tmc/apple: %v", err)
	}
	if info.Version == "" {
		info.Version = "v0.0.0"
	}
	if info.Replace != nil {
		replace = info.Replace.Dir
	}
	return info.Version, replace
}
