package jaccl

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
)

func Example() {
	old := backendFactory
	backendFactory = func(context.Context, Config) (backend, error) {
		return &fakeBackend{rank: 0, size: 1, net: newFakeNetwork(1)}, nil
	}
	defer func() { backendFactory = old }()

	g, _ := NewGroup(context.Background(), Config{
		Rank:        0,
		Coordinator: "127.0.0.1:1",
		Devices: [][][]string{
			{{}, {"rdma01"}},
			{{"rdma10"}, {}},
		},
	})
	fmt.Println(g.Rank(), g.Size())
	// Output: 0 2
}

func ExampleConfigFromEnv() {
	path := tempDevices()
	defer os.Remove(path)
	os.Setenv("JACCL_RANK", "0")
	os.Setenv("JACCL_COORDINATOR", "127.0.0.1:1")
	os.Setenv("JACCL_IBV_DEVICES", path)
	defer os.Unsetenv("JACCL_RANK")
	defer os.Unsetenv("JACCL_COORDINATOR")
	defer os.Unsetenv("JACCL_IBV_DEVICES")

	cfg, _ := ConfigFromEnv()
	fmt.Println(cfg.Rank, cfg.Coordinator)
	// Output: 0 127.0.0.1:1
}

func ExampleGroup_Barrier() {
	g := newFakeGroup(0, 1, newFakeNetwork(1))
	fmt.Println(g.Barrier(context.Background()) == nil)
	// Output: true
}

func ExampleAllSum() {
	g := newFakeGroup(0, 1, newFakeNetwork(1))
	values := []int32{1, 2, 3}
	_ = AllSum(context.Background(), g, values, values)
	fmt.Println(values)
	// Output: [1 2 3]
}

func tempDevices() string {
	data, _ := json.Marshal([][][]string{
		{{}, {"rdma01"}},
		{{"rdma10"}, {}},
	})
	f, _ := os.CreateTemp("", "gojaccl-devices-*.json")
	_, _ = f.Write(data)
	_ = f.Close()
	return f.Name()
}
