package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/tmc/gojaccl/internal/topology"
)

func runDevicesCommand(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("devices", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var ranks int
	var devicesText string
	var shape string
	fs.IntVar(&ranks, "ranks", 2, "number of ranks")
	fs.StringVar(&devicesText, "devices", "rdma_en1", "comma-separated RDMA devices for each connected edge")
	fs.StringVar(&shape, "shape", "mesh", "matrix shape: mesh, ring, or line")
	if err := fs.Parse(args); err != nil {
		return exitError{code: 2, err: err}
	}
	if fs.NArg() != 0 {
		return exitError{code: 2, err: fmt.Errorf("unexpected devices arguments")}
	}
	devices, err := parseDeviceList(devicesText)
	if err != nil {
		return exitError{code: 2, err: err}
	}
	matrix, err := buildDeviceMatrix(ranks, devices, shape)
	if err != nil {
		return exitError{code: 2, err: err}
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "\t")
	return enc.Encode(matrix)
}

func parseDeviceList(text string) ([]string, error) {
	var devices []string
	for _, field := range strings.Split(text, ",") {
		device := strings.TrimSpace(field)
		if device == "" {
			continue
		}
		devices = append(devices, device)
	}
	if len(devices) == 0 {
		return nil, fmt.Errorf("devices list is empty")
	}
	return devices, nil
}

func buildDeviceMatrix(ranks int, devices []string, shape string) ([][][]string, error) {
	if ranks < 2 {
		return nil, fmt.Errorf("ranks %d must be at least 2", ranks)
	}
	matrix := make([][][]string, ranks)
	for src := range matrix {
		matrix[src] = make([][]string, ranks)
		for dst := range matrix[src] {
			matrix[src][dst] = []string{}
		}
	}

	switch strings.ToLower(strings.TrimSpace(shape)) {
	case "mesh":
		for src := range matrix {
			for dst := range matrix[src] {
				if src != dst {
					matrix[src][dst] = append([]string(nil), devices...)
				}
			}
		}
	case "ring":
		for src := range matrix {
			matrix[src][(src+1)%ranks] = append([]string(nil), devices...)
			matrix[src][(src-1+ranks)%ranks] = append([]string(nil), devices...)
		}
	case "line":
		for src := range matrix {
			if src > 0 {
				matrix[src][src-1] = append([]string(nil), devices...)
			}
			if src+1 < ranks {
				matrix[src][src+1] = append([]string(nil), devices...)
			}
		}
	default:
		return nil, fmt.Errorf("unknown shape %q", shape)
	}
	if _, err := topology.Choose(matrix, false); err != nil {
		return nil, err
	}
	return matrix, nil
}
