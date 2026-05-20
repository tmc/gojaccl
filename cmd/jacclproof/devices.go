package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/tmc/gojaccl/internal/topology"
)

type edgeFlags []directedEdge

type directedEdge struct {
	src     int
	dst     int
	devices []string
}

func runDevicesCommand(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("devices", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var ranks int
	var devicesText string
	var shape string
	var edges edgeFlags
	fs.IntVar(&ranks, "ranks", 2, "number of ranks")
	fs.StringVar(&devicesText, "devices", "rdma_en1", "comma-separated RDMA devices for each connected edge")
	fs.StringVar(&shape, "shape", "mesh", "matrix shape: mesh, ring, or line")
	fs.Var(&edges, "edge", "directed edge src,dst=device[,device...]")
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
	if len(edges) > 0 {
		for _, edge := range edges {
			if edge.src < 0 || edge.src >= ranks || edge.dst < 0 || edge.dst >= ranks || edge.src == edge.dst {
				return exitError{code: 2, err: fmt.Errorf("edge %d,%d is out of range for %d ranks", edge.src, edge.dst, ranks)}
			}
			matrix[edge.src][edge.dst] = append([]string(nil), edge.devices...)
		}
		if _, err := topology.Choose(matrix, false); err != nil {
			return exitError{code: 2, err: err}
		}
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

func (f *edgeFlags) String() string {
	var parts []string
	for _, edge := range *f {
		parts = append(parts, fmt.Sprintf("%d,%d=%s", edge.src, edge.dst, strings.Join(edge.devices, ",")))
	}
	return strings.Join(parts, " ")
}

func (f *edgeFlags) Set(text string) error {
	left, right, ok := strings.Cut(text, "=")
	if !ok {
		return fmt.Errorf("edge %q missing =", text)
	}
	coords := strings.Split(left, ",")
	if len(coords) != 2 {
		return fmt.Errorf("edge %q must use src,dst=device", text)
	}
	src, err := strconv.Atoi(strings.TrimSpace(coords[0]))
	if err != nil {
		return fmt.Errorf("edge %q source: %w", text, err)
	}
	dst, err := strconv.Atoi(strings.TrimSpace(coords[1]))
	if err != nil {
		return fmt.Errorf("edge %q destination: %w", text, err)
	}
	devices, err := parseDeviceList(right)
	if err != nil {
		return err
	}
	*f = append(*f, directedEdge{src: src, dst: dst, devices: devices})
	return nil
}
