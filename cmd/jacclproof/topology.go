package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/tmc/gojaccl/internal/topology"
)

type topologyReport struct {
	Topology       string   `json:"topology"`
	Ranks          int      `json:"ranks"`
	DirectedEdges  int      `json:"directed_edges"`
	EmptyEdges     int      `json:"empty_edges"`
	TotalWires     int      `json:"total_wires"`
	MaxWires       int      `json:"max_wires"`
	Devices        []string `json:"devices,omitempty"`
	PrimaryDevices []string `json:"primary_devices,omitempty"`
	MatrixSHA256   string   `json:"matrix_sha256"`
}

func runTopologyCommand(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("topology", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var file string
	var preferRing bool
	fs.StringVar(&file, "file", "", "JSON device matrix file")
	fs.BoolVar(&preferRing, "prefer-ring", false, "prefer ring when both mesh and ring are valid")
	if err := fs.Parse(args); err != nil {
		return exitError{code: 2, err: err}
	}
	if fs.NArg() != 0 {
		return exitError{code: 2, err: fmt.Errorf("unexpected topology arguments")}
	}
	if file == "" {
		return exitError{code: 2, err: fmt.Errorf("file is required")}
	}
	matrix, err := readTopologyMatrix(file)
	if err != nil {
		return err
	}
	s, err := topology.Summarize(matrix, preferRing)
	if err != nil {
		return err
	}
	report := topologyReport{
		Topology:       s.Topology.String(),
		Ranks:          s.Ranks,
		DirectedEdges:  s.DirectedEdges,
		EmptyEdges:     s.EmptyEdges,
		TotalWires:     s.TotalWires,
		MaxWires:       s.MaxWires,
		Devices:        s.Devices,
		PrimaryDevices: s.PrimaryDevices,
		MatrixSHA256:   s.MatrixSHA256,
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "\t")
	return enc.Encode(report)
}

func readTopologyMatrix(path string) ([][][]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("devices: read %s: %w", path, err)
	}
	var matrix [][][]string
	if err := json.Unmarshal(data, &matrix); err != nil {
		return nil, fmt.Errorf("devices: parse %s: %w", path, err)
	}
	return matrix, nil
}
