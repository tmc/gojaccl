package topology

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Summary describes a device matrix without opening devices.
type Summary struct {
	Topology       Topology
	Ranks          int
	DirectedEdges  int
	EmptyEdges     int
	TotalWires     int
	MaxWires       int
	Devices        []string
	PrimaryDevices []string
	MatrixSHA256   string
}

// Summarize validates matrix, selects a topology, and counts usable paths.
func Summarize(matrix [][][]string, preferRing bool) (Summary, error) {
	topo, err := Choose(matrix, preferRing)
	if err != nil {
		return Summary{}, err
	}
	digest, err := MatrixDigest(matrix)
	if err != nil {
		return Summary{}, err
	}
	s := Summary{
		Topology:     topo,
		Ranks:        len(matrix),
		MatrixSHA256: digest,
	}
	for src, row := range matrix {
		for dst, paths := range row {
			if src == dst {
				continue
			}
			n := usablePathCount(paths)
			if n == 0 {
				s.EmptyEdges++
				continue
			}
			s.DirectedEdges++
			s.TotalWires += n
			if n > s.MaxWires {
				s.MaxWires = n
			}
			for _, path := range paths {
				path = strings.TrimSpace(path)
				if path == "" {
					continue
				}
				addUnique(&s.Devices, path)
			}
			if path := firstUsablePath(paths); path != "" {
				addUnique(&s.PrimaryDevices, path)
			}
		}
	}
	sort.Strings(s.Devices)
	sort.Strings(s.PrimaryDevices)
	return s, nil
}

// MatrixDigest returns the SHA-256 digest of the JSON matrix representation.
func MatrixDigest(matrix [][][]string) (string, error) {
	data, err := json.Marshal(matrix)
	if err != nil {
		return "", fmt.Errorf("topology: marshal matrix digest: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func firstUsablePath(paths []string) string {
	for _, path := range paths {
		if path = strings.TrimSpace(path); path != "" {
			return path
		}
	}
	return ""
}

func addUnique(list *[]string, value string) {
	for _, existing := range *list {
		if existing == value {
			return
		}
	}
	*list = append(*list, value)
}
