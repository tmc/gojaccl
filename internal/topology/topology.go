package topology

import (
	"fmt"
	"strings"
)

// Topology is the selected communication topology.
type Topology int

const (
	Unknown Topology = iota
	Mesh
	Ring
)

func (t Topology) String() string {
	switch t {
	case Mesh:
		return "mesh"
	case Ring:
		return "ring"
	default:
		return "unknown"
	}
}

// ValidateDeviceMatrix checks that matrix is a non-empty square rank matrix.
func ValidateDeviceMatrix(matrix [][][]string) error {
	n := len(matrix)
	if n == 0 {
		return fmt.Errorf("topology: empty device matrix")
	}
	for i, row := range matrix {
		if len(row) != n {
			return fmt.Errorf("topology: row %d has length %d, want %d", i, len(row), n)
		}
		for j, paths := range row {
			if paths == nil {
				return fmt.Errorf("topology: matrix[%d][%d] is nil", i, j)
			}
		}
	}
	return nil
}

// ValidateMesh checks that every non-self peer pair has a usable path.
func ValidateMesh(matrix [][][]string) error {
	if err := ValidateDeviceMatrix(matrix); err != nil {
		return err
	}
	n := len(matrix)
	if n < 2 {
		return fmt.Errorf("topology: mesh needs at least two ranks")
	}
	for src := 0; src < n; src++ {
		for dst := 0; dst < n; dst++ {
			if src == dst {
				continue
			}
			if !hasPath(matrix[src][dst]) {
				return fmt.Errorf("topology: missing mesh path from rank %d to rank %d", src, dst)
			}
			if !hasPath(matrix[dst][src]) {
				return fmt.Errorf("topology: one-way mesh path from rank %d to rank %d", src, dst)
			}
		}
	}
	return nil
}

// ValidateRing checks that only adjacent ranks are connected and all adjacent
// links have the same number of usable wires.
func ValidateRing(matrix [][][]string) error {
	if err := ValidateDeviceMatrix(matrix); err != nil {
		return err
	}
	n := len(matrix)
	if n < 2 {
		return fmt.Errorf("topology: ring needs at least two ranks")
	}

	wireCount := usablePathCount(matrix[0][1%n])
	if wireCount == 0 {
		return fmt.Errorf("topology: missing ring path from rank 0 to rank %d", 1%n)
	}
	for src := 0; src < n; src++ {
		left := (src - 1 + n) % n
		right := (src + 1) % n
		for dst := 0; dst < n; dst++ {
			if src == dst {
				continue
			}
			adjacent := dst == left || dst == right
			paths := usablePathCount(matrix[src][dst])
			if adjacent {
				if paths != wireCount {
					return fmt.Errorf("topology: ring path from rank %d to rank %d has %d wires, want %d", src, dst, paths, wireCount)
				}
				continue
			}
			if paths != 0 {
				return fmt.Errorf("topology: non-adjacent ring path from rank %d to rank %d", src, dst)
			}
		}
	}
	return nil
}

// Choose selects a valid topology using the MLX JACCL preference order.
func Choose(matrix [][][]string, preferRing bool) (Topology, error) {
	meshErr := ValidateMesh(matrix)
	ringErr := ValidateRing(matrix)
	if preferRing && ringErr == nil {
		return Ring, nil
	}
	if meshErr == nil {
		return Mesh, nil
	}
	if ringErr == nil {
		return Ring, nil
	}
	return Unknown, fmt.Errorf("topology: no usable topology: mesh: %v; ring: %v", meshErr, ringErr)
}

func hasPath(paths []string) bool {
	return usablePathCount(paths) > 0
}

func usablePathCount(paths []string) int {
	n := 0
	for _, p := range paths {
		if strings.TrimSpace(p) != "" {
			n++
		}
	}
	return n
}
