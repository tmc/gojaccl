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
	Line
	Connected
)

func (t Topology) String() string {
	switch t {
	case Mesh:
		return "mesh"
	case Ring:
		return "ring"
	case Line:
		return "line"
	case Connected:
		return "connected"
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

// ValidateLine checks that only adjacent ranks in rank order are connected.
func ValidateLine(matrix [][][]string) error {
	if err := ValidateDeviceMatrix(matrix); err != nil {
		return err
	}
	n := len(matrix)
	if n < 2 {
		return fmt.Errorf("topology: line needs at least two ranks")
	}

	wireCount := 0
	for src := 0; src < n; src++ {
		for dst := 0; dst < n; dst++ {
			if src == dst {
				continue
			}
			adjacent := dst == src-1 || dst == src+1
			paths := usablePathCount(matrix[src][dst])
			if !adjacent {
				if paths != 0 {
					return fmt.Errorf("topology: non-adjacent line path from rank %d to rank %d", src, dst)
				}
				continue
			}
			if paths == 0 {
				return fmt.Errorf("topology: missing line path from rank %d to rank %d", src, dst)
			}
			if wireCount == 0 {
				wireCount = paths
			}
			if paths != wireCount {
				return fmt.Errorf("topology: line path from rank %d to rank %d has %d wires, want %d", src, dst, paths, wireCount)
			}
		}
	}
	return nil
}

// ValidateConnected checks that every rank is reachable through bidirectional
// links. It permits partial connectivity as long as the graph is connected.
func ValidateConnected(matrix [][][]string) error {
	if err := ValidateDeviceMatrix(matrix); err != nil {
		return err
	}
	n := len(matrix)
	if n < 2 {
		return fmt.Errorf("topology: connected graph needs at least two ranks")
	}
	for src := 0; src < n; src++ {
		for dst := src + 1; dst < n; dst++ {
			forward := hasPath(matrix[src][dst])
			backward := hasPath(matrix[dst][src])
			if forward != backward {
				return fmt.Errorf("topology: one-way connected path between rank %d and rank %d", src, dst)
			}
		}
	}

	seen := make([]bool, n)
	queue := []int{0}
	seen[0] = true
	for len(queue) > 0 {
		src := queue[0]
		queue = queue[1:]
		for dst := 0; dst < n; dst++ {
			if src == dst || seen[dst] || !hasPath(matrix[src][dst]) {
				continue
			}
			seen[dst] = true
			queue = append(queue, dst)
		}
	}
	for rank, ok := range seen {
		if !ok {
			return fmt.Errorf("topology: rank %d is not connected", rank)
		}
	}
	return nil
}

// Choose selects a valid topology using the MLX JACCL preference order.
func Choose(matrix [][][]string, preferRing bool) (Topology, error) {
	meshErr := ValidateMesh(matrix)
	ringErr := ValidateRing(matrix)
	lineErr := ValidateLine(matrix)
	connectedErr := ValidateConnected(matrix)
	if preferRing && ringErr == nil {
		return Ring, nil
	}
	if meshErr == nil {
		return Mesh, nil
	}
	if ringErr == nil {
		return Ring, nil
	}
	if lineErr == nil {
		return Line, nil
	}
	if connectedErr == nil {
		return Connected, nil
	}
	return Unknown, fmt.Errorf("topology: no usable topology: mesh: %v; ring: %v; line: %v; connected: %v", meshErr, ringErr, lineErr, connectedErr)
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
