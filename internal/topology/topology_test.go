package topology

import "testing"

func TestValidateDeviceMatrix(t *testing.T) {
	t.Run("TwoRankSquare", func(t *testing.T) {
		if err := ValidateDeviceMatrix(mesh(2)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("ThreeRankSquare", func(t *testing.T) {
		if err := ValidateDeviceMatrix(mesh(3)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("RejectEmpty", func(t *testing.T) {
		if err := ValidateDeviceMatrix(nil); err == nil {
			t.Fatal("ValidateDeviceMatrix(nil) = nil")
		}
	})
	t.Run("RejectNonSquare", func(t *testing.T) {
		m := mesh(2)
		m = append(m, [][]string{{}, {}, {}})
		if err := ValidateDeviceMatrix(m); err == nil {
			t.Fatal("ValidateDeviceMatrix(non-square) = nil")
		}
	})
	t.Run("RejectRaggedRows", func(t *testing.T) {
		m := mesh(3)
		m[1] = m[1][:2]
		if err := ValidateDeviceMatrix(m); err == nil {
			t.Fatal("ValidateDeviceMatrix(ragged) = nil")
		}
	})
	t.Run("RejectNilPeerEntries", func(t *testing.T) {
		m := mesh(2)
		m[0][1] = nil
		if err := ValidateDeviceMatrix(m); err == nil {
			t.Fatal("ValidateDeviceMatrix(nil peer) = nil")
		}
	})
}

func TestValidateMesh(t *testing.T) {
	t.Run("TwoRankSymmetric", func(t *testing.T) {
		if err := ValidateMesh(mesh(2)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("ThreeRankFull", func(t *testing.T) {
		if err := ValidateMesh(mesh(3)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("SelfLinksIgnored", func(t *testing.T) {
		m := mesh(2)
		m[0][0] = []string{"self"}
		if err := ValidateMesh(m); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("MultipleWiresAllowed", func(t *testing.T) {
		m := mesh(2)
		m[0][1] = []string{"a", "b"}
		m[1][0] = []string{"c", "d"}
		if err := ValidateMesh(m); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("RejectMissingPeerPath", func(t *testing.T) {
		m := mesh(2)
		m[0][1] = []string{}
		if err := ValidateMesh(m); err == nil {
			t.Fatal("ValidateMesh(missing path) = nil")
		}
	})
	t.Run("RejectOneWayPeerPath", func(t *testing.T) {
		m := mesh(2)
		m[1][0] = []string{}
		if err := ValidateMesh(m); err == nil {
			t.Fatal("ValidateMesh(one-way) = nil")
		}
	})
}

func TestValidateRing(t *testing.T) {
	t.Run("TwoRankSymmetricAdjacent", func(t *testing.T) {
		if err := ValidateRing(ring(2)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("ThreeRankAdjacentOnly", func(t *testing.T) {
		if err := ValidateRing(ring(3)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("MultipleWiresPerNeighbor", func(t *testing.T) {
		m := ring(4)
		for i := range m {
			left := (i - 1 + len(m)) % len(m)
			right := (i + 1) % len(m)
			m[i][left] = []string{"a", "b"}
			m[i][right] = []string{"a", "b"}
		}
		if err := ValidateRing(m); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("RejectMissingForwardLink", func(t *testing.T) {
		m := ring(4)
		m[0][1] = []string{}
		if err := ValidateRing(m); err == nil {
			t.Fatal("ValidateRing(missing forward) = nil")
		}
	})
	t.Run("RejectMissingBackwardLink", func(t *testing.T) {
		m := ring(4)
		m[0][3] = []string{}
		if err := ValidateRing(m); err == nil {
			t.Fatal("ValidateRing(missing backward) = nil")
		}
	})
	t.Run("RejectOnlyNonAdjacentLinks", func(t *testing.T) {
		m := ring(4)
		m[0][2] = []string{"bad"}
		if err := ValidateRing(m); err == nil {
			t.Fatal("ValidateRing(non-adjacent) = nil")
		}
	})
}

func TestValidateLine(t *testing.T) {
	t.Run("TwoRankSymmetricAdjacent", func(t *testing.T) {
		if err := ValidateLine(line(2)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("ThreeRankLine", func(t *testing.T) {
		if err := ValidateLine(line(3)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("RejectMissingForwardLink", func(t *testing.T) {
		m := line(3)
		m[1][2] = []string{}
		if err := ValidateLine(m); err == nil {
			t.Fatal("ValidateLine(missing forward) = nil")
		}
	})
	t.Run("RejectMissingBackwardLink", func(t *testing.T) {
		m := line(3)
		m[1][0] = []string{}
		if err := ValidateLine(m); err == nil {
			t.Fatal("ValidateLine(missing backward) = nil")
		}
	})
	t.Run("RejectNonAdjacentLinks", func(t *testing.T) {
		m := line(3)
		m[0][2] = []string{"bad"}
		if err := ValidateLine(m); err == nil {
			t.Fatal("ValidateLine(non-adjacent) = nil")
		}
	})
	t.Run("RejectUnevenWires", func(t *testing.T) {
		m := line(3)
		m[1][2] = []string{"a", "b"}
		if err := ValidateLine(m); err == nil {
			t.Fatal("ValidateLine(uneven wires) = nil")
		}
	})
}

func TestValidateConnected(t *testing.T) {
	t.Run("AcceptsMesh", func(t *testing.T) {
		if err := ValidateConnected(mesh(4)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("AcceptsLine", func(t *testing.T) {
		if err := ValidateConnected(line(4)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("AcceptsPartiallyConnectedGraph", func(t *testing.T) {
		if err := ValidateConnected(partial(4)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("RejectDisconnectedGraph", func(t *testing.T) {
		m := partial(4)
		m[2][3] = []string{}
		m[3][2] = []string{}
		if err := ValidateConnected(m); err == nil {
			t.Fatal("ValidateConnected(disconnected) = nil")
		}
	})
	t.Run("RejectOneWayGraph", func(t *testing.T) {
		m := partial(4)
		m[3][2] = []string{}
		if err := ValidateConnected(m); err == nil {
			t.Fatal("ValidateConnected(one-way) = nil")
		}
	})
}

func TestChoose(t *testing.T) {
	t.Run("PreferRingWhenBothValid", func(t *testing.T) {
		got, err := Choose(mesh(3), true)
		if err != nil || got != Ring {
			t.Fatalf("Choose = %v, %v; want ring", got, err)
		}
	})
	t.Run("MeshWhenRingInvalid", func(t *testing.T) {
		got, err := Choose(mesh(4), true)
		if err != nil || got != Mesh {
			t.Fatalf("Choose = %v, %v; want mesh", got, err)
		}
	})
	t.Run("RingWhenOnlyRingValid", func(t *testing.T) {
		got, err := Choose(ring(4), false)
		if err != nil || got != Ring {
			t.Fatalf("Choose = %v, %v; want ring", got, err)
		}
	})
	t.Run("LineWhenOnlyLineValid", func(t *testing.T) {
		got, err := Choose(line(3), true)
		if err != nil || got != Line {
			t.Fatalf("Choose = %v, %v; want line", got, err)
		}
	})
	t.Run("ConnectedWhenOnlyConnectedValid", func(t *testing.T) {
		got, err := Choose(partial(4), true)
		if err != nil || got != Connected {
			t.Fatalf("Choose = %v, %v; want connected", got, err)
		}
	})
	t.Run("ErrorWhenNeitherValid", func(t *testing.T) {
		m := mesh(2)
		m[0][1] = []string{}
		m[1][0] = []string{}
		if _, err := Choose(m, false); err == nil {
			t.Fatal("Choose(invalid) = nil")
		}
	})
	t.Run("StableChoiceAcrossRanks", func(t *testing.T) {
		m := ring(4)
		for range m {
			got, err := Choose(m, true)
			if err != nil || got != Ring {
				t.Fatalf("Choose = %v, %v; want ring", got, err)
			}
		}
	})
}

func mesh(n int) [][][]string {
	m := make([][][]string, n)
	for i := range m {
		m[i] = make([][]string, n)
		for j := range m[i] {
			m[i][j] = []string{}
			if i != j {
				m[i][j] = []string{"dev"}
			}
		}
	}
	return m
}

func ring(n int) [][][]string {
	m := make([][][]string, n)
	for i := range m {
		m[i] = make([][]string, n)
		for j := range m[i] {
			m[i][j] = []string{}
		}
		left := (i - 1 + n) % n
		right := (i + 1) % n
		m[i][left] = []string{"dev"}
		m[i][right] = []string{"dev"}
	}
	return m
}

func line(n int) [][][]string {
	m := make([][][]string, n)
	for i := range m {
		m[i] = make([][]string, n)
		for j := range m[i] {
			m[i][j] = []string{}
		}
		if i > 0 {
			m[i][i-1] = []string{"dev"}
		}
		if i+1 < n {
			m[i][i+1] = []string{"dev"}
		}
	}
	return m
}

func partial(n int) [][][]string {
	m := line(n)
	if n > 2 {
		m[0][2] = []string{"dev"}
		m[2][0] = []string{"dev"}
	}
	return m
}
