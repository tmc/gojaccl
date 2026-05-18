package proof

import (
	"bytes"
	"strings"
	"testing"
)

func TestProcessSnapshotMatchesExecutableNames(t *testing.T) {
	input := strings.Join([]string{
		"123 bash             /bin/bash -lc go build ./examples/rdma/rdmaperf",
		"124 go               /Users/tmc/sdk/go/bin/go build ./examples/rdma/rdmaperf",
		"125 jaccld           /tmp/proof/bin/jaccld -socket /tmp/jaccld.sock",
		"126 jacclctl-d0a43   /tmp/proof/bin/jacclctl-d0a43 stats",
		"",
	}, "\n")
	var out bytes.Buffer
	n, err := WriteProcessSnapshot(&out, strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("matches = %d, want 2", n)
	}
	got := out.String()
	if strings.Contains(got, "go build") || strings.Contains(got, "examples/rdma/rdmaperf") {
		t.Fatalf("snapshot matched argument text:\n%s", got)
	}
	for _, want := range []string{"jaccld", "jacclctl-d0a43"} {
		if !strings.Contains(got, want) {
			t.Fatalf("snapshot missing %q in:\n%s", want, got)
		}
	}
}

func TestProcessSnapshotIgnoresArgumentOnlyMatches(t *testing.T) {
	input := strings.Join([]string{
		"123 bash             /bin/bash -lc go build ./examples/rdma/rdmaperf",
		"124 go               /Users/tmc/sdk/go/bin/go build ./examples/rdma/rdmaperf",
		"",
	}, "\n")
	var out bytes.Buffer
	n, err := WriteProcessSnapshot(&out, strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 || out.Len() != 0 {
		t.Fatalf("matches=%d output=%q, want none", n, out.String())
	}
}
