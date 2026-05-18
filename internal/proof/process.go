package proof

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// Process records one candidate gojaccl or RDMA process.
type Process struct {
	PID     string
	Name    string
	Command string
}

// ProcessSnapshot returns candidate gojaccl or RDMA processes from ps output.
func ProcessSnapshot(r io.Reader) ([]Process, error) {
	var out []Process
	scan := bufio.NewScanner(r)
	for scan.Scan() {
		line := strings.TrimSpace(scan.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		name := fields[1]
		if !isCandidateProcessName(name) {
			continue
		}
		cmd := ""
		if len(fields) > 2 {
			cmd = strings.Join(fields[2:], " ")
		}
		out = append(out, Process{
			PID:     fields[0],
			Name:    name,
			Command: cmd,
		})
	}
	if err := scan.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func isCandidateProcessName(name string) bool {
	switch name {
	case "jaccld", "gojaccl.test", "gojaccl-daemon", "rdmaperf":
		return true
	}
	return strings.HasPrefix(name, "jacclctl")
}

// WriteProcessSnapshot writes candidates in the same shape as ps based scripts.
func WriteProcessSnapshot(w io.Writer, ps io.Reader) (int, error) {
	procs, err := ProcessSnapshot(ps)
	if err != nil {
		return 0, err
	}
	for _, p := range procs {
		if p.Command == "" {
			fmt.Fprintf(w, "%s %s\n", p.PID, p.Name)
			continue
		}
		fmt.Fprintf(w, "%s %s %s\n", p.PID, p.Name, p.Command)
	}
	return len(procs), nil
}

// CurrentProcessSnapshot writes candidate processes from the local process table.
func CurrentProcessSnapshot(w io.Writer) (int, error) {
	if file := os.Getenv("RDMA_PROCESS_SNAPSHOT_PS_FILE"); file != "" {
		f, err := os.Open(file)
		if err != nil {
			return 0, err
		}
		defer f.Close()
		return WriteProcessSnapshot(w, f)
	}
	cmd := exec.Command("ps", "-axo", "pid=,ucomm=,command=")
	data, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("ps: %w", err)
	}
	return WriteProcessSnapshot(w, bytes.NewReader(data))
}
