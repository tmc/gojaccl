package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/tmc/gojaccl/internal/proof"
)

type exitError struct {
	code int
	err  error
}

func (e exitError) Error() string {
	if e.err == nil {
		return fmt.Sprintf("exit status %d", e.code)
	}
	return e.err.Error()
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		var x exitError
		if errors.As(err, &x) {
			if x.err != nil {
				fmt.Fprintln(os.Stderr, x.err)
			}
			os.Exit(x.code)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		usage(stderr)
		return exitError{code: 2, err: fmt.Errorf("missing command")}
	}
	switch args[0] {
	case "devices":
		return runDevicesCommand(args[1:], stdout, stderr)
	case "process-snapshot":
		return runProcessSnapshot(args[1:], stdout)
	case "rdma-metadata":
		return runRDMAMetadataPacket(args[1:], stdout, stderr)
	case "rdma-soak":
		return runRDMASoakPacket(args[1:], stdout, stderr)
	case "topology":
		return runTopologyCommand(args[1:], stdout, stderr)
	default:
		usage(stderr)
		return exitError{code: 2, err: fmt.Errorf("unknown command %q", args[0])}
	}
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage: jacclproof devices [-ranks n] [-shape mesh|ring|line] [-devices rdma_en1,rdma_en3]")
	fmt.Fprintln(w, "       jacclproof process-snapshot")
	fmt.Fprintln(w, "       jacclproof rdma-metadata -device rdma_en1|rdma_en3 -remote user@host -remote-tmp dir")
	fmt.Fprintln(w, "       jacclproof rdma-soak -device rdma_en1")
	fmt.Fprintln(w, "       jacclproof topology -file devices.json")
}

func runProcessSnapshot(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("process-snapshot", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return exitError{code: 2, err: err}
	}
	if fs.NArg() != 0 {
		return exitError{code: 2, err: fmt.Errorf("unexpected process-snapshot arguments")}
	}
	n, err := proof.CurrentProcessSnapshot(stdout)
	if err != nil {
		return err
	}
	if n == 0 {
		return exitError{code: 1}
	}
	return nil
}

const rdmaEn1MetadataRefusal = `refusing to run

Set CONFIRM_RDMA_EN1_METADATA_ONE_SHOT=one-shot-metadata after both laptops and
the intended Thunderbolt RDMA cable are connected. This is a one-shot metadata
packet; do not loop or retry after timeout, nonzero exit, missing output,
provider-state change, or process wedge.
`

const rdmaEn3MetadataRefusal = `refusing to run

Set CONFIRM_RDMA_EN3_METADATA_ONE_SHOT=one-shot-metadata after both laptops and
the second Thunderbolt cable are connected. This is a one-shot metadata packet;
do not loop or retry after timeout, nonzero exit, missing output, provider-state
change, or process wedge.
`

const rdmaEn1SoakRefusal = `refusing to run

Set CONFIRM_RDMA_EN1_SOAK_ONE_SHOT=one-shot-soak only after both physical hosts
are connected on the documented rdma_en1 path and the operator has approved a
one-shot soak. This command starts jaccld, transitions queue pairs to RTR, runs
daemon-backed smoke, and posts same-data-QP maintenance. Do not loop or retry
after timeout, nonzero exit, provider degradation, maintenance failure, or
process wedge.
`
