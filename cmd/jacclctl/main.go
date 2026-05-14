// Command jacclctl sends operator control requests to jaccld.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/tmc/gojaccl/internal/ipc"
	"github.com/tmc/gojaccl/internal/tcpchan"
)

const (
	defaultTCPDiagnosticPayload = "gojaccl tcp diagnostic"
	tcpDiagnosticAck            = "gojaccl tcp diagnostic ack"
)

func main() {
	log.SetFlags(0)
	var socket string
	flag.StringVar(&socket, "socket", ipc.DefaultSocket, "jaccld Unix-domain socket path")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "usage: jacclctl [flags] maintain\n")
		fmt.Fprintf(flag.CommandLine.Output(), "       jacclctl [flags] tcp-diagnostic (-listen addr | -dial addr)\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(2)
	}

	ctx := context.Background()
	switch flag.Arg(0) {
	case "maintain":
		if flag.NArg() != 1 {
			flag.Usage()
			os.Exit(2)
		}
		client, err := ipc.Dial(ctx, socket)
		if err != nil {
			log.Fatal(err)
		}
		defer client.Close()
		if err := client.Maintain(ctx); err != nil {
			log.Fatal(err)
		}
	case "tcp-diagnostic":
		if err := runTCPDiagnosticCommand(ctx, flag.Args()[1:], os.Stdout); err != nil {
			log.Fatal(err)
		}
	default:
		flag.Usage()
		os.Exit(2)
	}
}

func runTCPDiagnosticCommand(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("tcp-diagnostic", flag.ContinueOnError)
	fs.SetOutput(out)
	listen := fs.String("listen", "", "listen address for one diagnostic connection")
	dial := fs.String("dial", "", "dial address for one diagnostic connection")
	timeout := fs.Duration("timeout", 5*time.Second, "diagnostic timeout")
	payload := fs.String("payload", defaultTCPDiagnosticPayload, "diagnostic payload")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected tcp-diagnostic arguments")
	}
	if (*listen == "") == (*dial == "") {
		return fmt.Errorf("set exactly one of -listen or -dial")
	}
	if *timeout <= 0 {
		return fmt.Errorf("timeout %s must be positive", *timeout)
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	if *listen != "" {
		return tcpDiagnosticListen(ctx, *listen, []byte(*payload), out)
	}
	return tcpDiagnosticDial(ctx, *dial, []byte(*payload), out)
}

func tcpDiagnosticListen(ctx context.Context, addr string, payload []byte, out io.Writer) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("tcp diagnostic listen %s: %w", addr, err)
	}
	defer ln.Close()
	return serveTCPDiagnostic(ctx, ln, payload, out)
}

func serveTCPDiagnostic(ctx context.Context, ln net.Listener, payload []byte, out io.Writer) error {
	fmt.Fprintf(out, "tcp diagnostic listening addr=%s\n", ln.Addr())
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = ln.Close()
		case <-done:
		}
	}()
	conn, err := ln.Accept()
	close(done)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("tcp diagnostic accept: %w", err)
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	got, err := tcpchan.ReadFrame(conn)
	if err != nil {
		return fmt.Errorf("tcp diagnostic read: %w", err)
	}
	if string(got) != string(payload) {
		return fmt.Errorf("tcp diagnostic payload mismatch")
	}
	if err := tcpchan.WriteFrame(conn, []byte(tcpDiagnosticAck)); err != nil {
		return fmt.Errorf("tcp diagnostic write ack: %w", err)
	}
	fmt.Fprintf(out, "tcp diagnostic listen ok addr=%s bytes=%d\n", ln.Addr(), len(got))
	return nil
}

func tcpDiagnosticDial(ctx context.Context, addr string, payload []byte, out io.Writer) error {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("tcp diagnostic dial %s: %w", addr, err)
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if err := tcpchan.WriteFrame(conn, payload); err != nil {
		return fmt.Errorf("tcp diagnostic write: %w", err)
	}
	ack, err := tcpchan.ReadFrame(conn)
	if err != nil {
		return fmt.Errorf("tcp diagnostic read ack: %w", err)
	}
	if string(ack) != tcpDiagnosticAck {
		return fmt.Errorf("tcp diagnostic ack mismatch")
	}
	fmt.Fprintf(out, "tcp diagnostic dial ok addr=%s bytes=%d\n", addr, len(payload))
	return nil
}
