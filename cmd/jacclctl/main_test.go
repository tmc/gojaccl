package main

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

func TestTCPDiagnosticLoopback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	errc := make(chan error, 1)
	go func() {
		var out bytes.Buffer
		errc <- serveTCPDiagnostic(ctx, ln, []byte(defaultTCPDiagnosticPayload), &out)
	}()

	var out bytes.Buffer
	if err := tcpDiagnosticDial(ctx, addr, []byte(defaultTCPDiagnosticPayload), &out); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "tcp diagnostic dial ok") {
		t.Fatalf("diagnostic output = %q, want ok", out.String())
	}
}

func TestRunTCPDiagnosticCommandValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "missing mode",
			want: "set exactly one of -listen or -dial",
		},
		{
			name: "both modes",
			args: []string{"-listen", "127.0.0.1:0", "-dial", "127.0.0.1:1"},
			want: "set exactly one of -listen or -dial",
		},
		{
			name: "bad timeout",
			args: []string{"-dial", "127.0.0.1:1", "-timeout", "0"},
			want: "timeout 0s must be positive",
		},
		{
			name: "unexpected argument",
			args: []string{"-dial", "127.0.0.1:1", "extra"},
			want: "unexpected tcp-diagnostic arguments",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			err := runTCPDiagnosticCommand(context.Background(), tt.args, &out)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("runTCPDiagnosticCommand = %v, want %q", err, tt.want)
			}
		})
	}
}
