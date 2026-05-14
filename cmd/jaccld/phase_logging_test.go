package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestDaemonPhaseLogsCoverProofBoundaries(t *testing.T) {
	formats := readLogFormats(t, "main.go", "transport.go")
	required := []string{
		"jaccld phase=side_channel start rank=%d size=%d coordinator=%s",
		"jaccld phase=side_channel done rank=%d size=%d",
		"jaccld phase=slab start bytes=%d",
		"jaccld phase=slab done bytes=%d",
		"jaccld phase=resource_store done max_sessions=%d",
		"jaccld phase=maintenance_lease done length=%d",
		"jaccld phase=hardware_open start device=%q",
		"jaccld phase=hardware_open device_done name=%q",
		"jaccld phase=pd_alloc start",
		"jaccld phase=pd_alloc done",
		"jaccld phase=mr_register start length=%d",
		"jaccld phase=mr_register done addr_nonzero=%t lkey_nonzero=%t rkey_nonzero=%t length=%d",
		"jaccld phase=daemon_transport start",
		"jaccld phase=daemon_transport done",
		"jaccld phase=qp_setup start peer=%d",
		"jaccld phase=qp_setup done peer=%d qpn_nonzero=%t psn_nonzero=%t",
		"jaccld phase=destination_exchange start",
		"jaccld phase=destination_exchange gathered peers=%d",
		"jaccld phase=destination_exchange done",
		"jaccld phase=rtr start peer=%d",
		"jaccld phase=rtr done peer=%d",
		"jaccld phase=ready_barrier start",
		"jaccld phase=ready_barrier done",
		"jaccld phase=ipc_listen start socket=%s",
	}
	for _, want := range required {
		if !formats[want] {
			t.Fatalf("missing phase log %q", want)
		}
	}
}

func TestDaemonPhaseLogsDoNotExposeRawProviderMetadata(t *testing.T) {
	for format := range readLogFormats(t, "main.go", "transport.go") {
		if !strings.Contains(format, "jaccld phase=") {
			continue
		}
		forbidden := []string{
			"addr=%",
			"lkey=%",
			"rkey=%",
			"qpn=%",
			"psn=%",
			"gid",
			"lid",
		}
		for _, bad := range forbidden {
			if strings.Contains(strings.ToLower(format), bad) {
				t.Fatalf("phase log %q exposes raw provider metadata marker %q", format, bad)
			}
		}
	}
}

func readLogFormats(t *testing.T, paths ...string) map[string]bool {
	t.Helper()
	formats := make(map[string]bool)
	re := regexp.MustCompile(`log\.Printf\("([^"]+)"`)
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range re.FindAllStringSubmatch(string(data), -1) {
			formats[match[1]] = true
		}
	}
	return formats
}
