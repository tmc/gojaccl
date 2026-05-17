//go:build !darwin

package main

import (
	"os"
	"strings"
	"time"
)

func currentBootID() (string, error) {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err == nil {
		if id := strings.TrimSpace(string(data)); id != "" {
			return "linux-" + id, nil
		}
	}
	return "unknown-" + time.Now().UTC().Format("20060102T150405Z"), nil
}
