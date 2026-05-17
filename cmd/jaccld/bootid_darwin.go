//go:build darwin

package main

import (
	"fmt"
	"syscall"
)

func currentBootID() (string, error) {
	value, err := syscall.Sysctl("kern.boottime")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("darwin-%x", []byte(value)), nil
}
