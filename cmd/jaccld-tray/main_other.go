//go:build !darwin

package main

import "fmt"

func main() {
	fmt.Println("jaccld-tray is only supported on macOS")
}
