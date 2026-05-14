// Command jacclctl sends operator control requests to jaccld.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/tmc/gojaccl/internal/ipc"
)

func main() {
	log.SetFlags(0)
	var socket string
	flag.StringVar(&socket, "socket", ipc.DefaultSocket, "jaccld Unix-domain socket path")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "usage: jacclctl [flags] maintain\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}

	ctx := context.Background()
	client, err := ipc.Dial(ctx, socket)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	switch flag.Arg(0) {
	case "maintain":
		if err := client.Maintain(ctx); err != nil {
			log.Fatal(err)
		}
	default:
		flag.Usage()
		os.Exit(2)
	}
}
