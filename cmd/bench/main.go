// Command bench runs the k6 experiment against every profile and summarizes the results.
//
//	bench run [flags]        seed, validate and load each profile in Docker Compose
//	bench summarize [flags]  combine k6 summaries into a CSV and a median table
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	var err error
	switch os.Args[1] {
	case "run":
		err = runCommand(ctx, os.Args[2:])
	case "summarize":
		err = summarizeCommand(os.Args[2:])
	default:
		usage()
	}
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: bench run|summarize [flags]; use -h after a command for its flags")
	os.Exit(2)
}
