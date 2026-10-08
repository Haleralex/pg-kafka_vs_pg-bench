// Command bench runs the queue experiment against every profile and summarizes the results.
//
//	bench run [flags]        start each broker in Docker Compose and run cmd/loadgen against it
//	bench summarize [flags]  combine loadgen reports into a CSV and median tables
//	bench report [flags]     write results/report.html with charts and optionally serve it
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
	case "report":
		err = reportCommand(ctx, os.Args[2:])
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
	fmt.Fprintln(os.Stderr, "usage: bench run|summarize|report [flags]; use -h after a command for its flags")
	os.Exit(2)
}
