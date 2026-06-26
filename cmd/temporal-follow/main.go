// Command temporal-follow is a Temporal CLI extension. Invoked as
// `temporal follow -w <workflow_id> [-r <run_id>]`, it reconstructs and prints
// the full chain of runs for a workflow — from the original root run through
// every continue-as-new / reset hop to the final run.
package main

import (
	"fmt"
	"os"

	"github.com/spkane31/temporal-follow/internal/follow"
)

func main() {
	cmd := follow.NewCommand(os.Stdout, os.Stderr)
	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
