package follow

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/temporalio/cli/cliext"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
)

// NewCommand builds the single Temporal CLI extension command.
func NewCommand(stdout, stderr io.Writer) *cobra.Command {
	var commonOpts cliext.CommonOptions
	var clientOpts cliext.ClientOptions
	var workflowID, runID string

	cmd := &cobra.Command{
		Use:   "follow",
		Short: "Follow a workflow through continue-as-new and reset to its final run",
		Long: "Reconstructs and prints the full chain of runs for a workflow — from the " +
			"original root run through every continue-as-new / reset hop to the final run.",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				return fmt.Errorf("unexpected arguments: %v", args)
			}
			if workflowID == "" {
				return fmt.Errorf("--workflow-id/-w is required")
			}
			if err := validateOutput(commonOpts.Output.Value); err != nil {
				return err
			}

			ctx := cmd.Context()
			if commonOpts.CommandTimeout != 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, commonOpts.CommandTimeout.Duration())
				defer cancel()
			}

			logger, err := cliext.NewLogger(commonOpts, stderr)
			if err != nil {
				return err
			}
			builder := &cliext.ClientOptionsBuilder{
				CommonOptions: commonOpts,
				ClientOptions: clientOpts,
				EnvLookup:     envconfig.EnvLookupOS,
				Logger:        logger,
			}
			options, err := builder.Build(ctx)
			if err != nil {
				return err
			}

			dialCtx := ctx
			if commonOpts.ClientConnectTimeout != 0 {
				var cancel context.CancelFunc
				dialCtx, cancel = context.WithTimeout(ctx, commonOpts.ClientConnectTimeout.Duration())
				defer cancel()
			}
			c, err := client.DialContext(dialCtx, options)
			if err != nil {
				return fmt.Errorf("failed dialing Temporal service: %w", err)
			}
			defer c.Close()

			return Run(ctx, c, RunOptions{
				WorkflowID: workflowID,
				RunID:      runID,
				Namespace:  options.Namespace,
				Output:     commonOpts.Output.Value,
				Logger:     logger,
			}, stdout)
		},
	}

	flags := cmd.Flags()
	commonOpts.BuildFlags(flags)
	clientOpts.BuildFlags(flags)
	flags.StringVarP(&workflowID, "workflow-id", "w", "", "Workflow ID to follow (required).")
	flags.StringVarP(&runID, "run-id", "r", "", "Run ID to start from. Defaults to the workflow's current run.")

	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetIn(os.Stdin)
	return cmd
}
