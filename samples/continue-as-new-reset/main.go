// Command sample builds a real workflow chain on a Temporal dev server so the
// behavior of `temporal follow` can be verified by hand.
//
// It starts a workflow that continues-as-new a few times and then, optionally,
// resets the final run — producing a chain of continue-as-new hops followed by
// a reset hop. When it finishes it prints the exact `temporal follow` command to
// run against the workflow it created.
//
// Usage:
//
//	temporal server start-dev      # in another terminal
//	go run ./samples               # build the chain
//	temporal follow -w <printed-workflow-id>
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// SampleWorkflow continues-as-new `count` times, then completes. Each
// continue-as-new starts a fresh run of the same workflow ID.
func SampleWorkflow(ctx workflow.Context, count int) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("SampleWorkflow run", "remaining", count)
	if count <= 0 {
		return nil
	}
	return workflow.NewContinueAsNewError(ctx, SampleWorkflow, count-1)
}

func main() {
	address := flag.String("address", "localhost:7233", "Temporal server gRPC address.")
	namespace := flag.String("namespace", "default", "Temporal namespace.")
	canCount := flag.Int("continue-as-new-count", 2, "Number of continue-as-new hops to perform.")
	doReset := flag.Bool("reset", true, "Reset the final run to add a reset hop to the chain.")
	flag.Parse()

	if err := run(*address, *namespace, *canCount, *doReset); err != nil {
		log.Fatalf("sample failed: %v", err)
	}
}

func run(address, namespace string, canCount int, doReset bool) error {
	c, err := client.Dial(client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		return fmt.Errorf("connect to Temporal at %s (is `temporal server start-dev` running?): %w", address, err)
	}
	defer c.Close()

	ctx := context.Background()

	// Run a worker for the lifetime of the sample.
	taskQueue := "temporal-follow-sample"
	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflow(SampleWorkflow)
	if err := w.Start(); err != nil {
		return fmt.Errorf("start worker: %w", err)
	}
	defer w.Stop()

	// Start the workflow and let the continue-as-new chain run to completion.
	workflowID := "temporal-follow-sample-" + uuid.NewString()
	wfRun, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: taskQueue,
	}, SampleWorkflow, canCount)
	if err != nil {
		return fmt.Errorf("start workflow: %w", err)
	}
	rootRunID := wfRun.GetRunID()
	log.Printf("started workflow %q (root run %s) with %d continue-as-new hop(s)", workflowID, rootRunID, canCount)

	if err := wfRun.Get(ctx, nil); err != nil {
		return fmt.Errorf("await workflow: %w", err)
	}
	log.Printf("continue-as-new chain completed")

	if doReset {
		if err := resetFinalRun(ctx, c, namespace, workflowID); err != nil {
			return fmt.Errorf("reset final run: %w", err)
		}
	}

	printInstructions(workflowID, rootRunID, namespace, address)
	return nil
}

// resetFinalRun resets the workflow's current (final) run to its first workflow
// task, producing a new run linked from the previous one by a reset.
func resetFinalRun(ctx context.Context, c client.Client, namespace, workflowID string) error {
	// The latest run is the terminal run of the continue-as-new chain.
	desc, err := c.DescribeWorkflowExecution(ctx, workflowID, "")
	if err != nil {
		return fmt.Errorf("describe latest run: %w", err)
	}
	finalRunID := desc.GetWorkflowExecutionInfo().GetExecution().GetRunId()

	eventID, err := firstWorkflowTaskCompletedID(ctx, c, workflowID, finalRunID)
	if err != nil {
		return err
	}

	resp, err := c.WorkflowService().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 namespace,
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: finalRunID},
		Reason:                    "temporal-follow sample",
		WorkflowTaskFinishEventId: eventID,
		RequestId:                 uuid.NewString(),
		ResetReapplyType:          enumspb.RESET_REAPPLY_TYPE_NONE,
	})
	if err != nil {
		return err
	}
	resetRunID := resp.GetRunId()
	log.Printf("reset final run %s -> new run %s", finalRunID, resetRunID)

	if err := c.GetWorkflow(ctx, workflowID, resetRunID).Get(ctx, nil); err != nil {
		return fmt.Errorf("await reset run: %w", err)
	}
	log.Printf("reset run completed")
	return nil
}

// firstWorkflowTaskCompletedID returns the event id of the first
// WorkflowTaskCompleted event, a valid reset point.
func firstWorkflowTaskCompletedID(ctx context.Context, c client.Client, workflowID, runID string) (int64, error) {
	iter := c.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		ev, err := iter.Next()
		if err != nil {
			return 0, fmt.Errorf("read history: %w", err)
		}
		if ev.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			return ev.GetEventId(), nil
		}
	}
	return 0, fmt.Errorf("no WorkflowTaskCompleted event found for run %s", runID)
}

func printInstructions(workflowID, rootRunID, namespace, address string) {
	// Brief pause so visibility catches up before the user queries it.
	time.Sleep(500 * time.Millisecond)

	var b strings.Builder
	b.WriteString("\n────────────────────────────────────────────────────────────\n")
	fmt.Fprintf(&b, "Created workflow chain for: %s\n", workflowID)
	b.WriteString("\nFollow the full chain with the extension:\n")
	fmt.Fprintf(&b, "\n    temporal follow -w %s\n", workflowID)
	b.WriteString("\nOr follow from the original root run:\n")
	fmt.Fprintf(&b, "\n    temporal follow -w %s -r %s\n", workflowID, rootRunID)
	b.WriteString("\nJSON output:\n")
	fmt.Fprintf(&b, "\n    temporal follow -w %s -o json\n", workflowID)
	if namespace != "default" || address != "localhost:7233" {
		fmt.Fprintf(&b, "\n(add --address %s --namespace %s if not using the defaults)\n", address, namespace)
	}
	b.WriteString("────────────────────────────────────────────────────────────\n")

	_, _ = os.Stdout.WriteString(b.String())
}
