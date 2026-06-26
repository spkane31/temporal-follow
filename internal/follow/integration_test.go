package follow

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// These tests run against a live dev server (`temporal server start-dev`). When
// no server is reachable they skip, so `go test ./...` stays green offline.

const testNamespace = "default"

// canWorkflow continues-as-new `count` times, then completes.
func canWorkflow(ctx workflow.Context, count int) error {
	if count <= 0 {
		return nil
	}
	return workflow.NewContinueAsNewError(ctx, canWorkflow, count-1)
}

// simpleWorkflow completes immediately.
func simpleWorkflow(ctx workflow.Context) error {
	return nil
}

func testAddress() string {
	if a := os.Getenv("TEMPORAL_ADDRESS"); a != "" {
		return a
	}
	return "localhost:7233"
}

// dialTestClient connects to a dev server or skips the test if unreachable.
func dialTestClient(t *testing.T) client.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := client.DialContext(ctx, client.Options{HostPort: testAddress(), Namespace: testNamespace})
	if err != nil {
		t.Skipf("no Temporal dev server reachable at %s (run `temporal server start-dev`): %v", testAddress(), err)
	}
	return c
}

// startWorker registers the test workflows on a unique task queue and runs a
// worker for the duration of the test.
func startWorker(t *testing.T, c client.Client) string {
	t.Helper()
	taskQueue := "follow-test-" + uuid.NewString()
	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflow(canWorkflow)
	w.RegisterWorkflow(simpleWorkflow)
	if err := w.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	t.Cleanup(w.Stop)
	return taskQueue
}

func transitions(chain *Chain) []string {
	var ts []string
	for _, r := range chain.Runs[:max(0, len(chain.Runs)-1)] {
		ts = append(ts, r.TransitionToNext)
	}
	return ts
}

func TestBuildChain_ContinueAsNew(t *testing.T) {
	c := dialTestClient(t)
	defer c.Close()
	tq := startWorker(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wfID := "follow-can-" + uuid.NewString()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: wfID, TaskQueue: tq}, canWorkflow, 2)
	if err != nil {
		t.Fatalf("execute workflow: %v", err)
	}
	firstRunID := run.GetRunID()
	if err := run.Get(ctx, nil); err != nil {
		t.Fatalf("await workflow: %v", err)
	}

	chain, err := BuildChain(ctx, c, nil, testNamespace, wfID, firstRunID)
	if err != nil {
		t.Fatalf("BuildChain: %v", err)
	}

	if got := len(chain.Runs); got != 3 {
		t.Fatalf("expected 3 runs, got %d: %+v", got, chain.Runs)
	}
	if chain.Runs[0].RunID != firstRunID {
		t.Errorf("first run = %s, want %s", chain.Runs[0].RunID, firstRunID)
	}
	if chain.RequestedRunID != firstRunID {
		t.Errorf("requested run = %s, want %s", chain.RequestedRunID, firstRunID)
	}
	for i, tr := range transitions(chain) {
		if tr != transitionContinueAsNew {
			t.Errorf("transition %d = %q, want %q", i, tr, transitionContinueAsNew)
		}
	}
	if chain.FinalRunID != chain.Runs[2].RunID {
		t.Errorf("final run = %s, want %s", chain.FinalRunID, chain.Runs[2].RunID)
	}
}

func TestBuildChain_OmittedRunID(t *testing.T) {
	c := dialTestClient(t)
	defer c.Close()
	tq := startWorker(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wfID := "follow-can-latest-" + uuid.NewString()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: wfID, TaskQueue: tq}, canWorkflow, 1)
	if err != nil {
		t.Fatalf("execute workflow: %v", err)
	}
	rootRunID := run.GetRunID() // captured before Get; Get follows the chain forward.
	if err := run.Get(ctx, nil); err != nil {
		t.Fatalf("await workflow: %v", err)
	}

	// No run id: should resolve the latest run and still reconstruct the full chain.
	chain, err := BuildChain(ctx, c, nil, testNamespace, wfID, "")
	if err != nil {
		t.Fatalf("BuildChain: %v", err)
	}
	if got := len(chain.Runs); got != 2 {
		t.Fatalf("expected 2 runs, got %d: %+v", got, chain.Runs)
	}
	if chain.Runs[0].RunID != rootRunID {
		t.Errorf("first run = %s, want root %s", chain.Runs[0].RunID, rootRunID)
	}
	// Omitting -r resolves the latest run, which is the terminal run of the chain.
	if chain.RequestedRunID != chain.FinalRunID {
		t.Errorf("requested run = %s, want final %s", chain.RequestedRunID, chain.FinalRunID)
	}
	if chain.Runs[1].RunID != chain.FinalRunID {
		t.Errorf("final run = %s, want %s", chain.FinalRunID, chain.Runs[1].RunID)
	}
}

func TestBuildChain_TerminalOnly(t *testing.T) {
	c := dialTestClient(t)
	defer c.Close()
	tq := startWorker(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wfID := "follow-simple-" + uuid.NewString()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: wfID, TaskQueue: tq}, simpleWorkflow)
	if err != nil {
		t.Fatalf("execute workflow: %v", err)
	}
	if err := run.Get(ctx, nil); err != nil {
		t.Fatalf("await workflow: %v", err)
	}

	chain, err := BuildChain(ctx, c, nil, testNamespace, wfID, run.GetRunID())
	if err != nil {
		t.Fatalf("BuildChain: %v", err)
	}
	if got := len(chain.Runs); got != 1 {
		t.Fatalf("expected 1 run, got %d: %+v", got, chain.Runs)
	}
	if chain.Runs[0].TransitionToNext != "" {
		t.Errorf("terminal run transition = %q, want empty", chain.Runs[0].TransitionToNext)
	}
}

func TestBuildChain_Reset(t *testing.T) {
	c := dialTestClient(t)
	defer c.Close()
	tq := startWorker(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wfID := "follow-reset-" + uuid.NewString()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: wfID, TaskQueue: tq}, simpleWorkflow)
	if err != nil {
		t.Fatalf("execute workflow: %v", err)
	}
	origRunID := run.GetRunID()
	if err := run.Get(ctx, nil); err != nil {
		t.Fatalf("await workflow: %v", err)
	}

	// Reset to the first completed workflow task, producing a new run.
	resetEventID := firstWorkflowTaskCompletedID(ctx, t, c, wfID, origRunID)
	resp, err := c.WorkflowService().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 testNamespace,
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: origRunID},
		Reason:                    "temporal-follow integration test",
		WorkflowTaskFinishEventId: resetEventID,
		RequestId:                 uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("reset workflow: %v", err)
	}
	newRunID := resp.GetRunId()

	// Wait for the new run to finish so the chain is stable.
	if err := c.GetWorkflow(ctx, wfID, newRunID).Get(ctx, nil); err != nil {
		t.Fatalf("await reset run: %v", err)
	}

	chain, err := BuildChain(ctx, c, nil, testNamespace, wfID, origRunID)
	if err != nil {
		t.Fatalf("BuildChain: %v", err)
	}
	if got := len(chain.Runs); got != 2 {
		t.Fatalf("expected 2 runs, got %d: %+v", got, chain.Runs)
	}
	if chain.Runs[0].RunID != origRunID {
		t.Errorf("first run = %s, want %s", chain.Runs[0].RunID, origRunID)
	}
	if chain.Runs[0].TransitionToNext != transitionReset {
		t.Errorf("transition = %q, want %q", chain.Runs[0].TransitionToNext, transitionReset)
	}
	if chain.FinalRunID != newRunID {
		t.Errorf("final run = %s, want %s", chain.FinalRunID, newRunID)
	}
}

// TestBuildChain_ResetOfContinueAsNew covers resetting a continue-as-new run.
// The reset run's ContinuedExecutionRunId points at the continue-as-new parent,
// not the run that was reset, so a backward-only walk would skip the reset
// source. The forward walk must include every run in order.
func TestBuildChain_ResetOfContinueAsNew(t *testing.T) {
	c := dialTestClient(t)
	defer c.Close()
	tq := startWorker(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Two runs: root (continues-as-new) -> child (completes).
	wfID := "follow-reset-can-" + uuid.NewString()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: wfID, TaskQueue: tq}, canWorkflow, 1)
	if err != nil {
		t.Fatalf("execute workflow: %v", err)
	}
	rootRunID := run.GetRunID()
	if err := run.Get(ctx, nil); err != nil {
		t.Fatalf("await workflow: %v", err)
	}

	// The continue-as-new child is the latest run; reset it.
	desc, err := c.DescribeWorkflowExecution(ctx, wfID, "")
	if err != nil {
		t.Fatalf("describe latest: %v", err)
	}
	childRunID := desc.GetWorkflowExecutionInfo().GetExecution().GetRunId()

	resetEventID := firstWorkflowTaskCompletedID(ctx, t, c, wfID, childRunID)
	resp, err := c.WorkflowService().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 testNamespace,
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: childRunID},
		Reason:                    "temporal-follow integration test",
		WorkflowTaskFinishEventId: resetEventID,
		RequestId:                 uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("reset workflow: %v", err)
	}
	resetRunID := resp.GetRunId()
	if err := c.GetWorkflow(ctx, wfID, resetRunID).Get(ctx, nil); err != nil {
		t.Fatalf("await reset run: %v", err)
	}

	chain, err := BuildChain(ctx, c, nil, testNamespace, wfID, "")
	if err != nil {
		t.Fatalf("BuildChain: %v", err)
	}
	wantIDs := []string{rootRunID, childRunID, resetRunID}
	if len(chain.Runs) != len(wantIDs) {
		t.Fatalf("expected %d runs, got %d: %+v", len(wantIDs), len(chain.Runs), chain.Runs)
	}
	for i, want := range wantIDs {
		if chain.Runs[i].RunID != want {
			t.Errorf("run[%d] = %s, want %s", i, chain.Runs[i].RunID, want)
		}
	}
	if got := transitions(chain); len(got) != 2 || got[0] != transitionContinueAsNew || got[1] != transitionReset {
		t.Errorf("transitions = %v, want [%s %s]", got, transitionContinueAsNew, transitionReset)
	}
	if chain.FinalRunID != resetRunID {
		t.Errorf("final run = %s, want %s", chain.FinalRunID, resetRunID)
	}
}

// firstWorkflowTaskCompletedID returns the event id of the first
// WorkflowTaskCompleted event, a valid reset point.
func firstWorkflowTaskCompletedID(ctx context.Context, t *testing.T, c client.Client, wfID, runID string) int64 {
	t.Helper()
	iter := c.GetWorkflowHistory(ctx, wfID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		ev, err := iter.Next()
		if err != nil {
			t.Fatalf("read history: %v", err)
		}
		if ev.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			return ev.GetEventId()
		}
	}
	t.Fatalf("no WorkflowTaskCompleted event found for run %s", runID)
	return 0
}
