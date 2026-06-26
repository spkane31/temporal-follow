package follow

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

// maxHops bounds how far the chain walk will go, guarding against runaway loops
// in the presence of unexpected server state.
const maxHops = 10_000

// Transition labels describe how one run leads to the next in a chain.
const (
	transitionContinueAsNew = "continue-as-new"
	transitionReset         = "reset"
)

// Output formats.
const (
	OutputText  = "text"
	OutputJSON  = "json"
	OutputJSONL = "jsonl"
	OutputNone  = "none"
)

// TemporalClient is the subset of client.Client used to build a run chain. It
// exists so the chain logic is decoupled from the concrete SDK client;
// client.Client satisfies it directly.
type TemporalClient interface {
	DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error)
	GetWorkflowHistory(ctx context.Context, workflowID, runID string, isLongPoll bool, filterType enumspb.HistoryEventFilterType) client.HistoryEventIterator
	ListWorkflow(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error)
}

// RunOptions configures a follow run.
type RunOptions struct {
	WorkflowID string
	RunID      string
	Namespace  string
	Output     string
	Logger     *slog.Logger
}

// Run builds the workflow chain for the requested workflow and renders it to w.
func Run(ctx context.Context, c TemporalClient, opts RunOptions, w io.Writer) error {
	if opts.WorkflowID == "" {
		return fmt.Errorf("workflow id is required")
	}
	if opts.Namespace == "" {
		opts.Namespace = "default"
	}
	if err := validateOutput(opts.Output); err != nil {
		return err
	}

	chain, err := BuildChain(ctx, c, opts.Logger, opts.Namespace, opts.WorkflowID, opts.RunID)
	if err != nil {
		return err
	}

	switch normalizeOutput(opts.Output) {
	case OutputJSON, OutputJSONL:
		return renderJSON(w, chain)
	case OutputNone:
		return nil
	default:
		return renderText(w, chain)
	}
}

// RunNode is a single run within a workflow chain.
type RunNode struct {
	RunID            string                          `json:"runId"`
	Type             string                          `json:"type"`
	Status           enumspb.WorkflowExecutionStatus `json:"-"`
	StatusName       string                          `json:"status"`
	StartTime        *time.Time                      `json:"startTime,omitempty"`
	CloseTime        *time.Time                      `json:"closeTime,omitempty"`
	TransitionToNext string                          `json:"transitionToNext,omitempty"`
}

// Chain is the full ordered set of runs for a workflow, from the original root
// run through every continue-as-new / reset hop to the terminal run.
type Chain struct {
	WorkflowID     string    `json:"workflowId"`
	Namespace      string    `json:"namespace"`
	RequestedRunID string    `json:"requestedRunId"`
	FinalRunID     string    `json:"finalRunId"`
	Runs           []RunNode `json:"runs"`
}

// follower carries the client and context used while building a chain.
type follower struct {
	c         TemporalClient
	logger    *slog.Logger
	namespace string
	wf        string
}

// BuildChain reconstructs the full run chain for workflowID. If runID is empty,
// it resolves the workflow's current/latest run first.
//
// The chain is built by finding the root run (walking each run's
// ContinuedExecutionRunId backpointer to the origin) and then walking forward
// from the root through every continue-as-new / reset hop. The forward links
// (continue-as-new close events and ResetRunId) are authoritative; backpointers
// are only used to locate the root, because a reset run's ContinuedExecutionRunId
// points at the continue-as-new parent rather than the run that was reset.
//
// The requested run (the resolved run when runID was empty) is recorded as
// RequestedRunID so callers can highlight it.
func BuildChain(ctx context.Context, c TemporalClient, logger *slog.Logger, namespace, workflowID, runID string) (*Chain, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(noopWriter{}, nil))
	}
	f := &follower{c: c, logger: logger, namespace: namespace, wf: workflowID}

	// Resolve the requested run. An empty runID resolves to the latest run.
	startDesc, err := c.DescribeWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return nil, fmt.Errorf("describe workflow %q (run %q): %w", workflowID, runID, err)
	}
	requestedRunID := startDesc.GetWorkflowExecutionInfo().GetExecution().GetRunId()
	if requestedRunID == "" {
		return nil, fmt.Errorf("could not resolve run id for workflow %q", workflowID)
	}

	rootRunID, rootDesc, err := f.findRoot(ctx, requestedRunID, startDesc)
	if err != nil {
		return nil, err
	}

	runs, err := f.walkForward(ctx, rootRunID, rootDesc)
	if err != nil {
		return nil, err
	}

	chain := &Chain{
		WorkflowID:     workflowID,
		Namespace:      namespace,
		RequestedRunID: requestedRunID,
		Runs:           runs,
	}
	if len(runs) > 0 {
		chain.FinalRunID = runs[len(runs)-1].RunID
	}
	if !containsRun(runs, requestedRunID) {
		// Can happen when the requested run is a superseded reset branch that the
		// canonical forward chain (which follows the latest reset) bypasses.
		f.logger.Warn("requested run is not on the canonical forward chain", "runId", requestedRunID)
	}
	return chain, nil
}

// findRoot walks ContinuedExecutionRunId backpointers from fromRunID to the
// origin run (the one with no continued-execution backpointer) and returns it
// along with its describe response. fromDesc is the describe response for
// fromRunID and is reused when it is already the root.
func (f *follower) findRoot(ctx context.Context, fromRunID string, fromDesc *workflowservice.DescribeWorkflowExecutionResponse) (string, *workflowservice.DescribeWorkflowExecutionResponse, error) {
	seen := map[string]bool{fromRunID: true}
	cur := fromRunID
	desc := fromDesc
	for range maxHops {
		started, err := f.startedEvent(ctx, cur)
		if err != nil {
			return "", nil, err
		}
		prev := started.GetContinuedExecutionRunId()
		if prev == "" {
			return cur, desc, nil
		}
		if seen[prev] {
			f.logger.Warn("cycle detected finding root; stopping", "runId", prev)
			return cur, desc, nil
		}
		seen[prev] = true
		desc, err = f.c.DescribeWorkflowExecution(ctx, f.wf, prev)
		if err != nil {
			return "", nil, fmt.Errorf("describe run %q: %w", prev, err)
		}
		cur = prev
	}
	return cur, desc, nil
}

// walkForward builds the ordered run nodes from rootRunID forward to the
// terminal run, following continue-as-new (history close event) and reset
// (ResetRunId) links. rootDesc is the already-fetched describe response for
// rootRunID.
func (f *follower) walkForward(ctx context.Context, rootRunID string, rootDesc *workflowservice.DescribeWorkflowExecutionResponse) ([]RunNode, error) {
	var runs []RunNode
	seen := map[string]bool{}

	cur := rootRunID
	desc := rootDesc
	for range maxHops {
		seen[cur] = true
		node := nodeFromDesc(cur, desc)

		next, transition, err := f.nextRun(ctx, cur, desc)
		if err != nil {
			return nil, err
		}
		if next == "" {
			// Primary links exhausted; try the visibility fallback for resets
			// that the server did not expose via ResetRunId.
			fb, err := f.fallbackNext(ctx, cur, seen)
			if err != nil {
				f.logger.Warn("visibility fallback failed", "error", err)
			} else if fb != "" {
				f.logger.Info("extended chain via visibility fallback", "from", cur, "to", fb)
				next, transition = fb, transitionReset
			}
		}
		if next != "" && seen[next] {
			f.logger.Warn("cycle detected walking forward; stopping", "runId", next)
			next = ""
		}

		if next == "" {
			runs = append(runs, node)
			break
		}
		node.TransitionToNext = transition
		runs = append(runs, node)

		desc, err = f.c.DescribeWorkflowExecution(ctx, f.wf, next)
		if err != nil {
			return nil, fmt.Errorf("describe run %q: %w", next, err)
		}
		cur = next
	}
	return runs, nil
}

// containsRun reports whether runID appears in runs.
func containsRun(runs []RunNode, runID string) bool {
	for i := range runs {
		if runs[i].RunID == runID {
			return true
		}
	}
	return false
}

// nextRun determines the run that follows runID, if any. It returns the next
// run id and the transition label. desc is the describe response for runID
// (used for the ResetRunId reset pointer).
func (f *follower) nextRun(ctx context.Context, runID string, desc *workflowservice.DescribeWorkflowExecutionResponse) (string, string, error) {
	// Continue-as-new is recorded as the closing history event.
	closeEv, err := f.closeEvent(ctx, runID)
	if err != nil {
		return "", "", err
	}
	if closeEv != nil && closeEv.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW {
		if next := closeEv.GetWorkflowExecutionContinuedAsNewEventAttributes().GetNewExecutionRunId(); next != "" {
			return next, transitionContinueAsNew, nil
		}
	}
	// Reset is recorded as a forward pointer in the extended info.
	if next := desc.GetWorkflowExtendedInfo().GetResetRunId(); next != "" {
		return next, transitionReset, nil
	}
	return "", "", nil
}

// fallbackNext finds a run that continued from runID by scanning the workflow's
// runs in visibility and reconstructing the forward link from each run's
// ContinuedExecutionRunId backpointer. Used when ResetRunId is unavailable.
func (f *follower) fallbackNext(ctx context.Context, runID string, seen map[string]bool) (string, error) {
	query := fmt.Sprintf("WorkflowId = '%s'", f.wf)
	var token []byte
	for {
		resp, err := f.c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace:     f.namespace,
			Query:         query,
			NextPageToken: token,
		})
		if err != nil {
			return "", err
		}
		for _, ex := range resp.GetExecutions() {
			candidate := ex.GetExecution().GetRunId()
			if candidate == "" || seen[candidate] {
				continue
			}
			started, err := f.startedEvent(ctx, candidate)
			if err != nil {
				return "", err
			}
			if started.GetContinuedExecutionRunId() == runID {
				return candidate, nil
			}
		}
		token = resp.GetNextPageToken()
		if len(token) == 0 {
			return "", nil
		}
	}
}

// nodeFromDesc builds a RunNode from a run's describe response.
func nodeFromDesc(runID string, desc *workflowservice.DescribeWorkflowExecutionResponse) RunNode {
	info := desc.GetWorkflowExecutionInfo()
	node := RunNode{
		RunID:      runID,
		Type:       info.GetType().GetName(),
		Status:     info.GetStatus(),
		StatusName: statusName(info.GetStatus()),
	}
	if t := info.GetStartTime(); t != nil {
		st := t.AsTime()
		node.StartTime = &st
	}
	if t := info.GetCloseTime(); t != nil {
		ct := t.AsTime()
		node.CloseTime = &ct
	}
	return node
}

// startedEvent returns the WorkflowExecutionStarted attributes (the first event)
// for a run.
func (f *follower) startedEvent(ctx context.Context, runID string) (*historypb.WorkflowExecutionStartedEventAttributes, error) {
	iter := f.c.GetWorkflowHistory(ctx, f.wf, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	if !iter.HasNext() {
		return nil, fmt.Errorf("run %q has no history events", runID)
	}
	ev, err := iter.Next()
	if err != nil {
		return nil, fmt.Errorf("read first history event for run %q: %w", runID, err)
	}
	attrs := ev.GetWorkflowExecutionStartedEventAttributes()
	if attrs == nil {
		return nil, fmt.Errorf("first event of run %q is not WorkflowExecutionStarted", runID)
	}
	return attrs, nil
}

// closeEvent returns the closing history event for a run, or nil if the run is
// still open.
func (f *follower) closeEvent(ctx context.Context, runID string) (*historypb.HistoryEvent, error) {
	iter := f.c.GetWorkflowHistory(ctx, f.wf, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT)
	if !iter.HasNext() {
		return nil, nil
	}
	ev, err := iter.Next()
	if err != nil {
		return nil, fmt.Errorf("read close event for run %q: %w", runID, err)
	}
	return ev, nil
}

// statusName renders a workflow execution status without the verbose enum prefix.
func statusName(s enumspb.WorkflowExecutionStatus) string {
	switch s {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		return "Running"
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return "Completed"
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		return "Failed"
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return "Canceled"
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return "Terminated"
	case enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		return "ContinuedAsNew"
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return "TimedOut"
	default:
		return "Unspecified"
	}
}

// normalizeOutput maps blank/text to the default text output.
func normalizeOutput(output string) string {
	switch strings.ToLower(strings.TrimSpace(output)) {
	case "", OutputText:
		return OutputText
	default:
		return strings.ToLower(strings.TrimSpace(output))
	}
}

// validateOutput rejects unsupported output formats.
func validateOutput(output string) error {
	switch normalizeOutput(output) {
	case OutputText, OutputJSON, OutputJSONL, OutputNone:
		return nil
	default:
		return fmt.Errorf("invalid output %q, expected text, json, jsonl, or none", output)
	}
}

// noopWriter discards log output for the default logger.
type noopWriter struct{}

func (noopWriter) Write(p []byte) (int, error) { return len(p), nil }
