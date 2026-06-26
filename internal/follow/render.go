package follow

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"
	"time"
)

// renderJSON writes the chain as indented JSON.
func renderJSON(w io.Writer, chain *Chain) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(chain)
}

// renderText writes a human-readable view of the chain, marking the requested
// run and the final run.
func renderText(w io.Writer, chain *Chain) error {
	if _, err := fmt.Fprintf(w, "Workflow: %s   (namespace %s)\n\n", chain.WorkflowID, chain.Namespace); err != nil {
		return err
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for i, r := range chain.Runs {
		marker := "  "
		if r.RunID == chain.RequestedRunID {
			marker = "➤ "
		}

		transition := ""
		switch {
		case r.TransitionToNext != "":
			transition = "→ " + r.TransitionToNext
		case r.RunID == chain.FinalRunID:
			transition = "(final)"
		}

		tags := ""
		if r.RunID == chain.RequestedRunID {
			tags = "  ← requested"
		}

		// Errors writing to the tabwriter surface on Flush below.
		_, _ = fmt.Fprintf(tw, "%s%d.\t%s\t%s\tstarted %s\t%s%s\n",
			marker, i+1, r.RunID, r.StatusName, formatTime(r.StartTime), transition, tags)
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	if final := finalNode(chain); final != nil {
		if _, err := fmt.Fprintf(w, "\nFinal run: %s (%s)\n", final.RunID, final.StatusName); err != nil {
			return err
		}
	}
	return nil
}

func finalNode(chain *Chain) *RunNode {
	for i := range chain.Runs {
		if chain.Runs[i].RunID == chain.FinalRunID {
			return &chain.Runs[i]
		}
	}
	return nil
}

func formatTime(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return t.Format(time.RFC3339)
}
