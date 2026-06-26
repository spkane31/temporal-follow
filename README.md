# temporal-follow

A [Temporal CLI extension](https://github.com/temporalio/proposals/blob/master/cli/cli-extensions.md)
that **follows a workflow forward through continue-as-new and reset operations**
and prints the full chain of runs — from the original root run to the final run.

Each continue-as-new or reset spawns a *new run* of the same workflow ID. Given
any run in that chain, `temporal follow` reconstructs and displays the whole
chain so you can see where a workflow actually ended up.

```
$ temporal follow -w my-workflow-id

Workflow: my-workflow-id   (namespace default)

  1.  019f05aa-c183-7294-8ff6-2078e730f24e  ContinuedAsNew  started 2026-06-26T20:41:44Z  → continue-as-new
  2.  ac23efc8-c536-4745-922e-8db1bad60411  ContinuedAsNew  started 2026-06-26T20:41:44Z  → continue-as-new
➤ 3.  793757c0-545c-4e54-be5e-d8e098651873  Completed       started 2026-06-26T20:41:45Z  (final)  ← requested

Final run: 793757c0-545c-4e54-be5e-d8e098651873 (Completed)
```

## Install

```sh
go install github.com/spkane31/temporal-follow/cmd/temporal-follow@latest
```

Or, from a local checkout:

```sh
go install ./cmd/temporal-follow      # or: make install
```

Both install a binary named `temporal-follow` into `$(go env GOBIN)` (or
`$(go env GOPATH)/bin`). That directory **must be on your `PATH`** so the
`temporal` CLI can discover the extension:

```sh
export PATH="$(go env GOPATH)/bin:$PATH"
```

> The binary must be named exactly `temporal-follow` for `temporal follow` to
> find it — that is why it lives in `cmd/temporal-follow/` (the binary takes the
> directory's name).

Once on `PATH`, `temporal follow ...` is handled by this extension — the
`temporal` CLI execs `temporal-follow` as a subprocess and relays its output and
exit code.

## Usage

```
temporal follow -w <workflow-id> [-r <run-id>] [client/auth flags]
```

| Flag | Description |
| ---- | ----------- |
| `-w`, `--workflow-id` | Workflow ID to follow. **Required.** |
| `-r`, `--run-id` | Run ID to start from. Optional; defaults to the workflow's current/latest run. |
| `-o`, `--output` | `text` (default) or `json`. |

The full chain is always shown (root → … → final), regardless of which run you
pass; the requested run is marked with `➤ … ← requested`.

It can also be run directly without the parent CLI:

```sh
temporal-follow -w my-workflow-id -o json
```

## Authentication

`temporal-follow` connects exactly like the `temporal` CLI — it shares the same
[environment configuration](https://docs.temporal.io/cli#environment) and flags:

- Connection flags: `--address`, `--namespace`/`-n`, `--api-key`, `--tls*`,
  `--codec-endpoint`, `--grpc-meta`, …
- Environment config: `--env`, `--profile`, `--config-file`, and the
  `TEMPORAL_*` environment variables.

For example, against Temporal Cloud:

```sh
temporal follow -w my-workflow-id \
  --address my-namespace.acct.tmprl.cloud:7233 \
  --namespace my-namespace.acct \
  --api-key "$TEMPORAL_API_KEY"
```

## How it works

- **Continue-as-new** is followed via the closing history event
  (`WorkflowExecutionContinuedAsNew` → `NewExecutionRunId`).
- **Reset** is followed via `WorkflowExecutionExtendedInfo.ResetRunId` from
  `DescribeWorkflowExecution`. On older servers (or under event-based
  cross-cluster replication) where that field is empty, it falls back to
  scanning the workflow's runs in visibility and reconstructing the link from
  each run's `ContinuedExecutionRunId` backpointer.
- It walks **backward** to the root run via `ContinuedExecutionRunId`, then
  **forward** to the terminal run.

## Development

```sh
make build    # build the binary
make test     # integration tests (needs `temporal server start-dev`)
make lint     # gofmt, go vet, golangci-lint
make install  # go install
```

Tests run against a live dev server. Start one with `temporal server start-dev`;
tests skip automatically when no server is reachable (override the address with
`TEMPORAL_ADDRESS`).
