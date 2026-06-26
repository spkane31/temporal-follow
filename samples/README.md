# Sample: verify `temporal follow`

This sample builds a real workflow chain on a Temporal dev server so you can see
`temporal follow` reconstruct it. It runs a workflow that **continues-as-new**
a couple of times and then **resets** the final run — producing a chain of
continue-as-new hops followed by a reset hop.

## Run it

In one terminal, start a dev server:

```sh
temporal server start-dev
```

In another, build the chain:

```sh
go run ./samples/continue-as-new-reset
```

It prints the workflow ID it created and the exact command to follow it, e.g.:

```
Created workflow chain for: temporal-follow-sample-4098f70b-...

    temporal follow -w temporal-follow-sample-4098f70b-...
```

Run that command (with `temporal-follow` installed and on your `PATH`) to see
the full chain:

```
Workflow: temporal-follow-sample-4098f70b-...   (namespace default)

  1.  019f05b9-...  ContinuedAsNew  started ...  → continue-as-new
  2.  a6e5ed73-...  ContinuedAsNew  started ...  → continue-as-new
  3.  676e3742-...  Completed       started ...  → reset
➤ 4.  395eb03f-...  Completed       started ...  (final)  ← requested

Final run: 395eb03f-... (Completed)
```

## Flags

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `--address` | `localhost:7233` | Temporal server gRPC address. |
| `--namespace` | `default` | Temporal namespace. |
| `--continue-as-new-count` | `2` | Number of continue-as-new hops. |
| `--reset` | `true` | Reset the final run to add a reset hop. |

For example, a pure continue-as-new chain with no reset:

```sh
go run ./samples/continue-as-new-reset --continue-as-new-count 3 --reset=false
```
