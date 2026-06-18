# Working notes

The mutable worklog: current status, open tasks, and origin/context. Update this as
work happens. Durable design/conventions/non-goals belong in [../AGENTS.md](../AGENTS.md),
not here.

## Status

Alpha, working. Server, latency model, bandwidth pipe, counters, and CLI are done
and tested (`uv run pytest` green; ruff clean). Not yet published to PyPI.

## Open tasks

- Publish to PyPI (name reserved): `uv build` then `uv publish`.
- A live `stats()` readout while serving (low priority).

## Origin / context

Extracted from the `virtual-h5ad` project. The driving question there: does
`anndata[mask].to_memory()` over an Icechunk virtual repo fan out concurrent GETs,
or read serially? (Reading the call stack: it goes through zarr's
`codec_pipeline → asyncio.gather`, i.e. concurrent — so a hand-rolled thread-pool
reader was redundant.) snailmail is the harness to confirm that empirically.

That end-to-end benchmark is **blocked upstream** by an Icechunk bug: it drops the
port from virtual-chunk location URLs, so a repo pointed at
`http://127.0.0.1:<ephemeral>/...` is unreadable (the port-less location no longer
matches its container prefix). The `--port` argument exists partly so you can bind
port 80 to sidestep it until the Icechunk fix lands. Remove this note once the fix
ships and the benchmark runs.
