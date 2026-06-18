# snailmail

A local HTTP file server that serves a file over **HTTP Range** while injecting
**tunable latency and bandwidth limits** — and counts the GETs and peak concurrency
it sees.

Use it to benchmark range-based readers — object stores, Zarr/Icechunk virtual
chunks, tiled image formats — under realistic network conditions, on your laptop,
with no cloud and no root.

## Why you'd want it

Local disk hides the cost that dominates remote reads: every ranged read that
becomes a network round-trip. A read pattern that's instant against a warm page
cache can be minutes of serial round-trips against object storage. snailmail puts
the round-trip back — a per-request latency draw and a shared bandwidth pipe — so
you can see how a reader *actually* behaves over the wire, and crucially **whether
it overlaps its requests** (the single biggest performance lever). `max_in_flight`
is the honest signal that wall-clock alone can't give you.

## Install

```bash
uv add snailmail        # or: pip install snailmail
```

## Use it in a benchmark

```python
from snailmail import LatencyRangeServer

with LatencyRangeServer("big.h5ad", latency_ms=40, bandwidth_mbs=100) as server:
    server.reset_counts()
    read_something(server.url)         # http://127.0.0.1:<port>/big.h5ad
    print(server.stats())              # {'n_gets': 312, 'max_in_flight': 16, 'total_bytes': ..}
```

If your reader fetches serially, `max_in_flight` stays 1; if it fans out
concurrently, it climbs. Tune live between measurements with `set_latency_ms`,
`set_bandwidth_mbs`, and `reset_counts`.

## Or from the CLI

```bash
snailmail big.h5ad --latency-ms 45              # lognormal, mode 45 ms
snailmail big.h5ad --latency-ms 45 --sigma 0.7  # heavier tail
snailmail big.h5ad --latency-ms 20 --fixed      # deterministic 20 ms
snailmail big.h5ad --bandwidth-mbs 100 --port 8080
```

## What it models

- **Latency** — a draw from a **lognormal** distribution, which fits object-store
  GET RTT well (a unimodal hump with a long right tail). You set the PDF *mode* (the
  peak, `--latency-ms`); `--sigma` controls the tail. `--fixed` sleeps exactly the
  mode (a deterministic reference).
- **Bandwidth** — one shared FIFO pipe (`--bandwidth-mbs`): per-request round-trips
  stay parallel, but response *bytes* serialize through the pipe, so aggregate
  egress is capped and over-read costs real transfer time. Omit for unlimited.

HTTP correctness (206, `Content-Range`, suffix ranges, 416, conditional requests)
and on-disk streaming come from aiohttp's `web.FileResponse` — the file is never
loaded into RAM, so multi-gigabyte files work.

## Notes

- Loopback only (binds `127.0.0.1`); nothing leaves the machine.
- Consumers must opt into plain HTTP: obstore `client_options={"allow_http": True}`,
  icechunk `http_store({"allow_http": "true"})`.
- The injected latency is *added* to the real (sub-millisecond, local-SSD)
  range-read time, so the modelled RTT stays dominated by your knob.
- For *transport-accurate* shaping on real packets, use `tc netem` (Linux) or
  `dnctl`/`pfctl` (macOS) in front of any file server; snailmail trades that for
  zero-setup, in-process instrumentation.

Contributing? See [AGENTS.md](AGENTS.md). MIT licensed.
