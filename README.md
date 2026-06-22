# snailmail

[![PyPI](https://img.shields.io/pypi/v/snailmail.svg)](https://pypi.org/project/snailmail/)
[![CI](https://github.com/ianhi/snailmail/actions/workflows/ci.yml/badge.svg)](https://github.com/ianhi/snailmail/actions/workflows/ci.yml)

A local HTTP server that serves a directory over HTTP Range, injecting per-request
latency and a bandwidth cap, and counts GETs and peak concurrency.

Use it to benchmark range-based readers — object stores, Zarr/Icechunk virtual
chunks, tiled image formats — under realistic network conditions, on your laptop,
with no cloud and no root.

## Why you'd want it

Local disk hides the cost that dominates remote reads: network round-trips.
A read pattern that finishes instantly against a warm page cache can take
minutes of serial round-trips against object storage. snailmail adds a
per-request latency draw and a shared bandwidth pipe so you can measure how a
reader behaves over the wire. `max_in_flight` tells you peak concurrency, which
wall-clock time alone cannot.

## Install

```bash
uv add snailmail        # or: pip install snailmail
```

## Use it in a benchmark

snailmail serves a directory. Every file under the root is reachable at its path
relative to the root, which matches the shape of an object store or Icechunk virtual
dataset (one object per file). Point your reader at `server.base` and have it fetch
keys like `chunks/0.0.0`.

A key is served iff its resolved real path is a file **inside** the root. Symlinks are
followed, but a symlink whose target escapes the root is not served (it 404s) and is
not listed by `files()` or counted in `n_files` — index and serving agree.

### Serving a single file

To benchmark one file, use `LatencyRangeServer.from_file(path)` — it serves that file
directly (reachable at its basename), with no directory, no temp dir, and **no copy**,
so a multi-hundred-MB fixture costs nothing to set up:

```python
from snailmail import LatencyRangeServer, LogNormal

with LatencyRangeServer.from_file("CMU-1.tiff", latency=LogNormal(mode_ms=40)) as server:
    open_and_read(server.url("CMU-1.tiff"))   # server.files() == ["CMU-1.tiff"]
    print(server.stats())
```

It's the same server with one key: `describe()`, `files()`, `url()`, and `stats()`
behave exactly as in directory mode. The file is streamed from disk via the same
machinery, and since only that one path is ever served, there's no traversal surface —
every other key 404s.

```python
from snailmail import LatencyRangeServer, LogNormal

with LatencyRangeServer("my_zarr_store/", latency=LogNormal(mode_ms=40), bandwidth_mbs=100) as server:
    server.reset_counts()
    open_and_read(server.base)         # your reader: obstore, icechunk, zarr, ...
    print(server.stats())
    # {'n_gets': 312, 'n_requests': 312, 'n_misses': 0, 'max_in_flight': 16,
    #  'total_bytes': .., 'methods': {'GET': 312}, 'paths': {..}}
```

`open_and_read` stands in for the reader you're benchmarking. It makes HTTP GETs
(with `Range` headers) against `server.base`; snailmail injects the latency, meters
the bytes through the bandwidth pipe, and streams the file from disk in response. A
direct request looks like this:

```python
import urllib.request

with LatencyRangeServer("my_zarr_store/") as server:
    req = urllib.request.Request(server.url("chunks/0.0.0"), headers={"Range": "bytes=0-1023"})
    first_kib = urllib.request.urlopen(req).read()
```

`server.url(key)` builds the URL for a key; `server.files()` lists the served keys.
`stats()` is a snapshot of request counters since the last `reset_counts()`:
`n_requests` counts every request, `n_gets` only the GETs, and `n_misses` the
requests for keys that don't exist (404, like an object store's NoSuchKey). Tune
between measurements with `set_latency(dist)`, `set_bandwidth_mbs(x)`, and
`reset_counts()`.

Latency is a pluggable distribution passed as `latency=`:

```python
from snailmail import LogNormal, Normal, Exponential, Fixed

LogNormal(mode_ms=45, sigma=0.5)   # unimodal hump with long right tail; fits object-store GET RTT
Normal(mean_ms=45, std_ms=10)      # symmetric, truncated at 0
Exponential(mean_ms=45)            # peak at 0; a poor model for GET RTT
Fixed(20)                          # deterministic
```

`latency=None` (the default) injects no latency.

## From the CLI

```bash
snailmail ./store --dist lognormal --mode-ms 45 --sigma 0.5
snailmail ./store --dist normal --mean-ms 45 --std-ms 10
snailmail ./store --dist exponential --mean-ms 45
snailmail ./store --dist fixed --value-ms 20
snailmail ./store --bandwidth-mbs 100 --port 8080 --json   # no latency; JSON address line
```

The argument is the directory to serve.

`--json` prints a single machine-readable line and flushes it before serving,
so a script can spawn snailmail, read the bound address from stdout, and proceed.

The CLI rejects a flag that doesn't belong to the chosen `--dist`. Omit `--dist`
for no injected latency.

## What it models

**Latency** is a per-request draw from the chosen distribution. `lognormal` is
the recommended default: parameterise it by the PDF mode (`--mode-ms`) and shape
(`--sigma`). `normal`, `exponential`, and `fixed` are available for comparison.

**Bandwidth** is a single shared FIFO pipe (`--bandwidth-mbs`, MB/s = 1e6 bytes/s).
Per-request round-trips run in parallel, but response bytes serialize through the
pipe, so aggregate egress is capped and over-read costs real transfer time. Omit
for unlimited bandwidth.

HTTP correctness (206, `Content-Range`, suffix ranges, 416, conditional requests)
and on-disk streaming come from aiohttp's `web.FileResponse`. Files are never
loaded into RAM, so multi-gigabyte files work.

Missing keys return 404 and are counted in `n_misses`, matching object-store
NoSuchKey behavior.

## Notes

- Loopback only (binds `127.0.0.1`); nothing leaves the machine.
- Consumers must opt into plain HTTP: obstore `client_options={"allow_http": True}`,
  icechunk `http_store({"allow_http": "true"})`.
- The injected latency is added to the real (sub-millisecond, local-SSD)
  range-read time, so the modelled RTT is dominated by the configured value.
- For transport-accurate shaping on real packets, use `tc netem` (Linux) or
  `dnctl`/`pfctl` (macOS) in front of any file server. snailmail trades that
  for zero-setup, in-process instrumentation.

Contributing? See [AGENTS.md](AGENTS.md). MIT licensed.
