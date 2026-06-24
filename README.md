# snailmail

[![PyPI](https://img.shields.io/pypi/v/snailmail.svg)](https://pypi.org/project/snailmail/)
[![CI](https://github.com/ianhi/snailmail/actions/workflows/ci.yml/badge.svg)](https://github.com/ianhi/snailmail/actions/workflows/ci.yml)

snailmail gives you a local object store or HTTP server that emulates a slow remote
server, injecting per-request latency and a bandwidth cap to reproduce the conditions
of a remote data source and/or a low-throughput wifi connection. Specifically, snailmail
gives you:

- **Latency distributions, not only fixed numbers** — capture the effect of a slow tail.
- **Bandwidth limiting** — model a bad wifi connection.

This lets you develop for remote use cases entirely on your laptop. Local development
normally hides the cost that dominates remote reads: network round-trips.

There are two interfaces, each with tunable latency, bandwidth, and quirks (like
disallowing conditional puts):

- **`HTTPRangeServer`** serves a directory over HTTP Range — for chunk **data** reads
  (Zarr/Icechunk virtual chunks, tiled image formats, object-store GETs).
- **`ObjectStore`** is an in-process, S3-compatible store — for the object/**metadata**
  reads a tool like [Icechunk](https://icechunk.io) makes *around* the data (config,
  refs, snapshots, manifests).

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

```python
from snailmail import HTTPRangeServer, LogNormal

with HTTPRangeServer("my_zarr_store/", latency=LogNormal(mode_ms=40), bandwidth_mbs=100) as server:
    open_and_read(server.base)         # your reader: obstore, icechunk, zarr, ...
    print(server.stats())
    # {'n_gets': 312, 'n_requests': 312, 'n_misses': 0, 'max_in_flight': 16,
    #  'total_bytes': 41943040, 'methods': {'GET': 312}, 'paths': {..}}
```

`open_and_read` stands in for the reader you're benchmarking. It makes HTTP GETs
(with `Range` headers) against `server.base`; snailmail injects the latency, meters
the bytes through the bandwidth pipe, and streams the file from disk in response. A
direct request looks like this:

```python
import urllib.request

with HTTPRangeServer("my_zarr_store/") as server:
    req = urllib.request.Request(server.url("chunks/0.0.0"), headers={"Range": "bytes=0-1023"})
    first_kib = urllib.request.urlopen(req).read()
```

`server.url(key)` builds the URL for a key; `server.files()` lists the served keys.
`stats()` is a snapshot of request counters since the last `reset_counts()`:
`n_requests` counts every request, `n_gets` only the GETs, and `n_misses` the
requests for keys that don't exist (404, like an object store's NoSuchKey). Tune
between measurements with `set_latency(dist)`, `set_bandwidth_mbs(x)`, and
`reset_counts()`.

### Serving a single file

To benchmark one file, use `HTTPRangeServer.from_file(path)` — it serves that file
directly (reachable at its basename), with no directory, no temp dir, and **no copy**,
so a multi-hundred-MB fixture costs nothing to set up:

```python
from snailmail import HTTPRangeServer, LogNormal

with HTTPRangeServer.from_file("CMU-1.tiff", latency=LogNormal(mode_ms=40)) as server:
    open_and_read(server.url("CMU-1.tiff"))   # server.files() == ["CMU-1.tiff"]
    print(server.stats())
```

It's the same server with one key: `describe()`, `files()`, `url()`, and `stats()`
behave exactly as in directory mode. The file is streamed from disk via the same
machinery, and since only that one path is ever served, there's no traversal surface —
every other key 404s.

Latency is a pluggable distribution passed as `latency=`:

```python
from snailmail import LogNormal, Normal, Exponential, Fixed

LogNormal(mode_ms=45, sigma=0.5)   # unimodal hump with long right tail; fits object-store GET RTT
Normal(mean_ms=45, std_ms=10)      # symmetric, truncated at 0
Exponential(mean_ms=45)            # peak at 0; a poor model for GET RTT
Fixed(20)                          # deterministic
```

`latency=None` (the default) injects no latency.

## Inspecting traffic

Both servers expose the **same three views** of what a reader did, since the last
`reset_counts()`. They go from coarse to fine, so you can start with the headline and
drill down only when you need to:

```python
with HTTPRangeServer("store/", latency=LogNormal(mode_ms=45)) as server:
    open_and_read(server.base)

    server.stats()    # flat counters: GETs, bytes, misses, peak concurrency, raw paths
    server.report()   # high-level summary: totals + by_label / by_status breakdowns
    server.requests   # the individual RequestRecord objects, to drill into single requests
```

**`report()`** is the headline — a plain, JSON-serializable dict built from *exact*
counters, so it's easy to assert on or log:

```python
{'n_requests': 84, 'n_gets': 84, 'n_misses': 0, 'total_bytes': 9700000, 'max_in_flight': 4,
 'by_label': {'level 0': {'requests': 64, 'bytes': 8400000},
              'level 1': {'requests': 20, 'bytes': 1300000}},
 'by_status': {200: 82, 206: 2},
 'records_kept': 84, 'records_truncated': False}
```

`by_label` groups requests however you want via a **`classify=` function** passed to the
constructor (`key -> label`). It defaults to per-key counts; pass a coarser function to
roll related keys up — e.g. by top-level directory, or, for a chunked dataset, by
resolution level:

```python
HTTPRangeServer("store/", classify=lambda key: key.split("/")[0])
```

**`server.requests`** is a list of `RequestRecord` (a frozen dataclass) for drill-down —
each carries `method`, `key`, `status`, `nbytes`, the injected `latency_ms`, the total
`dur_ms` (so bandwidth-throttling shows up as `dur_ms` ≫ `latency_ms`), `in_flight`,
`label`, and the byte `range`. Filter it like any list, or load it into pandas:

```python
slowest = sorted(server.requests, key=lambda r: r.dur_ms, reverse=True)[:5]
refetched = [r for r in server.requests if r.status == 200 and r.key == "chunks/0.0.0"]
```

It's a **bounded** buffer (`max_records=100_000` by default; `None` for unbounded) so a
long run can't exhaust memory — the `report()`/`stats()` counts stay exact regardless,
and `report()["records_truncated"]` flags when the buffer has rolled.

For a **live trace**, snailmail emits one line per request to the stdlib `logging`
loggers `snailmail.http` and `snailmail.s3` (off until you opt in — it's plain `logging`,
so you control format, level, and where it goes):

```python
import logging
logging.getLogger("snailmail").setLevel(logging.INFO)
logging.getLogger("snailmail").addHandler(logging.StreamHandler())
# GET chunks/0.0.0 [level 0] -> 200  97405B  +45ms latency  113ms total  4 in flight
```

`ObjectStore` works identically — its `report()` additionally splits
`metadata_requests` vs `data_requests`, and each record carries the S3 `op` and whether
the write was `conditional`.

## From the CLI

```bash
snailmail ./store --dist lognormal --mode-ms 45 --sigma 0.5
snailmail ./store --dist normal --mean-ms 45 --std-ms 10
snailmail ./store --dist exponential --mean-ms 45
snailmail ./store --dist fixed --value-ms 20
snailmail ./store --bandwidth-mbs 100 --port 8080 --json   # no latency; JSON address line
snailmail ./store --dist lognormal --mode-ms 45 --log      # stream one line per request
```

The argument is the directory to serve.

`--json` prints a single machine-readable line and flushes it before serving,
so a script can spawn snailmail, read the bound address from stdout, and proceed.

`--log` streams a per-request line to stderr while serving (the `snailmail.http` log,
above); on exit it prints a one-line summary including the status breakdown.

The CLI rejects a flag that doesn't belong to the chosen `--dist`. Omit `--dist`
for no injected latency.

## Object storage (Icechunk metadata)

The range server above models reading chunk **data**. But a tool like
[Icechunk](https://icechunk.io) also reads and writes **metadata** — config, refs,
snapshots, manifests — from an object store. Put that metadata on local disk and those
reads are *free*: once your data reads are tuned down to ~1 request, the metadata
round-trips that now dominate are invisible, and you can't compare against the cloud
honestly.

`ObjectStore` closes that gap. It's a real S3-compatible object store —
[moto](https://github.com/getmoto/moto) running in-process, so list/get/put/delete and
conditional writes all behave like S3 — wrapped in the **same** per-request latency and
bandwidth model as the range server (see [What it models](#what-it-models)). Metadata
operations pay realistic RTT, and it counts them, split by repo component, so you can read
off the metadata cost of an open or read separately from the data cost.

It's a store first: latency is **optional** wire shaping. Omit it and `ObjectStore()` is
just a plain local S3 store (still counted); add `latency=`/`bandwidth_mbs=` to shape the
wire. It needs the `s3` extra (which pulls in moto):

```bash
uv add 'snailmail[s3]'        # or: pip install 'snailmail[s3]'
```

Point Icechunk at it with `snailmail.convenience.icechunk_storage(store, ...)` — a
standalone convenience that returns a ready-wired `icechunk.Storage` (path-style, plain
HTTP, dummy credentials). It's a free function under `snailmail.convenience`, deliberately
*not* a method on `ObjectStore`: the store stays a general S3 server, and domain-specific
wiring lives beside it.

```python
import icechunk
from snailmail import ObjectStore, LogNormal
from snailmail.convenience import icechunk_storage

with ObjectStore(latency=LogNormal(mode_ms=45)) as store:
    repo = icechunk.Repository.open(icechunk_storage(store, prefix="my-repo"))
    read_an_array(repo)        # the reopen + read you're benchmarking

    print(store.stats())
    # {'n_requests': 6, 'n_misses': 2, 'metadata_requests': 4, 'data_requests': 0,
    #  'ops': {'GET': 6}, 'methods': {'GET': 6}, 'max_in_flight': 3,
    #  'total_bytes': 2427, 'bytes_down': 2427, 'bytes_up': 0,
    #  'prefixes': {'config': 1, 'refs': 1, 'snapshots': 1, 'manifests': 1, 'other': 2},
    #  'prefix_bytes': {'config': 323, 'refs': 337, 'snapshots': 604, 'manifests': 355},
    #  'conditional_stripped': 0, 'conditional_rejected': 0}
```

`metadata_requests` (config/refs/snapshots/manifests/transactions) and `data_requests`
(chunks) split the cost the way a benchmark wants it; `prefixes` and `prefix_bytes` give
the per-component breakdown. The same `report()` / `requests` views and `snailmail.s3`
per-request log described under [Inspecting traffic](#inspecting-traffic) apply here —
`report()` rolls the components into `by_label` and adds the metadata/data split, and each
record carries the S3 `op` and whether the write was `conditional`. As with the range
server, tune between measurements with `set_latency(dist)`, `set_bandwidth_mbs(x)`, and
`reset_counts()`, and read the endpoint from `store.endpoint_url` if you're driving it with
another S3 client (e.g. `obstore` or `boto3`). The store is in-process and ephemeral —
objects live in memory (moto spools any object over ~5 MB to a temp file) and vanish on
exit. (`quiet=False` additionally surfaces moto's own werkzeug access log on stderr.)

### Two buckets, and the client link

Virtualizing with Icechunk involves **two** object stores: the Icechunk store itself
(config, refs, snapshots, manifests, native chunks) and the *remote bucket it virtualizes*
(the original NetCDF/HDF5/GRIB files the virtual chunks point into). Those are different
backends with different latencies, so model them as two `ObjectStore`s — point the repo at
the first, and Icechunk's virtual-chunk container at the second's `endpoint_url`. You then
get a separate `report()` for each: metadata cost vs. virtualized-data cost.

`bandwidth_mbs` caps each *source's* egress independently. But both buckets are read by one
machine over one connection — so a shared **`ClientLink`** models that single
uplink/downlink, and their combined traffic contends for it:

```python
from snailmail import ObjectStore, ClientLink, LogNormal

client = ClientLink(down_mbs=50, up_mbs=10)   # your laptop's connection (asymmetric)

ice  = ObjectStore(bucket="icechunk",    latency=LogNormal(mode_ms=30),  client=client)
data = ObjectStore(bucket="source-data", latency=LogNormal(mode_ms=150), client=client)

with ice, data:
    ...                       # repo on `ice`; virtual chunks resolved against `data`
    ice.report()              # metadata round-trips, on the fast bucket
    data.report()             # virtual-data fetches, on the slow bucket
    # ice + data downloads can't jointly exceed 50 MB/s — they share `client.down`
```

Pass the **same** `ClientLink` to every store that shares the connection. Each request's
bytes meter through its source pipe and then the shared client pipe, so the client link
becomes the aggregate bottleneck when both buckets are busy at once. A per-store
`reset_counts()` leaves the shared link alone; call `client.reset()` to clear it. (The
series composition slightly over-counts a single uncontended transfer; it's accurate in
the regime that matters — client link slower than cloud egress — and is the only thing that
captures cross-store contention. `ClientLink` is `ObjectStore`-only for now, since
`HTTPRangeServer`'s pipe is async and can't be shared across event loops.)

### Emulating store quirks (conditional writes)

Real object stores differ in which S3 features they implement, and those differences
change how a tool like Icechunk must be configured. `ObjectStore` emulates such quirks via
a `StoreBehavior` — grouped so the API stays stable as more quirks are added.

The first quirk is **conditional writes** (`If-None-Match` / `If-Match`, which Icechunk
uses to make ref creation and commits atomic). Not every store implements them — JASMIN's,
for instance, rejects them. `StoreBehavior(conditional_writes=...)` models each behavior
locally, with no cloud credentials:

| `conditional_writes` | Models a store that… | A conditional write… |
|---|---|---|
| `"enforce"` *(default)* | supports them (real S3) | is honored (compare-and-swap) |
| `"reject"` | does **not** implement them (e.g. JASMIN) | is refused with `501 NotImplemented` |
| `"ignore"` | accepts but silently ignores them | overwrites unconditionally |

```python
from snailmail import ObjectStore, StoreBehavior

# Behaves like JASMIN: reject conditional writes with NotImplemented.
with ObjectStore(behavior=StoreBehavior(conditional_writes="reject")) as store:
    ...
    print(store.stats()["conditional_rejected"])   # count of writes refused
```

`"ignore"` is the quieter hazard — the write *succeeds* but loses its atomicity guarantee,
so it surfaces lost-update bugs; `stats()["conditional_stripped"]` counts those.

This makes otherwise creds-only failures reproducible on a laptop. `repros/icechunk_2228.py`
is a self-contained reproduction of [icechunk#2228](https://github.com/earth-mover/icechunk/issues/2228)
(conditional-op settings silently dropped under `spec_version=1`) — run it with
`uv run repros/icechunk_2228.py`, no JASMIN account required.

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
