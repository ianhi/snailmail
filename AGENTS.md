# AGENTS.md

Onboarding for an agent or contributor picking up snailmail. Read the
[README](README.md) first for what it is and why; this file is the "how it works,
how to work on it" layer.

## Goal

A zero-setup, in-process harness to benchmark range-based readers under realistic
network conditions, and to answer concurrency questions honestly. The product is the
measurement: GET count, bytes, and **peak concurrency** (`max_in_flight`) — that
last one is the whole point. Wall-clock can't distinguish "fast because cached" from
"fast because concurrent"; `max_in_flight` can.

## Layout

```
src/snailmail/
  __init__.py     # public exports
  latency.py      # LatencyDist + LogNormal / Normal / Exponential / Fixed
  bandwidth.py    # AsyncSharedPipe
  server.py       # LatencyRangeServer (the threaded aiohttp wrapper)
  cli.py          # the `snailmail` CLI (main, --dist arg wiring)
tests/
  test_server.py   # range correctness, latency, bandwidth, concurrency, counters
  test_latency.py  # distributions + CLI --dist wiring
```

One file per concern; keep each small and single-purpose. The split is to stay
easily editable, not an invitation to grow a framework — the whole thing should stay
readable in a sitting.

## Develop

```bash
uv sync                       # aiohttp, numpy + dev: pytest, ruff
uv run pytest                 # all green
uv run ruff check src tests
```

## Conventions

- **Commits:** do not co-sign — no `Co-Authored-By` / tool trailers.
- **Comments:** tight and useful — explain *why*, not *what*. No session- or
  conversation-specific notes ("as we discussed", change logs, dates); a comment
  must make sense to someone reading the file cold a year from now.

## Design decisions (read before changing things)

- **aiohttp `web.FileResponse` owns all HTTP correctness** — 206, `Content-Range`,
  suffix ranges, 416, conditional requests — and streams from disk. Do **not**
  reimplement range handling; that was the whole reason to rewrite off the original
  hand-rolled `BaseHTTPRequestHandler`. The file is never read into RAM, so multi-GB
  files work. Our consumers issue single-range GETs only, so multi-range responses
  are out of scope.

- **Serves a directory, always.** The root is served with aiohttp's `add_static`
  (range-correct *and* traversal-safe — don't hand-roll path joining). One object per
  file is the shape that matters for the Icechunk/object-store use case; to benchmark
  a single file, point at the directory containing it. There is deliberately no
  single-file mode — it added a `url`-vs-`base` duality and a custom handler for no
  real benefit. `base` is the root; `url(key)` builds a key URL. `FileResponse` defers
  its 404 to send time, so **misses are detected up front** via `_target_size()`
  (which also yields the size for byte accounting), not by inspecting the response
  status — a miss is a read whose path resolves to no file under the root, counted in
  `n_misses`.
- **Latency = a pluggable `LatencyDist`** (`latency.py`): `LogNormal`, `Normal`,
  `Exponential`, `Fixed`. **Lognormal is the recommended default and the one to reach
  for** — object-store GET RTT is a unimodal hump with a long right tail, which it
  fits; it's parameterised by the PDF **mode** (`mode_ms`) and shape `sigma`. The
  others exist for comparison, not because they model object stores well — notably
  `Exponential`'s peak sits at the floor, which is *wrong* for GET RTT; offer it, but
  don't recommend it. Every dist **pre-generates its pool once with numpy and serves
  it round-robin** — O(1) in the hot path, no per-request RNG, exactly reproducible
  per seed. The pool index is unsynchronised on purpose: all requests run on one
  event-loop thread, so it's safe. If you ever move to multiple loops/threads, that
  assumption breaks. Negative draws (Normal's left tail) are truncated at 0.
- **Bandwidth = one shared FIFO pipe** (`AsyncSharedPipe`): per-request RTTs stay
  parallel; response *bytes* serialize through the pipe, so egress is capped and
  over-read costs real time.
- **Async, in a background thread.** One event loop means many requests' latency
  sleeps overlap with no thread-pool ceiling — exactly what makes the
  peak-concurrency measurement clean. `start()` spawns the loop thread; `stop()`
  stops it. Don't reintroduce thread-per-request.
- **Counters under a lock.** `stats()` is a post-hoc, atomic snapshot (counts, total
  bytes, peak `max_in_flight`, and per-method / per-path breakdowns) that persists
  until `reset_counts()`. The `Range` header is parsed a second time in `_account`
  **for accounting only** (counts + bandwidth bytes) — serving correctness still
  comes entirely from aiohttp. If you need exact served bytes, that's the seam.

- **Compose aiohttp, don't subclass it.** aiohttp has no server base class meant for
  extension (its docs steer you to middlewares/signals over subclassing
  `web.Application`). `LatencyRangeServer` is a threaded lifecycle + counters facade
  around `web.Application` + `AppRunner`/`TCPSite`; keep it that way. The one private
  touch is reading the bound ephemeral port off `site._server.sockets` — aiohttp
  exposes no public API for it.
- **Injected latency is added on top** of the real (sub-ms, local-SSD) range read, so
  the modelled RTT stays dominated by the knob. Revisit for spinning disks or very
  large single ranges.

## Non-goals

- **Transport-accurate shaping.** snailmail models latency and bandwidth at the
  application layer (a `sleep()` plus a byte pipe), not on real packets. For
  kernel-level RTT/bandwidth use `tc netem` (Linux) or `dnctl`/`pfctl` (macOS) in
  front of any file server. Don't grow snailmail toward packet shaping.
- **A general-purpose web server.** It serves one file on loopback for benchmarks.

## Working notes

Current status, open tasks, and origin/context live in
[docs/NOTES.md](docs/NOTES.md) — the mutable worklog agents update. Keep *this* file
durable (purpose, design, conventions, non-goals); put anything time-specific in the
worklog.
