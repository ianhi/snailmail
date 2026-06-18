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
  __init__.py      # exports LatencyRangeServer, LatencyModel, AsyncSharedPipe
  server.py        # all of it: the three classes + the `snailmail` CLI (main)
tests/test_server.py  # range correctness, latency, bandwidth, concurrency, counters
```

It's one module on purpose — keep it that way unless it genuinely outgrows it.

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
- **Latency = lognormal**, parameterised by the PDF **mode** (`latency_ms`) and shape
  `sigma`. Object-store GET RTT is a unimodal hump with a long right tail; lognormal
  fits, a shifted-exponential doesn't (its peak sits at the floor). Draws are
  **pre-generated once with numpy and served round-robin** — O(1) in the hot path, no
  per-request RNG, exactly reproducible. The pool index is unsynchronised on purpose:
  all requests run on one event-loop thread, so it's safe. If you ever move to
  multiple loops/threads, that assumption breaks.
- **Bandwidth = one shared FIFO pipe** (`AsyncSharedPipe`): per-request RTTs stay
  parallel; response *bytes* serialize through the pipe, so egress is capped and
  over-read costs real time.
- **Async, in a background thread.** One event loop means many requests' latency
  sleeps overlap with no thread-pool ceiling — exactly what makes the
  peak-concurrency measurement clean. `start()` spawns the loop thread; `stop()`
  stops it. Don't reintroduce thread-per-request.
- **Counters under a lock.** The `Range` header is parsed a second time in `_account`
  **for accounting only** (counts + bandwidth bytes) — serving correctness still
  comes entirely from aiohttp. If you need exact served bytes, that's the seam.
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
