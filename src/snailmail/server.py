"""A local HTTP file server with Range support and injected per-request latency.

HTTP/Range correctness (206, Content-Range, suffix ranges, 416, conditional
requests) and on-disk streaming are handled by **aiohttp's ``web.FileResponse``** —
snailmail reimplements none of it. It adds only the benchmark instrumentation:

  * RANDOM per-request latency from a LOGNORMAL distribution (:class:`LatencyModel`):
    object-store GET RTT is well-modelled by a lognormal (unimodal hump + long
    right tail). Parameterised by the PDF mode (``latency_ms``) and log-scale shape
    (``sigma``); ``random=False`` gives a deterministic reference. Every request
    sleeps a draw before responding.
  * a shared-pipe BANDWIDTH limiter (:class:`AsyncSharedPipe`) modelling one finite
    client downlink: response bytes are metered through a single pipe so aggregate
    egress is capped and over-read costs real time. ``bandwidth_mbs=None`` disables.
  * server-side counters: true GET count, per-request byte ranges, and PEAK
    concurrency (max requests in flight), so wall-clock can be read honestly (not a
    serial ``n*rtt`` assumption).

Served from disk (``FileResponse`` streams; the file is never loaded into RAM), so
arbitrarily large files work. The injected latency is *added* to the real (sub-ms,
local-SSD) range-read time, so the modelled RTT stays dominated by the knob.

Consumers must opt into plain HTTP: obstore ``client_options={"allow_http": True}``,
icechunk ``http_store({"allow_http": "true"})``.

LOCAL loopback only (binds 127.0.0.1). Use in-process via :class:`LatencyRangeServer`
(exposes counters + live ``set_latency_ms``/``set_bandwidth_mbs``) or as the
``snailmail`` CLI.
"""

from __future__ import annotations

import argparse
import asyncio
import math
import threading
import time
from pathlib import Path

import numpy as np
from aiohttp import web


class LatencyModel:
    """Per-request latency drawn from a LOGNORMAL distribution (or fixed).

    Parameterised by the PDF **mode** (peak, the knob you sweep) and log-scale
    **shape** ``sigma``::

        mu       = ln(mode_ms) + sigma**2          # so the PDF mode == mode_ms
        sleep_ms = LogNormal(mu, sigma)

    Derived: ``median_ms = exp(mu) = mode*exp(sigma**2)``; ``mean_ms =
    exp(mu + sigma**2/2)``. A deterministic mode (``random=False`` or
    ``mode_ms == 0``) sleeps exactly ``mode_ms`` and is the zero-latency reference.

    Draws are served from a pool of ``pool_size`` samples generated once (vectorised
    numpy), then read round-robin: O(1) per request, no per-request RNG in the hot
    path, and an exactly reproducible realised distribution. The pool is large enough
    that wrap-around reuse is immaterial for a benchmark.
    """

    def __init__(self, mode_ms: float = 0.0, *, sigma: float = 0.5,
                 random: bool = True, seed: int | None = None, pool_size: int = 1 << 16):
        self.mode_ms = float(mode_ms)
        self.sigma = float(sigma)
        self.random = bool(random)
        self._pool: np.ndarray | None = None
        self._i = 0
        if self.mode_ms > 0.0:
            self.mu = math.log(self.mode_ms) + self.sigma**2
            self.median_ms = math.exp(self.mu)
            self.mean_ms = math.exp(self.mu + self.sigma**2 / 2.0)
            if self.random:
                rng = np.random.default_rng(seed)
                self._pool = rng.lognormal(self.mu, self.sigma, size=pool_size) / 1e3  # s
        else:
            self.mu = float("nan")
            self.median_ms = self.mean_ms = 0.0

    def describe(self) -> dict:
        if not self.random or self.mode_ms == 0.0:
            return {"kind": "fixed", "mode_ms": self.mode_ms}
        return {
            "kind": "lognormal", "mode_ms": round(self.mode_ms, 4), "sigma": self.sigma,
            "mu": round(self.mu, 6), "median_ms": round(self.median_ms, 4),
            "mean_ms": round(self.mean_ms, 4), "pool_size": int(self._pool.size),
        }

    def draw_s(self) -> float:
        """Next latency (seconds). O(1); single-loop-thread, so the index is safe."""
        if self._pool is None:
            return self.mode_ms / 1e3 if self.mode_ms > 0 else 0.0
        i = self._i
        self._i = i + 1 if i + 1 < self._pool.size else 0
        return float(self._pool[i])

    def realized_percentiles(self, n: int = 50000) -> dict:
        if self._pool is None:
            return {"p10_ms": self.mode_ms, "p50_ms": self.mode_ms,
                    "p90_ms": self.mode_ms, "p99_ms": self.mode_ms,
                    "n": int(self.mode_ms and 1)}
        p = np.percentile(self._pool * 1e3, [10, 50, 90, 99])  # the pool we actually serve
        return {"p10_ms": round(float(p[0]), 3), "p50_ms": round(float(p[1]), 3),
                "p90_ms": round(float(p[2]), 3), "p99_ms": round(float(p[3]), 3),
                "n": int(self._pool.size)}


class AsyncSharedPipe:
    """A FIFO bandwidth limiter modelling ONE shared client downlink (async).

    Every response's byte transfer is reserved through a single pipe of ``B``
    bytes/s, so aggregate egress can't exceed ``B`` no matter how many requests
    overlap, and over-read directly costs pipe time. Per-request latency stays
    parallel (handled separately); only bytes serialize here. ``B is None`` disables.
    """

    def __init__(self, bytes_per_s: float | None):
        self.B = bytes_per_s if bytes_per_s and bytes_per_s > 0 else None
        self._lock = asyncio.Lock()
        self._free = 0.0  # loop-clock timestamp the pipe is next free

    async def transfer(self, nbytes: int) -> None:
        if self.B is None or nbytes <= 0:
            return
        loop = asyncio.get_running_loop()
        async with self._lock:
            start = max(loop.time(), self._free)
            self._free = start + nbytes / self.B
            finish = self._free
        delay = finish - loop.time()
        if delay > 0:
            await asyncio.sleep(delay)

    def reset(self) -> None:
        self._free = 0.0


class LatencyRangeServer:
    """Threaded localhost HTTP server: aiohttp Range serving + injected latency.

    Parameters
    ----------
    file_path:   file to serve (its basename becomes the URL path).
    latency_ms:  lognormal PDF MODE (peak) per-request latency, ms (mutable).
    random_latency, sigma, seed:  passed to :class:`LatencyModel`.
    bandwidth_mbs:  shared-pipe bandwidth, MB/s (1 MB = 1e6 bytes); None = unlimited.
    port:        TCP port to bind (0 = ephemeral; set a fixed port when a consumer
                 mishandles ephemeral ports).
    """

    def __init__(self, file_path, latency_ms: float = 0.0, *, random_latency: bool = True,
                 sigma: float = 0.5, seed: int | None = None,
                 bandwidth_mbs: float | None = None, port: int = 0):
        self.file_path = Path(file_path)
        self.size = self.file_path.stat().st_size  # on disk; never read into RAM
        self._random_latency, self._sigma, self._seed = random_latency, sigma, seed
        self.latency = LatencyModel(latency_ms, sigma=sigma, random=random_latency, seed=seed)
        self.set_bandwidth_mbs(bandwidth_mbs)
        self._req_port = port
        self.port: int | None = None
        self.ranges: list[tuple[int, int]] = []
        self.n_requests = self.n_gets = 0
        self._in_flight = self.max_in_flight = 0
        self._lock = threading.Lock()
        self._ready = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._runner: web.AppRunner | None = None

    # -- accounting (range parsed only for counters/bandwidth; serving is aiohttp's) --
    def _account(self, range_header: str | None) -> int:
        if range_header and range_header.startswith("bytes="):
            spec = range_header[6:].split(",")[0]
            a, _, b = spec.partition("-")
            if a == "":
                start, end = max(0, self.size - int(b)), self.size - 1
            else:
                start, end = int(a), (int(b) if b else self.size - 1)
            start, end = max(0, start), min(end, self.size - 1)
        else:
            start, end = 0, self.size - 1
        if start > end:
            return 0
        with self._lock:
            self.ranges.append((start, end))
        return end - start + 1

    def _middleware(self):
        @web.middleware
        async def mw(request: web.Request, handler):
            with self._lock:
                self.n_requests += 1
                if request.method == "GET":
                    self.n_gets += 1
                self._in_flight += 1
                self.max_in_flight = max(self.max_in_flight, self._in_flight)
            try:
                await asyncio.sleep(self.latency.draw_s())  # the injected RTT
                nbytes = self._account(request.headers.get("Range"))
                if request.method == "GET":
                    await self.pipe.transfer(nbytes)  # shared-pipe bandwidth
                return await handler(request)
            finally:
                with self._lock:
                    self._in_flight -= 1

        return mw

    async def _handle(self, request: web.Request) -> web.FileResponse:
        return web.FileResponse(self.file_path)  # aiohttp owns Range/206/416/streaming

    async def _start(self):
        app = web.Application(middlewares=[self._middleware()])
        app.router.add_route("*", "/{name}", self._handle)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", self._req_port)
        await site.start()
        self.port = site._server.sockets[0].getsockname()[1]
        self._ready.set()

    def _serve(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._start())
        self._loop.run_forever()

    def start(self) -> "LatencyRangeServer":
        threading.Thread(target=self._serve, daemon=True).start()
        self._ready.wait()
        return self

    def stop(self) -> None:
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/{self.file_path.name}"

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}/"

    def set_latency_ms(self, latency_ms: float) -> None:
        self.latency = LatencyModel(latency_ms, sigma=self._sigma,
                                    random=self._random_latency, seed=self._seed)

    def set_bandwidth_mbs(self, bandwidth_mbs: float | None) -> None:
        self.bandwidth_mbs = bandwidth_mbs if bandwidth_mbs and bandwidth_mbs > 0 else None
        self.pipe = AsyncSharedPipe(None if self.bandwidth_mbs is None
                                    else self.bandwidth_mbs * 1e6)

    def reset_counts(self) -> None:
        with self._lock:
            self.ranges = []
            self.n_requests = self.n_gets = 0
            self.max_in_flight = self._in_flight
        self.pipe.reset()

    @property
    def total_bytes(self) -> int:
        return sum(e - s + 1 for s, e in self.ranges)

    def stats(self) -> dict:
        """Atomic snapshot of the request counters."""
        with self._lock:
            return {"n_gets": self.n_gets, "n_requests": self.n_requests,
                    "max_in_flight": self.max_in_flight,
                    "total_bytes": sum(e - s + 1 for s, e in self.ranges)}

    def describe(self) -> dict:
        return {"file": str(self.file_path), "size_bytes": self.size, "url": self.url,
                "port": self.port, "latency": self.latency.describe(),
                "bandwidth_mbs": self.bandwidth_mbs}

    def realized_percentiles(self, n: int = 50000) -> dict:
        return self.latency.realized_percentiles(n)

    def __enter__(self) -> "LatencyRangeServer":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()


def main() -> None:
    ap = argparse.ArgumentParser(prog="snailmail", description="Serve a file over HTTP with "
                                 "injected latency + bandwidth limits, for benchmarking.")
    ap.add_argument("file", help="file to serve")
    ap.add_argument("--latency-ms", type=float, default=0.0, help="lognormal MODE (peak) ms")
    ap.add_argument("--fixed", action="store_true", help="deterministic latency instead of lognormal")
    ap.add_argument("--sigma", type=float, default=0.5, help="lognormal shape (random mode)")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--bandwidth-mbs", type=float, default=None, help="shared-pipe MB/s; omit = unlimited")
    ap.add_argument("--port", type=int, default=0, help="TCP port (0 = ephemeral)")
    args = ap.parse_args()

    server = LatencyRangeServer(
        args.file, latency_ms=args.latency_ms, random_latency=not args.fixed,
        sigma=args.sigma, seed=args.seed, bandwidth_mbs=args.bandwidth_mbs, port=args.port,
    ).start()
    print(f"serving {server.file_path} ({server.size} bytes)")
    print(f"server  : {server.describe()}")
    print(f"realized: {server.realized_percentiles()}")
    print(f"url     : {server.url}")
    print("Ctrl-C to stop.")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()
        print(f"\nstopped. served {server.n_gets} GETs, {server.total_bytes} bytes, "
              f"peak concurrency {server.max_in_flight}.")


if __name__ == "__main__":
    main()
