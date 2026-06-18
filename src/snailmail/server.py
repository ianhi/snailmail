"""A local HTTP server that serves a directory over Range with injected latency.

HTTP/Range correctness (206, Content-Range, suffix ranges, 416, conditional
requests), directory serving, and path-traversal safety are handled by **aiohttp's
static handler** — snailmail reimplements none of it. It adds only the benchmark
instrumentation:

  * per-request latency from a pluggable distribution (:mod:`snailmail.latency`).
  * a shared-pipe BANDWIDTH limiter (:class:`~snailmail.bandwidth.AsyncSharedPipe`)
    modelling one finite client downlink: response bytes are metered through a single
    pipe so aggregate egress is capped and over-read costs real time.
  * server-side counters: GET/request counts, bytes read, requested paths and
    methods, 404 misses, and PEAK concurrency (max requests in flight), so wall-clock
    can be read honestly (not a serial ``n*rtt`` assumption).

Serves every file under a root directory by relative path — one object per file, the
shape of an object-store / Icechunk virtual dataset. Files stream from disk and are
never loaded into RAM, so arbitrarily large files work. The injected latency is
*added* to the real (sub-ms, local-SSD) range-read time, so the modelled RTT stays
dominated by the knob.

Consumers must opt into plain HTTP: obstore ``client_options={"allow_http": True}``,
icechunk ``http_store({"allow_http": "true"})``.

LOCAL loopback only (binds 127.0.0.1). Use in-process via :class:`LatencyRangeServer`
(exposes counters + live :meth:`~LatencyRangeServer.set_latency` /
:meth:`~LatencyRangeServer.set_bandwidth_mbs`) or as the ``snailmail`` CLI.
"""

from __future__ import annotations

import asyncio
import threading
from collections import Counter
from pathlib import Path

from aiohttp import web

from snailmail.bandwidth import AsyncSharedPipe
from snailmail.latency import Fixed, LatencyDist


class LatencyRangeServer:
    """Threaded localhost HTTP server: aiohttp Range serving of a directory + latency.

    Parameters
    ----------
    root:        directory to serve. Every file beneath it is reachable at its path
                 relative to the root (range- and traversal-safe). To benchmark a
                 single file, put it in a directory and serve that.
    latency:     per-request latency distribution (a :class:`~snailmail.latency.LatencyDist`,
                 e.g. ``LogNormal(mode_ms=45)``); ``None`` injects no latency. Mutable
                 via :meth:`set_latency`.
    bandwidth_mbs:  shared-pipe bandwidth, MB/s (1 MB = 1e6 bytes); None = unlimited.
    port:        TCP port to bind (0 = ephemeral; set a fixed port when a consumer
                 mishandles ephemeral ports).
    """

    def __init__(
        self,
        root,
        *,
        latency: LatencyDist | None = None,
        bandwidth_mbs: float | None = None,
        port: int = 0,
    ):
        self.root = Path(root)
        if not self.root.is_dir():
            raise NotADirectoryError(f"root must be a directory: {self.root}")
        self._root_resolved = self.root.resolve()
        self.latency = latency if latency is not None else Fixed(0.0)
        self.set_bandwidth_mbs(bandwidth_mbs)
        self._req_port = port
        self.port: int | None = None
        self.total_bytes = 0
        self.n_requests = self.n_gets = self.n_misses = 0
        self.methods: Counter[str] = Counter()
        self.paths: Counter[str] = Counter()
        self._size_cache: dict[str, int] = {}  # loop-thread only, so no lock needed
        self._in_flight = self.max_in_flight = 0
        self._lock = threading.Lock()
        self._ready = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._runner: web.AppRunner | None = None
        self._startup_exc: BaseException | None = None

    # -- accounting (sizes/ranges parsed only for counters/bandwidth; serving is aiohttp's) --
    def _target_size(self, path: str) -> int | None:
        """Size of the file a request path maps to, or None if it resolves to no file.

        Done here rather than delegated to aiohttp on purpose: its static handler
        computes the same thing but never exposes it before serving, and FileResponse
        only decides a 404 at send time. We need the answer up front — the size to
        meter bandwidth before bytes flow, and the miss to count ``n_misses`` — so we
        do our own safe lookup with the stdlib (no aiohttp internals copied). The
        ``is_relative_to`` guard stops a ``..`` escape from being stat'd and
        mis-counted as a hit when aiohttp would serve it as a 404. Hits are cached by
        request path so repeats skip the filesystem.
        """
        if path in self._size_cache:
            return self._size_cache[path]
        try:
            target = (self.root / path.lstrip("/")).resolve()
            if not target.is_relative_to(self._root_resolved) or not target.is_file():
                return None  # traversal escape or missing file => a miss
            size = target.stat().st_size
        except OSError:
            return None
        self._size_cache[path] = size
        return size

    def _range_bytes(self, request: web.Request, size: int) -> int:
        """Bytes a GET will read against a known file size (pure; no side effects).

        Uses aiohttp's own ``request.http_range`` parser so this count matches what the
        static handler actually serves; a malformed Range raises ValueError there and
        aiohttp answers 416 (no body), so we count 0.
        """
        try:
            start, stop, _ = request.http_range.indices(size)
        except ValueError:
            return 0
        return max(0, stop - start)

    def _middleware(self):
        @web.middleware
        async def mw(request: web.Request, handler):
            # FileResponse defers its 404 to send time, so detect misses ourselves up
            # front (it's also the size we need for byte accounting).
            is_read = request.method in ("GET", "HEAD")
            size = self._target_size(request.path) if is_read else None
            with self._lock:
                self.n_requests += 1
                self.methods[request.method] += 1
                self.paths[request.path] += 1
                if request.method == "GET":
                    self.n_gets += 1
                if is_read and size is None:  # a miss still cost a round trip — count it
                    self.n_misses += 1
                self._in_flight += 1
                self.max_in_flight = max(self.max_in_flight, self._in_flight)
            nbytes = 0
            try:
                await asyncio.sleep(self.latency.draw_s())  # the injected RTT
                if request.method == "GET" and size is not None:
                    nbytes = self._range_bytes(request, size)
                    await self.pipe.transfer(nbytes)  # shared-pipe bandwidth
                return await handler(request)
            finally:
                with self._lock:
                    self._in_flight -= 1
                    self.total_bytes += nbytes

        return mw

    async def _start(self):
        app = web.Application(middlewares=[self._middleware()])
        # follow_symlinks=False (the default) keeps serving inside the root, matching
        # the traversal check in _target_size.
        app.router.add_static("/", self.root, follow_symlinks=False)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", self._req_port)
        await site.start()
        self.port = site._server.sockets[0].getsockname()[1]  # aiohttp has no public bound-port API

    def _serve(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._start())
        except BaseException as exc:  # surface startup failures instead of hanging start()
            self._startup_exc = exc
            self._ready.set()
            return
        self._ready.set()
        self._loop.run_forever()

    def start(self) -> "LatencyRangeServer":
        threading.Thread(target=self._serve, daemon=True).start()
        self._ready.wait()
        if self._startup_exc is not None:
            raise self._startup_exc
        return self

    def stop(self) -> None:
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)

    @property
    def base(self) -> str:
        """Root URL to point a reader (or object store) at; append a key to it."""
        return f"http://127.0.0.1:{self.port}/"

    def url(self, key: str) -> str:
        """URL for a single served key, e.g. ``server.url("chunks/0.0.0")``."""
        return f"{self.base}{key.lstrip('/')}"

    def files(self) -> list[str]:
        """The served keys: relative paths of every file under the root (sorted)."""
        return sorted(str(p.relative_to(self.root)) for p in self.root.rglob("*") if p.is_file())

    def set_latency(self, latency: LatencyDist) -> None:
        self.latency = latency

    def set_bandwidth_mbs(self, bandwidth_mbs: float | None) -> None:
        # AsyncSharedPipe owns the "<= 0 means unlimited" rule; read it back for display.
        self.pipe = AsyncSharedPipe(bandwidth_mbs * 1e6 if bandwidth_mbs else None)
        self.bandwidth_mbs = self.pipe.B / 1e6 if self.pipe.B else None

    def reset_counts(self) -> None:
        with self._lock:
            self.total_bytes = 0
            self.n_requests = self.n_gets = self.n_misses = 0
            self.methods = Counter()
            self.paths = Counter()
            self.max_in_flight = self._in_flight  # keep currently-active requests in the new window
        self.pipe.reset()

    def stats(self) -> dict:
        """Atomic snapshot of the request counters (persists until :meth:`reset_counts`)."""
        with self._lock:
            return {
                "n_gets": self.n_gets,
                "n_requests": self.n_requests,
                "n_misses": self.n_misses,
                "max_in_flight": self.max_in_flight,
                "total_bytes": self.total_bytes,
                "methods": dict(self.methods),
                "paths": dict(self.paths),
            }

    def describe(self) -> dict:
        return {
            "root": str(self.root),
            "base": self.base,
            "n_files": len(self.files()),
            "port": self.port,
            "latency": self.latency.describe(),
            "bandwidth_mbs": self.bandwidth_mbs,
        }

    def realized_percentiles(self) -> dict:
        return self.latency.percentiles()

    def __enter__(self) -> "LatencyRangeServer":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()
