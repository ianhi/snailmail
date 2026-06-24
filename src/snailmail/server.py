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

LOCAL loopback only (binds 127.0.0.1). Use in-process via :class:`HTTPRangeServer`
(exposes counters + live :meth:`~HTTPRangeServer.set_latency` /
:meth:`~HTTPRangeServer.set_bandwidth_mbs`) or as the ``snailmail`` CLI.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections import Counter
from collections.abc import Callable
from pathlib import Path

from aiohttp import web

from snailmail.bandwidth import AsyncSharedPipe
from snailmail.latency import Fixed, LatencyDist
from snailmail.record import PendingRecord, RequestLog, RequestRecord, identity


class HTTPRangeServer:
    """Threaded localhost HTTP server: aiohttp Range serving of a directory + latency.

    Serve a **directory** with the constructor, or a **single file** with
    :meth:`from_file`. The two share one observable surface — same :meth:`describe`
    keys, :meth:`files`, :meth:`url`, and :meth:`stats` semantics — so a consumer
    never branches on which one it's talking to; single-file is just a one-key server.

    Parameters
    ----------
    root:        directory to serve. Every file beneath it is reachable at its path
                 relative to the root (range- and traversal-safe). To serve a lone
                 file without a containing directory, use :meth:`from_file`.
    latency:     per-request latency distribution (a :class:`~snailmail.latency.LatencyDist`,
                 e.g. ``LogNormal(mode_ms=45)``); ``None`` injects no latency. Mutable
                 via :meth:`set_latency`.
    bandwidth_mbs:  shared-pipe bandwidth, MB/s (1 MB = 1e6 bytes); None = unlimited.
    port:        TCP port to bind (0 = ephemeral; set a fixed port when a consumer
                 mishandles ephemeral ports).
    classify:    ``key -> label`` grouping for :meth:`report`'s ``by_label`` breakdown.
                 Default labels each key as itself (per-key counts); pass a coarser
                 function (e.g. ``lambda k: k.split("/")[0]`` to group by top-level
                 directory) to roll related keys up however your benchmark needs.
    max_records: cap on retained per-request records for :attr:`requests` drill-down
                 (a bounded ring buffer; ``None`` = unbounded, ``0`` = counts only). The :meth:`report` and
                 :meth:`stats` counts stay exact regardless — only the record list is
                 capped, and :meth:`report` flags ``records_truncated`` when it rolls.

    Observability
    -------------
    Three complementary views of traffic since the last :meth:`reset_counts`:

    * :meth:`stats` — flat counters (GETs, bytes, peak concurrency, raw paths/methods).
    * :meth:`report` — a high-level summary dict: totals plus ``by_label`` / ``by_status``
      breakdowns. JSON-serializable; the headline for "what did my reader do?".
    * :attr:`requests` — the recent :class:`~snailmail.record.RequestRecord` objects to
      drill into individual requests (status, bytes, byte range, injected RTT, duration).

    A per-request line is also emitted to the stdlib ``snailmail.http`` logger at INFO
    (off until you add a handler / raise the level), so live tracing is standard
    ``logging`` config, not a bespoke switch.
    """

    def __init__(
        self,
        root,
        *,
        latency: LatencyDist | None = None,
        bandwidth_mbs: float | None = None,
        port: int = 0,
        classify: Callable[[str], str] = identity,
        max_records: int | None = 100_000,
    ):
        self.root = Path(root)
        if not self.root.is_dir():
            raise NotADirectoryError(f"root must be a directory: {self.root}")
        self._root_resolved = self.root.resolve()
        self._file: Path | None = None  # directory mode
        self._key: str | None = None
        self._init_common(
            latency=latency,
            bandwidth_mbs=bandwidth_mbs,
            port=port,
            classify=classify,
            max_records=max_records,
        )

    @classmethod
    def from_file(
        cls,
        path,
        *,
        latency: LatencyDist | None = None,
        bandwidth_mbs: float | None = None,
        port: int = 0,
        classify: Callable[[str], str] = identity,
        max_records: int | None = 100_000,
    ) -> "HTTPRangeServer":
        """Serve a single file directly, reachable at its basename.

        The file is streamed straight from disk by aiohttp's ``FileResponse`` — the
        same machinery ``add_static`` delegates each file to — so Range/206/416/
        conditional handling is identical to directory mode, with **no temp dir, no
        symlink, and no copy**. Because the served path is one fixed, pre-resolved
        absolute path (the request path is never joined to the filesystem), there is
        no path-traversal surface at all: every key but the file's own basename 404s.

        The result is observationally a one-file directory server: ``files()`` is
        ``[basename]``, ``describe()["n_files"]`` is 1, and ``url(basename)`` addresses
        it — the same dict shapes the constructor produces.
        """
        src = Path(path)
        if not src.is_file():
            raise FileNotFoundError(f"file not found: {src}")
        self = cls.__new__(cls)
        self._file = src.resolve()
        self._key = self._file.name
        self.root = self._file  # a label for describe(); never add_static'd
        self._root_resolved = self._file
        self._init_common(
            latency=latency,
            bandwidth_mbs=bandwidth_mbs,
            port=port,
            classify=classify,
            max_records=max_records,
        )
        return self

    def _init_common(
        self,
        *,
        latency: LatencyDist | None,
        bandwidth_mbs: float | None,
        port: int,
        classify: Callable[[str], str],
        max_records: int | None,
    ) -> None:
        """Shared init for both constructors (everything but the root/file wiring)."""
        self.latency = latency if latency is not None else Fixed(0.0)
        self.set_bandwidth_mbs(bandwidth_mbs)
        self._req_port = port
        self.port: int | None = None
        self.total_bytes = 0
        self.n_requests = self.n_gets = self.n_misses = 0
        self.methods: Counter[str] = Counter()
        self.paths: Counter[str] = Counter()
        self._log = RequestLog(
            classify=classify, max_records=max_records, logger_name="snailmail.http"
        )
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
        if self._file is not None:  # single-file mode: only the one key exists
            if path.lstrip("/") != self._key:
                return None
            try:
                size = self._file.stat().st_size
            except OSError:
                return None
            self._size_cache[path] = size
            return size
        try:
            target = (self.root / path.lstrip("/")).resolve()
            if not target.is_relative_to(self._root_resolved) or not target.is_file():
                return None  # traversal escape or missing file => a miss
            size = target.stat().st_size
        except OSError:
            return None
        self._size_cache[path] = size
        return size

    def _resolve_range(
        self, request: web.Request, size: int
    ) -> tuple[int, int, tuple[int, int] | None]:
        """Resolve a GET against a known file size in a single parse of the Range header.

        Returns ``(status, nbytes, range)``: the status aiohttp will send (200 whole / 206
        partial / 416 unsatisfiable — ``FileResponse`` defers this to send time, so we
        derive it here from aiohttp's own ``http_range`` parser), the body bytes that will
        be served, and the ``[start, stop)`` range (``None`` for a whole-object read).

        Scope: this derivation assumes the ``Range`` is honored as written, which covers
        the range/full/unsatisfiable GETs that object-store and chunk readers actually
        issue. It does **not** evaluate conditional precondition headers — an ``If-Range``
        that fails (or an ``If-*`` that yields 304) makes aiohttp send a different status
        than derived here. Those headers aren't used by the readers this serves; if you
        need exact status under them, read it off the wire instead.
        """
        has_range = request.headers.get("Range") is not None
        try:
            start, stop, _ = request.http_range.indices(size)
        except ValueError:  # malformed range -> aiohttp answers 416, no body
            return 416, 0, None
        nbytes = max(0, stop - start)
        if has_range and nbytes == 0:
            # A well-formed range that resolves to zero bytes is unsatisfiable (start at/
            # past EOF, or any range on an empty file) -> aiohttp sends 416, no body. indices()
            # clamps rather than raising, so this is the only signal that it's unsatisfiable.
            return 416, 0, None
        if start == 0 and stop == size:  # whole object (range absent, or a full-coverage range)
            return (206 if has_range else 200), size, None
        return 206, nbytes, (start, stop)

    def _middleware(self):
        @web.middleware
        async def mw(request: web.Request, handler):
            # FileResponse defers its 404 to send time, so detect misses ourselves up
            # front (it's also the size we need for byte accounting).
            is_read = request.method in ("GET", "HEAD")
            size = self._target_size(request.path) if is_read else None
            start = time.perf_counter()
            with self._lock:
                self.n_requests += 1
                self.methods[request.method] += 1
                self.paths[request.path] += 1
                if request.method == "GET":
                    self.n_gets += 1
                if is_read and size is None:  # a miss still cost a round trip — count it
                    self.n_misses += 1
                self._in_flight += 1
                in_flight = self._in_flight
                self.max_in_flight = max(self.max_in_flight, self._in_flight)
            nbytes = 0
            rng: tuple[int, int] | None = None
            # Derive the status up front (FileResponse only finalizes it at send time).
            status = 404 if (is_read and size is None) else 200
            if request.method == "GET" and size is not None:
                status, nbytes, rng = self._resolve_range(request, size)
            delay = self.latency.draw_s()
            try:
                await asyncio.sleep(delay)  # the injected RTT
                if nbytes:
                    await self.pipe.transfer(nbytes)  # shared-pipe bandwidth
                return await handler(request)
            except web.HTTPException as exc:  # unexpected raise: prefer aiohttp's status
                status = exc.status
                raise
            finally:
                with self._lock:
                    self._in_flight -= 1
                    self.total_bytes += nbytes
                key = request.path.lstrip("/")
                self._log.record(
                    PendingRecord(
                        t_start=start,
                        method=request.method,
                        key=key,
                        label=self._log.classify(key),
                        in_flight=in_flight,
                        latency_ms=delay * 1e3,
                        range=rng,
                    ),
                    status=status,
                    bytes_down=nbytes,
                )

        return mw

    async def _start(self):
        app = web.Application(middlewares=[self._middleware()])
        if self._file is not None:
            # Single-file mode: one route serving one pinned absolute path via
            # FileResponse (the class add_static uses per file). add_get registers HEAD
            # too (allow_head defaults True). Any other key falls through to aiohttp's
            # 404, which the middleware still counts as a miss. No path is joined to the
            # filesystem, so there is no traversal surface.
            file_path = self._file

            async def serve_one(request: web.Request) -> web.FileResponse:
                return web.FileResponse(file_path)

            app.router.add_get(f"/{self._key}", serve_one)
        else:
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

    def start(self) -> "HTTPRangeServer":
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
        """The served keys: relative paths of every file under the root (sorted).

        A path is a served key iff it maps to a real file *inside* the root — the
        same resolve-then-in-root rule aiohttp's static handler and ``_target_size``
        apply. ``p.is_file()`` alone would follow a symlink without re-checking the
        target, so a symlink whose target escapes the root (a 404 on GET, a miss in
        ``_target_size``) must not be listed here or ``n_files`` would over-count
        keys that can never be served.
        """
        if self._key is not None:  # single-file mode: the one served key
            return [self._key]
        keys = []
        for p in self.root.rglob("*"):
            try:
                target = p.resolve()
                if target.is_file() and target.is_relative_to(self._root_resolved):
                    keys.append(str(p.relative_to(self.root)))
            except OSError:
                continue  # broken/circular symlink => not a served key
        return sorted(keys)

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
        self._log.reset()

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

    @property
    def requests(self) -> list[RequestRecord]:
        """Recent per-request records for drilling down (bounded by ``max_records``).

        A plain list of :class:`~snailmail.record.RequestRecord` — filter it with a
        comprehension (``[r for r in s.requests if r.status == 404]``) or load it into
        pandas. The high-level counts in :meth:`report` stay exact even when this buffer
        has rolled.
        """
        return self._log.snapshot()

    def report(self) -> dict:
        """High-level summary: exact totals plus per-label / per-status breakdowns.

        Complements :meth:`stats` (flat counters) and :attr:`requests` (the records).
        The breakdown is computed from exact counters, so it is complete regardless of
        the ``max_records`` cap; ``records_truncated`` flags when the drill-down buffer
        dropped older records.
        """
        s = self.stats()
        return self._log.summary(
            {
                "n_requests": s["n_requests"],
                "n_gets": s["n_gets"],
                "n_misses": s["n_misses"],
                "total_bytes": s["total_bytes"],
                "max_in_flight": s["max_in_flight"],
            }
        )

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

    def __enter__(self) -> "HTTPRangeServer":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()
