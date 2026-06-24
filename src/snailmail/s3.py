"""A local S3 object store with injected latency, for benchmarking metadata round-trips.

Where :class:`~snailmail.server.HTTPRangeServer` models *reading chunk data* (range
GETs on known objects), this models a real **object store** — list / get / put / delete
over many keys — which is what a consumer like **Icechunk** needs for its repository
metadata (config, refs, snapshots, manifests, transactions). Putting the repo on the
local filesystem makes those metadata reads free; this makes them pay realistic cloud
latency and counts them, so a benchmark can read off *metadata* cost separately from the
*data* cost served by the range server.

Buy-not-build: the S3 protocol itself (ListObjectsV2 XML, conditional PUT, checksums,
DeleteObjects) is served by **moto**, an in-process S3-compatible server. snailmail adds
only the benchmark instrumentation, as a thin **WSGI middleware** wrapping moto's app:

  * per-request latency from a pluggable distribution (:mod:`snailmail.latency`) — one
    draw per request, so a reopen touching N metadata objects costs ~N x RTT.
  * a shared-pipe BANDWIDTH limiter (:class:`~snailmail.bandwidth.SharedPipe`) metering
    transferred bytes through one finite link (the sync twin of the range server's pipe,
    so numbers stay comparable).
  * counters by S3 operation (GET/LIST/PUT/HEAD/DELETE) and by Icechunk repo component
    (config/refs/snapshots/manifests/transactions/chunks), plus a metadata-vs-data
    rollup, bytes up/down, 404 misses, and peak concurrency.

Latency is optional — :class:`ObjectStore` is a store first. Omit ``latency`` and you get
a plain local S3 store (with counting and any emulated quirks); add it to shape the wire.

It can also emulate store quirks via :class:`StoreBehavior` — notably a store that does
**not** support conditional writes (e.g. JASMIN), which rejects them with S3
``NotImplemented``. That makes otherwise creds-only failures (such as icechunk#2228)
reproducible locally.

Wrapping moto's WSGI app in-process (rather than reverse-proxying it) means there is no
second host and therefore no SigV4/Host-rewriting problem: requests are signed for, and
served by, the same endpoint. moto ignores signatures, so dummy credentials are fine.

LOCAL loopback only (binds 127.0.0.1). ``moto[s3,server]`` is an optional dependency;
install it with ``pip install 'snailmail[s3]'``.
"""

from __future__ import annotations

import threading
import time
from collections import Counter
from dataclasses import asdict, dataclass
from typing import Any, Callable, Iterable, Literal

from snailmail.bandwidth import ClientLink, SharedPipe
from snailmail.latency import Fixed, LatencyDist
from snailmail.record import PendingRecord, RequestLog, RequestRecord

# WSGI keys for the S3/HTTP preconditions that make a write conditional. Icechunk uses
# these for atomic ref create/commit (If-None-Match: * / If-Match: <etag>).
_CONDITIONAL_ENVIRON_KEYS = (
    "HTTP_IF_NONE_MATCH",
    "HTTP_IF_MATCH",
    "HTTP_IF_UNMODIFIED_SINCE",
    "HTTP_IF_MODIFIED_SINCE",
)

# S3's response when the backend does not implement conditional writes (e.g. JASMIN's
# object store). Returning this — rather than honoring or silently dropping the
# precondition — is what reproduces icechunk#2228 when a client still sends the header.
_NOT_IMPLEMENTED_XML = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n'
    b"<Error><Code>NotImplemented</Code>"
    b"<Message>A header you provided implies functionality that is not implemented."
    b"</Message></Error>"
)

ConditionalWrites = Literal["enforce", "reject", "ignore"]
_CONDITIONAL_WRITES = ("enforce", "reject", "ignore")


@dataclass(frozen=True)
class StoreBehavior:
    """Quirks of the object store being emulated, beyond plain spec-compliant S3.

    Grouping store-fidelity knobs here keeps the :class:`ObjectStore` constructor stable as
    more are added (checksum handling, multipart support, list-pagination limits, …): a new
    quirk is a new field, not a new positional argument. Today there is one knob.

    Parameters
    ----------
    conditional_writes:  how the store treats a **conditional write** — S3
        ``If-None-Match`` / ``If-Match``, which Icechunk uses for atomic ref create and
        commit. One of:

          * ``"enforce"`` (default): honor the precondition — real S3 compare-and-swap.
          * ``"reject"``: refuse it with S3 ``NotImplemented``, modeling a store without
            the feature (e.g. JASMIN). Reproduces icechunk#2228.
          * ``"ignore"``: accept the write but drop the precondition — a silent
            unconditional overwrite, modeling a store that ignores conditionals (surfaces
            lost-update bugs).
    """

    conditional_writes: ConditionalWrites = "enforce"

    def __post_init__(self) -> None:
        if self.conditional_writes not in _CONDITIONAL_WRITES:
            raise ValueError(f"conditional_writes must be one of {_CONDITIONAL_WRITES}")


# Icechunk repository layout. Everything but ``chunks`` is metadata; ``other`` catches
# bucket-level and non-repo traffic (create-bucket, root LIST, health checks).
_DATA_PREFIX = "chunks"
_META_PREFIXES = frozenset({"refs", "snapshots", "manifests", "transactions", "config"})

StartResponse = Callable[..., Any]
WSGIApp = Callable[[dict, StartResponse], Iterable[bytes]]


def _key_of(path: str) -> str:
    """The object key from a path-style ``/bucket/key...`` PATH_INFO ('' for bucket root)."""
    stripped = path.lstrip("/")
    bucket, _, key = stripped.partition("/")
    return key


def _classify_op(method: str, key: str, qs: str) -> str:
    """Map an S3 request to a coarse operation for counting."""
    if method == "GET":
        # ListObjectsV2 carries ?list-type=2; a keyless GET is a (v1) bucket list.
        if "list-type" in qs or key == "":
            return "LIST"
        return "GET"
    if method == "POST" and "delete" in qs:
        return "DELETE"  # DeleteObjects (batch) is POST /bucket?delete
    return method  # PUT, DELETE, HEAD, other POST (e.g. multipart)


def _classify_prefix(key: str) -> str:
    """Bucket an object key by Icechunk repo component, regardless of any key prefix."""
    for seg in key.split("/"):
        if seg == _DATA_PREFIX:
            return _DATA_PREFIX
        if seg == "config.yaml":
            return "config"
        if seg in _META_PREFIXES:
            return seg
    return "other"


def _content_length(environ: dict) -> int:
    try:
        return int(environ.get("CONTENT_LENGTH") or 0)
    except (TypeError, ValueError):
        return 0


class LatencyMiddleware:
    """WSGI middleware adding latency, bandwidth, and S3-aware counters to any WSGI app.

    Generic by design: it wraps *any* WSGI application (so it can be unit-tested against a
    trivial app with no moto), and :class:`ObjectStore` wires it to moto. Per
    request it draws one latency sample (the injected RTT), meters transferred bytes
    through a shared :class:`~snailmail.bandwidth.SharedPipe`, and records counters under a
    lock. It is thread-safe: the host server (werkzeug, ``threaded=True``) handles each
    request on its own thread, so latency sleeps overlap just like real concurrent RTTs.

    Parameters
    ----------
    app:             the wrapped WSGI application (moto's S3 app, in practice).
    latency:         per-request latency distribution; ``None`` injects none. Mutable via
                     :meth:`set_latency`.
    bandwidth_mbs:   shared-pipe bandwidth, MB/s (1 MB = 1e6 bytes); None = unlimited.
    behavior:        emulated store quirks (:class:`StoreBehavior`); controls how
                     conditional writes are handled. Mutable via :meth:`set_behavior`.
    """

    def __init__(
        self,
        app: WSGIApp,
        *,
        latency: LatencyDist | None = None,
        bandwidth_mbs: float | None = None,
        behavior: StoreBehavior = StoreBehavior(),
        classify: Callable[[str], str] = _classify_prefix,
        max_records: int | None = 100_000,
        client: ClientLink | None = None,
    ):
        self.app = app
        self.latency: LatencyDist = latency if latency is not None else Fixed(0.0)
        self.behavior = behavior
        self.set_bandwidth_mbs(bandwidth_mbs)
        # The shared client uplink/downlink, metered after the source pipe. Cache the
        # directional pipes so the hot path skips the None-check and attribute walk.
        self.client = client
        self._client_up = client.up if client is not None else None
        self._client_down = client.down if client is not None else None
        self._lock = threading.Lock()
        self.log = RequestLog(
            classify=classify, max_records=max_records, logger_name="snailmail.s3"
        )
        # The icechunk-component classifier is already computed as `prefix` for the legacy
        # counters; when it's also the record label (the default), reuse it instead of
        # re-running the same key scan per request.
        self._label_is_prefix = classify is _classify_prefix
        self._init_counts()

    def _init_counts(self) -> None:
        self.n_requests = 0
        self.n_misses = 0
        self.conditional_stripped = 0
        self.conditional_rejected = 0
        self.bytes_down = 0
        self.bytes_up = 0
        self.methods: Counter[str] = Counter()
        self.ops: Counter[str] = Counter()
        self.prefixes: Counter[str] = Counter()
        self.prefix_bytes: Counter[str] = Counter()
        self._in_flight = 0
        self.max_in_flight = 0

    def __call__(self, environ: dict, start_response: StartResponse) -> Iterable[bytes]:
        t_start = time.perf_counter()
        method = environ.get("REQUEST_METHOD", "GET")
        key = _key_of(environ.get("PATH_INFO", ""))
        qs = environ.get("QUERY_STRING", "")
        op = _classify_op(method, key, qs)
        prefix = _classify_prefix(key)
        is_read = method in ("GET", "HEAD")

        is_write = method in ("PUT", "POST")
        conditional = is_write and any(hdr in environ for hdr in _CONDITIONAL_ENVIRON_KEYS)
        reject = conditional and self.behavior.conditional_writes == "reject"
        stripped = conditional and self.behavior.conditional_writes == "ignore"
        if stripped:  # drop the precondition so the backend writes unconditionally
            for hdr in _CONDITIONAL_ENVIRON_KEYS:
                environ.pop(hdr, None)

        # A rejected write never reaches the backend, so its body is not consumed/metered.
        up = 0 if (reject or not is_write) else _content_length(environ)

        with self._lock:
            self.n_requests += 1
            self.methods[method] += 1
            self.ops[op] += 1
            self.prefixes[prefix] += 1
            if stripped:
                self.conditional_stripped += 1
            if reject:
                self.conditional_rejected += 1
            self._in_flight += 1
            in_flight = self._in_flight
            self.max_in_flight = max(self.max_in_flight, self._in_flight)
            # Draw under the lock: draw_s() advances a shared round-robin index that is not
            # itself thread-safe, and this middleware is called from many request threads.
            delay = self.latency.draw_s()

        # The record's start-time fields, finalized in _Metered.close() once the downlink
        # byte count (and status) is known. ``prefix`` (hardcoded icechunk component) drives
        # the legacy prefix counters; ``label`` is the configurable report grouping — the
        # same value unless the caller passed a custom classify=.
        pending = PendingRecord(
            t_start=t_start,
            method=method,
            key=key,
            label=prefix if self._label_is_prefix else self.log.classify(key),
            in_flight=in_flight,
            latency_ms=delay * 1e3,
            bytes_up=up,
            op=op,
            conditional=bool(conditional),
        )

        time.sleep(delay)  # the injected RTT, once per request — slept outside the lock

        if up:  # uplink bytes (a write): the source pipe, then the shared client uplink
            self._meter(up, self._client_up)
            with self._lock:
                self.bytes_up += up
                self.prefix_bytes[prefix] += up

        if reject:  # model a store that doesn't implement conditional writes (e.g. JASMIN)
            start_response(
                "501 Not Implemented",
                [
                    ("Content-Type", "application/xml"),
                    ("Content-Length", str(len(_NOT_IMPLEMENTED_XML))),
                ],
            )
            return _Metered(
                self, [_NOT_IMPLEMENTED_XML], prefix, pending=pending, status_box={"status": 501}
            )

        captured: dict[str, int] = {}

        def _sr(status: str, headers: list, exc_info: Any = None) -> Any:
            captured["status"] = int(status.split(" ", 1)[0])
            return start_response(status, headers, exc_info)

        app_iter = self.app(environ, _sr)
        # moto (a werkzeug app) calls start_response before returning the body, so the
        # status is known now — count a read miss here rather than in close(), which the
        # server may run long after the client has already seen the 4xx.
        if is_read and captured.get("status", 200) >= 400:
            with self._lock:
                self.n_misses += 1
        return _Metered(self, app_iter, prefix, pending=pending, status_box=captured)

    def _meter(self, nbytes: int, client_pipe: SharedPipe | None) -> None:
        """Serialize ``nbytes`` through this source's pipe, then the shared client pipe.

        One definition of the two-stage transfer used by both the uplink (write) and
        downlink (response) paths; ``client_pipe`` is the pre-resolved direction
        (``self._client_up`` / ``self._client_down``), ``None`` when no client link.
        """
        self.pipe.transfer(nbytes)
        if client_pipe is not None:
            client_pipe.transfer(nbytes)

    # -- live controls + observability (mirrors HTTPRangeServer) --
    def set_latency(self, latency: LatencyDist) -> None:
        self.latency = latency

    def set_bandwidth_mbs(self, bandwidth_mbs: float | None) -> None:
        self.pipe = SharedPipe(bandwidth_mbs * 1e6 if bandwidth_mbs else None)
        self.bandwidth_mbs = self.pipe.B / 1e6 if self.pipe.B else None

    def set_behavior(self, behavior: StoreBehavior) -> None:
        self.behavior = behavior

    def reset_counts(self) -> None:
        with self._lock:
            in_flight = self._in_flight
            self._init_counts()
            self.max_in_flight = self._in_flight = in_flight  # carry in-flight into the new window
        self.pipe.reset()
        self.log.reset()

    def stats(self) -> dict:
        """Atomic snapshot of the counters (persists until :meth:`reset_counts`).

        ``metadata_requests`` / ``data_requests`` split the request count into Icechunk
        metadata (config/refs/snapshots/manifests/transactions) vs chunk data, which is
        the headline a benchmark wants: the cost of an open/read on each side.
        """
        with self._lock:
            prefixes = dict(self.prefixes)
            metadata_requests = sum(v for k, v in prefixes.items() if k in _META_PREFIXES)
            return {
                "n_requests": self.n_requests,
                "n_misses": self.n_misses,
                "conditional_stripped": self.conditional_stripped,
                "conditional_rejected": self.conditional_rejected,
                "max_in_flight": self.max_in_flight,
                "total_bytes": self.bytes_down + self.bytes_up,
                "bytes_down": self.bytes_down,
                "bytes_up": self.bytes_up,
                "ops": dict(self.ops),
                "methods": dict(self.methods),
                "prefixes": prefixes,
                "prefix_bytes": dict(self.prefix_bytes),
                "metadata_requests": metadata_requests,
                "data_requests": prefixes.get(_DATA_PREFIX, 0),
            }

    def report(self) -> dict:
        """High-level summary: exact totals plus per-label / per-status breakdowns.

        Complements :meth:`stats` (flat counters) and :attr:`requests` (the records).
        The breakdown is computed from exact counters, so it is complete regardless of
        the ``max_records`` cap; ``records_truncated`` flags when the drill-down buffer
        dropped older records. ``metadata_requests`` + ``data_requests`` +
        ``other_requests`` (anything outside the Icechunk components, e.g. repo-existence
        probes) sum to ``n_requests``.
        """
        s = self.stats()
        return self.log.summary(
            {
                "n_requests": s["n_requests"],
                "n_misses": s["n_misses"],
                "total_bytes": s["total_bytes"],
                "max_in_flight": s["max_in_flight"],
                "metadata_requests": s["metadata_requests"],
                "data_requests": s["data_requests"],
                "other_requests": s["n_requests"] - s["metadata_requests"] - s["data_requests"],
            }
        )

    @property
    def requests(self) -> list[RequestRecord]:
        """Recent per-request records for drilling down (bounded by ``max_records``)."""
        return self.log.snapshot()

    def describe(self) -> dict:
        return {
            "latency": self.latency.describe(),
            "bandwidth_mbs": self.bandwidth_mbs,
            "behavior": asdict(self.behavior),
        }


class _Metered:
    """Wraps a WSGI response body to meter downlink bytes as it streams.

    Bytes are metered through the pipe and added to the counters *before each chunk is
    yielded* — i.e. before the server writes it to the client — so a ``stats()`` read that
    races a just-finished response still sees the bytes. ``close()`` decrements the
    in-flight gauge and emits the per-request :class:`~snailmail.record.RequestRecord`
    (it runs after the response is fully sent, which is the first point the total
    downlink byte count is known). The WSGI server always calls ``close()`` if present.
    """

    def __init__(
        self,
        mw: LatencyMiddleware,
        app_iter: Iterable[bytes],
        prefix: str,
        *,
        pending: PendingRecord,
        status_box: dict,
    ):
        self._mw = mw
        self._it = app_iter
        self._prefix = prefix
        self._pending = pending
        self._status_box = (
            status_box  # filled by the WSGI start_response wrapper (status arrives late)
        )
        self._down = 0

    def __iter__(self):
        mw, prefix = self._mw, self._prefix
        for chunk in self._it:
            if chunk:
                n = len(chunk)
                self._down += n
                mw._meter(n, mw._client_down)  # source pipe, then the shared client downlink
                with mw._lock:
                    mw.bytes_down += n
                    mw.prefix_bytes[prefix] += n
            yield chunk

    def close(self) -> None:
        closer = getattr(self._it, "close", None)
        if closer is not None:
            closer()
        with self._mw._lock:
            self._mw._in_flight -= 1
        self._mw.log.record(
            self._pending, status=self._status_box.get("status", 0), bytes_down=self._down
        )


class ObjectStore:
    """Threaded localhost S3 object store (moto) wrapped in :class:`LatencyMiddleware`.

    A **store first**: starting it spins up an in-process moto S3 server on a loopback port
    and creates an empty bucket. ``latency``/``bandwidth_mbs`` are optional wire shaping —
    omit them for a plain local S3 store (still counted, still able to emulate quirks). Add
    them to benchmark under realistic cloud conditions.

    Point a consumer at :attr:`endpoint_url` (path-style, plain HTTP), or use
    :meth:`icechunk_storage` for a ready-wired ``icechunk.Storage``. Counters and live
    controls (:meth:`set_latency`/:meth:`set_bandwidth_mbs`/:meth:`set_behavior`) delegate
    to the middleware, mirroring :class:`~snailmail.server.HTTPRangeServer` so a
    benchmark talks to both the same way.

    Parameters
    ----------
    bucket:          bucket to create and serve (default ``"snailmail"``).
    prefix:          default key prefix for :meth:`icechunk_storage` (the repo root).
    latency:         per-request latency distribution; ``None`` (default) injects none.
    bandwidth_mbs:   shared-pipe bandwidth, MB/s; None = unlimited.
    behavior:        emulated store quirks (:class:`StoreBehavior`) — e.g.
                     ``StoreBehavior(conditional_writes="reject")`` models a store like
                     JASMIN and reproduces icechunk#2228.
    region:          S3 region reported to the client (default ``"us-east-1"``).
    port:            TCP port to bind (0 = ephemeral).
    quiet:           suppress werkzeug's per-request access log (default ``True``); set
                     ``False`` to see each S3 request moto serves on stderr. (This is
                     moto's own access log; for snailmail's structured per-request line
                     use the ``snailmail.s3`` logger — see Observability below.)
    classify:        ``key -> label`` grouping for :meth:`report`'s ``by_label`` breakdown.
                     Defaults to the Icechunk-component classifier (config / refs /
                     snapshots / manifests / transactions / chunks); pass your own to
                     group differently.
    max_records:     cap on retained per-request records for :attr:`requests` drill-down
                     (a bounded ring buffer; ``None`` = unbounded, ``0`` = counts only). :meth:`report` and
                     :meth:`stats` counts stay exact regardless; only the record list is
                     capped, and :meth:`report` flags ``records_truncated`` when it rolls.
    client:          a shared :class:`~snailmail.bandwidth.ClientLink` modelling the one
                     client uplink/downlink. ``bandwidth_mbs`` caps *this source's* egress;
                     pass the **same** ``ClientLink`` to several stores to also cap their
                     *combined* traffic through one connection — e.g. an Icechunk store and
                     the bucket it virtualizes both squeezing through your laptop's link.
                     ``None`` (default) = no client-side cap.

    Observability
    -------------
    Three complementary views of traffic since the last :meth:`reset_counts`, mirroring
    :class:`~snailmail.server.HTTPRangeServer`:

    * :meth:`stats` — flat counters (by op / component, bytes up/down, misses, peak).
    * :meth:`report` — high-level summary dict: totals, ``metadata_requests`` vs
      ``data_requests``, and ``by_label`` / ``by_status`` breakdowns. JSON-serializable.
    * :attr:`requests` — recent :class:`~snailmail.record.RequestRecord` objects to drill
      into individual requests (op, status, bytes, injected RTT, duration, conditional).

    A per-request line is also emitted to the stdlib ``snailmail.s3`` logger at INFO (off
    until you add a handler / raise the level).
    """

    def __init__(
        self,
        *,
        bucket: str = "snailmail",
        prefix: str = "",
        latency: LatencyDist | None = None,
        bandwidth_mbs: float | None = None,
        behavior: StoreBehavior = StoreBehavior(),
        region: str = "us-east-1",
        port: int = 0,
        quiet: bool = True,
        classify: Callable[[str], str] = _classify_prefix,
        max_records: int | None = 100_000,
        client: ClientLink | None = None,
    ):
        try:
            from moto.server import DomainDispatcherApplication, create_backend_app
        except ImportError as exc:  # optional dependency
            raise ImportError(
                "ObjectStore needs moto; install it with: pip install 'snailmail[s3]'"
            ) from exc
        self.bucket = bucket
        self.prefix = prefix
        self.region = region
        self.middleware = LatencyMiddleware(
            DomainDispatcherApplication(create_backend_app),
            latency=latency,
            bandwidth_mbs=bandwidth_mbs,
            behavior=behavior,
            classify=classify,
            max_records=max_records,
            client=client,
        )
        self._req_port = port
        self._quiet = quiet
        self.port: int | None = None
        self._server: Any = None
        self._ready = threading.Event()
        self._startup_exc: BaseException | None = None

    def _serve(self) -> None:
        from werkzeug.serving import WSGIRequestHandler, make_server

        handler = WSGIRequestHandler
        if self._quiet:
            # werkzeug logs one access line per request to stderr; silence it with a
            # handler whose log hooks are no-ops (localized; no global logging changes).
            class _QuietHandler(WSGIRequestHandler):
                def log(self, *args: Any, **kwargs: Any) -> None:
                    pass

            handler = _QuietHandler

        try:
            self._server = make_server(
                "127.0.0.1",
                self._req_port,
                self.middleware,
                threaded=True,
                request_handler=handler,
            )
            self.port = self._server.server_port
        except BaseException as exc:  # surface bind failures instead of hanging start()
            self._startup_exc = exc
            self._ready.set()
            return
        self._ready.set()
        self._server.serve_forever()

    def start(self) -> "ObjectStore":
        threading.Thread(target=self._serve, daemon=True).start()
        self._ready.wait()
        if self._startup_exc is not None:
            raise self._startup_exc
        self._create_bucket()
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()

    def _create_bucket(self) -> None:
        """Create the bucket, then zero the counters so the caller starts from a clean slate.

        Uses a signed botocore client (always present with ``moto[server]``) rather than a
        raw request: moto treats unsigned requests as anonymous and forbids reads of a
        private bucket, so the bucket must be created — and later read — as an authenticated
        caller, which is exactly how a real consumer (icechunk/obstore) talks to it.
        """
        import botocore.session

        client = botocore.session.get_session().create_client(
            "s3",
            endpoint_url=self.endpoint_url,
            region_name=self.region,
            aws_access_key_id="snailmail",
            aws_secret_access_key="snailmail",
        )
        kwargs: dict[str, Any] = {"Bucket": self.bucket}
        if self.region != "us-east-1":  # us-east-1 must NOT send a LocationConstraint
            kwargs["CreateBucketConfiguration"] = {"LocationConstraint": self.region}
        try:
            client.create_bucket(**kwargs)
        except (
            client.exceptions.BucketAlreadyOwnedByYou,
            client.exceptions.BucketAlreadyExists,
        ):
            pass  # already exists (e.g. restart on a fixed port) — fine
        self.reset_counts()

    @property
    def endpoint_url(self) -> str:
        """S3 endpoint to hand a client (path-style, plain HTTP)."""
        return f"http://127.0.0.1:{self.port}"

    def icechunk_storage(self, prefix: str | None = None):
        """A ready-wired ``icechunk.Storage`` pointing at this store (read+write)."""
        import icechunk

        return icechunk.s3_storage(
            bucket=self.bucket,
            prefix=self.prefix if prefix is None else prefix,
            endpoint_url=self.endpoint_url,
            allow_http=True,
            force_path_style=True,
            region=self.region,
            access_key_id="snailmail",
            secret_access_key="snailmail",
        )

    # -- delegate live controls + observability to the middleware --
    def set_latency(self, latency: LatencyDist) -> None:
        self.middleware.set_latency(latency)

    def set_bandwidth_mbs(self, bandwidth_mbs: float | None) -> None:
        self.middleware.set_bandwidth_mbs(bandwidth_mbs)

    def set_behavior(self, behavior: StoreBehavior) -> None:
        self.middleware.set_behavior(behavior)

    def reset_counts(self) -> None:
        self.middleware.reset_counts()

    def stats(self) -> dict:
        return self.middleware.stats()

    def report(self) -> dict:
        return self.middleware.report()

    @property
    def requests(self) -> list[RequestRecord]:
        """Recent per-request records for drilling down (bounded by ``max_records``)."""
        return self.middleware.requests

    def realized_percentiles(self) -> dict:
        return self.middleware.latency.percentiles()

    def describe(self) -> dict:
        d = self.middleware.describe()
        d.update(
            bucket=self.bucket,
            prefix=self.prefix,
            endpoint_url=self.endpoint_url,
            region=self.region,
            port=self.port,
        )
        return d

    def __enter__(self) -> "ObjectStore":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()
