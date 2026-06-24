"""Per-request records and the bounded recorder shared by both servers.

The logging story is deliberately thin — snailmail does not ship a logging framework.
It owns exactly three things that stdlib ``logging`` cannot provide on its own:

  * :class:`RequestRecord` — one immutable ``@dataclass`` per request (the drill-down
    unit), carrying the fields both servers can produce plus the server-specific extras
    (``range`` for HTTP; ``op``/``conditional`` for S3) as ``None`` on the other.
  * :class:`RequestLog` — a bounded ``collections.deque`` of recent records (always on,
    capped so memory can't run away) sitting alongside *exact* per-label / per-status
    counters, so the high-level summary stays complete even after the buffer has rolled.
  * a per-request line emitted to a stdlib ``logging.Logger`` (with the record attached
    via ``extra=`` for structured handlers). Levels, handlers, formatting, and JSON are
    the user's standard ``logging`` config — not our surface.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import Counter, deque
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RequestRecord:
    """One served request. The drill-down unit; aggregated into :meth:`RequestLog.breakdown`.

    Fields common to both servers are always set; the server-specific ones are ``None``
    on the server that has no such concept, so the record shape never forks. ``slots`` is
    set because a benchmark may retain up to ``max_records`` of these.
    """

    start_ms: float  # request start, ms since the server began serving
    method: str  # HTTP method (GET, PUT, ...)
    key: str  # object key / path served
    status: int  # response status (200, 206, 404, 416, 501, ...)
    nbytes: int  # total wire bytes for this request: request body (up) + response body (down)
    latency_ms: float  # the injected per-request latency (round-trip delay) drawn for this request
    dur_ms: float  # total wall time in the server (injected latency + bandwidth wait + work)
    in_flight: int  # concurrent requests at the moment this one was dispatched
    label: str  # classify(key): the group this request rolls up under in report()
    range: tuple[int, int] | None = None  # HTTP byte range [start, stop); None if whole-object
    op: str | None = None  # S3 operation (GET/LIST/PUT/...); None for HTTP
    conditional: bool | None = None  # S3 conditional-write header present; None for HTTP


@dataclass(frozen=True, slots=True)
class PendingRecord:
    """A request captured at dispatch; :meth:`finalize` completes it when the response ends.

    Both servers know most fields up front but learn the response's final downlink byte
    count (and, on S3, the status) only as the body streams. Carrying the early fields as
    one typed value — rather than a loose dict — lets the two servers build the same
    record the same way, differing only in which optional fields they fill.
    """

    t_start: float  # perf_counter at dispatch; basis for start_ms and dur_ms
    method: str
    key: str
    label: str
    in_flight: int
    latency_ms: float
    bytes_up: int = 0  # request-body bytes (S3 writes); 0 for reads
    range: tuple[int, int] | None = None
    op: str | None = None
    conditional: bool | None = None

    def finalize(self, *, t0: float, now: float, status: int, bytes_down: int) -> RequestRecord:
        """Complete the record once the response has been served."""
        return RequestRecord(
            start_ms=(self.t_start - t0) * 1e3,
            method=self.method,
            key=self.key,
            status=status,
            nbytes=self.bytes_up + bytes_down,
            latency_ms=self.latency_ms,
            dur_ms=(now - self.t_start) * 1e3,
            in_flight=self.in_flight,
            label=self.label,
            range=self.range,
            op=self.op,
            conditional=self.conditional,
        )


def identity(key: str) -> str:
    """Default HTTP classifier: each key is its own label (i.e. per-key counts)."""
    return key


class RequestLog:
    """Bounded record buffer + exact label/status counters + a stdlib logger emit.

    Thread-safe: HTTP calls :meth:`add` from the event-loop thread, S3 from request
    threads. ``max_records`` caps the drill-down buffer (``None`` = unbounded, ``0`` =
    keep counts only / no records); the counters are exact regardless, so :meth:`breakdown`
    is always complete and only the record list is limited to the most recent
    ``max_records`` requests.
    """

    def __init__(
        self,
        *,
        classify: Callable[[str], str] = identity,
        max_records: int | None = 100_000,
        logger_name: str,
    ):
        self.classify = classify
        self._logger = logging.getLogger(logger_name)
        self._lock = threading.Lock()
        self._maxlen = max_records
        self._t0 = time.perf_counter()  # start_ms / dur_ms are measured from here
        self._reset_state()

    def _reset_state(self) -> None:
        self.records: deque[RequestRecord] = deque(maxlen=self._maxlen)
        self.by_label: Counter[str] = Counter()
        self.label_bytes: Counter[str] = Counter()
        self.by_status: Counter[int] = Counter()
        self._total = 0  # records ever added since reset (to detect buffer truncation)

    def record(self, pending: PendingRecord, *, status: int, bytes_down: int) -> None:
        """Finalize a :class:`PendingRecord` against this log's clock, then store + emit it.

        The single entry point both servers use once a response is fully served — so the
        ``start_ms``/``dur_ms`` clock and the buffer/counter bookkeeping live in one place.
        """
        self.add(
            pending.finalize(
                t0=self._t0, now=time.perf_counter(), status=status, bytes_down=bytes_down
            )
        )

    def add(self, rec: RequestRecord) -> None:
        with self._lock:
            self.records.append(rec)
            self.by_label[rec.label] += 1
            self.label_bytes[rec.label] += rec.nbytes
            self.by_status[rec.status] += 1
            self._total += 1
        # Emit outside the lock. isEnabledFor() short-circuits when no handler wants it,
        # so the formatting cost is only paid when logging is actually configured on. The
        # line carries label (grep by component) and both clocks: "+Nms latency" is the
        # injected per-request delay, "Nms total" the real wall time the request took (so
        # bandwidth throttling, which inflates total beyond latency, shows up in the log).
        if self._logger.isEnabledFor(logging.INFO):
            self._logger.info(
                "%s %s [%s] -> %d  %dB  +%.0fms latency  %.0fms total  %d in flight",
                rec.method,
                rec.key,
                rec.label,
                rec.status,
                rec.nbytes,
                rec.latency_ms,
                rec.dur_ms,
                rec.in_flight,
                extra={"snailmail_record": rec},
            )

    def reset(self) -> None:
        with self._lock:
            self._reset_state()

    def snapshot(self) -> list[RequestRecord]:
        """A point-in-time copy of the retained records (oldest first)."""
        with self._lock:
            return list(self.records)

    def breakdown(self) -> dict:
        """Exact per-label and per-status summary, plus whether the buffer truncated."""
        with self._lock:
            by_label = {
                label: {"requests": n, "bytes": self.label_bytes[label]}
                for label, n in self.by_label.most_common()
            }
            return {
                "by_label": by_label,
                "by_status": dict(sorted(self.by_status.items())),
                "records_kept": len(self.records),
                "records_truncated": self._total > len(self.records),
            }

    def summary(self, totals: dict) -> dict:
        """A server's curated ``totals`` merged with the exact label/status breakdown.

        Both servers call this so the report shape — flat totals first, then ``by_label`` /
        ``by_status`` / ``records_*`` — is defined once here rather than in each server.
        """
        return {**totals, **self.breakdown()}
