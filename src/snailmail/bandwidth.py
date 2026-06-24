"""Bandwidth limiting for responses."""

from __future__ import annotations

import asyncio
import threading
import time


class AsyncSharedPipe:
    """A FIFO bandwidth limiter modelling ONE shared client downlink (async).

    Every response's byte transfer is reserved through a single pipe of ``B``
    bytes/s, so aggregate egress can't exceed ``B`` no matter how many requests
    overlap, and over-read directly costs pipe time. Per-request latency stays
    parallel (handled separately); only bytes serialize here. ``B is None`` disables.
    """

    def __init__(self, bytes_per_s: float | None):
        self.B: float | None = bytes_per_s if bytes_per_s and bytes_per_s > 0 else None
        self._lock: asyncio.Lock = asyncio.Lock()
        self._free: float = 0.0  # loop-clock timestamp the pipe is next free

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


class SharedPipe:
    """Synchronous twin of :class:`AsyncSharedPipe` for WSGI (thread-per-request) servers.

    Same model and math as the async pipe — one shared downlink of ``B`` bytes/s, every
    transfer reserved through a single FIFO so aggregate egress is capped and over-read
    costs real time — but it blocks the calling thread with ``time.sleep`` instead of
    awaiting, and guards the cursor with a ``threading.Lock``. Uses ``time.monotonic`` as
    the clock (the asyncio loop clock has no meaning off the loop). ``B is None`` disables.
    """

    def __init__(self, bytes_per_s: float | None):
        self.B: float | None = bytes_per_s if bytes_per_s and bytes_per_s > 0 else None
        self._lock: threading.Lock = threading.Lock()
        self._free: float = 0.0  # monotonic timestamp the pipe is next free

    def transfer(self, nbytes: int) -> None:
        if self.B is None or nbytes <= 0:
            return
        with self._lock:
            start = max(time.monotonic(), self._free)
            self._free = start + nbytes / self.B
            finish = self._free
        delay = finish - time.monotonic()
        if delay > 0:
            time.sleep(delay)

    def reset(self) -> None:
        self._free = 0.0


def _pipe_from_mbs(mbs: float | None) -> tuple[SharedPipe, float | None]:
    """A sync pipe for a MB/s cap (``None``/``<=0`` = unlimited) plus its realized MB/s.

    Folds the MB/s↔bytes/s conversion and the read-back of the realized cap that
    :class:`ClientLink` would otherwise repeat for its up and down pipes.
    """
    pipe = SharedPipe(mbs * 1e6 if mbs else None)
    return pipe, (pipe.B / 1e6 if pipe.B else None)


class ClientLink:
    """ONE client connection's bandwidth, shared across multiple stores.

    A store's ``bandwidth_mbs`` caps that *source's* egress. A ``ClientLink`` models the
    other half: the single uplink/downlink on the client side that *all* sources share.
    Pass the **same** ``ClientLink`` instance to several :class:`~snailmail.s3.ObjectStore`
    s and their combined traffic contends for one downlink (reads) and one uplink (writes) —
    e.g. an Icechunk metadata bucket and the remote bucket it virtualizes both squeezing
    through your laptop's connection — on top of each store's own per-source cap.

    Links are asymmetric like real connections: ``down_mbs`` meters response bytes,
    ``up_mbs`` request bytes (MB/s = 1e6 bytes/s; ``None`` = that direction unlimited).

    Accuracy: a request's bytes are metered through its source pipe **and** the matching
    client pipe in series, so a single uncontended transfer's modelled time is the *sum*
    of the two stages rather than ``1 / min(rate)`` — a slight over-count by the smaller
    term, negligible in the regime that matters (client link slower than cloud egress).
    What it captures exactly is the thing per-source pipes can't: aggregate contention for
    the one client link across all sharing stores. For transport-accurate shaping use
    ``tc netem`` / ``dnctl``; this is in-process instrumentation.

    Scope: ``ObjectStore`` only. ``HTTPRangeServer`` meters through an async pipe bound to
    its own event loop, which can't be shared across loops — so a ``ClientLink`` cannot
    (yet) span a range server and an object store. This is a deliberate limit, not an
    oversight; mixed HTTP+S3 benchmarks get per-source shaping on the HTTP side.
    """

    def __init__(self, down_mbs: float | None = None, up_mbs: float | None = None):
        self.down, self.down_mbs = _pipe_from_mbs(down_mbs)
        self.up, self.up_mbs = _pipe_from_mbs(up_mbs)

    def reset(self) -> None:
        """Reset both pipes' cursors. Call this, not a store's ``reset_counts()``, to clear
        a shared link — per-store resets deliberately leave the shared link untouched."""
        self.down.reset()
        self.up.reset()
