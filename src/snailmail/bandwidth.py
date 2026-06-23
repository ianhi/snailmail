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
