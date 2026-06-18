"""Bandwidth limiting for responses."""

from __future__ import annotations

import asyncio


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
