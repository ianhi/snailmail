"""snailmail — a local HTTP file server with injectable latency and bandwidth limits.

For benchmarking range / object-store / virtual-chunk reads under realistic network
conditions. See :class:`LatencyRangeServer`.
"""

from snailmail.server import AsyncSharedPipe, LatencyModel, LatencyRangeServer

__all__ = ["LatencyRangeServer", "LatencyModel", "AsyncSharedPipe"]
__version__ = "0.1.0"
