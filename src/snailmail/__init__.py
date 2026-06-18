"""snailmail — a local HTTP file server with injectable latency and bandwidth limits.

For benchmarking range / object-store / virtual-chunk reads under realistic network
conditions. See :class:`LatencyRangeServer`.
"""

from importlib.metadata import PackageNotFoundError, version

from snailmail.bandwidth import AsyncSharedPipe
from snailmail.latency import Exponential, Fixed, LatencyDist, LogNormal, Normal
from snailmail.server import LatencyRangeServer

try:
    __version__ = version("snailmail")  # derived from the git tag at build time (hatch-vcs)
except PackageNotFoundError:  # running from a source tree with no install
    __version__ = "0+unknown"

__all__ = [
    "LatencyRangeServer",
    "AsyncSharedPipe",
    "LatencyDist",
    "LogNormal",
    "Normal",
    "Exponential",
    "Fixed",
]
