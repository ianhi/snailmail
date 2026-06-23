"""snailmail — local HTTP-range and S3 servers with injectable latency and bandwidth limits.

For benchmarking range / object-store / virtual-chunk reads under realistic network
conditions. See :class:`HTTPRangeServer` (range/file serving) and :class:`ObjectStore`
(S3 object storage).
"""

from importlib.metadata import PackageNotFoundError, version

from snailmail.bandwidth import AsyncSharedPipe, SharedPipe
from snailmail.latency import Exponential, Fixed, LatencyDist, LogNormal, Normal
from snailmail.s3 import LatencyMiddleware, ObjectStore, StoreBehavior
from snailmail.server import HTTPRangeServer

try:
    __version__ = version("snailmail")  # derived from the git tag at build time (hatch-vcs)
except PackageNotFoundError:  # running from a source tree with no install
    __version__ = "0+unknown"

__all__ = [
    "HTTPRangeServer",
    "ObjectStore",
    "StoreBehavior",
    "LatencyMiddleware",
    "AsyncSharedPipe",
    "SharedPipe",
    "LatencyDist",
    "LogNormal",
    "Normal",
    "Exponential",
    "Fixed",
]
