"""snailmail — local HTTP-range and S3 servers with injectable latency and bandwidth limits.

For benchmarking range / object-store / virtual-chunk reads under realistic network
conditions. See :class:`HTTPRangeServer` (range/file serving) and :class:`ObjectStore`
(S3 object storage).
"""

import logging
from importlib.metadata import PackageNotFoundError, version

from snailmail.bandwidth import AsyncSharedPipe, ClientLink, SharedPipe
from snailmail.latency import Exponential, Fixed, LatencyDist, LogNormal, Normal
from snailmail.record import RequestRecord
from snailmail.s3 import LatencyMiddleware, ObjectStore, StoreBehavior
from snailmail.server import HTTPRangeServer

# Library logging hygiene: attach a no-op handler so per-request lines stay silent until
# the user opts in (e.g. logging.getLogger("snailmail").setLevel(logging.INFO) + a handler).
logging.getLogger("snailmail").addHandler(logging.NullHandler())

try:
    __version__ = version("snailmail")  # derived from the git tag at build time (hatch-vcs)
except PackageNotFoundError:  # running from a source tree with no install
    __version__ = "0+unknown"

__all__ = [
    "HTTPRangeServer",
    "ObjectStore",
    "StoreBehavior",
    "RequestRecord",
    "LatencyMiddleware",
    "ClientLink",
    "AsyncSharedPipe",
    "SharedPipe",
    "LatencyDist",
    "LogNormal",
    "Normal",
    "Exponential",
    "Fixed",
]
