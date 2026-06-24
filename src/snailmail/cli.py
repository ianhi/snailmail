"""The ``snailmail`` command-line entry point."""

from __future__ import annotations

import argparse
import json
import threading
from importlib.metadata import version

from snailmail.latency import Exponential, Fixed, LatencyDist, LogNormal, Normal
from snailmail.server import HTTPRangeServer

_request_log_enabled = False  # guards _enable_request_log() against stacking handlers

# CLI param -> which --dist owns it. A param set for the wrong dist is a user error,
# not silently ignored, so the realized latency always matches what was asked for.
_DIST_PARAMS = {
    "mode_ms": "lognormal",
    "sigma": "lognormal",
    "mean_ms": ("normal", "exponential"),
    "std_ms": "normal",
    "value_ms": "fixed",
}


def _latency_from_args(args: argparse.Namespace, ap: argparse.ArgumentParser) -> LatencyDist | None:
    if args.dist is None:
        for name in _DIST_PARAMS:
            if getattr(args, name) is not None:
                ap.error(f"--{name.replace('_', '-')} requires --dist")
        return None  # no injected latency
    for name, owner in _DIST_PARAMS.items():
        owners = (owner,) if isinstance(owner, str) else owner
        if getattr(args, name) is not None and args.dist not in owners:
            ap.error(f"--{name.replace('_', '-')} is not valid with --dist {args.dist}")
    if args.dist == "lognormal":
        if args.mode_ms is None:
            ap.error("--dist lognormal requires --mode-ms")
        opt = {} if args.sigma is None else {"sigma": args.sigma}  # else LogNormal's default
        return LogNormal(args.mode_ms, seed=args.seed, **opt)
    if args.dist == "normal":
        if args.mean_ms is None:
            ap.error("--dist normal requires --mean-ms")
        opt = {} if args.std_ms is None else {"std_ms": args.std_ms}  # else Normal's default
        return Normal(args.mean_ms, seed=args.seed, **opt)
    if args.dist == "exponential":
        if args.mean_ms is None:
            ap.error("--dist exponential requires --mean-ms")
        return Exponential(args.mean_ms, seed=args.seed)
    if args.value_ms is None:  # fixed
        ap.error("--dist fixed requires --value-ms")
    return Fixed(args.value_ms)


def _parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="snailmail",
        description="Serve a directory over HTTP with injected latency "
        "+ bandwidth limits, for benchmarking. Binds 127.0.0.1 and serves until interrupted.",
        epilog="examples:\n"
        "  snailmail ./store --dist lognormal --mode-ms 45 --sigma 0.5\n"
        "  snailmail ./store --dist normal --mean-ms 45 --std-ms 10\n"
        "  snailmail ./store --dist exponential --mean-ms 45\n"
        "  snailmail ./store --dist fixed --value-ms 20\n"
        "  snailmail ./store --bandwidth-mbs 100 --port 8080 --json   # no latency; JSON address line",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--version", action="version", version=f"%(prog)s {version('snailmail')}")
    ap.add_argument("root", help="directory to serve")
    ap.add_argument(
        "--dist",
        choices=["lognormal", "normal", "exponential", "fixed"],
        help="latency distribution; omit for no injected latency",
    )
    ap.add_argument("--mode-ms", type=float, help="[lognormal] PDF mode (peak), ms")
    ap.add_argument("--sigma", type=float, help="[lognormal] log-scale shape (default 0.5)")
    ap.add_argument("--mean-ms", type=float, help="[normal, exponential] mean latency, ms")
    ap.add_argument("--std-ms", type=float, help="[normal] standard deviation, ms")
    ap.add_argument("--value-ms", type=float, help="[fixed] deterministic latency, ms")
    ap.add_argument(
        "--seed", type=int, default=None, help="RNG seed for the latency pool (reproducible draws)"
    )
    ap.add_argument(
        "--bandwidth-mbs", type=float, default=None, help="shared-pipe MB/s; omit = unlimited"
    )
    ap.add_argument("--port", type=int, default=0, help="TCP port (0 = ephemeral)")
    ap.add_argument(
        "--json",
        action="store_true",
        help="emit the server description as one line of JSON instead of the banner",
    )
    ap.add_argument(
        "--log",
        action="store_true",
        help="log one line per request to stderr: METHOD key [label] -> status, "
        "bytes, injected RTT, total time, and in-flight count",
    )
    return ap


def _enable_request_log() -> None:
    """Send snailmail's per-request log lines to stderr (the ``--log`` flag).

    Configures only the ``snailmail`` logger (not the root logger), so this never turns
    on unrelated library logging — it just opts into the lines the server already emits.
    """
    import logging
    import sys

    global _request_log_enabled
    if _request_log_enabled:  # idempotent: don't stack handlers if main() runs twice
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(name)s %(message)s"))
    logger = logging.getLogger("snailmail")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    _request_log_enabled = True


def main() -> None:
    ap = _parser()
    args = ap.parse_args()

    if args.log:
        _enable_request_log()
    latency = _latency_from_args(args, ap)
    try:
        server = HTTPRangeServer(
            args.root,
            latency=latency,
            bandwidth_mbs=args.bandwidth_mbs,
            port=args.port,
        ).start()
    except NotADirectoryError as exc:
        ap.error(str(exc))
    # Flush so a consumer reading a pipe gets the bound address immediately, not after
    # block-buffering — it can't predict an ephemeral port otherwise.
    if args.json:
        print(json.dumps(server.describe()), flush=True)
    else:
        print(f"serving {server.root}/ ({len(server.files())} files)")
        print(f"base    : {server.base}")
        print(f"server  : {server.describe()}")
        print(f"realized: {server.realized_percentiles()}")
        print("Ctrl-C to stop.", flush=True)
    try:
        threading.Event().wait()  # block until Ctrl-C
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()
        rep = server.report()
        print(
            f"\nstopped. {rep['n_requests']} requests, {rep['n_gets']} GETs, "
            f"{rep['n_misses']} misses, {rep['total_bytes']} bytes, "
            f"peak concurrency {rep['max_in_flight']}, status {rep['by_status']}."
        )


if __name__ == "__main__":
    main()
