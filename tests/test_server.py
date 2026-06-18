"""Tests for snailmail: range correctness, latency, bandwidth, concurrency, counters."""

import os
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.request import Request, urlopen

import pytest

from snailmail import LatencyModel, LatencyRangeServer


@pytest.fixture
def datafile(tmp_path):
    p = tmp_path / "data.bin"
    p.write_bytes(os.urandom(2_000_000))  # 2 MB
    return p


def _get(url, start=None, length=None):
    headers = {}
    if start is not None:
        headers["Range"] = f"bytes={start}-{start + length - 1}"
    with urlopen(Request(url, headers=headers)) as r:
        return r.status, r.read()


def test_range_correctness(datafile):
    raw = datafile.read_bytes()
    with LatencyRangeServer(datafile) as s:
        status, body = _get(s.url, start=1000, length=500)
        assert status == 206
        assert body == raw[1000:1500]
        status, full = _get(s.url)
        assert status == 200 and full == raw


def test_latency_applied(datafile):
    with LatencyRangeServer(datafile, latency_ms=40, random_latency=False) as s:
        t = time.perf_counter()
        _get(s.url, 0, 100)
        assert time.perf_counter() - t >= 0.035  # ~40ms fixed, minus slack


def test_concurrency_overlaps(datafile):
    # 16 concurrent requests at 50ms each: if serial, ~800ms; concurrent, ~one RTT.
    with LatencyRangeServer(datafile, latency_ms=50, random_latency=False) as s:
        s.reset_counts()
        t = time.perf_counter()
        with ThreadPoolExecutor(max_workers=16) as ex:
            list(ex.map(lambda i: _get(s.url, i * 100, 100), range(16)))
        wall = time.perf_counter() - t
        assert s.max_in_flight > 1          # genuinely concurrent
        assert wall < 16 * 0.05 * 0.5       # nowhere near serial
        assert s.n_gets == 16


def test_counters_and_bytes(datafile):
    with LatencyRangeServer(datafile) as s:
        _get(s.url, 0, 1000)
        _get(s.url, 5000, 2000)
        st = s.stats()
        assert st["n_gets"] == 2
        assert st["total_bytes"] == 3000


def test_bandwidth_limit(datafile):
    # 1 MB through a 10 MB/s pipe should take ~0.1s of transfer time.
    with LatencyRangeServer(datafile, bandwidth_mbs=10) as s:
        t = time.perf_counter()
        _get(s.url, 0, 1_000_000)
        assert time.perf_counter() - t >= 0.08


def test_latency_model_pool_is_reproducible():
    a = LatencyModel(30, seed=0)
    b = LatencyModel(30, seed=0)
    assert [a.draw_s() for _ in range(5)] == [b.draw_s() for _ in range(5)]
    pct = a.realized_percentiles()
    assert pct["p10_ms"] < pct["p50_ms"] < pct["p90_ms"] < pct["p99_ms"]


def test_fixed_and_zero_latency():
    assert LatencyModel(0).draw_s() == 0.0
    assert LatencyModel(25, random=False).draw_s() == pytest.approx(0.025)


def test_serves_from_disk_not_ram(datafile):
    # The server must not slurp the file into memory: it only stats the size.
    s = LatencyRangeServer(datafile)
    assert s.size == datafile.stat().st_size
    assert not hasattr(s, "data")
