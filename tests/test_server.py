"""Tests for snailmail: range correctness, latency, bandwidth, concurrency, counters."""

import os
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.request import Request, urlopen

import pytest

from snailmail import Fixed, HTTPRangeServer, LogNormal


@pytest.fixture
def datadir(tmp_path):
    (tmp_path / "data.bin").write_bytes(os.urandom(2_000_000))  # 2 MB
    return tmp_path


def _get(url, start=None, length=None):
    headers = {}
    if start is not None:
        headers["Range"] = f"bytes={start}-{start + length - 1}"
    with urlopen(Request(url, headers=headers)) as r:
        return r.status, r.read()


def test_range_correctness(datadir):
    raw = (datadir / "data.bin").read_bytes()
    with HTTPRangeServer(datadir) as s:
        status, body = _get(s.url("data.bin"), start=1000, length=500)
        assert status == 206
        assert body == raw[1000:1500]
        status, full = _get(s.url("data.bin"))
        assert status == 200 and full == raw


def test_latency_applied(datadir):
    with HTTPRangeServer(datadir, latency=Fixed(40)) as s:
        t = time.perf_counter()
        _get(s.url("data.bin"), 0, 100)
        assert time.perf_counter() - t >= 0.035  # ~40ms fixed, minus slack


def test_concurrency_overlaps(datadir):
    # 16 concurrent requests at 50ms each: if serial, ~800ms; concurrent, ~one RTT.
    with HTTPRangeServer(datadir, latency=Fixed(50)) as s:
        s.reset_counts()
        t = time.perf_counter()
        with ThreadPoolExecutor(max_workers=16) as ex:
            list(ex.map(lambda i: _get(s.url("data.bin"), i * 100, 100), range(16)))
        wall = time.perf_counter() - t
        assert s.max_in_flight > 1  # genuinely concurrent
        assert wall < 16 * 0.05 * 0.5  # nowhere near serial
        assert s.n_gets == 16


def test_counters_and_bytes(datadir):
    with HTTPRangeServer(datadir) as s:
        _get(s.url("data.bin"), 0, 1000)
        _get(s.url("data.bin"), 5000, 2000)
        st = s.stats()
        assert st["n_gets"] == 2
        assert st["total_bytes"] == 3000
        assert st["methods"] == {"GET": 2}
        assert st["paths"] == {"/data.bin": 2}


def test_bandwidth_limit(datadir):
    # 1 MB through a 10 MB/s pipe should take ~0.1s of transfer time.
    with HTTPRangeServer(datadir, bandwidth_mbs=10) as s:
        t = time.perf_counter()
        _get(s.url("data.bin"), 0, 1_000_000)
        assert time.perf_counter() - t >= 0.08


def test_lognormal_pool_is_reproducible():
    a = LogNormal(30, seed=0)
    b = LogNormal(30, seed=0)
    assert [a.draw_s() for _ in range(5)] == [b.draw_s() for _ in range(5)]
    pct = a.percentiles()
    assert pct["p10_ms"] < pct["p50_ms"] < pct["p90_ms"] < pct["p99_ms"]


def test_fixed_and_zero_latency():
    assert LogNormal(0).draw_s() == 0.0  # degenerate mode <= 0
    assert Fixed(25).draw_s() == pytest.approx(0.025)


def test_requires_a_directory(tmp_path):
    f = tmp_path / "afile.bin"
    f.write_bytes(b"x")
    with pytest.raises(NotADirectoryError):
        HTTPRangeServer(f)


def test_startup_failure_propagates(datadir):
    # A bind failure must raise from start(), not hang forever waiting on _ready.
    with HTTPRangeServer(datadir) as running:
        with pytest.raises(OSError):
            HTTPRangeServer(datadir, port=running.port).start()


def test_serves_from_disk_not_ram(datadir):
    # The server streams files from disk; it must not slurp them into memory.
    s = HTTPRangeServer(datadir)
    assert s.files() == ["data.bin"]
    assert not hasattr(s, "data")


# -- per-request records / report (HTTPRangeServer) --


def test_report_and_records(datadir):
    raw = (datadir / "data.bin").read_bytes()
    with HTTPRangeServer(datadir, classify=lambda k: k.split(".")[-1]) as s:
        _get(s.url("data.bin"))  # full GET -> 200
        _get(s.url("data.bin"), 0, 500)  # partial GET -> 206
        try:
            _get(s.url("missing.bin"))  # -> 404
        except Exception:
            pass

        rep = s.report()
        assert rep["n_requests"] == 3
        assert rep["n_misses"] == 1
        assert rep["by_status"] == {200: 1, 206: 1, 404: 1}
        assert rep["by_label"]["bin"]["requests"] == 3  # all keys end in .bin
        assert rep["records_truncated"] is False

        recs = s.requests
        assert [r.status for r in recs] == [200, 206, 404]
        full, partial, miss = recs
        assert full.range is None and full.nbytes == len(raw)
        assert partial.range == (0, 500) and partial.nbytes == 500
        assert miss.status == 404 and miss.nbytes == 0
        assert all(r.label == "bin" for r in recs)


def test_unsatisfiable_range_recorded_as_416(datadir):
    # A well-formed range past EOF (and any range on an empty file) is unsatisfiable:
    # aiohttp answers 416, and the record's status must match the wire, not 206.
    (datadir / "empty.bin").write_bytes(b"")
    with HTTPRangeServer(datadir) as s:
        for key, rng in [("data.bin", "bytes=9000000-9999999"), ("empty.bin", "bytes=0-10")]:
            req = Request(s.url(key), headers={"Range": rng})
            try:
                wire = urlopen(req).status
            except Exception as exc:  # urllib raises HTTPError on 416
                wire = getattr(exc, "code", None)
            assert wire == 416
        assert [r.status for r in s.requests] == [416, 416]
        assert all(r.nbytes == 0 and r.range is None for r in s.requests)
        assert s.report()["by_status"] == {416: 2}


def test_reset_clears_records(datadir):
    with HTTPRangeServer(datadir) as s:
        _get(s.url("data.bin"))
        assert len(s.requests) == 1
        s.reset_counts()
        assert s.requests == []
        assert s.report()["n_requests"] == 0


def test_max_records_bounds_buffer_but_not_counts(datadir):
    with HTTPRangeServer(datadir, max_records=2) as s:
        for _ in range(5):
            _get(s.url("data.bin"), 0, 10)
        rep = s.report()
        assert rep["n_requests"] == 5  # counters exact
        assert rep["records_kept"] == 2  # buffer capped
        assert rep["records_truncated"] is True
        assert len(s.requests) == 2


def test_request_log_emits(datadir, caplog):
    import logging

    with caplog.at_level(logging.INFO, logger="snailmail.http"):
        with HTTPRangeServer(datadir) as s:
            _get(s.url("data.bin"), 0, 100)
    lines = [r.getMessage() for r in caplog.records if r.name == "snailmail.http"]
    assert any("GET data.bin" in m and "-> 206" in m for m in lines)
