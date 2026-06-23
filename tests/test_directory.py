"""Tests for snailmail's directory-serving mode, stats, describe, set_latency, and CLI."""

import os
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError

import pytest

from snailmail import Exponential, Fixed, HTTPRangeServer, LogNormal, Normal
from snailmail.cli import _parser


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get(url, start=None, length=None, headers=None):
    headers = dict(headers or {})
    if start is not None:
        headers["Range"] = f"bytes={start}-{start + length - 1}"
    with urlopen(Request(url, headers=headers)) as r:
        return r.status, r.read()


def _head(url):
    with urlopen(Request(url, method="HEAD")) as r:
        return r.status


def _get_status(url, start=None, length=None, headers=None):
    """Return (status, body) for a request that may return a 4xx."""
    try:
        return _get(url, start, length, headers)
    except HTTPError as exc:
        return exc.code, b""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def datadir(tmp_path):
    """A small directory tree that mimics an Icechunk-style chunk store."""
    chunks = tmp_path / "chunks"
    chunks.mkdir()
    meta = tmp_path / "meta"
    meta.mkdir()

    files = {
        "chunks/0.0.0": os.urandom(4096),
        "chunks/0.0.1": os.urandom(2048),
        "meta/zarr.json": os.urandom(512),
    }
    for rel, data in files.items():
        (tmp_path / rel).write_bytes(data)

    return tmp_path, files


# ---------------------------------------------------------------------------
# 1. Directory serving: range request -> 206; full GET -> 200
# ---------------------------------------------------------------------------


def test_dir_range_request(datadir):
    root, files = datadir
    raw = files["chunks/0.0.0"]
    with HTTPRangeServer(root) as s:
        url = s.base + "chunks/0.0.0"
        status, body = _get(url, start=100, length=200)
        assert status == 206
        assert body == raw[100:300]


def test_dir_full_get(datadir):
    root, files = datadir
    raw = files["meta/zarr.json"]
    with HTTPRangeServer(root) as s:
        url = s.base + "meta/zarr.json"
        status, body = _get(url)
        assert status == 200
        assert body == raw


# ---------------------------------------------------------------------------
# 2. files() lists keys; url(key) builds a key URL
# ---------------------------------------------------------------------------


def test_files_returns_sorted_keys(datadir):
    root, files = datadir
    with HTTPRangeServer(root) as s:
        assert s.files() == sorted(files.keys())


def test_url_builds_key_under_base(datadir):
    root, _ = datadir
    with HTTPRangeServer(root) as s:
        assert s.url("chunks/0.0.0") == s.base + "chunks/0.0.0"
        assert s.url("/chunks/0.0.0") == s.base + "chunks/0.0.0"  # leading slash stripped


# ---------------------------------------------------------------------------
# 2a. from_file(): single-file mode shares the directory-mode surface
# ---------------------------------------------------------------------------


@pytest.fixture
def onefile(tmp_path):
    """A lone file living outside any served directory, with a sibling that must
    never be served (proving from_file pins exactly one path)."""
    data = os.urandom(4096)
    f = tmp_path / "slide.tiff"
    f.write_bytes(data)
    (tmp_path / "SECRET.txt").write_bytes(b"do not serve me")
    return f, data


def test_from_file_range_and_full_get(onefile):
    f, data = onefile
    with HTTPRangeServer.from_file(f) as s:
        assert _get(s.url("slide.tiff"), start=100, length=200) == (206, data[100:300])
        assert _get(s.url("slide.tiff")) == (200, data)
        assert _head(s.url("slide.tiff")) == 200


def test_from_file_surface_matches_dir_mode(onefile):
    f, _ = onefile
    with HTTPRangeServer.from_file(f) as s:
        assert s.files() == ["slide.tiff"]
        d = s.describe()
        assert d["n_files"] == 1
        # Identical dict shape to dir mode — no resurrected url/file/size_bytes keys.
        assert set(d) == {"root", "base", "n_files", "port", "latency", "bandwidth_mbs"}
        assert "url" not in d and "file" not in d and "size_bytes" not in d


def test_from_file_only_serves_the_one_key(onefile):
    f, _ = onefile
    with HTTPRangeServer.from_file(f) as s:
        s.reset_counts()
        # The sibling and any other key 404 and count as misses; no traversal surface.
        assert _get_status(s.base + "SECRET.txt") == (404, b"")
        assert _get_status(s.base + "../SECRET.txt")[0] in (400, 403, 404)
        assert b"do not serve" not in _get_status(s.base + "../SECRET.txt")[1]
        assert s.stats()["n_misses"] >= 1


def test_from_file_hit_not_counted_as_miss(onefile):
    f, _ = onefile
    with HTTPRangeServer.from_file(f) as s:
        s.reset_counts()
        assert _get(s.url("slide.tiff"))[0] == 200
        assert s.stats()["n_misses"] == 0


def test_from_file_missing_path_errors():
    with pytest.raises(FileNotFoundError):
        HTTPRangeServer.from_file("/no/such/file.bin")


def test_constructor_still_rejects_a_file(onefile):
    f, _ = onefile
    with pytest.raises(NotADirectoryError):
        HTTPRangeServer(f)


# ---------------------------------------------------------------------------
# 2b. files()/n_files mirror what aiohttp actually serves (symlink consistency)
# ---------------------------------------------------------------------------


def test_symlink_target_inside_root_is_listed_and_served(tmp_path):
    real = tmp_path / "real.bin"
    real.write_bytes(b"a" * 256)
    os.symlink(real, tmp_path / "link.bin")
    with HTTPRangeServer(tmp_path) as s:
        assert s.files() == ["link.bin", "real.bin"]
        status, body = _get(s.url("link.bin"))
        assert status == 200 and body == b"a" * 256


def test_symlink_target_outside_root_not_listed_and_404s(tmp_path):
    # The natural "serve a big fixture without copying" move — a symlink whose
    # target lives outside the served root. aiohttp 404s it (resolved path escapes
    # root), so files()/n_files and _target_size must agree it is absent.
    outside = tmp_path / "outside"
    outside.mkdir()
    real = outside / "big.bin"
    real.write_bytes(b"x" * 1000)
    served = tmp_path / "served"
    served.mkdir()
    os.symlink(real, served / "big.bin")
    with HTTPRangeServer(served) as s:
        assert s.files() == []
        assert s.describe()["n_files"] == 0
        assert s._target_size("/big.bin") is None
        status, _ = _get_status(s.url("big.bin"))
        assert status == 404


# ---------------------------------------------------------------------------
# 3. Miss counting
# ---------------------------------------------------------------------------


def test_dir_missing_key_increments_misses(datadir):
    root, _ = datadir
    with HTTPRangeServer(root) as s:
        s.reset_counts()
        status, _ = _get_status(s.base + "does_not_exist.bin")
        assert status == 404
        assert s.stats()["n_misses"] == 1


def test_hit_does_not_increment_misses(datadir):
    root, _ = datadir
    with HTTPRangeServer(root) as s:
        s.reset_counts()
        status, _ = _get(s.url("chunks/0.0.0"))
        assert status == 200
        assert s.stats()["n_misses"] == 0


# ---------------------------------------------------------------------------
# 4. Path traversal protection
# ---------------------------------------------------------------------------


def test_path_traversal_blocked(datadir):
    root, _ = datadir
    with HTTPRangeServer(root) as s:
        # These paths all try to escape the served root — server must return 4xx
        traversal_urls = [
            s.base + "../../etc/passwd",
            s.base + "../other_dir/secret.txt",
            s.base + "chunks/../../etc/hostname",
        ]
        for url in traversal_urls:
            status, body = _get_status(url)
            # Must return 404 (or possibly 400/403), but never serve an outside file
            assert status in (404, 400, 403), f"Expected 4xx for {url!r}, got {status}"
            # Confirm no real system file content leaked
            assert b"root:" not in body, f"Traversal succeeded for {url!r}"


# ---------------------------------------------------------------------------
# 5. total_bytes: sum of range lengths; HEAD does NOT inflate it
# ---------------------------------------------------------------------------


def test_total_bytes_sums_ranges(datadir):
    root, _ = datadir
    with HTTPRangeServer(root) as s:
        s.reset_counts()
        _get(s.base + "chunks/0.0.0", start=0, length=100)
        _get(s.base + "chunks/0.0.1", start=50, length=200)
        assert s.stats()["total_bytes"] == 300


def test_head_does_not_inflate_total_bytes(datadir):
    root, _ = datadir
    with HTTPRangeServer(root) as s:
        s.reset_counts()
        _head(s.base + "chunks/0.0.0")
        assert s.stats()["total_bytes"] == 0


# ---------------------------------------------------------------------------
# 6. stats() methods / paths reflect what was requested
# ---------------------------------------------------------------------------


def test_stats_methods_and_paths(datadir):
    root, _ = datadir
    with HTTPRangeServer(root) as s:
        s.reset_counts()
        _get(s.base + "chunks/0.0.0")
        _get(s.base + "chunks/0.0.0")
        _head(s.base + "meta/zarr.json")
        st = s.stats()
        assert st["methods"].get("GET", 0) == 2
        assert st["methods"].get("HEAD", 0) == 1
        assert st["paths"]["/chunks/0.0.0"] == 2
        assert st["paths"]["/meta/zarr.json"] == 1


# ---------------------------------------------------------------------------
# 7. describe() key sets differ between file and dir mode
# ---------------------------------------------------------------------------


def test_describe_dir_keys(datadir):
    root, files = datadir
    with HTTPRangeServer(root) as s:
        d = s.describe()
        assert "root" in d
        assert "base" in d
        assert "port" in d
        assert "n_files" in d
        assert "latency" in d
        assert "bandwidth_mbs" in d
        # These must NOT appear in dir mode
        assert "url" not in d
        assert "file" not in d
        assert "size_bytes" not in d
        assert d["n_files"] == len(files)


# ---------------------------------------------------------------------------
# 8. set_latency live swap
# ---------------------------------------------------------------------------


def test_set_latency_live_swap(datadir):
    root, _ = datadir
    with HTTPRangeServer(root, latency=Fixed(0)) as s:
        s.set_latency(Fixed(40))
        t = time.perf_counter()
        _get(s.url("chunks/0.0.0"), 0, 100)
        elapsed = time.perf_counter() - t
        assert elapsed >= 0.035, (
            f"Expected >= 35ms after set_latency(Fixed(40)), got {elapsed * 1000:.1f}ms"
        )


# ---------------------------------------------------------------------------
# 9. CLI: --version exits 0
# ---------------------------------------------------------------------------


def test_version_exits_zero():
    with pytest.raises(SystemExit) as exc_info:
        _parser().parse_args(["--version"])
    assert exc_info.value.code == 0


# ---------------------------------------------------------------------------
# 10. Latency describe() degenerate / pool_size consistency
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "dist,is_degenerate",
    [
        (LogNormal(0), True),
        (LogNormal(30, seed=0), False),
        (Normal(15, std_ms=0), True),
        (Normal(15, std_ms=5, seed=0), False),
        (Exponential(0), True),
        (Exponential(30, seed=0), False),
    ],
)
def test_degenerate_flag_and_pool_size(dist, is_degenerate):
    d = dist.describe()
    assert d["degenerate"] is is_degenerate
    if is_degenerate:
        assert "pool_size" not in d
    else:
        assert "pool_size" in d
        assert d["pool_size"] > 0


# ---------------------------------------------------------------------------
# 11. Robustness: a malformed Range must not crash the request (RFC 7233: ignore it)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_range", ["bytes=bad", "bytes=abc-def", "bytes=-"])
def test_malformed_range_does_not_500(datadir, bad_range):
    root, _ = datadir
    with HTTPRangeServer(root) as s:
        status, _ = _get_status(s.url("chunks/0.0.0"), headers={"Range": bad_range})
        assert status != 500


# ---------------------------------------------------------------------------
# 12. CLI: a non-directory root is a clean error, not a traceback
# ---------------------------------------------------------------------------


def test_cli_non_directory_root_errors_cleanly(tmp_path):
    f = tmp_path / "afile.bin"
    f.write_bytes(b"x")
    from snailmail.cli import main
    import sys

    argv = sys.argv
    sys.argv = ["snailmail", str(f)]
    try:
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 2  # argparse usage error, not an uncaught traceback
    finally:
        sys.argv = argv
