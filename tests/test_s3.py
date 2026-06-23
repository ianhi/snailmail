"""Tests for the object store.

Three layers, each independently runnable:

  * ``LatencyMiddleware`` unit tests drive the WSGI middleware against a trivial in-process
    app — no moto, no sockets — covering op classification, counters, byte metering,
    latency, and the conditional-write behaviors.
  * ``ObjectStore`` integration tests (skipped without ``moto``) exercise the real
    in-process S3 server through a signed botocore client.
  * An end-to-end test (skipped without ``icechunk``) creates, reopens, and reads an
    Icechunk repo against the store and checks the metadata round-trips are counted.
"""

import time

import pytest

from snailmail import Fixed, LatencyMiddleware, StoreBehavior


# ---------------------------------------------------------------------------
# LatencyMiddleware unit tests (no moto): drive WSGI directly
# ---------------------------------------------------------------------------


def _drive(mw, method, path, qs="", headers=None, body=b""):
    """Push one request through the middleware; return (status, body, captured-env-app-saw)."""
    environ = {"REQUEST_METHOD": method, "PATH_INFO": path, "QUERY_STRING": qs}
    if body:
        environ["CONTENT_LENGTH"] = str(len(body))
    for key, value in (headers or {}).items():
        environ["HTTP_" + key.upper().replace("-", "_")] = value
    captured = {}

    def start_response(status, resp_headers, exc_info=None):
        captured["status"] = int(status.split(" ", 1)[0])

    body_iter = mw(environ, start_response)
    data = b"".join(body_iter)
    closer = getattr(body_iter, "close", None)
    if closer is not None:
        closer()
    return captured.get("status"), data


def _echo_app(response=b"hello world", status="200 OK"):
    def app(environ, start_response):
        start_response(status, [("Content-Type", "application/octet-stream")])
        return [response]

    return app


def test_op_and_prefix_classification():
    mw = LatencyMiddleware(_echo_app(), latency=Fixed(0))
    _drive(mw, "GET", "/bkt/chunks/0.0.0")  # object data GET
    _drive(mw, "GET", "/bkt/refs/branch.main/ref.json")  # metadata GET
    _drive(mw, "GET", "/bkt", qs="list-type=2")  # ListObjectsV2
    _drive(mw, "HEAD", "/bkt/snapshots/abc")
    _drive(mw, "PUT", "/bkt/manifests/m1", body=b"x" * 10)
    _drive(mw, "DELETE", "/bkt/transactions/t1")
    _drive(mw, "POST", "/bkt", qs="delete")  # batch delete

    st = mw.stats()
    assert st["ops"] == {"GET": 2, "LIST": 1, "HEAD": 1, "PUT": 1, "DELETE": 2}
    assert st["prefixes"]["chunks"] == 1
    assert st["data_requests"] == 1
    # refs + snapshots + manifests + transactions = 4 metadata requests
    assert st["metadata_requests"] == 4
    assert st["n_requests"] == 7


def test_byte_metering_both_directions():
    # A real S3 PUT returns an empty body; only the GET streams bytes back.
    def app(environ, start_response):
        start_response("200 OK", [])
        return [b"R" * 321] if environ["REQUEST_METHOD"] == "GET" else [b""]

    mw = LatencyMiddleware(app, latency=Fixed(0))
    _drive(mw, "PUT", "/bkt/refs/x", body=b"U" * 123)
    _drive(mw, "GET", "/bkt/refs/x")
    st = mw.stats()
    assert st["bytes_up"] == 123
    assert st["bytes_down"] == 321
    assert st["total_bytes"] == 444
    assert st["prefix_bytes"]["refs"] == 444


def test_latency_is_applied_per_request():
    mw = LatencyMiddleware(_echo_app(), latency=Fixed(40))
    t0 = time.perf_counter()
    _drive(mw, "GET", "/bkt/refs/x")
    assert time.perf_counter() - t0 >= 0.035


def test_misses_counted_for_reads():
    mw = LatencyMiddleware(_echo_app(response=b"", status="404 NOT FOUND"), latency=Fixed(0))
    _drive(mw, "GET", "/bkt/refs/missing")
    _drive(mw, "HEAD", "/bkt/refs/missing")
    assert mw.stats()["n_misses"] == 2


def test_conditional_writes_enforce_passes_precondition_through():
    seen = {}

    def app(environ, start_response):
        seen["inm"] = environ.get("HTTP_IF_NONE_MATCH")
        start_response("200 OK", [])
        return [b""]

    mw = LatencyMiddleware(app, latency=Fixed(0))  # default: conditional_writes="enforce"
    _drive(mw, "PUT", "/bkt/refs/x", headers={"If-None-Match": "*"}, body=b"z")
    assert seen["inm"] == "*"
    assert mw.stats()["conditional_stripped"] == 0


def test_conditional_writes_ignore_strips_precondition():
    seen = {}

    def app(environ, start_response):
        seen["inm"] = environ.get("HTTP_IF_NONE_MATCH")
        seen["ifm"] = environ.get("HTTP_IF_MATCH")
        start_response("200 OK", [])
        return [b""]

    mw = LatencyMiddleware(
        app, latency=Fixed(0), behavior=StoreBehavior(conditional_writes="ignore")
    )
    _drive(mw, "PUT", "/bkt/refs/x", headers={"If-None-Match": "*", "If-Match": "abc"}, body=b"z")
    assert seen["inm"] is None and seen["ifm"] is None
    assert mw.stats()["conditional_stripped"] == 1


def test_conditional_writes_reject_returns_not_implemented():
    reached = {"app": False}

    def app(environ, start_response):
        reached["app"] = True
        start_response("200 OK", [])
        return [b""]

    mw = LatencyMiddleware(
        app, latency=Fixed(0), behavior=StoreBehavior(conditional_writes="reject")
    )
    status, body = _drive(mw, "PUT", "/bkt/refs/x", headers={"If-None-Match": "*"}, body=b"z")
    assert status == 501
    assert b"NotImplemented" in body
    assert reached["app"] is False  # rejected before the backend
    assert mw.stats()["conditional_rejected"] == 1


def test_conditional_writes_reject_allows_unconditional_writes():
    # Only *conditional* writes are rejected; plain PUTs must still go through.
    mw = LatencyMiddleware(
        _echo_app(response=b""),
        latency=Fixed(0),
        behavior=StoreBehavior(conditional_writes="reject"),
    )
    status, _ = _drive(mw, "PUT", "/bkt/refs/x", body=b"z")
    assert status == 200
    assert mw.stats()["conditional_rejected"] == 0


def test_store_behavior_validates_conditional_writes():
    with pytest.raises(ValueError):
        StoreBehavior(conditional_writes="bogus")  # type: ignore[arg-type]


def test_reset_counts_zeroes_counters():
    mw = LatencyMiddleware(_echo_app(), latency=Fixed(0))
    _drive(mw, "GET", "/bkt/refs/x")
    mw.reset_counts()
    assert mw.stats()["n_requests"] == 0
    assert mw.stats()["total_bytes"] == 0


def test_set_latency_live_swap():
    mw = LatencyMiddleware(_echo_app(), latency=Fixed(0))
    mw.set_latency(Fixed(40))
    t0 = time.perf_counter()
    _drive(mw, "GET", "/bkt/refs/x")
    assert time.perf_counter() - t0 >= 0.035


# ---------------------------------------------------------------------------
# ObjectStore integration tests (require moto)
# ---------------------------------------------------------------------------

pytest.importorskip("moto", reason="needs the [s3] extra (moto)")


@pytest.fixture
def store():
    from snailmail import ObjectStore

    with ObjectStore(latency=Fixed(0)) as s:
        yield s


def _client(store):
    import botocore.session

    return botocore.session.get_session().create_client(
        "s3",
        endpoint_url=store.endpoint_url,
        region_name="us-east-1",
        aws_access_key_id="snailmail",
        aws_secret_access_key="snailmail",
    )


def test_store_put_get_roundtrip_and_counts(store):
    c = _client(store)
    store.reset_counts()
    body = b"x" * 2048
    c.put_object(Bucket="snailmail", Key="refs/branch.main", Body=body)
    got = c.get_object(Bucket="snailmail", Key="refs/branch.main")["Body"].read()
    assert got == body
    st = store.stats()
    assert st["ops"]["PUT"] == 1 and st["ops"]["GET"] == 1
    assert st["bytes_up"] == 2048 and st["bytes_down"] == 2048
    assert st["metadata_requests"] == 2  # both under refs/


def test_store_applies_latency(store):
    c = _client(store)
    store.set_latency(Fixed(60))
    store.reset_counts()
    t0 = time.perf_counter()
    c.put_object(Bucket="snailmail", Key="refs/x", Body=b"a")
    assert time.perf_counter() - t0 >= 0.05


def test_store_conditional_writes_enforce_rejects_overwrite():
    from snailmail import ObjectStore

    with ObjectStore(latency=Fixed(0)) as s:  # default: conditional_writes="enforce"
        c = _client(s)
        c.put_object(Bucket="snailmail", Key="refs/x", Body=b"first")
        with pytest.raises(Exception):  # PreconditionFailed
            c.put_object(Bucket="snailmail", Key="refs/x", Body=b"second", IfNoneMatch="*")
        assert c.get_object(Bucket="snailmail", Key="refs/x")["Body"].read() == b"first"


def test_store_conditional_writes_ignore_overwrites():
    from snailmail import ObjectStore

    with ObjectStore(latency=Fixed(0), behavior=StoreBehavior(conditional_writes="ignore")) as s:
        c = _client(s)
        c.put_object(Bucket="snailmail", Key="refs/x", Body=b"first")
        c.put_object(Bucket="snailmail", Key="refs/x", Body=b"second", IfNoneMatch="*")
        assert c.get_object(Bucket="snailmail", Key="refs/x")["Body"].read() == b"second"
        assert s.stats()["conditional_stripped"] == 1


def test_store_conditional_writes_reject_returns_not_implemented():
    import botocore.exceptions

    from snailmail import ObjectStore

    with ObjectStore(latency=Fixed(0), behavior=StoreBehavior(conditional_writes="reject")) as s:
        c = _client(s)
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            c.put_object(Bucket="snailmail", Key="refs/x", Body=b"first", IfNoneMatch="*")
        assert exc.value.response["Error"]["Code"] == "NotImplemented"
        assert s.stats()["conditional_rejected"] == 1


# ---------------------------------------------------------------------------
# End-to-end: Icechunk create -> reopen -> read (requires icechunk + zarr)
# ---------------------------------------------------------------------------


def test_icechunk_create_reopen_read_pays_and_counts_metadata():
    icechunk = pytest.importorskip("icechunk", reason="needs icechunk")
    zarr = pytest.importorskip("zarr", reason="needs zarr")
    import numpy as np

    from snailmail import ObjectStore

    with ObjectStore(latency=Fixed(0)) as s:
        storage = s.icechunk_storage(prefix="repo")

        repo = icechunk.Repository.create(storage)
        session = repo.writable_session("main")
        root = zarr.create_group(session.store)
        arr = root.create_array("temp", shape=(100,), chunks=(25,), dtype="int32")
        arr[:] = np.arange(100, dtype="int32")
        session.commit("init")

        # Reopen + read under latency; assert it pays and the metadata ops are counted.
        s.set_latency(Fixed(50))
        s.reset_counts()

        t0 = time.perf_counter()
        repo2 = icechunk.Repository.open(storage)
        session2 = repo2.readonly_session(branch="main")
        out = zarr.open_group(session2.store, mode="r")["temp"][:]
        elapsed = time.perf_counter() - t0

        assert np.array_equal(out, np.arange(100, dtype="int32"))
        st = s.stats()
        assert elapsed >= 0.045, f"reopen paid no latency: {elapsed}"
        assert st["metadata_requests"] >= 1, st
        assert st["n_requests"] >= 1


def test_icechunk_2228_conditional_create_dropped_on_spec_v1():
    """Local reproduction of icechunk#2228 (no JASMIN creds needed).

    Against a store that rejects conditional writes, ``unsafe_use_conditional_*=False``
    is honored under spec_version 2 (create succeeds) but dropped under spec_version 1
    (icechunk still sends a conditional PUT, which the store answers with NotImplemented).
    """
    icechunk = pytest.importorskip("icechunk", reason="needs icechunk")

    from snailmail import ObjectStore

    def create(store, spec_version):
        config = icechunk.RepositoryConfig(
            storage=icechunk.StorageSettings(
                unsafe_use_conditional_create=False,
                unsafe_use_conditional_update=False,
            )
        )
        storage = store.icechunk_storage(prefix=f"v{spec_version}")
        icechunk.Repository.create(storage, config, spec_version=spec_version)

    with ObjectStore(latency=Fixed(0), behavior=StoreBehavior(conditional_writes="reject")) as s:
        # spec_version 2: settings honored, no conditional write is issued -> succeeds.
        create(s, 2)

        # spec_version 1: settings dropped, a conditional create is issued -> rejected.
        s.reset_counts()
        with pytest.raises(icechunk.IcechunkError):
            create(s, 1)
        assert s.stats()["conditional_rejected"] >= 1
