"""Optional, domain-specific conveniences — deliberately kept off the core objects.

snailmail's servers (:class:`~snailmail.s3.ObjectStore`, :class:`~snailmail.server.HTTPRangeServer`)
are general and domain-agnostic. Helpers that wire a specific consumer (Icechunk, ...) to a
store live here instead of as methods on the store, and take the store as an argument. They
are not exported at the top level — reach them via ``snailmail.convenience``. Importing this
module does not import the consumer libraries; that happens lazily inside each function.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snailmail.s3 import ObjectStore


def icechunk_storage(store: ObjectStore, prefix: str | None = None):
    """A ready-wired ``icechunk.Storage`` pointing at ``store`` (path-style, plain HTTP, dummy creds).

    ``prefix`` overrides the store's default key prefix (the repo root); ``None`` uses
    ``store.prefix``. Reads only the generic S3 attributes of the store
    (``bucket`` / ``prefix`` / ``endpoint_url`` / ``region``).
    """
    import icechunk

    return icechunk.s3_storage(
        bucket=store.bucket,
        prefix=store.prefix if prefix is None else prefix,
        endpoint_url=store.endpoint_url,
        allow_http=True,
        force_path_style=True,
        region=store.region,
        access_key_id="snailmail",
        secret_access_key="snailmail",
    )
