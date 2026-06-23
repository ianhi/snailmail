#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["icechunk", "zarr", "numpy", "boto3", "snailmail[s3]"]
#
# [tool.uv.sources]
# snailmail = { path = "..", editable = true }
# ///

#
#   uv run repros/icechunk_2228.py --spec-version 1   # reproduces the failure, no creds
#   uv run repros/icechunk_2228.py                    # works (spec_version 2)

import argparse
import inspect
import random
import string
import sys

import boto3
import icechunk
import numpy as np
import zarr
from botocore.config import Config

from snailmail import ObjectStore, StoreBehavior

parser = argparse.ArgumentParser()
parser.add_argument(
    "--spec-version",
    type=int,
    default=None,
    help="On icechunk 2.x, force the on-disk format version (e.g. 1). Ignored on 1.x (always native v1).",
)
parser.add_argument(
    "--bucket", default=None, help="Reuse an existing bucket instead of creating one."
)
args = parser.parse_args()

# --- replaces the JASMIN endpoint + creds: a local snailmail store that, like JASMIN,
#     does not implement conditional writes (rejects them with NotImplemented) ---
snailmail_store = ObjectStore(behavior=StoreBehavior(conditional_writes="reject")).start()
endpoint = snailmail_store.endpoint_url
access_key = "snailmail"
secret_key = "snailmail"
region = "us-east-1"

suffix = "".join(random.choices(string.ascii_lowercase, k=6))
prefix = f"icechunk-repro-{suffix}"

print(f"icechunk library version: {icechunk.__version__}")
print(f"requested spec_version:   {args.spec_version}")

s3 = boto3.client(
    "s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    endpoint_url=endpoint,
    region_name=region,
    config=Config(s3={"addressing_style": "path"}),
)
bucket = args.bucket
if bucket is None:
    bucket = f"arraylake-integration-test-icechunk-repro-{suffix}"
    s3.create_bucket(Bucket=bucket)
    print(f"created JASMIN bucket:     {bucket}")
else:
    print(f"reusing JASMIN bucket:     {bucket}")

storage = icechunk.s3_storage(
    bucket=bucket,
    prefix=prefix,
    region=region,
    endpoint_url=endpoint,
    allow_http=endpoint.startswith("http://"),
    force_path_style=True,  # required for JASMIN
    access_key_id=access_key,
    secret_access_key=secret_key,
)

config = icechunk.RepositoryConfig(
    storage=icechunk.StorageSettings(
        unsafe_use_conditional_update=False,
        unsafe_use_conditional_create=False,
        retries=icechunk.StorageRetriesSettings(max_tries=10),
    )
)

# Only pass spec_version when the installed icechunk supports it (2.x).
create_kwargs = {"config": config}
supports_spec_version = "spec_version" in inspect.signature(icechunk.Repository.create).parameters
if args.spec_version is not None:
    if supports_spec_version:
        create_kwargs["spec_version"] = args.spec_version
    else:
        print("NOTE: this icechunk has no spec_version arg; creating native v1 repo.")

# Try repo create flow
try:
    repo = icechunk.Repository.create(storage, **create_kwargs)
    sv = getattr(repo, "spec_version", "n/a")
    print(f"create OK (spec_version={sv})")

    session = repo.writable_session(branch="main")
    root = zarr.open_group(session.store, mode="a")
    root.create_array("data", shape=(4,), dtype="i4", chunks=(4,))
    root["data"][:] = np.arange(4)
    commit_id = session.commit("repro commit")
    print(f"commit OK ({commit_id})")
    print("RESULT: SUCCESS — did not reproduce on this version/spec_version")
    sys.exit(0)
except Exception as e:
    print(f"RESULT: FAILED ({type(e).__name__})")
    print(str(e)[:900])
    sys.exit(1)
