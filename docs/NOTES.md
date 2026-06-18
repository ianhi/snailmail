# Working notes

The mutable worklog: current status, open tasks, and origin/context. Update this as
work happens. Durable design/conventions/non-goals belong in [../AGENTS.md](../AGENTS.md),
not here.

## Status

Alpha, working, release-staged for v0.1.0. Committed and pushed to
`github.com/ianhi/snailmail` (main). Server, latency model, bandwidth pipe, counters,
and CLI are done and tested (`uv run pytest`, `uv run ruff check`, `uv run mypy` all
green). prek hooks (ruff lint + format + mypy + file hygiene) are configured.

Release prep done:
- CI: `.github/workflows/ci.yml` runs ruff + pytest on Linux/macOS across Python
  3.10–3.13 via uv (`fetch-depth: 0` so hatch-vcs sees tags).
- Release: `.github/workflows/release.yml` — tag `vX.Y.Z` → `uv build` →
  `pypa/gh-action-pypi-publish` via OIDC Trusted Publishing (`id-token: write`,
  environment `pypi`), guarded against publishing dev/local versions.
- Git-based versioning via hatch-vcs (`dynamic = ["version"]`); the version comes
  from the git tag. Untagged builds get a dev version; tagging `v0.1.0` on a clean
  tree yields `0.1.0`. `__version__` is read from installed metadata.
- Packaging verified: `uv build` produces a clean sdist (src, tests, README, CHANGELOG,
  LICENSE, AGENTS — no docs worklog / CI / lock) + wheel; SPDX `License-Expression: MIT`;
  `twine check` passes
  (README renders). Fresh-venv install: imports, the CLI (`--dist`/`--json`/
  `--version`), and a live 206 range serve all work.
- pyproject metadata filled in: per-version Python + OS classifiers, Repository /
  Changelog URLs.
- `CHANGELOG.md` (Keep a Changelog) with a 0.1.0 entry.

API expanded for 0.1.0 (deliberately, with the owner):
- Package split by concern: `latency.py`, `bandwidth.py`, `server.py`, `cli.py`.
- Pluggable latency distributions (`LogNormal`/`Normal`/`Exponential`/`Fixed`) with
  explicit per-distribution parameters; no overloaded magic knob, no shorthand.
- **Directory-only** serving (via aiohttp `add_static`); `base` is the root,
  `url(key)` builds a key URL, `files()` lists keys. Single-file mode was dropped to
  keep one concept. The Icechunk/object-store many-objects case.
- `stats()` now also reports `n_misses` (404s) and per-method / per-path counts.
- Independent pre-release subagent passes were run: code/packaging review, a black-box
  CLI-discovery test, an acceptance test (31/31), plus `/simplify` and `/code-review`.
  Findings addressed, including: a malformed-`Range` 500 (now 416 via `request.http_range`),
  a non-directory root traceback (now a clean argparse error), and `start()` hanging on a
  bind failure (now propagates).
- Hand-rolled bits collapsed onto stdlib/aiohttp: Range parsing → `request.http_range`;
  traversal check → `Path.is_relative_to`; bandwidth normalization owned by
  `AsyncSharedPipe`; keep-alive → `threading.Event().wait()`. `_target_size` stays — see
  its docstring (aiohttp won't expose pre-serve size/miss). Considered Starlette/Uvicorn
  and rejected: the HTTP side is already delegated; the rest is the instrument.

## Open tasks

- **Finish Trusted Publishing setup (needs the maintainer).** `.github/workflows/release.yml`
  is in place: tag `vX.Y.Z` → `uv build` → `pypa/gh-action-pypi-publish` via OIDC
  (`id-token: write`, environment `pypi`), with a guard that refuses dev/local
  versions. Remaining: on PyPI, register the trusted publisher for project `snailmail`,
  owner/repo `ianhi/snailmail`, workflow `release.yml`, environment `pypi`; then tag a
  clean `v0.1.0` to publish. Do NOT tag/publish without explicit sign-off.
- **Time-varying bandwidth** — deferred (see below). Candidate for 0.2.0.
- An *empirical* latency distribution (feed measured samples/percentiles) — 0.2.0.
- A live `stats()` readout while serving (low priority).

## Deferred: time-varying bandwidth

The shared pipe's rate `B` is a single chokepoint that could become a function of
time (schedule / sinusoid / random walk). Left out of 0.1.0 on purpose: to keep
benchmarks reproducible and attributable it needs a *seeded, declarative* schedule,
which is its own mini-design, and it drifts toward the transport-shaping non-goal.
The seam is there in `bandwidth.py` when we want it.

## Origin / context

Extracted from the `virtual-h5ad` project. The driving question there: does
`anndata[mask].to_memory()` over an Icechunk virtual repo fan out concurrent GETs,
or read serially? (Reading the call stack: it goes through zarr's
`codec_pipeline → asyncio.gather`, i.e. concurrent — so a hand-rolled thread-pool
reader was redundant.) snailmail is the harness to confirm that empirically.

That end-to-end benchmark is **blocked upstream** by an Icechunk bug: it drops the
port from virtual-chunk location URLs, so a repo pointed at
`http://127.0.0.1:<ephemeral>/...` is unreadable (the port-less location no longer
matches its container prefix). The `--port` argument exists partly so you can bind
port 80 to sidestep it until the Icechunk fix lands. Remove this note once the fix
ships and the benchmark runs.
