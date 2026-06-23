# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-06-22

### Added
- `ObjectStore`: a local, in-process S3-compatible object store (backed by **moto**) with
  the same injectable latency/bandwidth model as the range server — for benchmarking the
  **metadata** round-trips of a consumer like Icechunk (config/refs/snapshots/manifests),
  which are free and invisible on a local filesystem. Optional `[s3]` extra. Latency is
  optional, so `ObjectStore()` is also just a plain local S3 store. `icechunk_storage()`
  returns a ready-wired `icechunk.Storage`; `stats()` splits cost into `metadata_requests`
  vs `data_requests` with per-repo-component byte counts. Storage is in-memory and ephemeral.
- `StoreBehavior` for emulating store quirks. `conditional_writes="enforce"|"reject"|"ignore"`
  models how a store treats S3 conditional writes — `"reject"` returns `NotImplemented`
  like JASMIN, making the failure in [icechunk#2228](https://github.com/earth-mover/icechunk/issues/2228)
  reproducible locally with no cloud credentials (see `repros/icechunk_2228.py`).
- `LatencyMiddleware` (the generic WSGI middleware under `ObjectStore`) and `SharedPipe`
  (the synchronous twin of `AsyncSharedPipe`).

### Changed
- Renamed `LatencyRangeServer` to `HTTPRangeServer`. No back-compat alias (pre-1.0). Both
  servers now share the same shape: optional `latency=`/`bandwidth_mbs=` wire shaping, with
  store-specific quirks under `behavior=`.

## [0.2.0] - 2026-06-18

### Added
- `LatencyRangeServer.from_file(path, ...)`: serve a single file directly, reachable at
  its basename, with no containing directory, no temp dir, no symlink, and no copy — so
  a large fixture costs nothing to set up. It's streamed from disk via aiohttp's
  `FileResponse` (the same machinery directory mode uses), and because only the one
  pinned path is ever served there is no path-traversal surface (every other key 404s).
  The result is observationally a one-key directory server: `describe()`, `files()`,
  `url()`, and `stats()` behave identically to directory mode.

## [0.1.1] - 2026-06-18

### Fixed
- `files()` / `describe()["n_files"]` over-counted symlinks whose target escapes the
  served root — they were listed but 404 on GET, since aiohttp (and `_target_size`)
  serve a key only when its resolved real path is a file inside the root. `files()` now
  applies that same resolve-then-in-root rule, so the index matches what is actually
  served. Documented the root-containment rule in the README.

## [0.1.0] - 2026-06-18

Initial public release.

### Added
- `LatencyRangeServer`: a loopback HTTP server that serves a **directory tree** over
  HTTP Range with injectable latency and bandwidth limits, for benchmarking range /
  object-store / virtual-chunk reads. One object per file (the shape of an Icechunk
  virtual dataset), range- and traversal-safe; `base` is the root, `url(key)` builds a
  key URL, and `files()` lists the served keys.
- Pluggable per-request latency distributions: `LogNormal` (the recommended default),
  `Normal`, `Exponential`, and `Fixed`, each with explicit, distribution-specific
  parameters. Draws are pre-generated and served round-robin (O(1), reproducible per
  seed).
- Shared FIFO bandwidth pipe (`AsyncSharedPipe`) so response bytes serialize through a
  capped egress while round-trips stay parallel.
- Request accounting via `stats()`: GET/request counts, 404 misses (`n_misses`), total
  bytes, peak concurrency (`max_in_flight`), and per-method / per-path breakdowns;
  persists until `reset_counts()`.
- `snailmail` CLI with a `--dist` selector and explicit per-distribution flags, a
  `--json` machine-readable address line (flushed before serving), and `--version`.

[0.2.0]: https://github.com/ianhi/snailmail/releases/tag/v0.2.0
[0.1.1]: https://github.com/ianhi/snailmail/releases/tag/v0.1.1
[0.1.0]: https://github.com/ianhi/snailmail/releases/tag/v0.1.0
