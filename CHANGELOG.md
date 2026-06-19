# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.1.1]: https://github.com/ianhi/snailmail/releases/tag/v0.1.1
[0.1.0]: https://github.com/ianhi/snailmail/releases/tag/v0.1.0
