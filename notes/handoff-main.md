# Handoff: snailmail (main)

## State (verified this session)

Release-staged for **v0.1.0**, committed and pushed to `github.com/ianhi/snailmail`
(main), clean working tree. All gates green:

```bash
uv run pytest                 # 43 passed
uv run ruff check src tests   # clean
uv run mypy                   # clean (5 files)
```

## Read first

- **AGENTS.md** — durable: purpose, layout, design decisions, conventions, non-goals.
- **docs/NOTES.md** — the worklog: full status, open tasks, deferred ideas, origin.
  Keep it current as you work.

This file is just the pointer + the single immediate next action; everything
time-specific is in NOTES.md.

## Conventions (from AGENTS.md)

- Do **not** co-sign commits (no `Co-Authored-By` / tool trailers).
- Comments tight, *why* not *what*, nothing session-specific.
- Keep the gates green; prek hooks (ruff + format + mypy + hygiene) run on commit.

## Next action — cut the v0.1.0 release (HUMAN-GATED)

The library is done; the only thing left is publishing, which needs the maintainer.
**Do not publish, tag, or cut a GitHub Release without explicit, in-the-moment
sign-off.** Once signed off:

1. **PyPI Trusted Publisher** (one-time, maintainer): register for project
   `snailmail` → owner/repo `ianhi/snailmail`, workflow `release.yml`, environment
   `pypi`.
2. **Pre-flight:** `uv build` then `uvx twine check dist/*` (README renders, SPDX
   license ok); confirm a fresh-venv install imports and the CLI runs
   (`--dist` / `--json` / `--version`).
3. **Release:** cut a GitHub **Release** on tag `v0.1.0` from a clean tree (hatch-vcs
   derives the version from the tag). `.github/workflows/release.yml` builds and
   publishes via OIDC and refuses dev/local versions.
4. **Verify:** in a clean env, `pip install snailmail` and check `snailmail --version`
   reports `0.1.0`.

## After release — 0.2.0 candidates (detailed in docs/NOTES.md)

- Time-varying bandwidth — needs a *seeded, declarative* schedule; seam is in
  `bandwidth.py`. Watch the transport-shaping non-goal.
- Empirical latency distribution — feed measured samples/percentiles into a
  `LatencyDist`.
- Live `stats()` readout while serving (low priority).

## Not snailmail's problem

The downstream Icechunk virtual-read concurrency benchmark (origin in NOTES.md) is
blocked by an **upstream Icechunk bug** that drops the port from virtual-chunk URLs.
Don't try to fix that here; `--port` exists to bind 80 as a workaround.

## Suggested opening prompt for the next session

> Read AGENTS.md and docs/NOTES.md. snailmail is release-staged for v0.1.0 with all
> gates green; don't publish or tag without my explicit go-ahead. [then your task]
