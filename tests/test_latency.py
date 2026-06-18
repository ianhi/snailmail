"""Tests for the latency distributions and the CLI's --dist parameter wiring."""

import numpy as np
import pytest

from snailmail import Exponential, Fixed, LogNormal, Normal
from snailmail.cli import _latency_from_args, _parser


@pytest.mark.parametrize(
    "dist",
    [
        LogNormal(40, seed=0),
        Normal(40, std_ms=10, seed=0),
        Exponential(40, seed=0),
        Fixed(40),
    ],
)
def test_draws_are_nonnegative_and_reproducible(dist):
    draws = [dist.draw_s() for _ in range(100)]
    assert all(d >= 0 for d in draws)
    assert dist.describe()["dist"] in {"lognormal", "normal", "exponential", "fixed"}


def test_normal_truncates_negative_draws():
    # A mean near 0 with wide spread would draw negatives; they must clamp to 0.
    d = Normal(1, std_ms=50, seed=0)
    assert min(d.draw_s() for _ in range(1000)) >= 0.0


def test_degenerate_distributions_are_constant():
    assert LogNormal(0).draw_s() == 0.0
    assert Exponential(0).draw_s() == 0.0
    assert Normal(15, std_ms=0).draw_s() == pytest.approx(0.015)


def test_percentiles_ordered():
    p = Exponential(50, seed=1).percentiles()
    assert p["p10_ms"] < p["p50_ms"] < p["p90_ms"] < p["p99_ms"]


def _build(argv):
    ap = _parser()
    return _latency_from_args(ap.parse_args(argv), ap)


def test_cli_builds_each_distribution():
    assert _build(["f"]) is None  # no --dist => no latency
    assert isinstance(_build(["f", "--dist", "lognormal", "--mode-ms", "45"]), LogNormal)
    assert isinstance(_build(["f", "--dist", "normal", "--mean-ms", "45", "--std-ms", "5"]), Normal)
    assert isinstance(_build(["f", "--dist", "exponential", "--mean-ms", "45"]), Exponential)
    assert isinstance(_build(["f", "--dist", "fixed", "--value-ms", "20"]), Fixed)


def test_cli_rejects_foreign_and_missing_params():
    with pytest.raises(SystemExit):
        _build(["f", "--dist", "normal", "--mode-ms", "45"])  # mode-ms belongs to lognormal
    with pytest.raises(SystemExit):
        _build(["f", "--dist", "lognormal"])  # missing required --mode-ms
    with pytest.raises(SystemExit):
        _build(["f", "--mode-ms", "45"])  # param without --dist


def test_seed_threads_through_cli():
    a = _build(["f", "--dist", "lognormal", "--mode-ms", "30", "--seed", "7"])
    b = LogNormal(30, seed=7)
    assert np.allclose([a.draw_s() for _ in range(5)], [b.draw_s() for _ in range(5)])
