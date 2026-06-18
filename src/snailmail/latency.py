"""Per-request latency distributions.

Object-store GET RTT is well-modelled by a lognormal (a unimodal hump with a long
right tail), so :class:`LogNormal` is the recommended default; :class:`Normal`,
:class:`Exponential`, and :class:`Fixed` are there for comparison.

Every distribution pre-generates a sample pool once with vectorised numpy and serves
it round-robin, so :meth:`LatencyDist.draw_s` is O(1) with no per-request RNG and the
realised distribution is exactly reproducible for a given seed.
"""

from __future__ import annotations

import math

import numpy as np


class LatencyDist:
    """Base class: a pre-generated sample pool served round-robin.

    Subclasses set either ``_pool`` (a numpy array of *seconds*) via :meth:`_fill` or
    ``_const_s`` (a scalar, for a degenerate zero-spread distribution), and implement
    :meth:`describe`.
    """

    def __init__(self):
        # A None pool means degenerate (zero-spread): draw a constant instead.
        self._pool: np.ndarray | None = None
        self._const_s = 0.0
        self._i = 0

    def _fill(self, samples_ms: np.ndarray) -> None:
        # Latency can't be negative; truncate at 0 (matters for Normal's left tail).
        self._pool = np.clip(np.asarray(samples_ms, dtype=float), 0.0, None) / 1e3

    def _set_const_ms(self, ms: float) -> None:
        self._const_s = max(0.0, ms) / 1e3

    def draw_s(self) -> float:
        """Next latency (seconds). O(1); single-loop-thread, so the index is safe."""
        if self._pool is None:
            return self._const_s
        v = float(self._pool[self._i])
        self._i = (self._i + 1) % self._pool.size
        return v

    def percentiles(self) -> dict:
        """Realised p10/p50/p90/p99 (ms) of the pool actually served."""
        if self._pool is None:
            v = round(self._const_s * 1e3, 3)
            return {"p10_ms": v, "p50_ms": v, "p90_ms": v, "p99_ms": v, "n": 1 if v else 0}
        p = np.percentile(self._pool * 1e3, [10, 50, 90, 99])
        return {
            "p10_ms": round(float(p[0]), 3),
            "p50_ms": round(float(p[1]), 3),
            "p90_ms": round(float(p[2]), 3),
            "p99_ms": round(float(p[3]), 3),
            "n": int(self._pool.size),
        }

    def describe(self) -> dict:
        raise NotImplementedError


class Fixed(LatencyDist):
    """Deterministic latency: every request sleeps exactly ``value_ms``."""

    def __init__(self, value_ms: float = 0.0):
        super().__init__()
        self.value_ms = float(value_ms)
        self._set_const_ms(self.value_ms)

    def describe(self) -> dict:
        return {"dist": "fixed", "value_ms": round(self.value_ms, 4)}


class LogNormal(LatencyDist):
    """Lognormal latency, parameterised by the PDF **mode** (peak) and shape ``sigma``::

        mu       = ln(mode_ms) + sigma**2          # so the PDF mode == mode_ms
        sleep_ms = LogNormal(mu, sigma)

    Fits object-store GET RTT well — a unimodal hump with a long right tail. Derived:
    ``median_ms = exp(mu)``; ``mean_ms = exp(mu + sigma**2/2)``. ``mode_ms <= 0`` is
    the degenerate zero-latency reference.
    """

    def __init__(
        self,
        mode_ms: float,
        *,
        sigma: float = 0.5,
        seed: int | None = None,
        pool_size: int = 1 << 16,
    ):
        super().__init__()
        self.mode_ms = float(mode_ms)
        self.sigma = float(sigma)
        if self.mode_ms > 0.0:
            self.mu = math.log(self.mode_ms) + self.sigma**2
            self.median_ms = math.exp(self.mu)
            self.mean_ms = math.exp(self.mu + self.sigma**2 / 2.0)
            rng = np.random.default_rng(seed)
            self._fill(rng.lognormal(self.mu, self.sigma, size=pool_size))
        else:
            self.mu = float("nan")
            self.median_ms = self.mean_ms = 0.0

    def describe(self) -> dict:
        d = {
            "dist": "lognormal",
            "mode_ms": round(self.mode_ms, 4),
            "sigma": self.sigma,
            "degenerate": self._pool is None,
        }
        if self._pool is not None:
            d.update(
                mu=round(self.mu, 6),
                median_ms=round(self.median_ms, 4),
                mean_ms=round(self.mean_ms, 4),
                pool_size=int(self._pool.size),
            )
        return d


class Normal(LatencyDist):
    """Gaussian latency, ``mean_ms`` +/- ``std_ms``, truncated at 0 (no negative draws).

    ``std_ms <= 0`` is the degenerate deterministic case (sleeps ``mean_ms``).
    """

    def __init__(
        self,
        mean_ms: float,
        *,
        std_ms: float = 0.0,
        seed: int | None = None,
        pool_size: int = 1 << 16,
    ):
        super().__init__()
        self.mean_ms = float(mean_ms)
        self.std_ms = float(std_ms)
        if self.std_ms > 0.0:
            rng = np.random.default_rng(seed)
            self._fill(rng.normal(self.mean_ms, self.std_ms, size=pool_size))
        else:
            self._set_const_ms(self.mean_ms)

    def describe(self) -> dict:
        d = {
            "dist": "normal",
            "mean_ms": round(self.mean_ms, 4),
            "std_ms": round(self.std_ms, 4),
            "degenerate": self._pool is None,
        }
        if self._pool is not None:
            d["pool_size"] = int(self._pool.size)
        return d


class Exponential(LatencyDist):
    """Exponential latency with the given ``mean_ms`` (mode at 0; heavy single tail).

    ``mean_ms <= 0`` is the degenerate zero-latency reference.
    """

    def __init__(self, mean_ms: float, *, seed: int | None = None, pool_size: int = 1 << 16):
        super().__init__()
        self.mean_ms = float(mean_ms)
        if self.mean_ms > 0.0:
            rng = np.random.default_rng(seed)
            self._fill(rng.exponential(self.mean_ms, size=pool_size))

    def describe(self) -> dict:
        d = {
            "dist": "exponential",
            "mean_ms": round(self.mean_ms, 4),
            "degenerate": self._pool is None,
        }
        if self._pool is not None:
            d["pool_size"] = int(self._pool.size)
        return d
