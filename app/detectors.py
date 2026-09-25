"""Streaming anomaly detectors, one instance per (device, metric) stream.

Every detector implements `observe(x) -> score | None`: it returns a score
when `x` is anomalous and None otherwise. Anomalous readings never update the
baseline, so a fault can't teach a detector that the fault is normal.

- zscore : rolling-window z-score. Great at spikes; slow drift leaks into the
           window before it crosses the threshold.
- ewma   : exponentially weighted mean/variance. O(1) memory, similar
           trade-offs to zscore with a tunable memory length.
- cusum  : two-sided CUSUM on standardized residuals. Accumulates small,
           persistent deviations, so it catches drift early; a lone spike
           also trips it.
- hybrid : point z-test for spikes + CUSUM on clipped residuals for drift
           (clipping stops one spike from tripping CUSUM) + a flatline test
           for stuck sensors. This is the default.
"""
from __future__ import annotations

import math
from collections import deque
from collections.abc import Callable
from typing import Protocol


class Detector(Protocol):
    name: str

    def observe(self, x: float) -> float | None: ...


class RollingStats:
    """O(1) rolling mean/std over a fixed window using running sums."""

    def __init__(self, window: int) -> None:
        self.buf: deque[float] = deque(maxlen=window)
        self.s = 0.0
        self.ss = 0.0

    def add(self, x: float) -> None:
        if len(self.buf) == self.buf.maxlen:
            old = self.buf[0]
            self.s -= old
            self.ss -= old * old
        self.buf.append(x)
        self.s += x
        self.ss += x * x

    def ready(self) -> bool:
        return len(self.buf) >= (self.buf.maxlen or 0) // 2

    def mean(self) -> float:
        return self.s / len(self.buf)

    def std(self) -> float:
        n = len(self.buf)
        var = (self.ss - self.s * self.s / n) / max(n - 1, 1)
        return math.sqrt(max(var, 0.0)) or 1e-9

    def z(self, x: float) -> float:
        return (x - self.mean()) / self.std()


class ZScore:
    name = "zscore"

    def __init__(self, window: int = 100, threshold: float = 4.0) -> None:
        self.stats = RollingStats(window)
        self.threshold = threshold

    def observe(self, x: float) -> float | None:
        if self.stats.ready():
            z = self.stats.z(x)
            if abs(z) >= self.threshold:
                return z
        self.stats.add(x)
        return None


class _EwmaBaseline:
    """EWMA mean/variance, seeded from a plain average over the warmup period."""

    def __init__(self, alpha: float, warmup: int) -> None:
        self.alpha = alpha
        self.warmup = warmup
        self.n = 0
        self.mean = 0.0
        self.var = 0.0
        self._seed: list[float] = []

    def ready(self) -> bool:
        return self.n >= self.warmup

    def z(self, x: float) -> float:
        return (x - self.mean) / (math.sqrt(self.var) or 1e-9)

    def add(self, x: float) -> None:
        self.n += 1
        if self.n <= self.warmup:
            self._seed.append(x)
            if self.n == self.warmup:
                m = sum(self._seed) / len(self._seed)
                self.mean = m
                self.var = sum((v - m) ** 2 for v in self._seed) / max(len(self._seed) - 1, 1)
                self._seed = []
            return
        d = x - self.mean
        self.mean += self.alpha * d
        self.var = (1 - self.alpha) * (self.var + self.alpha * d * d)


class Ewma:
    name = "ewma"

    def __init__(self, alpha: float = 0.02, threshold: float = 4.0, warmup: int = 50) -> None:
        self.base = _EwmaBaseline(alpha, warmup)
        self.threshold = threshold

    def observe(self, x: float) -> float | None:
        if self.base.ready():
            z = self.base.z(x)
            if abs(z) >= self.threshold:
                return z
        self.base.add(x)
        return None


class Cusum:
    """Two-sided CUSUM. `k` is the slack (in std units), `h` the decision threshold.

    While in alarm the baseline is frozen. The alarm clears (and the sums
    reset) as soon as a reading looks normal again, so a recovered stream
    stops alerting immediately instead of slowly bleeding the sum down.
    """

    name = "cusum"

    def __init__(self, k: float = 1.0, h: float = 5.0, alpha: float = 0.02,
                 warmup: int = 50, clip: float | None = None, reset_z: float = 1.0) -> None:
        self.base = _EwmaBaseline(alpha, warmup)
        self.k, self.h, self.clip, self.reset_z = k, h, clip, reset_z
        self.hi = 0.0
        self.lo = 0.0

    def observe(self, x: float) -> float | None:
        if not self.base.ready():
            self.base.add(x)
            return None
        z = self.base.z(x)
        if (self.hi > self.h or self.lo > self.h) and abs(z) < self.reset_z:
            self.hi = self.lo = 0.0  # recovered
        zc = max(-self.clip, min(self.clip, z)) if self.clip else z
        self.hi = max(0.0, self.hi + zc - self.k)
        self.lo = max(0.0, self.lo - zc - self.k)
        if self.hi > self.h:
            return self.hi
        if self.lo > self.h:
            return -self.lo
        self.base.add(x)
        return None


class Flatline:
    """Flags a sensor that stops moving: variance of the last `n` readings collapses."""

    def __init__(self, n: int = 8, ratio: float = 0.02) -> None:
        self.recent: deque[float] = deque(maxlen=n)
        self.ratio = ratio

    def observe(self, x: float, baseline_std: float) -> bool:
        self.recent.append(x)
        if len(self.recent) < (self.recent.maxlen or 0):
            return False
        return (max(self.recent) - min(self.recent)) < self.ratio * baseline_std


class Hybrid:
    name = "hybrid"

    def __init__(self, threshold: float = 4.0, k: float = 1.0, h: float = 5.0,
                 alpha: float = 0.02, warmup: int = 50) -> None:
        self.base = _EwmaBaseline(alpha, warmup)
        self.threshold = threshold
        self.cusum = Cusum(k=k, h=h, clip=3.0)
        self.cusum.base = self.base  # share one baseline
        self.flat = Flatline()

    def observe(self, x: float) -> float | None:
        if not self.base.ready():
            self.base.add(x)
            return None
        std = math.sqrt(self.base.var) or 1e-9
        z = self.base.z(x)
        if abs(z) >= self.threshold:
            # Keep CUSUM state consistent without learning the spike.
            self.cusum.hi = max(0.0, self.cusum.hi + math.copysign(3.0, z) - self.cusum.k)
            self.cusum.lo = max(0.0, self.cusum.lo - math.copysign(3.0, z) - self.cusum.k)
            self.flat.observe(x, std)
            return z
        if self.flat.observe(x, std):
            return 0.0  # a stuck sensor has no meaningful deviation score
        return self.cusum.observe(x)  # CUSUM updates the shared baseline when normal


DETECTORS: dict[str, Callable[[], Detector]] = {
    "zscore": ZScore,
    "ewma": Ewma,
    "cusum": Cusum,
    "hybrid": Hybrid,
}


def make_factory(name: str, **params: float) -> Callable[[], Detector]:
    if name not in DETECTORS:
        raise ValueError(f"unknown detector {name!r}; choose from {sorted(DETECTORS)}")
    cls = DETECTORS[name]
    return lambda: cls(**params)
