"""Streaming anomaly detection using a rolling z-score per (device, metric).

Only normal-looking readings update the baseline, so a fault doesn't
teach the detector that the fault is normal.
"""
from __future__ import annotations

import math
from collections import defaultdict, deque

from .bus import EventBus, Subscription
from .events import Anomaly, Reading


class RollingStats:
    def __init__(self, window: int) -> None:
        self.buf: deque[float] = deque(maxlen=window)

    def add(self, x: float) -> None:
        self.buf.append(x)

    def ready(self) -> bool:
        return len(self.buf) >= self.buf.maxlen // 2

    def z(self, x: float) -> float:
        n = len(self.buf)
        mean = sum(self.buf) / n
        var = sum((v - mean) ** 2 for v in self.buf) / max(n - 1, 1)
        return (x - mean) / (math.sqrt(var) or 1e-9)


async def run_detector(bus: EventBus, readings: Subscription[Reading],
                       threshold: float = 4.0, window: int = 100) -> None:
    stats: dict[tuple[str, str], RollingStats] = defaultdict(lambda: RollingStats(window))
    async for r in readings:
        s = stats[(r.device_id, r.metric)]
        if s.ready():
            z = s.z(r.value)
            if abs(z) >= threshold:
                bus.publish(Anomaly(device_id=r.device_id, metric=r.metric,
                                    value=r.value, zscore=z, fault=r.fault))
                continue  # don't let anomalies pollute the baseline
        s.add(r.value)
