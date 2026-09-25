"""Run a streaming detector over the bus: one detector instance per (device, metric)."""
from __future__ import annotations

from collections.abc import Callable

from .bus import EventBus, Subscription
from .detectors import Detector, ZScore
from .events import Anomaly, Reading


async def run_detector(bus: EventBus, readings: Subscription[Reading],
                       factory: Callable[[], Detector] | None = None,
                       on_anomaly: Callable[[Anomaly], None] | None = None) -> None:
    factory = factory or ZScore
    detectors: dict[tuple[str, str], Detector] = {}
    async for r in readings:
        key = (r.device_id, r.metric)
        det = detectors.get(key)
        if det is None:
            det = detectors[key] = factory()
        score = det.observe(r.value)
        if score is not None:
            a = Anomaly(device_id=r.device_id, metric=r.metric, value=r.value, score=score,
                        fault=r.fault, detector=det.name, reading_ts=r.ts)
            if on_anomaly:
                on_anomaly(a)
            if not bus.closed:
                bus.publish(a)
