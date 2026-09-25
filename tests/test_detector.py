import asyncio
import random

import pytest

from app.bus import EventBus
from app.detector import run_detector
from app.events import Anomaly, Reading


@pytest.mark.asyncio
async def test_flags_spike_not_normal_noise():
    bus = EventBus()
    readings = bus.subscribe(Reading)
    alerts = bus.subscribe(Anomaly)
    task = asyncio.create_task(run_detector(bus, readings, threshold=4.0, window=100))
    rng = random.Random(0)
    for _ in range(200):
        bus.publish(Reading(device_id="d", metric="t", value=rng.gauss(60, 1)))
    bus.publish(Reading(device_id="d", metric="t", value=75.0, fault="spike"))
    await asyncio.sleep(0.05)
    readings_closed = bus.close()
    await task
    found = [a async for a in alerts]
    assert any(a.fault == "spike" for a in found)
    assert sum(1 for a in found if a.fault is None) <= 1  # at most rare noise
