import asyncio
import random

import pytest

from app.bus import EventBus
from app.compare import evaluate
from app.detector import run_detector
from app.detectors import DETECTORS, Cusum, Hybrid, ZScore, make_factory
from app.events import Anomaly, Reading
from app.simulator import FaultConfig


@pytest.mark.asyncio
async def test_flags_spike_not_normal_noise():
    bus = EventBus()
    readings = bus.subscribe(Reading)
    alerts = bus.subscribe(Anomaly)
    task = asyncio.create_task(run_detector(bus, readings, make_factory("zscore", window=100)))
    rng = random.Random(0)
    for _ in range(200):
        bus.publish(Reading(device_id="d", metric="t", value=rng.gauss(60, 1)))
    bus.publish(Reading(device_id="d", metric="t", value=75.0, fault="spike"))
    await asyncio.sleep(0.05)
    bus.close()
    await task
    found = [a async for a in alerts]
    assert any(a.fault == "spike" for a in found)
    assert sum(1 for a in found if a.fault is None) <= 1  # at most rare noise
    assert all(a.detector == "zscore" and a.reading_ts is not None for a in found)


def _noise(rng, n, mean=0.0):
    return [rng.gauss(mean, 1.0) for _ in range(n)]


@pytest.mark.parametrize("name", sorted(DETECTORS))
def test_quiet_on_pure_noise(name):
    rng = random.Random(1)
    det = DETECTORS[name]()
    flags = sum(det.observe(x) is not None for x in _noise(rng, 5000))
    assert flags < 25  # < 0.5% false alarm rate on clean data


def test_cusum_catches_slow_drift_that_zscore_misses_early():
    rng = random.Random(2)
    warm = _noise(rng, 200)
    drift = [rng.gauss(0, 1) + 0.3 * i for i in range(1, 15)]  # reaches ~4.2 sigma at the end
    z, c = ZScore(), Cusum()
    for x in warm:
        z.observe(x)
        c.observe(x)
    first_z = next((i for i, x in enumerate(drift) if z.observe(x) is not None), None)
    first_c = next((i for i, x in enumerate(drift) if c.observe(x) is not None), None)
    assert first_c is not None
    assert first_z is None or first_c < first_z


def test_cusum_clears_after_recovery():
    rng = random.Random(3)
    c = Cusum()
    for x in _noise(rng, 200):
        c.observe(x)
    for i in range(20):
        c.observe(3.0 + 0.1 * i)
    after = [c.observe(x) for x in _noise(rng, 50)]
    assert sum(a is not None for a in after) <= 2


def test_hybrid_flags_stuck_sensor():
    rng = random.Random(4)
    h = Hybrid()
    for x in _noise(rng, 200):
        h.observe(x)
    flags = [h.observe(0.1) for _ in range(20)]
    assert sum(f is not None for f in flags) >= 10


def test_hybrid_beats_zscore_on_drift_in_comparison():
    faults = FaultConfig(stuck_prob=0.0005)
    z = evaluate("zscore", 5, 2000, 11, faults).summary()
    h = evaluate("hybrid", 5, 2000, 11, faults).summary()
    assert h["recall[drift]"] > z["recall[drift]"]
    assert h["precision"] > 0.9


def test_unknown_detector_rejected():
    with pytest.raises(ValueError):
        make_factory("nope")
