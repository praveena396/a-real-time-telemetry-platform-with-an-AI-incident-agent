"""Headless benchmark: devices -> bus -> detector -> scorer, then report measured results.

Usage: python -m app.main --devices 20 --rate 10 --seconds 30 --detector hybrid
"""
from __future__ import annotations

import argparse
import asyncio
import time
from collections import defaultdict
from typing import Any

from .bus import EventBus, Subscription
from .detector import run_detector
from .detectors import DETECTORS, make_factory
from .events import Anomaly, Reading
from .simulator import FaultConfig, Fleet, run_device


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    return s[min(len(s) - 1, int(q * len(s)))]


async def score(readings_tap: Subscription[Reading], anomalies: Subscription[Anomaly],
                results: dict[str, Any]) -> None:
    """Count ground-truth faults and detections so we can report honest numbers."""
    async def count_faults() -> None:
        async for r in readings_tap:
            results["readings"] += 1
            if r.fault:
                results["faulty"][r.fault] += 1

    async def count_alerts() -> None:
        async for a in anomalies:
            results["alerts"] += 1
            if a.fault:
                results["true_alerts"] += 1
                results["caught"][a.fault] += 1
            results["latencies"].append(a.ts - (a.reading_ts or a.ts))

    await asyncio.gather(count_faults(), count_alerts())


async def run(devices: int, rate: float, seconds: float, seed: int,
              detector: str = "hybrid") -> dict[str, Any]:
    bus = EventBus()
    detector_in = bus.subscribe(Reading, maxsize=5000, name="detector")
    tap = bus.subscribe(Reading, maxsize=5000, name="scorer")
    alerts = bus.subscribe(Anomaly, maxsize=1000, name="alerts")
    results: dict[str, Any] = {"readings": 0, "alerts": 0, "true_alerts": 0, "latencies": [],
                               "faulty": defaultdict(int), "caught": defaultdict(int)}

    stop = asyncio.Event()
    fleet = Fleet(devices, FaultConfig(stuck_prob=0.0005), seed)
    tasks = [asyncio.create_task(run_device(bus, d, rate, stop)) for d in fleet.devices.values()]
    workers = [asyncio.create_task(run_detector(bus, detector_in, make_factory(detector))),
               asyncio.create_task(score(tap, alerts, results))]

    start = time.perf_counter()
    try:
        await asyncio.sleep(seconds)
    finally:
        stop.set()
        await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - start
        await asyncio.sleep(0.2)      # let queues drain
        bus.close()
        await asyncio.gather(*workers)

    lat = results.pop("latencies")
    faulty, caught = results.pop("faulty"), results.pop("caught")
    out: dict[str, Any] = dict(results)
    out.update(
        detector=detector,
        elapsed_s=round(elapsed, 2),
        events_per_s=round(bus.published / elapsed),
        alert_precision=round(results["true_alerts"] / max(results["alerts"], 1), 3),
        fault_recall=round(sum(caught.values()) / max(sum(faulty.values()), 1), 3),
        **{f"recall[{k}]": round(caught[k] / v, 3) for k, v in sorted(faulty.items())},
        p50_alert_latency_ms=round(percentile(lat, 0.50) * 1000, 3),
        p95_alert_latency_ms=round(percentile(lat, 0.95) * 1000, 3),
        **bus.stats(),
    )
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--devices", type=int, default=20)
    p.add_argument("--rate", type=float, default=10.0, help="readings per second per metric per device")
    p.add_argument("--seconds", type=float, default=30.0)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--detector", choices=sorted(DETECTORS), default="hybrid")
    a = p.parse_args()
    try:
        res = asyncio.run(run(a.devices, a.rate, a.seconds, a.seed, a.detector))
    except KeyboardInterrupt:
        print("stopped")
        return
    for k, v in res.items():
        print(f"{k:>24}: {v}")


if __name__ == "__main__":
    main()
