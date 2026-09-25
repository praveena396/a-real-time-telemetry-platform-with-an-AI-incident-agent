"""Wire devices -> bus -> detector -> alert sink, then report measured results.

Usage: python -m app.main --devices 20 --rate 10 --seconds 30
"""
from __future__ import annotations

import argparse
import asyncio
import random
import time

from .bus import EventBus
from .detector import run_detector
from .events import Anomaly, Reading
from .simulator import FaultConfig, run_device


async def score(readings_tap, anomalies, results: dict) -> None:
    """Count ground-truth faults and detections so we can report honest numbers."""
    async def count_faults():
        async for r in readings_tap:
            results["readings"] += 1
            if r.fault:
                results["faulty_readings"] += 1

    async def count_alerts():
        async for a in anomalies:
            results["alerts"] += 1
            if a.fault:
                results["true_alerts"] += 1
            latency = time.time() - a.ts
            results["latencies"].append(latency)

    await asyncio.gather(count_faults(), count_alerts())


async def run(devices: int, rate: float, seconds: float, seed: int) -> dict:
    bus = EventBus()
    detector_in = bus.subscribe(Reading, maxsize=5000, name="detector")
    tap = bus.subscribe(Reading, maxsize=5000, name="scorer")
    alerts = bus.subscribe(Anomaly, maxsize=1000, name="alerts")
    results = {"readings": 0, "faulty_readings": 0, "alerts": 0, "true_alerts": 0, "latencies": []}

    stop = asyncio.Event()
    rng = random.Random(seed)
    tasks = [asyncio.create_task(run_device(bus, f"dev-{i:03d}", rate, stop, FaultConfig(), random.Random(rng.random())))
             for i in range(devices)]
    workers = [asyncio.create_task(run_detector(bus, detector_in)),
               asyncio.create_task(score(tap, alerts, results))]

    start = time.perf_counter()
    try:
        await asyncio.sleep(seconds)
    finally:
        stop.set()
        await asyncio.gather(*tasks)
        await asyncio.sleep(0.2)      # let queues drain
        bus.close()
        await asyncio.gather(*workers)
    elapsed = time.perf_counter() - start

    lat = sorted(results.pop("latencies")) or [0.0]
    results.update(
        elapsed_s=round(elapsed, 2),
        events_per_s=round(bus.published / elapsed),
        alert_precision=round(results["true_alerts"] / max(results["alerts"], 1), 3),
        fault_recall=round(results["true_alerts"] / max(results["faulty_readings"], 1), 3),
        p50_alert_latency_ms=round(lat[len(lat) // 2] * 1000, 2),
        p95_alert_latency_ms=round(lat[int(len(lat) * 0.95) - 1 if len(lat) > 1 else 0] * 1000, 2),
        **bus.stats(),
    )
    return results


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--devices", type=int, default=20)
    p.add_argument("--rate", type=float, default=10.0, help="readings per second per metric per device")
    p.add_argument("--seconds", type=float, default=30.0)
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()
    try:
        res = asyncio.run(run(a.devices, a.rate, a.seconds, a.seed))
    except KeyboardInterrupt:
        print("stopped")
        return
    for k, v in res.items():
        print(f"{k:>24}: {v}")


if __name__ == "__main__":
    main()
