"""Find the pipeline's real capacity by ramping load until it can't keep up.

Usage:
    python -m app.loadtest                          # detector only (in-memory store)
    python -m app.loadtest --dsn postgresql://...   # also write every reading to the DB

Each step runs devices x rate x 3 metrics for --step-seconds. A step "holds"
when the achieved publish rate is >= 95% of the target AND no subscriber
dropped events AND the detector's end-of-step backlog is small. The last
step that holds is the sustainable capacity on this machine.

Everything (simulator, bus, detector, writer) shares one Python process and
one CPU core, so the simulator's own cost is included in the number.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import platform
import time
from typing import Any

from .bus import EventBus
from .detector import run_detector
from .detectors import make_factory
from .events import Reading
from .main import percentile
from .simulator import FaultConfig, Fleet, run_device
from .storage.base import MemoryStore, Store
from .storage.writer import BatchedWriter


async def step(devices: int, rate: float, seconds: float, store: Store, detector: str) -> dict[str, Any]:
    bus = EventBus()
    det_in = bus.subscribe(Reading, maxsize=20_000, name="detector")
    wsub = bus.subscribe(Reading, maxsize=50_000, name="db-readings")
    writer = BatchedWriter("readings", wsub, store.write_readings)
    lag: list[float] = []

    async def lag_probe() -> None:
        # How stale is the reading the detector is about to process?
        while not bus.closed:
            if det_in.depth():
                head = det_in.queue._queue[0]  # type: ignore[attr-defined]
                if isinstance(head, Reading):
                    lag.append(time.time() - head.ts)
            await asyncio.sleep(0.05)

    stop = asyncio.Event()
    fleet = Fleet(devices, FaultConfig(), seed=1)
    workers = [asyncio.create_task(run_detector(bus, det_in, make_factory(detector))),
               asyncio.create_task(writer.run()), asyncio.create_task(lag_probe())]
    t0 = time.perf_counter()
    tasks = [asyncio.create_task(run_device(bus, d, rate, stop)) for d in fleet.devices.values()]
    await asyncio.sleep(seconds)
    stop.set()
    await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - t0
    backlog = det_in.depth()
    bus.close()
    await asyncio.gather(*workers)
    target = devices * rate * 3
    achieved = bus.published / elapsed
    dropped = det_in.dropped + wsub.dropped + writer.rows_dropped
    return {"devices": devices, "rate_hz": rate, "target_eps": round(target), "achieved_eps": round(achieved),
            "dropped": dropped, "detector_backlog": backlog,
            "p95_detector_lag_ms": round(percentile(lag, 0.95) * 1000, 1),
            "db_rows": writer.rows_written,
            "holds": achieved >= 0.95 * target and dropped == 0 and backlog < 1000}


async def run(dsn: str | None, start_devices: int, rate: float, factor: float, max_steps: int,
              seconds: float, detector: str) -> None:
    store: Store = MemoryStore(per_stream=2000)
    if dsn:
        from .storage.postgres import PgStore
        store = await PgStore.connect(dsn)
    print(f"python {platform.python_version()} on {platform.machine()} ({os.cpu_count()} cpus), "
          f"store={'postgres' if dsn else 'memory'}, detector={detector}")
    devices, best = start_devices, None
    for _ in range(max_steps):
        r = await step(devices, rate, seconds, store, detector)
        print("  ".join(f"{k}={v}" for k, v in r.items()), flush=True)
        if not r["holds"]:
            break
        best = r
        devices = int(devices * factor)
    await store.close()
    if best:
        print(f"\nsustainable: {best['achieved_eps']} events/s "
              f"({best['devices']} devices x {rate} Hz x 3 metrics), 0 drops")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--devices", type=int, default=50)
    p.add_argument("--rate", type=float, default=10.0)
    p.add_argument("--factor", type=float, default=1.5)
    p.add_argument("--max-steps", type=int, default=12)
    p.add_argument("--step-seconds", type=float, default=10.0)
    p.add_argument("--detector", default="hybrid")
    a = p.parse_args()
    asyncio.run(run(a.dsn, a.devices, a.rate, a.factor, a.max_steps, a.step_seconds, a.detector))


if __name__ == "__main__":
    main()
