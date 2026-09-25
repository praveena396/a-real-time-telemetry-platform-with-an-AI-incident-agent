"""Measure sustained database write throughput and flush latency.

Usage: python -m app.bench_writer --dsn postgresql://... --rows 200000 --batch 500

Readings are published onto the bus as fast as the writer drains them (the
producer yields whenever the writer's queue is half full), so the number
reported is what the writer + database can sustain, not the simulator rate.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import time

from .bus import EventBus
from .events import Reading
from .main import percentile
from .simulator import FaultConfig, generate
from .storage.postgres import PgStore
from .storage.writer import BatchedWriter


async def bench(dsn: str, rows: int, batch: int, interval: float) -> dict[str, float]:
    store = await PgStore.connect(dsn)
    bus = EventBus()
    sub = bus.subscribe(Reading, maxsize=batch * 20, name="db-readings")
    w = BatchedWriter("readings", sub, store.write_readings, batch_size=batch, flush_interval=interval)
    task = asyncio.create_task(w.run())
    now = time.time()
    data = [Reading(device_id=r.device_id, metric=r.metric, value=r.value, fault=r.fault, ts=now + i * 1e-4)
            for i, r in enumerate(generate(100, rows // 300 + 1, FaultConfig(), seed=1))][:rows]
    t0 = time.perf_counter()
    for r in data:
        while sub.depth() > sub.maxsize // 2:  # noqa: ASYNC110 - polling is fine in a benchmark
            await asyncio.sleep(0.001)
        bus.publish(r)
    bus.close()
    await task
    elapsed = time.perf_counter() - t0
    await store.close()
    lat = w.flush_latencies
    return {"rows": w.rows_written, "timescaledb": float(store.timescale), "elapsed_s": round(elapsed, 2),
            "rows_per_s": round(w.rows_written / elapsed), "batches": len(lat),
            "flush_p50_ms": round(percentile(lat, 0.5) * 1000, 2),
            "flush_p95_ms": round(percentile(lat, 0.95) * 1000, 2),
            "bus_dropped": sub.dropped, "writer_dropped": w.rows_dropped}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--rows", type=int, default=200_000)
    p.add_argument("--batch", type=int, default=500)
    p.add_argument("--interval", type=float, default=0.2)
    a = p.parse_args()
    if not a.dsn:
        raise SystemExit("set --dsn or DATABASE_URL")
    for k, v in asyncio.run(bench(a.dsn, a.rows, a.batch, a.interval)).items():
        print(f"{k:>14}: {v}")


if __name__ == "__main__":
    main()
