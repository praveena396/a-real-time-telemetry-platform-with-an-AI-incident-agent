"""Batched database writer.

Consumes one bus subscription and flushes when either `batch_size` rows are
buffered or `flush_interval` seconds have passed, whichever comes first.

If the database is down, the batch stays buffered and the writer retries with
exponential backoff. The buffer is bounded (`max_buffer`): when it overflows,
the oldest rows are discarded and counted, the same drop-oldest policy the bus
uses. Meanwhile the bus subscription itself keeps absorbing bursts, so a
database outage never stalls the detector or the dashboard.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

from ..bus import Subscription
from ..metrics import DB_FAILURES, DB_ROWS, DB_ROWS_DROPPED, DB_WRITE_LATENCY

log = logging.getLogger(__name__)


class BatchedWriter:
    def __init__(self, name: str, sub: Subscription[Any], write: Callable[[list[Any]], Awaitable[None]], *,
                 batch_size: int = 500, flush_interval: float = 0.2, max_buffer: int = 100_000,
                 max_backoff: float = 5.0) -> None:
        self.name = name
        self.sub = sub
        self.write = write
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_buffer = max_buffer
        self.max_backoff = max_backoff
        self.buffer: list[Any] = []
        self.rows_written = 0
        self.rows_dropped = 0
        self.failures = 0
        self.flush_latencies: list[float] = []  # seconds, kept for benchmarks
        self._backoff = 0.0
        self._retry_at = 0.0

    async def run(self) -> None:
        deadline = time.monotonic() + self.flush_interval
        closed = False
        while not closed:
            wait = max(0.0, deadline - time.monotonic())
            batch = await self.sub.get_batch(self.batch_size, wait)
            if batch is None:
                closed = True
            else:
                self._append(batch)
            now = time.monotonic()
            if len(self.buffer) >= self.batch_size or now >= deadline or closed:
                if now >= self._retry_at:
                    await self._flush_all(final=closed)
                deadline = time.monotonic() + self.flush_interval

    def _append(self, rows: list[Any]) -> None:
        self.buffer.extend(rows)
        overflow = len(self.buffer) - self.max_buffer
        if overflow > 0:
            del self.buffer[:overflow]
            self.rows_dropped += overflow
            DB_ROWS_DROPPED.labels(self.name).inc(overflow)

    async def _flush_all(self, final: bool = False) -> None:
        attempts = 0
        while self.buffer:
            chunk = self.buffer[: self.batch_size]
            t0 = time.perf_counter()
            try:
                await self.write(chunk)
            except Exception as e:  # any DB error means "retry later"
                self.failures += 1
                DB_FAILURES.labels(self.name).inc()
                self._backoff = min(self.max_backoff, (self._backoff * 2) or 0.1)
                self._retry_at = time.monotonic() + self._backoff
                log.warning("%s write failed (%s); retry in %.1fs, %d rows buffered",
                            self.name, type(e).__name__, self._backoff, len(self.buffer))
                attempts += 1
                if not final or attempts >= 3:
                    return
                await asyncio.sleep(self._backoff)
                continue
            dt = time.perf_counter() - t0
            DB_WRITE_LATENCY.labels(self.name).observe(dt)
            DB_ROWS.labels(self.name).inc(len(chunk))
            if len(self.flush_latencies) < 100_000:
                self.flush_latencies.append(dt)
            self.rows_written += len(chunk)
            del self.buffer[: len(chunk)]
            self._backoff = 0.0
            self._retry_at = 0.0
