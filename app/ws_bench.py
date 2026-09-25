"""Open N WebSocket clients against a running server and measure delivery.

Usage: python -m app.ws_bench --url ws://localhost:8000/ws --clients 10 --seconds 15
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time

import websockets


async def client(url: str, seconds: float, out: list[dict[str, float]]) -> None:
    events = 0
    lag: list[float] = []
    dropped = 0
    async with websockets.connect(url, max_size=None) as ws:
        end = time.time() + seconds
        while time.time() < end:
            try:
                msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=1))
            except TimeoutError:
                continue
            now = time.time()
            events += len(msg["events"])
            dropped = msg["dropped"]
            lag.extend(now - e["ts"] for e in msg["events"] if e["type"] == "reading")
    lag.sort()
    out.append({"events_per_s": events / seconds, "dropped": dropped,
                "p50_lag_ms": lag[len(lag) // 2] * 1000 if lag else 0,
                "p95_lag_ms": lag[int(len(lag) * 0.95)] * 1000 if lag else 0})


async def run(url: str, clients: int, seconds: float) -> None:
    out: list[dict[str, float]] = []
    await asyncio.gather(*(client(url, seconds, out) for _ in range(clients)))
    tot = sum(o["events_per_s"] for o in out)
    print(f"clients={clients} total_events_per_s={tot:.0f} per_client={tot / clients:.0f}")
    print(f"p50_lag_ms(max over clients)={max(o['p50_lag_ms'] for o in out):.1f} "
          f"p95_lag_ms(max)={max(o['p95_lag_ms'] for o in out):.1f} "
          f"dropped(total)={sum(o['dropped'] for o in out):.0f}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="ws://localhost:8000/ws?types=reading,anomaly")
    p.add_argument("--clients", type=int, default=10)
    p.add_argument("--seconds", type=float, default=15)
    a = p.parse_args()
    asyncio.run(run(a.url, a.clients, a.seconds))


if __name__ == "__main__":
    main()
