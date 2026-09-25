"""FastAPI service: REST endpoints, live WebSocket stream, Prometheus metrics.

Run: uvicorn app.api.server:app --port 8000
"""
from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel

from ..config import Settings
from ..events import Event
from ..metrics import WS_CLIENTS, WS_SENT
from ..runtime import Pipeline
from ..simulator import BASELINES, FAULT_KINDS
from ..storage.base import MemoryStore, Store

log = logging.getLogger(__name__)


class FaultIn(BaseModel):
    metric: Literal["temperature", "vibration", "pressure"]
    kind: Literal["spike", "drift", "stuck"]


def event_json(e: Event) -> dict[str, Any]:
    d = dataclasses.asdict(e)
    d["type"] = type(e).__name__.lower()
    return d


async def open_store(settings: Settings) -> Store:
    if not settings.database_url:
        log.info("DATABASE_URL not set: using in-memory store")
        return MemoryStore()
    from ..storage.postgres import PgStore

    for attempt in range(30):  # the database container may still be starting
        try:
            return await PgStore.connect(settings.database_url)
        except (OSError, ConnectionError) as e:
            log.warning("database not ready (%s), retrying (%d)", e, attempt + 1)
            await asyncio.sleep(1)
    raise RuntimeError("could not connect to database")


def create_app(settings: Settings | None = None, store: Store | None = None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        st = store or await open_store(settings)
        pipeline = Pipeline(settings, st)
        app.state.store, app.state.pipeline = st, pipeline
        await pipeline.start()
        try:
            yield
        finally:
            await pipeline.stop()
            await st.close()

    app = FastAPI(title="Telemetry Sentinel", version="0.3.0", lifespan=lifespan)

    def pipe(request: Request) -> Pipeline:
        p: Pipeline = request.app.state.pipeline
        return p

    def db(request: Request) -> Store:
        s: Store = request.app.state.store
        return s

    # ---------------- health & metrics ----------------
    @app.get("/api/health")
    async def health(request: Request) -> dict[str, Any]:
        st = db(request)
        db_ok = await st.ping()
        backend = "memory" if isinstance(st, MemoryStore) else (
            "timescaledb" if getattr(st, "timescale", False) else "postgres")
        return {"status": "ok" if db_ok else "degraded", "db": {"backend": backend, "ok": db_ok},
                **pipe(request).stats()}

    @app.get("/metrics")
    async def metrics() -> PlainTextResponse:
        return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    # ---------------- devices & readings ----------------
    @app.get("/api/devices")
    async def devices(request: Request) -> list[dict[str, Any]]:
        rows = await db(request).devices()
        fleet = pipe(request).fleet
        for r in rows:
            if r["device_id"] in fleet:
                r["actions"] = fleet[r["device_id"]].actions[-5:]
        return rows

    @app.get("/api/devices/{device_id}/readings")
    async def readings(request: Request, device_id: str, metric: str | None = None,
                       seconds: float = Query(60, gt=0, le=3600),
                       limit: int = Query(2000, gt=0, le=20_000)) -> list[dict[str, Any]]:
        return await db(request).recent_readings(device_id, metric, time.time() - seconds, limit)

    @app.get("/api/devices/{device_id}/history")
    async def history(request: Request, device_id: str,
                      minutes: int = Query(30, gt=0, le=24 * 60)) -> list[dict[str, Any]]:
        return await db(request).device_history(device_id, minutes)

    @app.get("/api/anomalies")
    async def anomalies(request: Request, limit: int = Query(100, gt=0, le=5000),
                        device_id: str | None = None) -> list[dict[str, Any]]:
        return await db(request).list_anomalies(limit, device_id)

    @app.post("/api/devices/{device_id}/faults", status_code=202)
    async def inject_fault(request: Request, device_id: str, body: FaultIn) -> dict[str, str]:
        """Demo hook: inject a fault so you can watch it flow through the system."""
        try:
            pipe(request).inject(device_id, body.metric, body.kind)
        except KeyError:
            raise HTTPException(404, f"unknown device {device_id}") from None
        return {"device_id": device_id, "metric": body.metric, "kind": body.kind}

    @app.get("/api/meta")
    async def meta(request: Request) -> dict[str, Any]:
        return {"metrics": {m: {"mean": mu, "std": sd} for m, (mu, sd) in BASELINES.items()},
                "fault_kinds": FAULT_KINDS, "detector": settings.detector,
                "devices": sorted(pipe(request).fleet.devices)}

    # ---------------- live stream ----------------
    @app.websocket("/ws")
    async def stream(ws: WebSocket, types: str = "reading,anomaly,incident,proposal,actionexecuted",
                     device: str | None = None) -> None:
        """Push events to one browser.

        Each client gets its own bounded bus subscription (drop-oldest), so a
        slow browser loses its own oldest events and can't slow the pipeline
        or other clients. Events are sent in batches every ~100 ms.
        """
        await ws.accept()
        p: Pipeline = ws.app.state.pipeline
        wanted = set(types.split(","))
        sub = p.bus.subscribe(Event, maxsize=5000, name="ws")
        WS_CLIENTS.inc()

        async def receive_until_closed() -> None:
            with contextlib.suppress(WebSocketDisconnect):
                while True:
                    await ws.receive_text()

        closed = asyncio.create_task(receive_until_closed())
        try:
            while not closed.done():
                batch = await sub.get_batch(1000, 0.1)
                if batch is None:
                    break
                out = [event_json(e) for e in batch
                       if type(e).__name__.lower() in wanted and (device is None or
                                                                 getattr(e, "device_id", None) == device)]
                if out:
                    await ws.send_text(json.dumps({"events": out, "dropped": sub.dropped}))
                    WS_SENT.inc(len(out))
        except (WebSocketDisconnect, RuntimeError):
            pass
        finally:
            closed.cancel()
            p.bus.unsubscribe(sub)
            WS_CLIENTS.dec()

    # ---------------- dashboard (production build) ----------------
    static = Path(settings.static_dir) if settings.static_dir else None
    if static and (static / "index.html").exists():
        app.mount("/assets", StaticFiles(directory=static / "assets"), name="assets")

        @app.get("/{path:path}", include_in_schema=False)
        async def spa(path: str) -> FileResponse:
            f = (static / path).resolve()
            if path and f.is_file() and f.is_relative_to(static.resolve()):
                return FileResponse(f)
            return FileResponse(static / "index.html")

    return app


app = create_app()
