"""The live pipeline: simulated fleet -> bus -> detector -> writers (-> incidents -> agent).

```
Fleet ─► EventBus ─┬─► detector ─► Anomaly ─┬─► anomaly writer ─► DB
                   ├─► reading writer ─► DB  └─► incident grouper ─► Incident ─► agent ─► Proposal
                   └─► WebSocket clients (one bounded subscription each)
```
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from .agent.actions import ActionExecutor
from .agent.agent import IncidentAgent, run_agent
from .agent.diagnosers import Diagnoser, GeminiClient, GeminiDiagnoser, HeuristicDiagnoser
from .bus import EventBus
from .config import Settings
from .detector import run_detector
from .detectors import make_factory
from .events import Anomaly, Incident, Reading
from .incidents import IncidentGrouper, run_grouper
from .metrics import ANOMALIES, DETECTION_LATENCY, register_bus
from .simulator import FaultConfig, Fleet, run_device
from .storage.base import Store
from .storage.writer import BatchedWriter

log = logging.getLogger(__name__)


def _observe_anomaly(a: Anomaly) -> None:
    ANOMALIES.labels(a.detector, a.metric).inc()
    if a.reading_ts is not None:
        DETECTION_LATENCY.observe(max(0.0, time.time() - a.reading_ts))


class Pipeline:
    def __init__(self, settings: Settings, store: Store) -> None:
        self.settings = settings
        self.store = store
        self.bus = EventBus()
        k = settings.fault_scale
        self.fleet = Fleet(settings.devices, FaultConfig(spike_prob=0.004 * k, drift_prob=0.001 * k,
                                                         stuck_prob=0.0005 * k), settings.seed)
        self.agent: IncidentAgent | None = None
        self.executor = ActionExecutor(store, self.fleet, self.bus)
        self.stop_event = asyncio.Event()
        self.started_at = time.time()
        self.writers: list[BatchedWriter] = []
        self._device_tasks: list[asyncio.Task[None]] = []
        self._worker_tasks: list[asyncio.Task[Any]] = []

    async def start(self) -> None:
        s, bus = self.settings, self.bus
        register_bus(bus)
        detector_in = bus.subscribe(Reading, maxsize=20_000, name="detector")
        self.writers = [
            BatchedWriter("readings", bus.subscribe(Reading, maxsize=50_000, name="db-readings"),
                          self.store.write_readings, batch_size=s.batch_size, flush_interval=s.flush_interval_s),
            BatchedWriter("anomalies", bus.subscribe(Anomaly, maxsize=10_000, name="db-anomalies"),
                          self.store.write_anomalies, batch_size=s.batch_size, flush_interval=s.flush_interval_s),
        ]
        self._worker_tasks = [
            asyncio.create_task(run_detector(bus, detector_in, make_factory(s.detector), _observe_anomaly)),
            *(asyncio.create_task(w.run()) for w in self.writers),
            asyncio.create_task(run_grouper(bus, bus.subscribe(Anomaly, maxsize=10_000, name="grouper"),
                                            IncidentGrouper(s.incident_quiet_s, s.incident_max_s))),
        ]
        incidents = bus.subscribe(Incident, maxsize=1000, name="agent")
        if s.agent_enabled:
            budget = s.agent_max_per_min if s.gemini_api_key else None  # the heuristic costs nothing
            self.agent = IncidentAgent(self.store, self.make_diagnoser(), bus, budget)
            self._worker_tasks.append(asyncio.create_task(
                run_agent(self.agent, incidents, s.agent_max_concurrency)))
        else:
            self._worker_tasks.append(asyncio.create_task(self._save_incidents(incidents)))
        self._device_tasks = [asyncio.create_task(run_device(bus, d, s.rate_hz, self.stop_event))
                              for d in self.fleet.devices.values()]
        log.info("pipeline started: %d devices at %.1f Hz, detector=%s", s.devices, s.rate_hz, s.detector)

    def make_diagnoser(self) -> Diagnoser:
        s = self.settings
        if s.gemini_api_key:
            log.info("incident agent: Gemini (%s)", s.gemini_model)
            return GeminiDiagnoser(GeminiClient(s.gemini_api_key, s.gemini_model,
                                                max_rpm=s.gemini_rpm))
        log.info("incident agent: heuristic (set GEMINI_API_KEY to use the LLM)")
        return HeuristicDiagnoser()

    async def _save_incidents(self, incidents: Any) -> None:
        async for inc in incidents:
            await self.store.save_incident(inc)

    async def stop(self) -> None:
        self.stop_event.set()
        await asyncio.gather(*self._device_tasks, return_exceptions=True)
        await asyncio.sleep(0.1)  # let the detector drain
        self.bus.close()
        await asyncio.wait_for(asyncio.gather(*self._worker_tasks, return_exceptions=True), timeout=10)

    def inject(self, device_id: str, metric: str, kind: str) -> None:
        if device_id not in self.fleet:
            raise KeyError(device_id)
        self.fleet[device_id].inject(metric, kind)

    def stats(self) -> dict[str, Any]:
        return {
            "uptime_s": round(time.time() - self.started_at, 1),
            "published": self.bus.published,
            "subscribers": {s.name: {"depth": s.depth(), "dropped": s.dropped}
                            for s in self.bus.subscriptions if s.name != "ws"},
            "agent": self.agent.diagnoser.name if self.agent else None,
            "ws_clients": sum(1 for s in self.bus.subscriptions if s.name == "ws"),
            "writers": {w.name: {"written": w.rows_written, "buffered": len(w.buffer),
                                 "dropped": w.rows_dropped, "failures": w.failures} for w in self.writers},
        }
