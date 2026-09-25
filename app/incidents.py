"""Group anomalies into incidents so the agent sees one problem, not fifty alerts.

Anomalies on the same device are merged while they keep arriving. An incident
closes once the device has been quiet for `quiet_s` seconds, or is force-closed
at `max_s` seconds so a long fault still reaches the agent promptly.

Time comes from the anomaly (when its reading was produced) and from the
`now` passed to `sweep`, so the grouper is deterministic in offline replays.
"""
from __future__ import annotations

import asyncio
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field

from .bus import EventBus, Subscription
from .events import Anomaly, Incident
from .metrics import INCIDENTS

MAX_SAMPLES = 50


@dataclass
class _Open:
    device_id: str
    first: float
    last: float
    anomalies: list[Anomaly] = field(default_factory=list)


def _event_time(a: Anomaly) -> float:
    return a.reading_ts if a.reading_ts is not None else a.ts


class IncidentGrouper:
    def __init__(self, quiet_s: float = 5.0, max_s: float = 60.0) -> None:
        self.quiet_s = quiet_s
        self.max_s = max_s
        self.open: dict[str, _Open] = {}

    def add(self, a: Anomaly) -> list[Incident]:
        t = _event_time(a)
        closed = []
        cur = self.open.get(a.device_id)
        if cur and t - cur.last >= self.quiet_s:
            closed.append(self._close(cur))
            cur = None
        if cur is None:
            cur = self.open[a.device_id] = _Open(a.device_id, t, t)
        cur.anomalies.append(a)
        cur.last = max(cur.last, t)
        if cur.last - cur.first >= self.max_s:
            closed.append(self._close(cur))
        return closed

    def sweep(self, now: float) -> list[Incident]:
        return [self._close(o) for o in list(self.open.values()) if now - o.last >= self.quiet_s]

    def flush(self) -> list[Incident]:
        return [self._close(o) for o in list(self.open.values())]

    def _close(self, o: _Open) -> Incident:
        del self.open[o.device_id]
        labels = Counter(a.fault or "none" for a in o.anomalies)
        truth = labels.most_common(1)[0][0]
        # Keep the strongest anomalies as samples, in time order.
        top = sorted(o.anomalies, key=lambda a: abs(a.score), reverse=True)[:MAX_SAMPLES]
        samples = tuple((_event_time(a), a.metric, round(a.value, 4), round(a.score, 3))
                        for a in sorted(top, key=_event_time))
        return Incident(
            incident_id=f"inc-{o.device_id}-{int(o.first * 1000)}",
            device_id=o.device_id, started=o.first, ended=o.last, anomaly_count=len(o.anomalies),
            metrics=tuple(sorted({a.metric for a in o.anomalies})),
            max_score=max((abs(a.score) for a in o.anomalies), default=0.0),
            truth=None if truth == "none" else truth,
            labels=tuple(sorted(k for k in labels if k != "none")), samples=samples,
        )


async def run_grouper(bus: EventBus, anomalies: Subscription[Anomaly], grouper: IncidentGrouper,
                      on_incident: Callable[[Incident], object] | None = None,
                      clock: Callable[[], float] = time.time, tick: float = 0.5) -> None:
    def emit(incidents: list[Incident]) -> None:
        for inc in incidents:
            INCIDENTS.inc()
            if on_incident:
                on_incident(inc)
            if not bus.closed:
                bus.publish(inc)

    while True:
        batch = await anomalies.get_batch(500, tick)
        if batch is None:
            emit(grouper.flush())
            return
        for a in batch:
            emit(grouper.add(a))
        emit(grouper.sweep(clock()))
        await asyncio.sleep(0)
