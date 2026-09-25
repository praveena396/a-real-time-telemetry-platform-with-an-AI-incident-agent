"""Storage interface plus an in-memory implementation.

The API, the agent tools and the writer all talk to `Store`. `PgStore`
(TimescaleDB) is the real backend; `MemoryStore` keeps the app runnable with
no database and makes unit tests and agent evals fast and deterministic.
"""
from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import asdict
from typing import Any, Protocol

from ..events import Anomaly, Incident, Proposal, Reading


class NotFoundError(KeyError):
    pass


class InvalidTransitionError(ValueError):
    pass


def reading_row(r: Reading) -> dict[str, Any]:
    return {"ts": r.ts, "device_id": r.device_id, "metric": r.metric, "value": r.value, "fault": r.fault}


def anomaly_row(a: Anomaly) -> dict[str, Any]:
    return {"ts": a.ts, "reading_ts": a.reading_ts, "device_id": a.device_id, "metric": a.metric,
            "value": a.value, "score": a.score, "detector": a.detector, "fault": a.fault}


def incident_row(i: Incident) -> dict[str, Any]:
    d = asdict(i)
    d["metrics"] = list(i.metrics)
    d["labels"] = list(i.labels)
    d["samples"] = [list(s) for s in i.samples]
    return d


def proposal_row(p: Proposal) -> dict[str, Any]:
    d = asdict(p)
    d.setdefault("decided_at", None)
    d.setdefault("decided_by", None)
    d.setdefault("note", None)
    return d


# Allowed proposal state transitions (anything else is rejected).
TRANSITIONS = {
    "pending": {"approved", "rejected"},
    "approved": {"executed", "failed"},
}


class Store(Protocol):
    async def write_readings(self, rows: list[Reading]) -> None: ...
    async def write_anomalies(self, rows: list[Anomaly]) -> None: ...
    async def recent_readings(self, device_id: str, metric: str | None = None,
                              since: float | None = None, limit: int = 500) -> list[dict[str, Any]]: ...
    async def device_history(self, device_id: str, minutes: int = 30) -> list[dict[str, Any]]: ...
    async def list_anomalies(self, limit: int = 100, device_id: str | None = None) -> list[dict[str, Any]]: ...
    async def devices(self) -> list[dict[str, Any]]: ...
    async def save_incident(self, inc: Incident) -> None: ...
    async def get_incident(self, incident_id: str) -> dict[str, Any]: ...
    async def list_incidents(self, limit: int = 100) -> list[dict[str, Any]]: ...
    async def save_proposal(self, p: Proposal) -> None: ...
    async def get_proposal(self, proposal_id: str) -> dict[str, Any]: ...
    async def list_proposals(self, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]: ...
    async def set_proposal_status(self, proposal_id: str, status: str, actor: str,
                                  note: str | None = None) -> dict[str, Any]: ...
    async def audit(self, event: str, actor: str, proposal_id: str | None = None,
                    details: dict[str, Any] | None = None) -> None: ...
    async def list_audit(self, limit: int = 200) -> list[dict[str, Any]]: ...
    async def ping(self) -> bool: ...
    async def close(self) -> None: ...


class MemoryStore:
    def __init__(self, per_stream: int = 20_000, max_anomalies: int = 10_000) -> None:
        self.readings: dict[tuple[str, str], deque[dict[str, Any]]] = defaultdict(
            lambda: deque(maxlen=per_stream))
        self.anomalies: deque[dict[str, Any]] = deque(maxlen=max_anomalies)
        self.incidents: dict[str, dict[str, Any]] = {}
        self.proposals: dict[str, dict[str, Any]] = {}
        self.audit_log: list[dict[str, Any]] = []

    async def write_readings(self, rows: list[Reading]) -> None:
        for r in rows:
            self.readings[(r.device_id, r.metric)].append(reading_row(r))

    async def write_anomalies(self, rows: list[Anomaly]) -> None:
        self.anomalies.extend(anomaly_row(a) for a in rows)

    async def recent_readings(self, device_id: str, metric: str | None = None,
                              since: float | None = None, limit: int = 500) -> list[dict[str, Any]]:
        rows = [r for (d, m), q in self.readings.items() if d == device_id and metric in (None, m) for r in q]
        if since is not None:
            rows = [r for r in rows if r["ts"] >= since]
        rows.sort(key=lambda r: r["ts"])
        return rows[-limit:]

    async def device_history(self, device_id: str, minutes: int = 30) -> list[dict[str, Any]]:
        cutoff = time.time() - minutes * 60
        buckets: dict[tuple[float, str], list[float]] = defaultdict(list)
        for (d, m), q in self.readings.items():
            if d != device_id:
                continue
            for r in q:
                if r["ts"] >= cutoff:
                    buckets[(r["ts"] // 60 * 60, m)].append(r["value"])
        return [{"bucket": b, "metric": m, "avg": sum(v) / len(v), "min": min(v), "max": max(v), "n": len(v)}
                for (b, m), v in sorted(buckets.items())]

    async def list_anomalies(self, limit: int = 100, device_id: str | None = None) -> list[dict[str, Any]]:
        rows = [a for a in self.anomalies if device_id in (None, a["device_id"])]
        return list(reversed(rows[-limit:]))

    async def devices(self) -> list[dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for (d, m), q in self.readings.items():
            if not q:
                continue
            e = out.setdefault(d, {"device_id": d, "last_ts": 0.0, "latest": {}, "recent_anomalies": 0})
            e["latest"][m] = q[-1]["value"]
            e["last_ts"] = max(e["last_ts"], q[-1]["ts"])
        cutoff = time.time() - 300
        for a in self.anomalies:
            if a["ts"] >= cutoff and a["device_id"] in out:
                out[a["device_id"]]["recent_anomalies"] += 1
        return sorted(out.values(), key=lambda e: e["device_id"])

    async def save_incident(self, inc: Incident) -> None:
        self.incidents[inc.incident_id] = incident_row(inc)

    async def get_incident(self, incident_id: str) -> dict[str, Any]:
        try:
            return self.incidents[incident_id]
        except KeyError:
            raise NotFoundError(incident_id) from None

    async def list_incidents(self, limit: int = 100) -> list[dict[str, Any]]:
        return sorted(self.incidents.values(), key=lambda i: i["started"], reverse=True)[:limit]

    async def save_proposal(self, p: Proposal) -> None:
        self.proposals[p.proposal_id] = proposal_row(p)

    async def get_proposal(self, proposal_id: str) -> dict[str, Any]:
        try:
            return self.proposals[proposal_id]
        except KeyError:
            raise NotFoundError(proposal_id) from None

    async def list_proposals(self, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        rows = [p for p in self.proposals.values() if status in (None, p["status"])]
        return sorted(rows, key=lambda p: p["ts"], reverse=True)[:limit]

    async def set_proposal_status(self, proposal_id: str, status: str, actor: str,
                                  note: str | None = None) -> dict[str, Any]:
        p = await self.get_proposal(proposal_id)
        if status not in TRANSITIONS.get(p["status"], set()):
            raise InvalidTransitionError(f"{p['status']} -> {status}")
        p.update(status=status, decided_at=time.time(), decided_by=actor, note=note)
        return p

    async def audit(self, event: str, actor: str, proposal_id: str | None = None,
                    details: dict[str, Any] | None = None) -> None:
        self.audit_log.append({"id": len(self.audit_log) + 1, "ts": time.time(), "event": event,
                               "actor": actor, "proposal_id": proposal_id, "details": details or {}})

    async def list_audit(self, limit: int = 200) -> list[dict[str, Any]]:
        return list(reversed(self.audit_log[-limit:]))

    async def ping(self) -> bool:
        return True

    async def close(self) -> None:
        return None
