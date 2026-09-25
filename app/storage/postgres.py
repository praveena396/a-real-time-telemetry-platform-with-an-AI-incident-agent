"""TimescaleDB (PostgreSQL) store using asyncpg.

If the timescaledb extension is available we get a hypertable, a one-minute
continuous aggregate and a 7-day retention policy. On plain PostgreSQL the
same schema works with an ordinary table and a view, so development and CI
don't depend on the extension.
"""
from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg

from ..events import Anomaly, Incident, Proposal, Reading
from .base import TRANSITIONS, InvalidTransitionError, NotFoundError

log = logging.getLogger(__name__)
SCHEMA = (Path(__file__).parent / "schema.sql").read_text()

TIMESCALE_SETUP = [
    "SELECT create_hypertable('readings', 'ts', if_not_exists => TRUE, migrate_data => TRUE)",
    "SELECT create_hypertable('anomalies', 'ts', if_not_exists => TRUE, migrate_data => TRUE)",
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS readings_1m
    WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
    SELECT time_bucket('1 minute', ts) AS bucket, device_id, metric,
           avg(value) AS avg, min(value) AS min, max(value) AS max, count(*) AS n
    FROM readings GROUP BY bucket, device_id, metric
    WITH NO DATA
    """,
    """SELECT add_continuous_aggregate_policy('readings_1m', start_offset => INTERVAL '2 hours',
       end_offset => INTERVAL '1 minute', schedule_interval => INTERVAL '1 minute', if_not_exists => TRUE)""",
    "SELECT add_retention_policy('readings', INTERVAL '7 days', if_not_exists => TRUE)",
    "SELECT add_retention_policy('anomalies', INTERVAL '30 days', if_not_exists => TRUE)",
]

PLAIN_ROLLUP = """
CREATE OR REPLACE VIEW readings_1m AS
SELECT date_trunc('minute', ts) AS bucket, device_id, metric,
       avg(value) AS avg, min(value) AS min, max(value) AS max, count(*) AS n
FROM readings GROUP BY 1, 2, 3
"""


def _dt(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, UTC)


def _epoch(v: datetime | None) -> float | None:
    return v.timestamp() if v else None


class PgStore:
    def __init__(self, pool: asyncpg.Pool, timescale: bool) -> None:
        self.pool = pool
        self.timescale = timescale

    @classmethod
    async def connect(cls, dsn: str, min_size: int = 2, max_size: int = 10) -> PgStore:
        async def init(conn: asyncpg.Connection) -> None:
            await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")

        pool = await asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size, init=init)
        async with pool.acquire() as conn:
            await conn.execute(SCHEMA)
            timescale = await cls._setup_timescale(conn)
        log.info("database ready (timescaledb=%s)", timescale)
        return cls(pool, timescale)

    @staticmethod
    async def _setup_timescale(conn: Any) -> bool:
        try:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
        except asyncpg.PostgresError:
            log.warning("timescaledb extension not available; using plain PostgreSQL tables")
            await conn.execute(PLAIN_ROLLUP)
            return False
        for stmt in TIMESCALE_SETUP:
            await conn.execute(stmt)
        return True

    async def close(self) -> None:
        await self.pool.close()

    # --- hot path: COPY is far faster than INSERT for batches ---
    async def write_readings(self, rows: list[Reading]) -> None:
        async with self.pool.acquire() as conn:
            await conn.copy_records_to_table(
                "readings", columns=["ts", "device_id", "metric", "value", "fault"],
                records=[(_dt(r.ts), r.device_id, r.metric, r.value, r.fault) for r in rows])

    async def write_anomalies(self, rows: list[Anomaly]) -> None:
        async with self.pool.acquire() as conn:
            await conn.copy_records_to_table(
                "anomalies",
                columns=["ts", "reading_ts", "device_id", "metric", "value", "score", "detector", "fault"],
                records=[(_dt(a.ts), _dt(a.reading_ts or a.ts), a.device_id, a.metric, a.value, a.score,
                          a.detector, a.fault) for a in rows])

    # --- queries ---
    async def recent_readings(self, device_id: str, metric: str | None = None,
                              since: float | None = None, limit: int = 500) -> list[dict[str, Any]]:
        rows = await self.pool.fetch(
            """SELECT extract(epoch FROM ts)::float8 AS ts, device_id, metric, value, fault FROM readings
               WHERE device_id = $1 AND ($2::text IS NULL OR metric = $2)
                 AND ts >= COALESCE($3::timestamptz, now() - INTERVAL '1 hour')
               ORDER BY ts DESC LIMIT $4""",
            device_id, metric, _dt(since) if since else None, limit)
        return [dict(r) for r in reversed(rows)]

    async def device_history(self, device_id: str, minutes: int = 30) -> list[dict[str, Any]]:
        rows = await self.pool.fetch(
            """SELECT extract(epoch FROM bucket)::float8 AS bucket, metric, avg, min, max, n
               FROM readings_1m WHERE device_id = $1 AND bucket >= now() - make_interval(mins => $2)
               ORDER BY bucket, metric""", device_id, minutes)
        return [dict(r) for r in rows]

    async def list_anomalies(self, limit: int = 100, device_id: str | None = None) -> list[dict[str, Any]]:
        rows = await self.pool.fetch(
            """SELECT extract(epoch FROM ts)::float8 AS ts, extract(epoch FROM reading_ts)::float8 AS reading_ts,
                      device_id, metric, value, score, detector, fault
               FROM anomalies WHERE ($1::text IS NULL OR device_id = $1)
               ORDER BY ts DESC LIMIT $2""", device_id, limit)
        return [dict(r) for r in rows]

    async def devices(self) -> list[dict[str, Any]]:
        rows = await self.pool.fetch(
            """SELECT DISTINCT ON (device_id, metric) device_id, metric, value,
                      extract(epoch FROM ts)::float8 AS ts
               FROM readings WHERE ts > now() - INTERVAL '5 minutes'
               ORDER BY device_id, metric, ts DESC""")
        counts = {r["device_id"]: r["n"] for r in await self.pool.fetch(
            """SELECT device_id, count(*) AS n FROM anomalies
               WHERE ts > now() - INTERVAL '5 minutes' GROUP BY device_id""")}
        out: dict[str, dict[str, Any]] = {}
        for r in rows:
            e = out.setdefault(r["device_id"], {"device_id": r["device_id"], "last_ts": 0.0, "latest": {},
                                                "recent_anomalies": counts.get(r["device_id"], 0)})
            e["latest"][r["metric"]] = r["value"]
            e["last_ts"] = max(e["last_ts"], r["ts"])
        return list(out.values())

    # --- incidents, proposals, audit ---
    async def save_incident(self, inc: Incident) -> None:
        await self.pool.execute(
            """INSERT INTO incidents (incident_id, device_id, started, ended, anomaly_count, metrics,
                                      max_score, truth, samples)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (incident_id) DO NOTHING""",
            inc.incident_id, inc.device_id, _dt(inc.started), _dt(inc.ended), inc.anomaly_count,
            list(inc.metrics), inc.max_score, inc.truth, [list(s) for s in inc.samples])

    @staticmethod
    def _incident(r: asyncpg.Record) -> dict[str, Any]:
        d = dict(r)
        d["started"], d["ended"] = _epoch(d["started"]), _epoch(d["ended"])
        d["ts"] = _epoch(d.pop("created_at"))
        return d

    async def get_incident(self, incident_id: str) -> dict[str, Any]:
        r = await self.pool.fetchrow("SELECT * FROM incidents WHERE incident_id = $1", incident_id)
        if r is None:
            raise NotFoundError(incident_id)
        return self._incident(r)

    async def list_incidents(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = await self.pool.fetch("SELECT * FROM incidents ORDER BY started DESC LIMIT $1", limit)
        return [self._incident(r) for r in rows]

    async def save_proposal(self, p: Proposal) -> None:
        await self.pool.execute(
            """INSERT INTO proposals (proposal_id, incident_id, device_id, diagnosis, cause, action, params,
                                      confidence, agent, status, created_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)""",
            p.proposal_id, p.incident_id, p.device_id, p.diagnosis, p.cause, p.action, p.params,
            p.confidence, p.agent, p.status, _dt(p.ts))

    @staticmethod
    def _proposal(r: asyncpg.Record) -> dict[str, Any]:
        d = dict(r)
        d["ts"] = _epoch(d.pop("created_at"))
        d["decided_at"] = _epoch(d["decided_at"])
        return d

    async def get_proposal(self, proposal_id: str) -> dict[str, Any]:
        r = await self.pool.fetchrow("SELECT * FROM proposals WHERE proposal_id = $1", proposal_id)
        if r is None:
            raise NotFoundError(proposal_id)
        return self._proposal(r)

    async def list_proposals(self, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        rows = await self.pool.fetch(
            """SELECT * FROM proposals WHERE ($1::text IS NULL OR status = $1)
               ORDER BY created_at DESC LIMIT $2""", status, limit)
        return [self._proposal(r) for r in rows]

    async def set_proposal_status(self, proposal_id: str, status: str, actor: str,
                                  note: str | None = None) -> dict[str, Any]:
        allowed_from = [s for s, nxt in TRANSITIONS.items() if status in nxt]
        # Conditional UPDATE makes the transition atomic: two people approving
        # at once can't both succeed.
        r = await self.pool.fetchrow(
            """UPDATE proposals SET status = $2, decided_at = now(), decided_by = $3, note = $4
               WHERE proposal_id = $1 AND status = ANY($5::text[]) RETURNING *""",
            proposal_id, status, actor, note, allowed_from)
        if r is None:
            current = await self.get_proposal(proposal_id)  # raises NotFoundError
            raise InvalidTransitionError(f"{current['status']} -> {status}")
        return self._proposal(r)

    async def audit(self, event: str, actor: str, proposal_id: str | None = None,
                    details: dict[str, Any] | None = None) -> None:
        await self.pool.execute(
            "INSERT INTO audit_log (event, actor, proposal_id, details) VALUES ($1, $2, $3, $4)",
            event, actor, proposal_id, details or {})

    async def list_audit(self, limit: int = 200) -> list[dict[str, Any]]:
        rows = await self.pool.fetch(
            """SELECT id, extract(epoch FROM ts)::float8 AS ts, event, actor, proposal_id, details
               FROM audit_log ORDER BY id DESC LIMIT $1""", limit)
        return [dict(r) for r in rows]

    async def rows_per_second_probe(self) -> float:
        """Used by the load test: rows written in the last 10 s."""
        n = await self.pool.fetchval("SELECT count(*) FROM readings WHERE ts > now() - INTERVAL '10 seconds'")
        return float(n) / 10.0

