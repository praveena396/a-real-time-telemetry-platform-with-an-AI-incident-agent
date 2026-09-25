"""Read-only investigation tools shared by the in-process agent and the MCP server.

Tools return compact summaries instead of raw rows: an LLM reasons better
over "slope 0.4/s, flat for 0 readings" plus ~40 sampled points than over
1,000 floats, and it costs a fraction of the tokens.

Ground-truth fault labels are stripped here. The agent never sees them.
"""
from __future__ import annotations

import itertools
import math
import time
from typing import Any

from ..simulator import BASELINES
from ..storage.base import Store
from .guardrails import ALLOWED_ACTIONS


def summarize(rows: list[dict[str, Any]], max_points: int = 40) -> dict[str, Any]:
    if not rows:
        return {"count": 0}
    vals = [r["value"] for r in rows]
    ts = [r["ts"] for r in rows]
    n = len(vals)
    mean = sum(vals) / n
    std = math.sqrt(sum((v - mean) ** 2 for v in vals) / max(n - 1, 1))
    t0 = ts[0]
    # least-squares slope, value units per second
    tm = sum(t - t0 for t in ts) / n
    den = sum((t - t0 - tm) ** 2 for t in ts)
    slope = sum((t - t0 - tm) * (v - mean) for t, v in zip(ts, vals, strict=True)) / den if den else 0.0
    flat = run = 1
    for a, b in itertools.pairwise(vals):
        run = run + 1 if abs(a - b) < 1e-9 else 1
        flat = max(flat, run)
    step = max(1, math.ceil(n / max_points))
    return {
        "count": n, "start": round(t0, 3), "end": round(ts[-1], 3),
        "mean": round(mean, 4), "std": round(std, 4), "min": round(min(vals), 4), "max": round(max(vals), 4),
        "slope_per_s": round(slope, 4), "longest_flat_run": flat,
        "points": [[round(t - t0, 2), round(v, 4)] for t, v in zip(ts[::step], vals[::step], strict=True)],
    }


class AgentTools:
    def __init__(self, store: Store) -> None:
        self.store = store

    async def get_incident(self, incident_id: str) -> dict[str, Any]:
        inc = await self.store.get_incident(incident_id)
        return {k: v for k, v in inc.items() if k not in ("truth", "labels")} | {
            "normal_ranges": {m: {"mean": mu, "std": sd} for m, (mu, sd) in BASELINES.items()},
            "allowed_actions": {a: sorted(p) for a, p in ALLOWED_ACTIONS.items()},
        }

    async def query_readings(self, device_id: str, metric: str, start: float | None = None,
                             end: float | None = None, max_points: int = 40) -> dict[str, Any]:
        if metric not in BASELINES:
            return {"error": f"unknown metric {metric!r}; choose from {sorted(BASELINES)}"}
        end = end if end is not None else time.time()
        start = start if start is not None else end - 60
        if end - start > 3600:
            return {"error": "window too large (max 3600 s)"}
        rows = await self.store.recent_readings(device_id, metric, since=start, limit=20_000)
        rows = [r for r in rows if r["ts"] <= end]
        return {"device_id": device_id, "metric": metric,
                **summarize(rows, max(5, min(int(max_points), 200)))}

    async def get_device_history(self, device_id: str, minutes: int = 30) -> dict[str, Any]:
        rows = await self.store.device_history(device_id, max(1, min(int(minutes), 24 * 60)))
        return {"device_id": device_id, "buckets": [
            {k: (round(v, 4) if isinstance(v, float) else v) for k, v in r.items()} for r in rows]}


# JSON-schema tool declarations (Gemini function calling format).
TOOL_DECLARATIONS: list[dict[str, Any]] = [
    {"name": "get_incident",
     "description": "Get an incident: device, time window, metrics involved, strongest anomaly samples, "
                    "normal operating ranges and the allowed actions.",
     "parameters": {"type": "object", "properties": {"incident_id": {"type": "string"}},
                    "required": ["incident_id"]}},
    {"name": "query_readings",
     "description": "Summarize one device metric over a time window (epoch seconds): count, mean, std, "
                    "min, max, least-squares slope per second, longest run of identical values, and "
                    "sampled [seconds_from_start, value] points.",
     "parameters": {"type": "object", "properties": {
         "device_id": {"type": "string"},
         "metric": {"type": "string", "enum": sorted(BASELINES)},
         "start": {"type": "number"}, "end": {"type": "number"},
         "max_points": {"type": "integer"}},
         "required": ["device_id", "metric", "start", "end"]}},
    {"name": "get_device_history",
     "description": "One-minute rollups (avg/min/max/count) per metric for the last N minutes.",
     "parameters": {"type": "object", "properties": {
         "device_id": {"type": "string"}, "minutes": {"type": "integer"}}, "required": ["device_id"]}},
    {"name": "propose_action",
     "description": "Submit your final diagnosis and ONE proposed action. It is validated and queued for "
                    "human approval; nothing runs automatically. If it is rejected you get the errors "
                    "and may fix and resubmit.",
     "parameters": {"type": "object", "properties": {
         "incident_id": {"type": "string"},
         "diagnosis": {"type": "string", "enum": ["spike", "drift", "stuck", "noise", "unknown"]},
         "cause": {"type": "string", "description": "One or two sentences of evidence-based reasoning."},
         "action": {"type": "string", "enum": sorted(ALLOWED_ACTIONS)},
         "params": {"type": "object", "properties": {"metric": {"type": "string"}}},
         "confidence": {"type": "number"}},
         "required": ["incident_id", "diagnosis", "cause", "action", "params", "confidence"]}},
]
