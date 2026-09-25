"""MCP server exposing the incident-investigation tools to any MCP client
(Claude Desktop, Claude Code, an MCP-enabled Gemini agent, ...).

    DATABASE_URL=postgresql://postgres:postgres@localhost:5432/telemetry \\
        python -m app.agent.mcp_server            # stdio transport

The same guardrails apply as for the built-in agent: `propose_action`
validates the proposal and stores it as *pending*. There is deliberately no
tool that approves or executes anything; that only happens through a human
in the dashboard.
"""
from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any

from mcp.server.mcpserver import MCPServer

from ..events import Proposal
from ..storage.base import NotFoundError, Store
from .guardrails import NO_OP_ACTIONS, validate_proposal
from .tools import AgentTools

mcp = MCPServer(
    name="telemetry-sentinel",
    instructions="Investigate telemetry incidents. Read with get_incident / query_readings / "
                 "get_device_history, then submit exactly one propose_action. Proposals need human approval.",
)

_store: Store | None = None
_lock = asyncio.Lock()


async def get_store() -> Store:
    global _store
    async with _lock:
        if _store is None:
            dsn = os.environ.get("DATABASE_URL")
            if not dsn:
                raise RuntimeError("DATABASE_URL is not set")
            from ..storage.postgres import PgStore
            _store = await PgStore.connect(dsn, min_size=1, max_size=4)
    return _store


def set_store(store: Store) -> None:
    """For tests and embedding: use an existing store."""
    global _store
    _store = store


@mcp.tool()
async def list_incidents(limit: int = 20) -> list[dict[str, Any]]:
    """Most recent incidents (newest first)."""
    rows = await (await get_store()).list_incidents(max(1, min(limit, 200)))
    return [{k: v for k, v in r.items() if k not in ("truth", "labels", "samples")} for r in rows]


@mcp.tool()
async def get_incident(incident_id: str) -> dict[str, Any]:
    """Incident details, normal operating ranges and the allowed actions."""
    try:
        return await AgentTools(await get_store()).get_incident(incident_id)
    except NotFoundError:
        return {"error": f"incident {incident_id} not found"}


@mcp.tool()
async def query_readings(device_id: str, metric: str, start: float, end: float,
                         max_points: int = 40) -> dict[str, Any]:
    """Summary statistics (mean, std, slope, longest flat run) and sampled points for one metric
    between two epoch timestamps."""
    return await AgentTools(await get_store()).query_readings(device_id, metric, start, end, max_points)


@mcp.tool()
async def get_device_history(device_id: str, minutes: int = 30) -> dict[str, Any]:
    """One-minute rollups for a device."""
    return await AgentTools(await get_store()).get_device_history(device_id, minutes)


@mcp.tool()
async def propose_action(incident_id: str, diagnosis: str, cause: str, action: str,
                         params: dict[str, Any], confidence: float) -> dict[str, Any]:
    """Submit a diagnosis and ONE action for human approval. Nothing is executed automatically."""
    store = await get_store()
    try:
        incident = await store.get_incident(incident_id)
    except NotFoundError:
        return {"accepted": False, "errors": [f"incident {incident_id} not found"]}
    raw = {"incident_id": incident_id, "diagnosis": diagnosis, "cause": cause, "action": action,
           "params": params, "confidence": confidence}
    valid, errors = validate_proposal(raw, incident)
    if valid is None:
        await store.audit("proposal_rejected_validation", "mcp", None,
                          {"incident_id": incident_id, "errors": errors})
        return {"accepted": False, "errors": errors}
    p = Proposal(proposal_id=f"prop-{uuid.uuid4().hex[:12]}", incident_id=incident_id,
                 device_id=incident["device_id"], diagnosis=valid.diagnosis, cause=valid.cause,
                 action=valid.action, params=valid.params, confidence=valid.confidence, agent="mcp",
                 status="auto_closed" if valid.action in NO_OP_ACTIONS else "pending")
    await store.save_proposal(p)
    await store.audit("proposal_created", "mcp", p.proposal_id,
                      {"incident_id": incident_id, "diagnosis": p.diagnosis, "action": p.action})
    return {"accepted": True, "proposal_id": p.proposal_id,
            "status": "pending human approval" if p.status == "pending" else "auto-closed (no-op action)"}


if __name__ == "__main__":
    mcp.run("stdio")
