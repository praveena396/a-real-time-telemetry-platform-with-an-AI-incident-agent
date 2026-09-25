"""Incident agent: incident -> diagnosis -> validated, pending proposal.

The agent can only *propose*. Proposals are stored as `pending` and wait for
a human in the dashboard (see actions.py). Every step is written to the audit log.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid

from ..bus import EventBus, Subscription
from ..events import Incident, Proposal
from ..metrics import AGENT_LATENCY, PROPOSALS
from ..storage.base import Store
from .diagnosers import Diagnoser
from .guardrails import validate_proposal
from .tools import AgentTools

log = logging.getLogger(__name__)


class IncidentAgent:
    def __init__(self, store: Store, diagnoser: Diagnoser, bus: EventBus | None = None,
                 max_per_min: int | None = None) -> None:
        self.store = store
        self.tools = AgentTools(store)
        self.diagnoser = diagnoser
        self.bus = bus
        self.max_per_min = max_per_min
        self._recent: list[float] = []

    def _within_budget(self) -> bool:
        if not self.max_per_min:
            return True
        now = time.monotonic()
        self._recent = [t for t in self._recent if now - t < 60]
        if len(self._recent) >= self.max_per_min:
            return False
        self._recent.append(now)
        return True

    async def handle(self, inc: Incident) -> Proposal | None:
        t0 = time.perf_counter()
        await self.store.save_incident(inc)
        pending = [p for p in await self.store.list_proposals("pending", limit=1000)
                   if p["device_id"] == inc.device_id]
        if pending:
            # One open proposal per device: don't flood the approver.
            await self.store.audit("incident_skipped", "agent", pending[0]["proposal_id"],
                                   {"incident_id": inc.incident_id, "reason": "device has a pending proposal"})
            PROPOSALS.labels("skipped").inc()
            return None
        if not self._within_budget():
            await self.store.audit("incident_skipped", "agent", None,
                                   {"incident_id": inc.incident_id, "reason": "rate limit"})
            PROPOSALS.labels("rate_limited").inc()
            return None
        try:
            result = await self.diagnoser.diagnose(inc.incident_id, self.tools)
        except Exception as e:  # an LLM/API failure must not kill the agent loop
            log.exception("diagnoser failed for %s", inc.incident_id)
            await self.store.audit("agent_error", self.diagnoser.name, None,
                                   {"incident_id": inc.incident_id, "error": f"{type(e).__name__}: {e}"})
            PROPOSALS.labels("error").inc()
            return None
        if result.rejected:
            PROPOSALS.labels("invalid").inc(result.rejected)
            await self.store.audit("proposal_rejected_validation", self.diagnoser.name, None,
                                   {"incident_id": inc.incident_id, "errors": result.errors,
                                    "count": result.rejected})
        # Defense in depth: validate again, whatever the diagnoser claims.
        incident_row = await self.store.get_incident(inc.incident_id)
        valid, errors = validate_proposal(result.raw, incident_row) if result.raw else (None, result.errors)
        if valid is None:
            PROPOSALS.labels("no_proposal").inc()
            await self.store.audit("no_proposal", self.diagnoser.name, None,
                                   {"incident_id": inc.incident_id, "errors": errors})
            return None
        p = Proposal(proposal_id=f"prop-{uuid.uuid4().hex[:12]}", incident_id=inc.incident_id,
                     device_id=inc.device_id, diagnosis=valid.diagnosis, cause=valid.cause,
                     action=valid.action, params=valid.params, confidence=valid.confidence,
                     agent=self.diagnoser.name)
        await self.store.save_proposal(p)
        await self.store.audit("proposal_created", self.diagnoser.name, p.proposal_id,
                               {"incident_id": inc.incident_id, "diagnosis": p.diagnosis, "action": p.action,
                                "params": p.params, "confidence": p.confidence,
                                "tool_calls": result.tool_calls})
        PROPOSALS.labels("created").inc()
        AGENT_LATENCY.observe(time.perf_counter() - t0)
        if self.bus and not self.bus.closed:
            self.bus.publish(p)
        return p


async def run_agent(agent: IncidentAgent, incidents: Subscription[Incident], concurrency: int = 2) -> None:
    sem = asyncio.Semaphore(concurrency)
    running: set[asyncio.Task[Proposal | None]] = set()

    async def one(inc: Incident) -> Proposal | None:
        async with sem:
            return await agent.handle(inc)

    async for inc in incidents:
        t = asyncio.create_task(one(inc))
        running.add(t)
        t.add_done_callback(running.discard)
    if running:
        await asyncio.gather(*running, return_exceptions=True)
