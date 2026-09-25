"""Human approval and execution of agent proposals.

Approve -> re-check the allowlist -> execute against the device -> audit.
Reject  -> audit. State changes are atomic in the store, so a proposal can't
be approved twice or approved and rejected concurrently.
"""
from __future__ import annotations

import logging
from typing import Any

from ..bus import EventBus
from ..events import ActionExecuted, ProposalUpdated
from ..metrics import PROPOSALS
from ..simulator import Fleet
from ..storage.base import Store
from .guardrails import check_allowlist

log = logging.getLogger(__name__)


class ActionExecutor:
    def __init__(self, store: Store, fleet: Fleet, bus: EventBus | None = None) -> None:
        self.store = store
        self.fleet = fleet
        self.bus = bus

    def _publish(self, event: Any) -> None:
        if self.bus and not self.bus.closed:
            self.bus.publish(event)

    async def approve(self, proposal_id: str, actor: str, note: str | None = None) -> dict[str, Any]:
        p = await self.store.set_proposal_status(proposal_id, "approved", actor, note)
        await self.store.audit("proposal_approved", actor, proposal_id, {"note": note})
        PROPOSALS.labels("approved").inc()
        self._publish(ProposalUpdated(proposal_id=proposal_id, device_id=p["device_id"],
                                      status="approved", actor=actor))
        errors = check_allowlist(p["action"], p["params"])
        try:
            if errors:
                raise ValueError("; ".join(errors))
            if p["device_id"] not in self.fleet:
                raise KeyError(f"unknown device {p['device_id']}")
            self.fleet[p["device_id"]].apply_action(p["action"], p["params"].get("metric"))
        except (ValueError, KeyError) as e:
            p = await self.store.set_proposal_status(proposal_id, "failed", actor, str(e))
            await self.store.audit("action_failed", "system", proposal_id, {"error": str(e)})
            self._publish(ProposalUpdated(proposal_id=proposal_id, device_id=p["device_id"],
                                          status="failed", actor="system"))
            return p
        p = await self.store.set_proposal_status(proposal_id, "executed", actor, note)
        await self.store.audit("action_executed", "system", proposal_id,
                               {"action": p["action"], "params": p["params"], "device_id": p["device_id"]})
        PROPOSALS.labels("executed").inc()
        self._publish(ActionExecuted(proposal_id=proposal_id, device_id=p["device_id"], action=p["action"],
                                     params=p["params"]))
        self._publish(ProposalUpdated(proposal_id=proposal_id, device_id=p["device_id"],
                                      status="executed", actor="system"))
        return p

    async def reject(self, proposal_id: str, actor: str, note: str | None = None) -> dict[str, Any]:
        p = await self.store.set_proposal_status(proposal_id, "rejected", actor, note)
        await self.store.audit("proposal_rejected", actor, proposal_id, {"note": note})
        PROPOSALS.labels("rejected").inc()
        self._publish(ProposalUpdated(proposal_id=proposal_id, device_id=p["device_id"],
                                      status="rejected", actor=actor))
        return p
