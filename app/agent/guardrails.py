"""Guardrails between the LLM and anything that can change the system.

1. Schema validation: every proposal must parse into `ProposalIn` (types,
   ranges, lengths, no extra fields).
2. Allowlist: the action must be one of ALLOWED_ACTIONS, with exactly the
   parameters that action accepts, and the parameter values must be valid.
3. Consistency: the proposal must be for the incident's own device, and any
   metric it names must be one that incident actually involved.
4. Human approval: a valid proposal is only ever stored as `pending`.
   Execution happens in `actions.py`, after a person approves it, and
   re-checks the allowlist first.
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from ..simulator import BASELINES

Diagnosis = Literal["spike", "drift", "stuck", "noise", "unknown"]

# action -> allowed parameter names
ALLOWED_ACTIONS: dict[str, set[str]] = {
    "recalibrate_sensor": {"metric"},    # re-zeroes a drifting sensor
    "restart_device": set(),              # clears a frozen (stuck) sensor
    "increase_monitoring": {"metric"},   # watch closely; changes nothing
    "schedule_maintenance": set(),        # creates a ticket; changes nothing
    "no_action": set(),                   # transient event, nothing to do
}

# What a correct response to each ground-truth fault looks like (used by evals).
EXPECTED_ACTIONS: dict[str, set[str]] = {
    "spike": {"no_action", "increase_monitoring"},
    "drift": {"recalibrate_sensor"},
    "stuck": {"restart_device"},
    "noise": {"no_action", "increase_monitoring"},
}


class ProposalIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    incident_id: str = Field(min_length=1, max_length=100)
    diagnosis: Diagnosis
    cause: str = Field(min_length=3, max_length=500)
    action: str = Field(min_length=1, max_length=50)
    params: dict[str, Any] = Field(default_factory=dict)
    confidence: float = Field(ge=0.0, le=1.0)


def validate_proposal(raw: Any, incident: dict[str, Any]) -> tuple[ProposalIn | None, list[str]]:
    """Returns (proposal, []) when valid, else (None, errors). Never raises."""
    try:
        p = ProposalIn.model_validate(raw)
    except ValidationError as e:
        return None, [f"schema: {'.'.join(map(str, err['loc']))}: {err['msg']}" for err in e.errors()]
    errors = check_allowlist(p.action, p.params)
    if p.incident_id != incident["incident_id"]:
        errors.append(f"incident_id {p.incident_id!r} does not match {incident['incident_id']!r}")
    metric = p.params.get("metric")
    if metric is not None and metric not in incident["metrics"]:
        errors.append(f"metric {metric!r} is not part of this incident ({', '.join(incident['metrics'])})")
    return (None, errors) if errors else (p, [])


def check_allowlist(action: str, params: dict[str, Any]) -> list[str]:
    if action not in ALLOWED_ACTIONS:
        return [f"action {action!r} is not allowed; allowed: {sorted(ALLOWED_ACTIONS)}"]
    errors = []
    allowed = ALLOWED_ACTIONS[action]
    extra = set(params) - allowed
    if extra:
        errors.append(f"unexpected params for {action}: {sorted(extra)}")
    missing = allowed - set(params)
    if missing:
        errors.append(f"missing params for {action}: {sorted(missing)}")
    if "metric" in params and params["metric"] not in BASELINES:
        errors.append(f"unknown metric {params['metric']!r}")
    return errors
