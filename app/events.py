"""Typed, immutable events that flow through the bus."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Event:
    ts: float = field(default_factory=time.time, kw_only=True)


@dataclass(frozen=True)
class Reading(Event):
    device_id: str
    metric: str
    value: float
    fault: str | None = None  # ground-truth label from the simulator; None = normal


@dataclass(frozen=True)
class Anomaly(Event):
    device_id: str
    metric: str
    value: float
    score: float
    fault: str | None  # carried through so we can score the detector honestly
    detector: str = "zscore"
    reading_ts: float | None = None  # when the triggering reading was produced


@dataclass(frozen=True)
class Incident(Event):
    """A group of anomalies on one device that close together in time."""

    incident_id: str
    device_id: str
    started: float
    ended: float
    anomaly_count: int
    metrics: tuple[str, ...]
    max_score: float
    # Majority ground-truth label of the grouped anomalies. Used only for
    # evaluation; the agent never sees it.
    truth: str | None = None
    labels: tuple[str, ...] = ()  # every ground-truth fault kind present (evaluation only)
    samples: tuple[tuple[float, str, float, float], ...] = ()  # (ts, metric, value, score)


@dataclass(frozen=True)
class Proposal(Event):
    """An action the incident agent proposes. Nothing runs until a human approves."""

    proposal_id: str
    incident_id: str
    device_id: str
    diagnosis: str
    cause: str
    action: str
    params: dict[str, Any]
    confidence: float
    agent: str
    status: str = "pending"


@dataclass(frozen=True)
class ActionExecuted(Event):
    proposal_id: str
    device_id: str
    action: str
    params: dict[str, Any]


@dataclass(frozen=True)
class ProposalUpdated(Event):
    proposal_id: str
    device_id: str
    status: str
    actor: str
