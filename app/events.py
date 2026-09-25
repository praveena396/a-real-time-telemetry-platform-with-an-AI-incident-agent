"""Typed, immutable events that flow through the bus."""
from __future__ import annotations

import time
from dataclasses import dataclass, field


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
    zscore: float
    fault: str | None  # carried through so we can score the detector honestly
