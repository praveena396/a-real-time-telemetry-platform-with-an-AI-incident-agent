"""Simulated devices emitting telemetry, with injectable ground-truth faults."""
from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass

from .bus import EventBus
from .events import Reading

BASELINES = {"temperature": (60.0, 1.5), "vibration": (2.0, 0.2), "pressure": (100.0, 2.0)}


@dataclass
class FaultConfig:
    spike_prob: float = 0.004   # chance per reading of a sudden spike
    drift_prob: float = 0.001   # chance per reading to start a slow drift
    drift_len: int = 40         # readings a drift lasts


async def run_device(bus: EventBus, device_id: str, rate_hz: float, stop: asyncio.Event,
                     faults: FaultConfig, rng: random.Random) -> None:
    drift_left = {m: 0 for m in BASELINES}
    drift_offset = {m: 0.0 for m in BASELINES}
    interval = 1.0 / rate_hz
    while not stop.is_set():
        for metric, (mean, std) in BASELINES.items():
            value, fault = rng.gauss(mean, std), None
            if drift_left[metric] == 0 and rng.random() < faults.drift_prob:
                drift_left[metric] = faults.drift_len
                drift_offset[metric] = 0.0
            if drift_left[metric] > 0:
                drift_offset[metric] += std * 0.4  # grows until detectable
                value += drift_offset[metric]
                drift_left[metric] -= 1
                fault = "drift"
            elif rng.random() < faults.spike_prob:
                value += std * rng.choice([-1, 1]) * rng.uniform(6, 10)
                fault = "spike"
            bus.publish(Reading(device_id=device_id, metric=metric, value=value, fault=fault))
        await asyncio.sleep(interval)
