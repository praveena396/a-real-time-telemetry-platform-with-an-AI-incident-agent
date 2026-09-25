"""Simulated devices emitting telemetry, with injectable ground-truth faults.

`DeviceModel` is a plain synchronous state machine so the exact same data can
be produced live (asyncio, `run_device`) or offline (`generate`) for
repeatable detector comparisons and agent evals.
"""
from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Iterator
from dataclasses import dataclass

from .bus import EventBus
from .events import Reading

BASELINES = {"temperature": (60.0, 1.5), "vibration": (2.0, 0.2), "pressure": (100.0, 2.0)}
FAULT_KINDS = ("spike", "drift", "stuck")


@dataclass
class FaultConfig:
    spike_prob: float = 0.004   # chance per reading of a sudden spike
    drift_prob: float = 0.001   # chance per reading to start a slow drift
    drift_len: int = 40         # readings a drift lasts
    drift_step: float = 0.4     # drift growth per reading, in standard deviations
    stuck_prob: float = 0.0     # chance per reading of a sensor freezing at one value
    stuck_len: int = 30


@dataclass
class _MetricState:
    drift_left: int = 0
    drift_offset: float = 0.0
    stuck_left: int = 0
    stuck_value: float = 0.0
    forced_spike: bool = False


class DeviceModel:
    def __init__(self, device_id: str, faults: FaultConfig, rng: random.Random) -> None:
        self.device_id = device_id
        self.faults = faults
        self.rng = rng
        self.state = {m: _MetricState() for m in BASELINES}
        self.actions: list[str] = []

    # --- fault injection (demo / tests) and remediation (agent actions) ---
    def inject(self, metric: str, kind: str) -> None:
        if metric not in self.state or kind not in FAULT_KINDS:
            raise ValueError(f"unknown metric/fault {metric}/{kind}")
        s = self.state[metric]
        mean, _ = BASELINES[metric]
        if kind == "spike":
            s.forced_spike = True
        elif kind == "drift":
            s.drift_left, s.drift_offset = self.faults.drift_len, 0.0
        else:
            s.stuck_left, s.stuck_value = self.faults.stuck_len, mean + self.rng.gauss(0, 0.1)

    def apply_action(self, action: str, metric: str | None = None) -> None:
        """Remediation actions clear the corresponding fault state."""
        self.actions.append(action)
        metrics = [metric] if metric in self.state else list(self.state)
        for m in metrics:
            s = self.state[m]
            if action in ("recalibrate_sensor", "restart_device"):
                s.drift_left, s.drift_offset = 0, 0.0
            if action == "restart_device":
                s.stuck_left = 0

    def active_faults(self) -> dict[str, str]:
        out = {}
        for m, s in self.state.items():
            if s.drift_left:
                out[m] = "drift"
            elif s.stuck_left:
                out[m] = "stuck"
        return out

    def step(self, ts: float) -> list[Reading]:
        f, rng, out = self.faults, self.rng, []
        for metric, (mean, std) in BASELINES.items():
            s = self.state[metric]
            value, fault = rng.gauss(mean, std), None
            if s.drift_left == 0 and s.stuck_left == 0:
                if rng.random() < f.drift_prob:
                    s.drift_left, s.drift_offset = f.drift_len, 0.0
                elif f.stuck_prob and rng.random() < f.stuck_prob:
                    s.stuck_left, s.stuck_value = f.stuck_len, value
            if s.drift_left > 0:
                s.drift_offset += std * f.drift_step  # grows until detectable
                value += s.drift_offset
                s.drift_left -= 1
                fault = "drift"
            elif s.stuck_left > 0:
                value = s.stuck_value
                s.stuck_left -= 1
                fault = "stuck"
            elif s.forced_spike or rng.random() < f.spike_prob:
                s.forced_spike = False
                value += std * rng.choice([-1, 1]) * rng.uniform(6, 10)
                fault = "spike"
            out.append(Reading(device_id=self.device_id, metric=metric, value=value, fault=fault, ts=ts))
        return out


class Fleet:
    """All simulated devices, addressable by id (used by the API and agent actions)."""

    def __init__(self, n: int, faults: FaultConfig, seed: int) -> None:
        rng = random.Random(seed)
        self.devices = {
            f"dev-{i:03d}": DeviceModel(f"dev-{i:03d}", faults, random.Random(rng.random()))
            for i in range(n)
        }

    def __getitem__(self, device_id: str) -> DeviceModel:
        return self.devices[device_id]

    def __contains__(self, device_id: object) -> bool:
        return device_id in self.devices


async def run_device(bus: EventBus, device: DeviceModel, rate_hz: float, stop: asyncio.Event) -> None:
    """Emit readings at `rate_hz`, scheduling against absolute time to avoid drift."""
    interval = 1.0 / rate_hz
    loop = asyncio.get_running_loop()
    next_t = loop.time()
    while not stop.is_set():
        for r in device.step(time.time()):
            bus.publish(r)
        next_t += interval
        delay = next_t - loop.time()
        if delay < -1.0:  # fell badly behind (overloaded): don't try to catch up in a burst
            next_t = loop.time()
            delay = 0
        await asyncio.sleep(max(delay, 0))


def generate(devices: int, steps: int, faults: FaultConfig, seed: int,
             rate_hz: float = 10.0, start_ts: float = 1_700_000_000.0) -> Iterator[Reading]:
    """Deterministic offline stream: `steps` ticks for each of `devices` devices."""
    fleet = Fleet(devices, faults, seed)
    for i in range(steps):
        ts = start_ts + i / rate_hz
        for d in fleet.devices.values():
            yield from d.step(ts)
