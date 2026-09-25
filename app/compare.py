"""Compare detectors on the same seeded, labeled data.

Usage: python -m app.compare --devices 20 --steps 3000 --seed 7 [--markdown]

Metrics per detector:
- precision        : flagged readings that were really faulty
- fp_per_10k       : false alarms per 10,000 normal readings
- recall[kind]     : faulty readings of that kind that were flagged
- episodes[kind]   : fault episodes (one injected fault) flagged at least once
- drift_delay      : median readings from drift start to first flag
"""
from __future__ import annotations

import argparse
import statistics
from collections import defaultdict
from dataclasses import dataclass, field

from .detectors import DETECTORS, Detector
from .simulator import FAULT_KINDS, FaultConfig, generate


@dataclass
class Score:
    flagged: int = 0
    true_flagged: int = 0
    normal: int = 0
    faulty: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    caught: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    episodes: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    episodes_caught: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    drift_delays: list[int] = field(default_factory=list)

    def summary(self) -> dict[str, float | None]:
        out: dict[str, float | None] = {
            "precision": round(self.true_flagged / max(self.flagged, 1), 3),
            "fp_per_10k": round((self.flagged - self.true_flagged) / max(self.normal, 1) * 10_000, 2),
        }
        for k in FAULT_KINDS:
            out[f"recall[{k}]"] = round(self.caught[k] / self.faulty[k], 3) if self.faulty[k] else None
            out[f"episodes[{k}]"] = (round(self.episodes_caught[k] / self.episodes[k], 3)
                                     if self.episodes[k] else None)
        out["drift_delay"] = statistics.median(self.drift_delays) if self.drift_delays else None
        return out


def evaluate(name: str, devices: int, steps: int, seed: int, faults: FaultConfig) -> Score:
    factory = DETECTORS[name]
    dets: dict[tuple[str, str], Detector] = {}
    sc = Score()
    # per-stream episode tracking: (kind, position within episode, caught yet?)
    ep: dict[tuple[str, str], list] = {}
    for r in generate(devices, steps, faults, seed):
        key = (r.device_id, r.metric)
        det = dets.get(key) or dets.setdefault(key, factory())
        hit = det.observe(r.value) is not None
        cur = ep.get(key)
        if r.fault and (cur is None or cur[0] != r.fault or r.fault == "spike"):
            cur = ep[key] = [r.fault, 0, False]
            sc.episodes[r.fault] += 1
        elif not r.fault:
            ep.pop(key, None)
            cur = None
        if r.fault:
            sc.faulty[r.fault] += 1
            assert cur is not None
            if hit:
                sc.caught[r.fault] += 1
                if not cur[2]:
                    cur[2] = True
                    sc.episodes_caught[r.fault] += 1
                    if r.fault == "drift":
                        sc.drift_delays.append(cur[1])
            cur[1] += 1
        else:
            sc.normal += 1
        if hit:
            sc.flagged += 1
            sc.true_flagged += bool(r.fault)
    return sc


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--devices", type=int, default=20)
    p.add_argument("--steps", type=int, default=3000, help="readings per metric per device")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--detectors", default=",".join(DETECTORS))
    p.add_argument("--stuck-prob", type=float, default=0.0005)
    p.add_argument("--markdown", action="store_true")
    a = p.parse_args()
    faults = FaultConfig(stuck_prob=a.stuck_prob)
    rows = {n: evaluate(n, a.devices, a.steps, a.seed, faults).summary() for n in a.detectors.split(",")}
    cols = list(next(iter(rows.values())))
    if a.markdown:
        print("| detector | " + " | ".join(cols) + " |")
        print("|---" * (len(cols) + 1) + "|")
        for n, r in rows.items():
            print(f"| {n} | " + " | ".join("-" if r[c] is None else str(r[c]) for c in cols) + " |")
    else:
        print(f"{'detector':>10} " + " ".join(f"{c:>17}" for c in cols))
        for n, r in rows.items():
            print(f"{n:>10} " + " ".join(f"{'-' if r[c] is None else r[c]!s:>17}" for c in cols))


if __name__ == "__main__":
    main()
