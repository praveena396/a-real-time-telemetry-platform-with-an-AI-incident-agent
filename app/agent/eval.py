"""Agent eval harness: record incidents with known ground truth, replay them through a diagnoser.

Record (deterministic, seeded):
    python -m app.agent.eval record --out evals/incidents.jsonl.gz --devices 8 --steps 6000
Replay:
    python -m app.agent.eval run --file evals/incidents.jsonl.gz --agent heuristic
    GEMINI_API_KEY=... python -m app.agent.eval run --agent gemini --limit 40

Each record holds one incident (with its majority ground-truth label) plus
the raw readings around it, minus fault labels. The replay loads them into
a fresh MemoryStore so the agent investigates with the same tools it uses live.
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import os
import statistics
import time
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any

from ..detectors import DETECTORS, Detector
from ..events import Anomaly, Incident, Reading
from ..incidents import IncidentGrouper
from ..simulator import FaultConfig, generate
from ..storage.base import MemoryStore, incident_row
from .diagnosers import DEFAULT_GEMINI_MODEL, Diagnoser, GeminiClient, GeminiDiagnoser, HeuristicDiagnoser
from .guardrails import EXPECTED_ACTIONS, validate_proposal
from .tools import AgentTools

METRICS = ("temperature", "vibration", "pressure")


# Faults at half the benchmark rate: fewer overlapping faults on one device,
# so most incidents have one clear ground truth.
EVAL_FAULTS = FaultConfig(spike_prob=0.002, drift_prob=0.0005, stuck_prob=0.00025)


def _open(path: Path, mode: str) -> Any:
    return gzip.open(path, mode + "t") if path.suffix == ".gz" else path.open(mode)


def record(out: Path, devices: int, steps: int, seed: int, detector: str = "hybrid",
           before_s: float = 20.0) -> int:
    dets: dict[tuple[str, str], Detector] = {}
    grouper = IncidentGrouper(quiet_s=5.0, max_s=60.0)
    history: dict[str, deque[Reading]] = defaultdict(lambda: deque(maxlen=3 * 1000))
    n = 0
    out.parent.mkdir(parents=True, exist_ok=True)

    def dump(f: Any, inc: Incident) -> None:
        ctx = [[round(r.ts, 3), METRICS.index(r.metric), round(r.value, 4)]
               for r in history[inc.device_id] if inc.started - before_s <= r.ts <= inc.ended + 1.0]
        f.write(json.dumps({"incident": incident_row(inc), "readings": ctx}) + "\n")

    with _open(out, "w") as f:
        last_ts = 0.0
        for r in generate(devices, steps, EVAL_FAULTS, seed):
            history[r.device_id].append(r)
            key = (r.device_id, r.metric)
            det = dets.get(key) or dets.setdefault(key, DETECTORS[detector]())
            score = det.observe(r.value)
            closed: list[Incident] = []
            if score is not None:
                closed += grouper.add(Anomaly(device_id=r.device_id, metric=r.metric, value=r.value,
                                              score=score, fault=r.fault, detector=detector,
                                              reading_ts=r.ts, ts=r.ts))
            if r.ts != last_ts:  # once per tick
                closed += grouper.sweep(r.ts)
                last_ts = r.ts
            for inc in closed:
                dump(f, inc)
                n += 1
        for inc in grouper.flush():
            dump(f, inc)
            n += 1
    return n


def load(path: Path, limit: int | None = None) -> list[dict[str, Any]]:
    with _open(path, "r") as f:
        rows = [json.loads(line) for line in f if line.strip()]
    return rows[:limit] if limit else rows


def _incident(d: dict[str, Any]) -> Incident:
    return Incident(**{**d, "metrics": tuple(d["metrics"]), "labels": tuple(d.get("labels", ())),
                       "samples": tuple(tuple(s) for s in d["samples"])})


async def replay(records: list[dict[str, Any]], diagnoser: Diagnoser, delay: float = 0.0,
                 max_repeated_errors: int = 3, progress: bool = False) -> dict[str, Any]:
    confusion: Counter[tuple[str, str]] = Counter()
    last_error, repeats, aborted = "", 0, None
    correct_dx = correct_any = correct_action = no_proposal = rejected = 0
    latencies: list[float] = []
    tool_calls: list[int] = []
    for i, rec in enumerate(records, 1):
        store = MemoryStore()
        inc = _incident(rec["incident"])
        await store.write_readings([Reading(device_id=inc.device_id, metric=METRICS[m], value=v, ts=ts)
                                    for ts, m, v in rec["readings"]])
        await store.save_incident(inc)
        truth = inc.truth or "noise"
        t0 = time.perf_counter()
        try:
            res = await diagnoser.diagnose(inc.incident_id, AgentTools(store))
        except Exception as e:  # count API failures as misses rather than aborting the run
            error = f"{type(e).__name__}: {e}"
            print(f"  {inc.incident_id}: diagnoser error {error}")
            no_proposal += 1
            confusion[(truth, "error")] += 1
            repeats = repeats + 1 if error == last_error else 1
            last_error = error
            if repeats >= max_repeated_errors:
                # The same error every time (bad model name, bad key) won't fix itself.
                aborted = f"stopped after {repeats} identical errors: {error}"
                print(f"  {aborted}")
                break
            continue
        repeats = 0
        latencies.append(time.perf_counter() - t0)
        tool_calls.append(res.tool_calls)
        rejected += res.rejected
        valid, _ = validate_proposal(res.raw, await store.get_incident(inc.incident_id)) if res.raw else (None, [])
        if valid is None:
            no_proposal += 1
            confusion[(truth, "none")] += 1
            continue
        confusion[(truth, valid.diagnosis)] += 1
        if progress:
            print(f"  [{i}/{len(records)}] {inc.incident_id}: {valid.diagnosis} -> {valid.action} "
                  f"(truth: {truth}, {time.perf_counter() - t0:.1f}s)", flush=True)
        correct_dx += valid.diagnosis == truth
        correct_any += valid.diagnosis in (inc.labels or ("noise",))
        correct_action += valid.action in EXPECTED_ACTIONS.get(truth, set())
        if delay:
            await asyncio.sleep(delay)
    if aborted:
        return {"agent": diagnoser.name, "aborted": aborted}
    n = len(records)
    by_truth = Counter(r["incident"]["truth"] or "noise" for r in records)
    return {
        "agent": diagnoser.name, "incidents": n, "by_truth": dict(by_truth),
        "multi_fault_incidents": sum(len(r["incident"].get("labels", ())) > 1 for r in records),
        "diagnosis_accuracy": round(correct_dx / max(n, 1), 3),
        "diagnosis_any_label": round(correct_any / max(n, 1), 3),
        "action_accuracy": round(correct_action / max(n, 1), 3),
        "per_class_recall": {t: round(confusion[(t, t)] / c, 3) for t, c in sorted(by_truth.items())},
        "validation_rejections": rejected, "no_proposal": no_proposal,
        "mean_latency_ms": round(statistics.mean(latencies) * 1000, 1) if latencies else None,
        "p95_latency_ms": round(sorted(latencies)[int(0.95 * (len(latencies) - 1))] * 1000, 1) if latencies else None,
        "mean_tool_calls": round(statistics.mean(tool_calls), 2) if tool_calls else None,
        "confusion": {f"{t}->{p}": c for (t, p), c in sorted(confusion.items())},
    }


def main() -> None:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("record")
    r.add_argument("--out", type=Path, default=Path("evals/incidents.jsonl.gz"))
    r.add_argument("--devices", type=int, default=8)
    r.add_argument("--steps", type=int, default=6000)
    r.add_argument("--seed", type=int, default=2024)
    e = sub.add_parser("run")
    e.add_argument("--file", type=Path, default=Path("evals/incidents.jsonl.gz"))
    e.add_argument("--agent", choices=["heuristic", "gemini"], default="heuristic")
    e.add_argument("--limit", type=int)
    e.add_argument("--delay", type=float, default=0.0, help="seconds between incidents (API rate limits)")
    e.add_argument("--rpm", type=float, default=float(os.environ.get("GEMINI_RPM", "8")),
                   help="max Gemini requests per minute (free-tier keys allow only a few)")
    e.add_argument("--json", action="store_true")
    a = p.parse_args()
    if a.cmd == "record":
        print(f"recorded {record(a.out, a.devices, a.steps, a.seed)} incidents to {a.out}")
        return
    diagnoser: Diagnoser
    if a.agent == "gemini":
        key = os.environ.get("GEMINI_API_KEY")
        if not key:
            raise SystemExit("set GEMINI_API_KEY")
        diagnoser = GeminiDiagnoser(GeminiClient(key, os.environ.get("GEMINI_MODEL", DEFAULT_GEMINI_MODEL),
                                                 max_rpm=a.rpm))
    else:
        diagnoser = HeuristicDiagnoser()
    res = asyncio.run(replay(load(a.file, a.limit), diagnoser, a.delay, progress=a.agent == "gemini"))
    if a.json:
        print(json.dumps(res, indent=2))
    else:
        for k, v in res.items():
            print(f"{k:>22}: {v}")


if __name__ == "__main__":
    main()
