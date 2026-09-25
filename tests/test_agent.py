import asyncio
import random

import pytest

from app.agent import mcp_server
from app.agent.actions import ActionExecutor
from app.agent.agent import IncidentAgent
from app.agent.diagnosers import DiagnosisResult, GeminiDiagnoser, HeuristicDiagnoser
from app.agent.eval import load, record, replay
from app.agent.guardrails import validate_proposal
from app.agent.tools import AgentTools
from app.bus import EventBus
from app.events import ActionExecuted, Anomaly, Incident, Reading
from app.incidents import IncidentGrouper
from app.simulator import FaultConfig, Fleet
from app.storage.base import InvalidTransitionError, MemoryStore

T0 = 1_700_000_000.0


def anomaly(t, device="dev-000", metric="temperature", fault="drift", score=5.0):
    return Anomaly(device_id=device, metric=metric, value=70.0, score=score, fault=fault, reading_ts=t, ts=t)


# ---------------- grouping ----------------
def test_grouper_merges_until_quiet():
    g = IncidentGrouper(quiet_s=5, max_s=60)
    for i in range(10):
        assert g.add(anomaly(T0 + i * 0.1)) == []
    assert g.add(anomaly(T0 + 1, device="dev-001", fault="spike")) == []
    assert g.sweep(T0 + 3) == []
    closed = g.sweep(T0 + 7)
    assert {c.device_id for c in closed} == {"dev-000", "dev-001"}
    inc = next(c for c in closed if c.device_id == "dev-000")
    assert inc.anomaly_count == 10 and inc.truth == "drift" and inc.metrics == ("temperature",)
    assert inc.started == T0 and inc.ended == pytest.approx(T0 + 0.9)


def test_grouper_splits_after_gap_and_caps_duration():
    g = IncidentGrouper(quiet_s=5, max_s=10)
    g.add(anomaly(T0))
    closed = g.add(anomaly(T0 + 6))  # gap > quiet: previous incident closes
    assert len(closed) == 1 and closed[0].anomaly_count == 1
    closed = [c for i in range(1, 12) for c in g.add(anomaly(T0 + 6 + i))]
    assert len(closed) == 1 and closed[0].ended - closed[0].started >= 10


def test_grouper_majority_label_and_all_labels():
    g = IncidentGrouper()
    for i, f in enumerate(["stuck", "stuck", None]):
        g.add(anomaly(T0 + i * 0.1, fault=f))
    (inc,) = g.flush()
    assert inc.truth == "stuck" and inc.labels == ("stuck",)


# ---------------- guardrails ----------------
INC = {"incident_id": "inc-1", "device_id": "dev-000", "metrics": ["temperature"]}


def good(**kw):
    return {"incident_id": "inc-1", "diagnosis": "drift", "cause": "gradual rise", "action": "recalibrate_sensor",
            "params": {"metric": "temperature"}, "confidence": 0.8, **kw}


@pytest.mark.parametrize("raw, fragment", [
    (good(action="rm -rf /"), "not allowed"),
    (good(action="restart_device"), "unexpected params"),
    (good(params={}), "missing params"),
    (good(params={"metric": "pressure"}), "not part of this incident"),
    (good(confidence=3), "schema"),
    (good(diagnosis="aliens"), "schema"),
    (good(incident_id="inc-2"), "does not match"),
    ({**good(), "execute_now": True}, "schema"),
    ("not even a dict", "schema"),
])
def test_guardrails_reject(raw, fragment):
    p, errors = validate_proposal(raw, INC)
    assert p is None and any(fragment in e for e in errors), errors


def test_guardrails_accept():
    p, errors = validate_proposal(good(), INC)
    assert errors == [] and p is not None and p.action == "recalibrate_sensor"


# ---------------- diagnosis ----------------
async def seeded_store(kind: str, metric: str = "temperature") -> tuple[MemoryStore, Incident]:
    """200 normal readings, then a fault, stored with an incident describing it."""
    rng = random.Random(5)
    store = MemoryStore()
    rows, t = [], T0
    for _ in range(200):
        t += 0.1
        rows.append(Reading(device_id="dev-000", metric=metric, value=rng.gauss(60, 1.5), ts=t))
    start = t + 0.1
    if kind == "drift":
        vals = [rng.gauss(60, 1.5) + 0.6 * i for i in range(1, 41)]
    elif kind == "stuck":
        vals = [60.3] * 30
    else:
        vals = [75.0]
    for v in vals:
        t += 0.1
        rows.append(Reading(device_id="dev-000", metric=metric, value=v, ts=t))
    await store.write_readings(rows)
    n = len(vals)
    inc = Incident(incident_id="inc-1", device_id="dev-000", started=start, ended=start + 0.1 * (n - 1),
                   anomaly_count=n, metrics=(metric,), max_score=6.0, truth=kind,
                   samples=tuple((start + 0.1 * i, metric, v, 5.0) for i, v in enumerate(vals)))
    await store.save_incident(inc)
    return store, inc


@pytest.mark.asyncio
@pytest.mark.parametrize("kind, action", [("drift", "recalibrate_sensor"), ("stuck", "restart_device"),
                                          ("spike", "no_action")])
async def test_heuristic_diagnoses(kind, action):
    store, inc = await seeded_store(kind)
    res = await HeuristicDiagnoser().diagnose(inc.incident_id, AgentTools(store))
    assert res.raw["diagnosis"] == kind and res.raw["action"] == action


@pytest.mark.asyncio
async def test_tools_hide_ground_truth():
    store, inc = await seeded_store("drift")
    tools = AgentTools(store)
    got = await tools.get_incident(inc.incident_id)
    assert "truth" not in got and "labels" not in got
    q = await tools.query_readings("dev-000", "temperature", inc.started, inc.ended)
    assert "fault" not in str(q) and q["slope_per_s"] > 0


class FakeLLM:
    """Scripted Gemini responses: each call pops the next list of function calls."""

    def __init__(self, script):
        self.script = list(script)
        self.seen = []

    async def generate(self, system, contents, tools):
        self.seen.append(contents[-1])
        calls = self.script.pop(0)
        return {"candidates": [{"content": {"role": "model", "parts": [
            {"functionCall": {"name": n, "args": a}} for n, a in calls]}}]}


@pytest.mark.asyncio
async def test_gemini_loop_uses_tools_and_self_corrects():
    store, inc = await seeded_store("drift")
    bad = {**good(), "action": "delete_everything"}
    llm = FakeLLM([
        [("get_incident", {"incident_id": "inc-1"})],
        [("query_readings", {"device_id": "dev-000", "metric": "temperature",
                             "start": inc.started - 20, "end": inc.ended})],
        [("propose_action", bad)],
        [("propose_action", good())],
    ])
    res = await GeminiDiagnoser(llm).diagnose("inc-1", AgentTools(store))
    assert res.raw["action"] == "recalibrate_sensor" and res.rejected == 1 and res.tool_calls == 4
    # the model was shown the validation errors
    rejection = llm.seen[-1]["parts"][0]["functionResponse"]["response"]["result"]
    assert rejection["accepted"] is False and "not allowed" in rejection["errors"][0]


@pytest.mark.asyncio
async def test_gemini_gives_up_after_repeated_invalid_proposals():
    store, _ = await seeded_store("drift")
    bad = ("propose_action", {**good(), "action": "format_disk"})
    res = await GeminiDiagnoser(FakeLLM([[bad]] * 5), max_rejections=2).diagnose("inc-1", AgentTools(store))
    assert res.raw is None and res.rejected == 3


# ---------------- agent + approval ----------------
@pytest.mark.asyncio
async def test_agent_creates_pending_proposal_and_audits():
    store, inc = await seeded_store("drift")
    bus = EventBus()
    sub = bus.subscribe(Incident)
    from app.events import Proposal
    props = bus.subscribe(Proposal)
    agent = IncidentAgent(store, HeuristicDiagnoser(), bus)
    p = await agent.handle(inc)
    assert p and p.status == "pending" and p.action == "recalibrate_sensor"
    assert (await store.list_proposals("pending"))[0]["proposal_id"] == p.proposal_id
    assert (await store.list_audit())[0]["event"] == "proposal_created"
    assert props.depth() == 1 and sub.depth() == 0
    # second incident on same device while one is pending: skipped
    again = await agent.handle(Incident(**{**inc.__dict__, "incident_id": "inc-2"}))
    assert again is None and (await store.list_audit())[0]["event"] == "incident_skipped"


@pytest.mark.asyncio
async def test_agent_survives_diagnoser_crash_and_invalid_output():
    store, inc = await seeded_store("drift")

    class Crashy:
        name = "crashy"

        async def diagnose(self, *_):
            raise RuntimeError("API down")

    class Liar:
        name = "liar"

        async def diagnose(self, incident_id, tools):
            return DiagnosisResult({**good(), "action": "shutdown_plant"})

    assert await IncidentAgent(store, Crashy()).handle(inc) is None
    assert await IncidentAgent(store, Liar()).handle(inc) is None
    events = [a["event"] for a in await store.list_audit()]
    assert events[:2] == ["no_proposal", "agent_error"]
    assert await store.list_proposals() == []


@pytest.mark.asyncio
async def test_agent_rate_limit():
    store, inc = await seeded_store("spike")
    agent = IncidentAgent(store, HeuristicDiagnoser(), max_per_min=1)
    first = await agent.handle(inc)
    assert first is not None and first.status == "auto_closed"  # spike -> no_action, closed by policy
    assert await agent.handle(Incident(**{**inc.__dict__, "incident_id": "inc-2"})) is None
    assert (await store.list_audit())[0]["details"]["reason"] == "rate limit"


@pytest.mark.asyncio
async def test_approval_executes_action_on_device():
    store, inc = await seeded_store("drift")
    fleet = Fleet(1, FaultConfig(), seed=1)
    fleet["dev-000"].inject("temperature", "drift")
    bus = EventBus()
    executed = bus.subscribe(ActionExecuted)
    p = await IncidentAgent(store, HeuristicDiagnoser()).handle(inc)
    ex = ActionExecutor(store, fleet, bus)
    out = await ex.approve(p.proposal_id, "alice", "confirmed on site")
    assert out["status"] == "executed"
    assert fleet["dev-000"].active_faults() == {}  # recalibration cleared the drift
    assert executed.depth() == 1
    events = [a["event"] for a in await store.list_audit()]
    assert events[:3] == ["action_executed", "proposal_approved", "proposal_created"]
    with pytest.raises(InvalidTransitionError):
        await ex.reject(p.proposal_id, "bob")


@pytest.mark.asyncio
async def test_reject_does_not_execute():
    store, inc = await seeded_store("stuck")
    fleet = Fleet(1, FaultConfig(), seed=1)
    fleet["dev-000"].inject("temperature", "stuck")
    p = await IncidentAgent(store, HeuristicDiagnoser()).handle(inc)
    out = await ActionExecutor(store, fleet).reject(p.proposal_id, "alice", "false alarm")
    assert out["status"] == "rejected" and fleet["dev-000"].active_faults() == {"temperature": "stuck"}


@pytest.mark.asyncio
async def test_noop_proposals_skip_the_queue_and_dont_block_device():
    store, inc = await seeded_store("spike")
    agent = IncidentAgent(store, HeuristicDiagnoser())
    p = await agent.handle(inc)
    assert p.action == "no_action" and p.status == "auto_closed"
    assert await store.list_proposals("pending") == []
    assert (await store.list_audit())[0]["event"] == "proposal_auto_closed"
    assert await agent.handle(Incident(**{**inc.__dict__, "incident_id": "inc-2"})) is not None


# ---------------- MCP server ----------------
@pytest.mark.asyncio
async def test_mcp_tools_validate_and_queue():
    store, _ = await seeded_store("drift")
    mcp_server.set_store(store)
    try:
        bad = await mcp_server.propose_action("inc-1", "drift", "x rising", "open_valve", {}, 0.5)
        assert bad["accepted"] is False
        ok = await mcp_server.propose_action("inc-1", "drift", "temperature rising steadily",
                                             "recalibrate_sensor", {"metric": "temperature"}, 0.8)
        assert ok["accepted"] and (await store.get_proposal(ok["proposal_id"]))["status"] == "pending"
        listed = await mcp_server.list_incidents()
        assert listed[0]["incident_id"] == "inc-1" and "truth" not in listed[0]
        tools = {t.name for t in await mcp_server.mcp.list_tools()}
        assert "propose_action" in tools and not any("approve" in t or "execute" in t for t in tools)
    finally:
        mcp_server.set_store(None)


# ---------------- eval harness ----------------
@pytest.mark.asyncio
async def test_eval_record_and_replay(tmp_path):
    path = tmp_path / "inc.jsonl.gz"
    n = await asyncio.to_thread(record, path, 3, 1500, 3)
    assert n > 5
    res = await replay(load(path), HeuristicDiagnoser())
    assert res["incidents"] == n and res["diagnosis_accuracy"] > 0.7 and res["validation_rejections"] == 0


def test_eval_dataset_is_committed_and_scores():
    from pathlib import Path
    recs = load(Path("evals/incidents.jsonl.gz"), limit=40)
    res = asyncio.run(replay(recs, HeuristicDiagnoser()))
    assert res["diagnosis_accuracy"] > 0.8
