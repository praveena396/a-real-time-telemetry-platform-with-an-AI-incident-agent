import asyncio
import time

import pytest

from app.bus import EventBus
from app.events import Anomaly, Incident, Proposal, Reading
from app.storage.base import InvalidTransitionError, NotFoundError
from app.storage.writer import BatchedWriter


def readings(n, device="dev-000", metric="temperature", t0=None):
    t0 = t0 or time.time() - n * 0.1
    return [Reading(device_id=device, metric=metric, value=float(i), ts=t0 + i * 0.1) for i in range(n)]


def incident(iid="inc-1"):
    now = time.time()
    return Incident(incident_id=iid, device_id="dev-000", started=now - 5, ended=now, anomaly_count=3,
                    metrics=("temperature",), max_score=7.5, truth="drift",
                    samples=((now, "temperature", 70.0, 7.5),))


def proposal(pid="p-1", iid="inc-1"):
    return Proposal(proposal_id=pid, incident_id=iid, device_id="dev-000", diagnosis="drift",
                    cause="slow upward drift", action="recalibrate_sensor",
                    params={"metric": "temperature"}, confidence=0.8, agent="heuristic")


async def _store_contract(store):
    await store.write_readings(readings(50))
    await store.write_readings(readings(10, metric="pressure"))
    rows = await store.recent_readings("dev-000", "temperature", limit=20)
    assert len(rows) == 20 and rows[-1]["value"] == 49.0 and rows[0]["ts"] < rows[-1]["ts"]
    assert len(await store.recent_readings("dev-000")) == 60

    await store.write_anomalies([Anomaly(device_id="dev-000", metric="temperature", value=99.0, score=8.0,
                                         fault="spike", detector="hybrid", reading_ts=time.time())])
    an = await store.list_anomalies(10)
    assert an[0]["value"] == 99.0 and an[0]["detector"] == "hybrid"

    devs = await store.devices()
    assert devs[0]["device_id"] == "dev-000" and devs[0]["recent_anomalies"] == 1
    assert set(devs[0]["latest"]) == {"temperature", "pressure"}

    hist = await store.device_history("dev-000", minutes=10)
    assert sum(h["n"] for h in hist) == 60

    await store.save_incident(incident())
    assert (await store.get_incident("inc-1"))["metrics"] == ["temperature"]
    assert len(await store.list_incidents()) == 1
    with pytest.raises(NotFoundError):
        await store.get_incident("nope")

    await store.save_proposal(proposal())
    assert (await store.list_proposals("pending"))[0]["params"] == {"metric": "temperature"}
    p = await store.set_proposal_status("p-1", "approved", "alice")
    assert p["status"] == "approved" and p["decided_by"] == "alice"
    with pytest.raises(InvalidTransitionError):
        await store.set_proposal_status("p-1", "rejected", "bob")  # already decided
    await store.set_proposal_status("p-1", "executed", "system")

    await store.audit("proposal_approved", "alice", "p-1", {"why": "looks right"})
    log = await store.list_audit()
    assert log[0]["event"] == "proposal_approved" and log[0]["details"] == {"why": "looks right"}


@pytest.mark.asyncio
async def test_memory_store_contract(memory_store):
    await _store_contract(memory_store)


@pytest.mark.asyncio
async def test_postgres_store_contract(pg_store):
    await _store_contract(pg_store)


@pytest.mark.asyncio
async def test_postgres_concurrent_approvals_only_one_wins(pg_store):
    await pg_store.save_incident(incident())
    await pg_store.save_proposal(proposal())
    results = await asyncio.gather(
        *(pg_store.set_proposal_status("p-1", s, u) for s, u in [("approved", "a"), ("rejected", "b")]),
        return_exceptions=True)
    assert sum(isinstance(r, InvalidTransitionError) for r in results) == 1


@pytest.mark.asyncio
async def test_writer_flushes_on_batch_size():
    bus = EventBus()
    sub = bus.subscribe(Reading, maxsize=10_000)
    batches = []

    async def write(rows):
        batches.append(len(rows))

    w = BatchedWriter("readings", sub, write, batch_size=100, flush_interval=10)
    task = asyncio.create_task(w.run())
    for r in readings(250):
        bus.publish(r)
    await asyncio.sleep(0.05)
    assert sum(batches) >= 200 and max(batches) <= 100  # two full batches, no waiting for the timer
    bus.close()
    await task
    assert w.rows_written == 250


@pytest.mark.asyncio
async def test_writer_flushes_on_interval():
    bus = EventBus()
    sub = bus.subscribe(Reading)
    got = []

    async def write(rows):
        got.extend(rows)

    task = asyncio.create_task(BatchedWriter("r", sub, write, batch_size=500, flush_interval=0.05).run())
    for r in readings(3):
        bus.publish(r)
    await asyncio.sleep(0.2)
    assert len(got) == 3
    bus.close()
    await task


@pytest.mark.asyncio
async def test_writer_retries_when_db_down_then_recovers():
    bus = EventBus()
    sub = bus.subscribe(Reading)
    state = {"up": False}
    got = []

    async def write(rows):
        if not state["up"]:
            raise ConnectionError("db down")
        got.extend(rows)

    w = BatchedWriter("r", sub, write, batch_size=10, flush_interval=0.02, max_backoff=0.05)
    task = asyncio.create_task(w.run())
    for r in readings(30):
        bus.publish(r)
    await asyncio.sleep(0.2)
    assert w.failures >= 1 and not got and len(w.buffer) == 30
    state["up"] = True
    await asyncio.sleep(0.3)
    assert len(got) == 30 and w.rows_dropped == 0
    bus.close()
    await task


@pytest.mark.asyncio
async def test_writer_buffer_is_bounded_and_counts_drops():
    bus = EventBus()
    sub = bus.subscribe(Reading, maxsize=10_000)

    async def write(rows):
        raise ConnectionError("db down")

    w = BatchedWriter("r", sub, write, batch_size=10, flush_interval=0.01, max_buffer=50, max_backoff=0.01)
    task = asyncio.create_task(w.run())
    for r in readings(200):
        bus.publish(r)
    await asyncio.sleep(0.1)
    assert len(w.buffer) == 50 and w.rows_dropped == 150
    assert w.buffer[-1].value == 199.0  # newest rows kept
    bus.close()
    await task
