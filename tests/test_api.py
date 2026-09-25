import time

import pytest
from fastapi.testclient import TestClient

from app.api.server import create_app
from app.config import Settings
from app.storage.base import MemoryStore


@pytest.fixture
def client():
    settings = Settings(database_url=None, devices=3, rate_hz=50, detector="hybrid", agent_enabled=False)
    with TestClient(create_app(settings, MemoryStore())) as c:
        yield c


def wait_for(fn, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        v = fn()
        if v:
            return v
        time.sleep(0.05)
    raise AssertionError("condition not met")


def test_health_and_metrics(client):
    h = client.get("/api/health").json()
    assert h["status"] == "ok" and h["db"]["backend"] == "memory"
    wait_for(lambda: client.get("/api/health").json()["published"] > 0)
    m = client.get("/metrics").text
    assert "sentinel_bus_published" in m and 'sentinel_bus_queue_depth{subscriber="detector"}' in m


def test_devices_and_readings(client):
    devs = wait_for(lambda: client.get("/api/devices").json())
    assert {d["device_id"] for d in devs} <= {"dev-000", "dev-001", "dev-002"}
    rows = wait_for(lambda: client.get("/api/devices/dev-000/readings", params={"metric": "pressure"}).json())
    assert all(r["metric"] == "pressure" for r in rows)
    assert client.get("/api/devices/dev-000/readings", params={"seconds": 0}).status_code == 422


def test_injected_spike_becomes_anomaly(client):
    time.sleep(1.2)  # detector warm-up (50 readings at 50 Hz)
    r = client.post("/api/devices/dev-001/faults", json={"metric": "temperature", "kind": "spike"})
    assert r.status_code == 202
    found = wait_for(lambda: [a for a in client.get("/api/anomalies", params={"device_id": "dev-001"}).json()
                              if a["fault"] == "spike" and a["metric"] == "temperature"])
    assert found[0]["detector"] == "hybrid"


def test_bad_fault_requests(client):
    assert client.post("/api/devices/nope/faults", json={"metric": "temperature", "kind": "spike"}).status_code == 404
    assert client.post("/api/devices/dev-000/faults", json={"metric": "x", "kind": "spike"}).status_code == 422


def test_websocket_streams_filtered_events(client):
    with client.websocket_connect("/ws?types=reading&device=dev-002") as ws:
        msg = ws.receive_json()
        assert msg["events"]
        assert all(e["type"] == "reading" and e["device_id"] == "dev-002" for e in msg["events"])
        h = client.get("/api/health").json()
        assert h["ws_clients"] == 1
    wait_for(lambda: client.get("/api/health").json()["ws_clients"] == 0)
