"""Smoke tests: the benchmark entry points run and report sane numbers."""
import pytest

from app import loadtest, main
from app.compare import main as compare_main
from app.storage.base import MemoryStore


@pytest.mark.asyncio
async def test_headless_benchmark_runs():
    res = await main.run(devices=3, rate=50, seconds=2.0, seed=1, detector="hybrid")
    assert res["readings"] > 500 and res["dropped[detector]"] == 0
    assert 0 <= res["alert_precision"] <= 1


@pytest.mark.asyncio
async def test_loadtest_step_holds_at_low_load():
    r = await loadtest.step(5, 10, 1.0, MemoryStore(), "hybrid")
    assert r["holds"] and r["dropped"] == 0 and r["db_rows"] > 0


def test_compare_markdown(capsys, monkeypatch):
    monkeypatch.setattr("sys.argv", ["compare", "--devices", "2", "--steps", "300", "--markdown"])
    compare_main()
    out = capsys.readouterr().out
    assert out.startswith("| detector |") and "| hybrid |" in out
