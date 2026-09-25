"""Runtime settings, read from environment variables (see .env.example)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    database_url: str | None = field(default_factory=lambda: os.environ.get("DATABASE_URL") or None)
    devices: int = field(default_factory=lambda: int(_env("SIM_DEVICES", "20")))
    rate_hz: float = field(default_factory=lambda: float(_env("SIM_RATE_HZ", "10")))
    seed: int = field(default_factory=lambda: int(_env("SIM_SEED", "42")))
    detector: str = field(default_factory=lambda: _env("DETECTOR", "hybrid"))
    # Multiplier on the benchmark fault rates. Live demos want rare faults
    # (inject more from the dashboard); benchmarks use 1.0.
    fault_scale: float = field(default_factory=lambda: float(_env("SIM_FAULT_SCALE", "0.02")))
    # incident grouping
    incident_quiet_s: float = field(default_factory=lambda: float(_env("INCIDENT_QUIET_S", "5")))
    incident_max_s: float = field(default_factory=lambda: float(_env("INCIDENT_MAX_S", "60")))
    # agent
    agent_enabled: bool = field(default_factory=lambda: _env("AGENT_ENABLED", "1") == "1")
    gemini_api_key: str | None = field(default_factory=lambda: os.environ.get("GEMINI_API_KEY") or None)
    gemini_model: str = field(default_factory=lambda: _env("GEMINI_MODEL", "gemini-2.5-flash"))
    agent_max_concurrency: int = field(default_factory=lambda: int(_env("AGENT_MAX_CONCURRENCY", "2")))
    # Budget on diagnoses per minute (protects LLM quota and cost).
    agent_max_per_min: int = field(default_factory=lambda: int(_env("AGENT_MAX_PER_MIN", "10")))
    # writer
    batch_size: int = field(default_factory=lambda: int(_env("WRITER_BATCH_SIZE", "500")))
    flush_interval_s: float = field(default_factory=lambda: float(_env("WRITER_FLUSH_S", "0.2")))
    static_dir: str | None = field(default_factory=lambda: os.environ.get("STATIC_DIR") or None)
