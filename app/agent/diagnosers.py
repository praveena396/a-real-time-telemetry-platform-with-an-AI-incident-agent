"""Diagnosers turn an incident into a raw proposal (a dict, not yet validated).

- HeuristicDiagnoser: deterministic rules over the same tools the LLM uses.
  It needs no API key, so the system works offline and in CI, and it's the
  baseline the LLM must beat in evals.
- GeminiDiagnoser: an LLM tool-use loop (Gemini function calling). It
  investigates with the read-only tools and must finish by calling
  `propose_action`. Invalid proposals are returned to the model with the
  validation errors so it can correct itself (bounded retries).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Protocol

import httpx

from ..simulator import BASELINES
from .guardrails import validate_proposal
from .tools import TOOL_DECLARATIONS, AgentTools

log = logging.getLogger(__name__)


class DiagnosisResult:
    def __init__(self, raw: dict[str, Any] | None, rejected: int = 0, tool_calls: int = 0,
                 errors: list[str] | None = None) -> None:
        self.raw = raw
        self.rejected = rejected      # proposals the guardrails bounced during the loop
        self.tool_calls = tool_calls
        self.errors = errors or []


class Diagnoser(Protocol):
    name: str

    async def diagnose(self, incident_id: str, tools: AgentTools) -> DiagnosisResult: ...


class HeuristicDiagnoser:
    name = "heuristic"

    async def diagnose(self, incident_id: str, tools: AgentTools) -> DiagnosisResult:
        inc = await tools.get_incident(incident_id)
        dev, t0, t1 = inc["device_id"], inc["started"], inc["ended"]
        per_metric: dict[str, int] = {}
        for _, metric, _, _ in inc["samples"]:
            per_metric[metric] = per_metric.get(metric, 0) + 1
        best: tuple[float, dict[str, Any]] | None = None
        calls = 1
        for metric in inc["metrics"]:
            before = await tools.query_readings(dev, metric, t0 - 20, t0 - 0.05)
            during = await tools.query_readings(dev, metric, t0 - 0.05, t1 + 0.05)
            calls += 2
            mu, sd = BASELINES[metric]
            if before.get("count", 0) >= 20:
                mu, sd = before["mean"], max(before["std"], 1e-6)
            duration = t1 - t0
            n = per_metric.get(metric, 0)
            shift = abs(during.get("mean", mu) - mu) / sd
            peak = max(abs(during.get("max", mu) - mu), abs(during.get("min", mu) - mu)) / sd
            trend = abs(during.get("slope_per_s", 0.0)) * max(duration, 0.1) / sd
            if during.get("longest_flat_run", 0) >= 5:
                cand = (3.0, {"diagnosis": "stuck", "action": "restart_device", "params": {},
                              "confidence": 0.9,
                              "cause": f"{metric} repeated the same value {during['longest_flat_run']} times "
                                       f"in a row; the sensor looks frozen."})
            elif during.get("count", 0) >= 5 and n >= 3 and (trend >= 2.0 or shift >= 2.0):
                cand = (2.0 + min(trend, 10) / 10, {
                    "diagnosis": "drift", "action": "recalibrate_sensor", "params": {"metric": metric},
                    "confidence": 0.8,
                    "cause": f"{metric} moved {shift:.1f} std from baseline over {duration:.1f}s "
                             f"(slope {during.get('slope_per_s', 0):.3f}/s): gradual drift, not a spike."})
            elif peak >= 4.0:
                cand = (1.0 + min(peak, 20) / 20, {
                    "diagnosis": "spike", "action": "no_action", "params": {}, "confidence": 0.75,
                    "cause": f"{metric} briefly reached {peak:.1f} std from baseline and returned to normal."})
            else:
                cand = (0.0, {"diagnosis": "noise", "action": "no_action", "params": {}, "confidence": 0.5,
                              "cause": f"{metric} stayed within {peak:.1f} std of baseline; "
                                       f"likely a statistical false alarm."})
            if best is None or cand[0] > best[0]:
                best = cand
        assert best is not None
        return DiagnosisResult({"incident_id": incident_id, **best[1]}, tool_calls=calls)


SYSTEM_PROMPT = """You are an on-call reliability engineer investigating telemetry incidents from industrial devices.
Each device reports temperature, vibration and pressure at ~10 Hz.

Fault types you can diagnose:
- spike: one or two isolated extreme readings that immediately return to normal (transient; usually no action).
- drift: a gradual, sustained move away from baseline over seconds (sensor needs recalibration).
- stuck: the sensor repeats exactly the same value many times (frozen sensor; restart the device).
- noise: a statistical false alarm, values within normal variation.
- unknown: evidence is insufficient.

Process:
1. Call get_incident to read the incident.
2. Call query_readings for each involved metric: once for ~20 s before `started` (baseline) and once
   covering `started`..`ended`. Compare mean, slope_per_s and longest_flat_run to the baseline.
3. Call propose_action exactly once with your diagnosis, a short evidence-based cause, ONE allowed action
   and a confidence between 0 and 1. recalibrate_sensor and increase_monitoring need params {"metric": ...};
   all other actions need params {}.
A human reviews every proposal before anything runs. Never invent data you did not retrieve."""


class LLMClient(Protocol):
    async def generate(self, system: str, contents: list[dict[str, Any]],
                       tools: list[dict[str, Any]]) -> dict[str, Any]: ...


# Verified working on a new free-tier key (Sept 2026); older versions return 404
# and the gemini-flash-latest alias was rejected. Override with GEMINI_MODEL, and
# check what your key can use with `python -m app.agent.gemini_check`.
DEFAULT_GEMINI_MODEL = "gemini-3.5-flash"


class GeminiAPIError(RuntimeError):
    """A non-retryable Gemini API error, carrying Google's own error message."""

    def __init__(self, status: int, model: str, message: str) -> None:
        super().__init__(f"Gemini API {status} for model {model!r}: {message}")
        self.status = status


class GeminiClient:
    """Minimal Gemini REST client (generateContent with function calling)."""

    URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

    def __init__(self, api_key: str, model: str = DEFAULT_GEMINI_MODEL, timeout: float = 60.0,
                 max_rpm: float | None = None, max_attempts: int = 6) -> None:
        self.model = model
        self.http = httpx.AsyncClient(timeout=timeout, headers={"x-goog-api-key": api_key})
        # Free-tier keys allow only a few requests per minute, and one incident
        # takes several calls, so space calls out instead of bursting.
        self.min_interval = 60.0 / max_rpm if max_rpm else 0.0
        self.max_attempts = max_attempts
        self._last_call = 0.0
        self._lock = asyncio.Lock()

    async def _pace(self) -> None:
        async with self._lock:
            wait = self._last_call + self.min_interval - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_call = time.monotonic()

    @staticmethod
    def _retry_after(r: httpx.Response, attempt: int) -> float:
        """How long to wait before retrying: Google's RetryInfo delay if given, else exponential."""
        try:
            for d in r.json()["error"].get("details", []):
                if str(d.get("@type", "")).endswith("RetryInfo") and "retryDelay" in d:
                    return min(float(str(d["retryDelay"]).rstrip("s")) + 1.0, 120.0)
        except (ValueError, KeyError, TypeError, AttributeError):
            pass
        return min(2.0 ** (attempt + 1), 60.0)

    async def generate(self, system: str, contents: list[dict[str, Any]],
                       tools: list[dict[str, Any]]) -> dict[str, Any]:
        body = {"systemInstruction": {"parts": [{"text": system}]}, "contents": contents,
                "tools": [{"functionDeclarations": tools}],
                "toolConfig": {"functionCallingConfig": {"mode": "ANY"}},
                "generationConfig": {"temperature": 0}}
        for attempt in range(self.max_attempts):
            await self._pace()
            r = await self.http.post(self.URL.format(model=self.model), json=body)
            if r.status_code in (429, 500, 503) and attempt < self.max_attempts - 1:
                wait = self._retry_after(r, attempt)
                log.warning("Gemini %s; retrying in %.0fs", r.status_code, wait)
                await asyncio.sleep(wait)
                continue
            if r.is_error:
                try:
                    message = r.json()["error"]["message"]
                except (ValueError, KeyError, TypeError):
                    message = r.text[:300]
                raise GeminiAPIError(r.status_code, self.model, message)
            data: dict[str, Any] = r.json()
            return data
        raise RuntimeError("unreachable")


class GeminiDiagnoser:
    name = "gemini"

    def __init__(self, client: LLMClient, max_turns: int = 8, max_rejections: int = 2) -> None:
        model = getattr(client, "model", None)
        if model:
            self.name = f"gemini ({model})"
        self.client = client
        self.max_turns = max_turns
        self.max_rejections = max_rejections

    async def diagnose(self, incident_id: str, tools: AgentTools) -> DiagnosisResult:
        incident = await tools.store.get_incident(incident_id)
        contents: list[dict[str, Any]] = [
            {"role": "user", "parts": [{"text": f"Investigate incident {incident_id} and propose an action."}]}]
        rejected = calls = 0
        errors: list[str] = []
        for _ in range(self.max_turns):
            resp = await self.client.generate(SYSTEM_PROMPT, contents, TOOL_DECLARATIONS)
            try:
                content = resp["candidates"][0]["content"]
            except (KeyError, IndexError):
                return DiagnosisResult(None, rejected, calls, [f"no candidate in response: {json.dumps(resp)[:200]}"])
            contents.append({"role": "model", "parts": content.get("parts", [])})
            fcalls = [p["functionCall"] for p in content.get("parts", []) if "functionCall" in p]
            if not fcalls:
                contents.append({"role": "user", "parts": [{"text": "Use the tools; finish with propose_action."}]})
                continue
            responses = []
            for fc in fcalls:
                calls += 1
                name, args = fc.get("name"), dict(fc.get("args") or {})
                if name == "propose_action":
                    args.setdefault("params", {})
                    _, errs = validate_proposal(args, incident)
                    if not errs:
                        return DiagnosisResult(args, rejected, calls, errors)
                    rejected += 1
                    errors.extend(errs)
                    if rejected > self.max_rejections:
                        return DiagnosisResult(None, rejected, calls, errors)
                    result: dict[str, Any] = {"accepted": False, "errors": errs}
                else:
                    result = await self._call_tool(tools, name, args)
                responses.append({"functionResponse": {"name": name, "response": {"result": result}}})
            contents.append({"role": "user", "parts": responses})
        return DiagnosisResult(None, rejected, calls, [*errors, "max turns reached without a proposal"])

    @staticmethod
    async def _call_tool(tools: AgentTools, name: str | None, args: dict[str, Any]) -> dict[str, Any]:
        try:
            if name == "get_incident":
                return await tools.get_incident(str(args["incident_id"]))
            if name == "query_readings":
                return await tools.query_readings(
                    str(args["device_id"]), str(args["metric"]), float(args["start"]), float(args["end"]),
                    int(args.get("max_points", 40)))
            if name == "get_device_history":
                return await tools.get_device_history(str(args["device_id"]), int(args.get("minutes", 30)))
        except (KeyError, TypeError, ValueError) as e:
            return {"error": f"bad arguments: {e}"}
        return {"error": f"unknown tool {name!r}"}
