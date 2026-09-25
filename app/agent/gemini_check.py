"""Check which Gemini models work with your API key, before running the agent.

Usage (key in the GEMINI_API_KEY environment variable):
    python -m app.agent.gemini_check
    python -m app.agent.gemini_check gemini-3.5-flash gemini-3.5-flash

For each model it makes two calls: a plain prompt, then a prompt with function
calling (which the agent needs). It prints OK or Google's own error message.
The key itself is never printed.
"""
from __future__ import annotations

import os
import sys

import httpx

from .diagnosers import DEFAULT_GEMINI_MODEL
from .tools import TOOL_DECLARATIONS

BASE = "https://generativelanguage.googleapis.com/v1beta"
DEFAULT_MODELS = [DEFAULT_GEMINI_MODEL, "gemini-3.5-flash", "gemini-3-flash-preview", "gemini-2.5-flash"]


def _error(r: httpx.Response) -> str:
    try:
        return str(r.json()["error"]["message"])
    except (ValueError, KeyError, TypeError):
        return r.text[:300] or r.reason_phrase


def check(http: httpx.Client, model: str) -> str:
    url = f"{BASE}/models/{model}:generateContent"
    plain = {"contents": [{"role": "user", "parts": [{"text": "Reply with the single word: ok"}]}]}
    r = http.post(url, json=plain)
    if r.is_error:
        return f"FAILED plain call ({r.status_code}): {_error(r)}"
    tools = {**plain, "tools": [{"functionDeclarations": TOOL_DECLARATIONS}],
             "toolConfig": {"functionCallingConfig": {"mode": "ANY"}}}
    r = http.post(url, json=tools)
    if r.is_error:
        return f"plain call OK, FAILED function calling ({r.status_code}): {_error(r)}"
    return "OK (plain + function calling)"


def main() -> None:
    key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not key:
        raise SystemExit("GEMINI_API_KEY is not set")
    if any(ord(c) < 32 for c in key):
        raise SystemExit("GEMINI_API_KEY contains control characters (did the paste fail?); set it again")
    print(f"key: {len(key)} characters, starts with {key[:3]!r}")
    models = sys.argv[1:] or DEFAULT_MODELS
    with httpx.Client(timeout=60, headers={"x-goog-api-key": key}) as http:
        r = http.get(f"{BASE}/models", params={"pageSize": 1})
        print(f"list models: {'OK' if not r.is_error else f'FAILED ({r.status_code}): {_error(r)}'}")
        for m in models:
            print(f"{m:<28} {check(http, m)}")
    print(f"\nThen run the eval with a model that says OK, e.g.:\n"
          f"  $env:GEMINI_MODEL = \"<model>\"   (the agent's default is {DEFAULT_GEMINI_MODEL})")


if __name__ == "__main__":
    main()
