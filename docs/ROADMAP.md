# Roadmap: what's done, and what you still need to do

All the code for Phases 0–7 is in this repo. The work that is left is the part only you can do: run it
on your own machine, measure your own numbers, record the demo, and be able to explain every design choice.
Work through this list in order.

## Phase 0: Own the foundation (1 evening)
- [ ] Clone the repo, run `pip install -r requirements-dev.txt`, `pytest`, `python -m app.main --seconds 30`.
- [ ] Read `app/bus.py`, `app/detectors.py`, `app/storage/writer.py` and `app/agent/guardrails.py` until you can
      whiteboard each one from memory. Use [INTERVIEW.md](INTERVIEW.md) to test yourself.
- [ ] Add your CPU model and RAM to the README's Results section (12 threads and Python 3.14.5 are already recorded).

## Phase 1: Detection (done). Your task: re-measure
- [ ] `python -m app.compare --markdown` → paste your table into the README.
- [ ] Try `--seed 1`, `--seed 2`, … Are the rankings stable? (They should be. If not, say so.)
- [ ] `python -m app.main --detector zscore` vs `--detector hybrid`: compare live precision and recall.

## Phase 2: TimescaleDB storage (done). Your task: re-measure
- [ ] `docker compose up db`, then `python -m app.bench_writer --dsn postgresql://postgres:postgres@localhost:5432/telemetry`
- [ ] Try `--batch 100`, `500`, `2000`, `5000`. Plot rows/s against flush p95. This is the batching trade-off:
      bigger batches give more throughput but higher latency.
- [ ] In `psql`: `SELECT * FROM readings_1m ORDER BY bucket DESC LIMIT 10;` and
      `SELECT * FROM timescaledb_information.jobs;` (these show the continuous aggregate and retention policies).

## Phase 3: API + live streaming (done). Your task: re-measure
- [ ] Start the stack, then `python -m app.ws_bench --clients 10` and `--clients 50`. Where does per-client lag grow?

## Phase 4: Dashboard (done). Your task
- [ ] `cd dashboard && npm install && npm run dev`. Inject each fault type and time how long it takes to appear on the chart.

## Phase 5: Monitoring and alerting (done). Your task
- [ ] `docker compose up --build`, open Grafana, then `docker compose stop db`. Watch `DatabaseWritesFailing` fire in
      Prometheus (`/alerts`), then `docker compose start db` and confirm `writers.readings.dropped` stays 0 in `/api/health`.
- [ ] Screenshot the firing alert for your README.

## Phase 6: LLM incident agent (done). Your task: get the real LLM numbers
- [ ] Get a free Gemini API key (Google AI Studio). Put it in `.env` as `GEMINI_API_KEY`.
- [ ] `python -m app.agent.eval run --agent gemini --limit 60 --delay 5` (the delay respects free-tier rate limits).
- [ ] Add the Gemini row to the eval table: diagnosis accuracy, correct action, validation rejections, latency.
      Whether the LLM beats the heuristic baseline or not, it's a finding. Report it honestly.
- [ ] Connect the MCP server to Claude Desktop or Claude Code and investigate an incident through it:
      `{"command": "python", "args": ["-m", "app.agent.mcp_server"], "env": {"DATABASE_URL": "..."}}`

## Phase 7: Engineering polish (mostly done). Your task
- [ ] Push to GitHub and check the Actions tab: lint, mypy, tests against TimescaleDB, dashboard build, docker build.
- [x] Run `python -m app.loadtest` on your laptop: 85.9k events/s sustained, 0 drops.
- [ ] Run `python -m app.loadtest --dsn ...` and `python -m app.bench_writer --dsn ...` on your laptop once Docker is installed.
- [ ] Record a 2-minute demo video: inject drift → chart → incident → agent proposal → approve → audit log →
      stop the DB → alert fires → DB returns → no data lost. Link it at the top of the README.

## Phase 8: Resume and interviews
Use only numbers you measured yourself. Template (fill in the brackets):

> Built a real-time telemetry platform in Python asyncio: a typed pub/sub event bus with per-consumer
> backpressure, streaming anomaly detection (CUSUM + z-score hybrid, [X]% drift recall vs [Y]% for z-score),
> and TimescaleDB storage sustaining [N]k events/s with zero loss through a database outage.

> Added an LLM incident agent (Gemini function calling, MCP server) with schema validation, an action allowlist,
> human approval and an audit log; [A]% diagnosis accuracy on [M] replayed incidents vs a [B]% rules baseline.

## Ideas beyond the plan (optional)
- Replace the simulator with a real MQTT source (e.g. Mosquitto) behind the same `Reading` event.
- Run several app replicas behind Redis Streams or NATS instead of the in-process bus: what changes about backpressure?
- Per-device adaptive thresholds, or seasonality (daily cycles) in the simulator and detector.
- Authentication for approvers (the `actor` field is currently self-declared).
