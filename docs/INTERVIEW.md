# Interview prep: questions this project will get, with the answers in the code

Answer each question out loud before reading the answer. Every answer points to the file that backs it up.

**1. Why drop-oldest instead of blocking the producer, or dropping the newest?**
Blocking lets one slow consumer stall every device and every other consumer. Dropping the newest keeps
stale data and throws away what's happening *now*. For live telemetry, the latest reading is the most
valuable one. The cost is data loss, so every drop is counted per subscriber, exported to Prometheus
and alerted on (`EventsDropped`). Where loss is unacceptable (the database), the writer adds its own
buffer and retry. `app/bus.py`, `app/storage/writer.py`

**2. What happens when producers outpace consumers?**
Each subscriber's queue fills independently. The detector queue (20k) and the DB queue (50k) absorb bursts.
Past that, the oldest events drop and the counters rise. The load test finds this point: in-memory it was
about 38k events/s on 4 vCPUs, and about 23k/s with every reading written to TimescaleDB. `app/loadtest.py`

**3. Why batch database writes? Why COPY?**
Per-row INSERTs pay a network round trip and statement overhead for every reading. COPY streams a whole
batch in one command. The flush triggers (500 rows or 200 ms) cap both the batch size and how stale the
data can get: under heavy load batches fill quickly, and under light load the timer bounds latency.
`bench_writer --batch` shows the trade-off: larger batches give more rows/s but a higher flush p95.

**4. What happens when the database goes down?**
The write raises. The batch stays buffered and the writer retries with exponential backoff (0.1 s → 5 s).
Detection and the dashboard are unaffected because they read the bus, not the DB. The buffer is bounded,
so memory can't grow without limit. When it overflows, the oldest rows go, counted and alerted
(`DatabaseRowsLost`). Verified live: ~12k rows buffered during an outage, 0 lost.

**5. Why does the z-score miss drift, and why does CUSUM catch it?**
A z-score compares each point with a recent window. A slow drift moves the window along with it, and each
single point looks only slightly high. CUSUM sums the small deviations (`S = max(0, S + z - k)`), so a
persistent 1–2σ shift crosses the threshold `h` within a few readings. The hybrid detector clips each
residual at ±3 before it enters CUSUM, so one spike can't trip it; the point test handles spikes.
`app/detectors.py`, results table in the README.

**6. How do you keep the LLM from doing something unsafe?**
Layered defences, in `app/agent/guardrails.py`, `agent.py` and `actions.py`:
1. The tools are read-only.
2. The only write the agent can make is a *proposal*.
3. Proposals pass a strict schema and an action allowlist with exact parameters, and must match the
   incident and its metrics.
4. They are re-validated before they are stored and again before they are executed.
5. A human approves each one in the UI.
6. Every step goes to an append-only audit log.

The MCP server has no approve or execute tool at all. Prompt injection can at worst produce a bad
*proposal* that a person then reads.

**7. Why group anomalies into incidents?**
A 4-second drift produces about 30 anomalies. Calling an LLM 30 times costs 30× as much, hits rate limits,
and floods the approver. The grouper merges per device until 5 s of quiet. On top of that, the agent
allows one pending proposal per device and has a per-minute budget. `app/incidents.py`

**8. How do you know the agent is any good?**
The simulator labels every fault. `eval record` captures incidents together with their ground truth and
the surrounding readings. `eval run` replays them through any diagnoser using the same tools, then
reports diagnosis accuracy, correct-action rate, validation rejections and latency. The rule-based
heuristic is the baseline the LLM has to beat. `app/agent/eval.py`

**9. Why does the dashboard not re-render on every event?**
At about 600 events/s, one React update per event would dominate the CPU. Events go into a mutable
store, and components re-render on a 250 ms tick, so render cost is constant regardless of event rate.
`dashboard/src/live.ts`

**10. What would you change for production?**
- Swap the in-process bus for a durable log (Kafka, Redpanda or NATS JetStream) so consumers can replay
  after a crash. That changes backpressure from dropping to consumer lag.
- Authenticate approvers instead of trusting a self-declared name.
- Use schema migrations instead of `CREATE IF NOT EXISTS`.
- Run multiple app replicas, partitioned by device.
- Retrain detector thresholds per device.
