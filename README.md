# Telemetry Sentinel

Real-time telemetry pipeline in pure Python asyncio: simulated devices stream
readings through a typed pub/sub event bus into a streaming anomaly detector,
with measured detection quality and latency.

```
devices (simulator) ──► EventBus ──► detector (rolling z-score) ──► Anomaly events
                           │                                            │
                           └──────────────► scorer ◄────────────────────┘
```

## Design decisions

- **Per-subscriber bounded queues.** A slow consumer never blocks the publisher
  or other consumers.
- **Drop-oldest backpressure.** For live telemetry, fresh data beats stale data.
  Drops are counted per subscriber so data loss is visible, not silent.
- **Type-based routing.** Subscribing to a base event type receives all subtypes.
- **Ground-truth fault labels.** The simulator labels every injected fault, so
  detector precision and recall are measured, not guessed.
- **Clean baseline.** Readings flagged as anomalous don't update the rolling
  statistics, so faults don't teach the detector that faults are normal.

## Run

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pytest
python -m app.main --devices 20 --rate 10 --seconds 30
```

## Results

Record your own measured numbers here (events/s, precision, recall, p50/p95
alert latency, drops) along with your machine specs.

## Roadmap

- TimescaleDB storage with batched asyncpg writes
- React + TypeScript live dashboard over WebSockets
- LLM incident agent with schema-validated, human-approved actions
