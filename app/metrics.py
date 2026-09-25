"""Prometheus metrics for pipeline health.

Bus counters (published, per-subscriber drops and queue depth) are read
straight from the live EventBus at scrape time by a custom collector, so the
hot publish path pays nothing for instrumentation.
"""
from __future__ import annotations

from collections.abc import Iterable

from prometheus_client import REGISTRY, Counter, Gauge, Histogram
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, Metric
from prometheus_client.registry import Collector

from .bus import EventBus

FAST_BUCKETS = (0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1.0)

DB_WRITE_LATENCY = Histogram("sentinel_db_write_seconds", "Time to write one batch", ["table"],
                             buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5))
DB_ROWS = Counter("sentinel_db_rows_written_total", "Rows written to the database", ["table"])
DB_FAILURES = Counter("sentinel_db_write_failures_total", "Failed batch writes (will retry)", ["table"])
DB_ROWS_DROPPED = Counter("sentinel_db_rows_dropped_total",
                          "Rows discarded because the writer buffer overflowed", ["table"])
DETECTION_LATENCY = Histogram("sentinel_detection_latency_seconds",
                              "Reading produced -> anomaly published", buckets=FAST_BUCKETS)
ANOMALIES = Counter("sentinel_anomalies_total", "Anomalies detected", ["detector", "metric"])
INCIDENTS = Counter("sentinel_incidents_total", "Incidents opened by the grouper")
PROPOSALS = Counter("sentinel_proposals_total", "Agent proposals by outcome", ["outcome"])
AGENT_LATENCY = Histogram("sentinel_agent_seconds", "Incident -> proposal time",
                          buckets=(0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60))
WS_CLIENTS = Gauge("sentinel_ws_clients", "Connected WebSocket clients")
WS_SENT = Counter("sentinel_ws_events_sent_total", "Events sent to WebSocket clients")


class BusCollector(Collector):
    def __init__(self, bus: EventBus) -> None:
        self.bus = bus

    def collect(self) -> Iterable[Metric]:
        pub = CounterMetricFamily("sentinel_bus_published", "Events published on the bus")
        pub.add_metric([], self.bus.published)
        drops = CounterMetricFamily("sentinel_bus_dropped", "Events dropped (drop-oldest)", labels=["subscriber"])
        depth = GaugeMetricFamily("sentinel_bus_queue_depth", "Current queue depth", labels=["subscriber"])
        fill = GaugeMetricFamily("sentinel_bus_queue_fill_ratio", "Queue depth / capacity", labels=["subscriber"])
        # Aggregate by name: many WebSocket clients share the "ws" name.
        agg: dict[str, list[float]] = {}
        for s in self.bus.subscriptions:
            a = agg.setdefault(s.name, [0, 0, 0])
            a[0] += s.dropped
            a[1] += s.depth()
            a[2] = max(a[2], s.depth() / s.maxsize)
        for name, (d, q, f) in agg.items():
            drops.add_metric([name], d)
            depth.add_metric([name], q)
            fill.add_metric([name], f)
        yield from (pub, drops, depth, fill)


_collector: BusCollector | None = None


def register_bus(bus: EventBus) -> None:
    """(Re)point the scrape-time collector at the current bus."""
    global _collector
    if _collector is not None:
        REGISTRY.unregister(_collector)
    _collector = BusCollector(bus)
    REGISTRY.register(_collector)
