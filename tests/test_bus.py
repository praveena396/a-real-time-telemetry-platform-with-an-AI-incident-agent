
import pytest

from app.bus import EventBus
from app.events import Anomaly, Event, Reading


def reading(v=1.0):
    return Reading(device_id="d", metric="m", value=v)


async def drain(sub):
    return [e async for e in sub]


@pytest.mark.asyncio
async def test_delivers_to_subscriber():
    bus = EventBus()
    sub = bus.subscribe(Reading)
    bus.publish(reading(1.0))
    bus.close()
    assert [e.value for e in await drain(sub)] == [1.0]


@pytest.mark.asyncio
async def test_routes_by_type():
    bus = EventBus()
    readings = bus.subscribe(Reading)
    anomalies = bus.subscribe(Anomaly)
    everything = bus.subscribe(Event)  # base class receives all subclasses
    bus.publish(reading())
    bus.publish(Anomaly(device_id="d", metric="m", value=9.0, score=5.0, fault="spike"))
    bus.close()
    assert len(await drain(readings)) == 1
    assert len(await drain(anomalies)) == 1
    assert len(await drain(everything)) == 2


@pytest.mark.asyncio
async def test_full_queue_drops_oldest_and_counts():
    bus = EventBus()
    sub = bus.subscribe(Reading, maxsize=3)
    for v in range(5):
        bus.publish(reading(float(v)))
    assert sub.dropped == 2
    bus.close()
    # close() makes room for the sentinel by dropping one more oldest event
    assert [e.value for e in await drain(sub)] == [3.0, 4.0]


@pytest.mark.asyncio
async def test_slow_consumer_does_not_block_publisher():
    bus = EventBus()
    bus.subscribe(Reading, maxsize=10)  # nobody consumes this
    fast = bus.subscribe(Reading, maxsize=10_000)
    for v in range(1000):
        bus.publish(reading(float(v)))
    bus.close()
    assert len(await drain(fast)) == 1000


@pytest.mark.asyncio
async def test_publish_after_close_raises():
    bus = EventBus()
    bus.close()
    with pytest.raises(RuntimeError):
        bus.publish(reading())
