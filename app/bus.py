"""A typed asyncio pub/sub event bus with per-subscriber bounded queues.

Design decisions (be ready to explain these in interviews):
- Each subscriber gets its own bounded queue, so a slow consumer never blocks
  the publisher or other consumers.
- Backpressure policy: when a queue is full, drop the OLDEST event. For live
  telemetry, fresh data is worth more than stale data. Drops are counted.
- Routing is by event type (isinstance), so subscribing to a base class
  receives all of its subclasses.
- Shutdown pushes a sentinel into every queue so consumers exit cleanly.
- Subscribers can come and go at runtime (e.g. browser WebSocket clients);
  unsubscribing closes only that subscriber's queue.
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any, Generic, TypeVar

from .events import Event

E = TypeVar("E", bound=Event)
_CLOSED = object()


class Subscription(Generic[E]):
    def __init__(self, event_type: type[E], maxsize: int, name: str) -> None:
        self.event_type = event_type
        self.name = name
        self.maxsize = maxsize
        self.queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=maxsize)
        self.dropped = 0
        self.delivered = 0

    def offer(self, event: object) -> None:
        if self.queue.full():
            self.queue.get_nowait()  # drop oldest to make room
            self.dropped += 1
        self.queue.put_nowait(event)
        self.delivered += 1

    def _close(self) -> None:
        if self.queue.full():
            self.queue.get_nowait()
        self.queue.put_nowait(_CLOSED)

    def depth(self) -> int:
        return self.queue.qsize()

    async def get_batch(self, max_items: int, wait: float) -> list[E] | None:
        """Wait up to `wait` seconds for events, return up to `max_items` of them.

        Returns None once the subscription is closed. Used by consumers that
        prefer batches (DB writer, WebSocket fan-out) over one-at-a-time.
        """
        items: list[E] = []
        try:
            first = await asyncio.wait_for(self.queue.get(), wait)
        except TimeoutError:
            return items
        if first is _CLOSED:
            return None
        items.append(first)
        while len(items) < max_items and not self.queue.empty():
            nxt = self.queue.get_nowait()
            if nxt is _CLOSED:
                self.queue.put_nowait(_CLOSED)  # re-arm so the next call ends
                break
            items.append(nxt)
        return items

    async def __aiter__(self) -> AsyncIterator[E]:
        while True:
            item = await self.queue.get()
            if item is _CLOSED:
                return
            yield item


class EventBus:
    def __init__(self) -> None:
        self._subs: list[Subscription[Any]] = []
        self.published = 0
        self._closed = False

    def subscribe(self, event_type: type[E], *, maxsize: int = 1000, name: str = "") -> Subscription[E]:
        sub: Subscription[E] = Subscription(event_type, maxsize, name or event_type.__name__)
        self._subs.append(sub)
        return sub

    def unsubscribe(self, sub: Subscription[Any]) -> None:
        if sub in self._subs:
            self._subs.remove(sub)
            sub._close()

    def publish(self, event: Event) -> None:
        if self._closed:
            raise RuntimeError("bus is closed")
        self.published += 1
        for sub in self._subs:
            if isinstance(event, sub.event_type):
                sub.offer(event)

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        self._closed = True
        for sub in self._subs:
            sub._close()

    @property
    def subscriptions(self) -> list[Subscription[Any]]:
        return list(self._subs)

    def stats(self) -> dict[str, int]:
        return {"published": self.published, **{f"dropped[{s.name}]": s.dropped for s in self._subs}}
