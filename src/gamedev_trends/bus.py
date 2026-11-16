"""Event bus: topics, wire format, publisher and subscriber protocols, in-memory implementation."""

import asyncio
import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Annotated, Protocol

from pydantic import Field, TypeAdapter

from gamedev_trends.contract import Erasure, Event, Excerpt, Observation

OBSERVATIONS_TOPIC: str = "radar.observations.v1"
ERASURES_TOPIC: str = "radar.erasures.v1"

logger: logging.Logger = logging.getLogger(__name__)
_EVENT_ADAPTER: TypeAdapter[Observation | Erasure | Excerpt] = TypeAdapter(
    Annotated[Observation | Erasure | Excerpt, Field(discriminator="kind")]
)


def topic_for(event: Event) -> str:
    return ERASURES_TOPIC if isinstance(event, Erasure) else OBSERVATIONS_TOPIC


def partition_key(event: Event) -> bytes:
    return f"{event.source}|{event.entity_type}|{event.entity_id}".encode()


def encode(event: Event) -> bytes:
    return event.model_dump_json().encode("utf-8")


def decode(data: bytes) -> Event:
    return _EVENT_ADAPTER.validate_json(data)


class PublishError(Exception):
    """At least one event was not acknowledged by the broker."""


class Publisher(Protocol):
    async def publish(self, events: Sequence[Event]) -> None: ...
    async def close(self) -> None: ...


class Subscriber(Protocol):
    async def poll(self, max_events: int = 500, timeout: float = 1.0) -> list[Event]: ...
    async def commit(self) -> None: ...
    async def close(self) -> None: ...


@dataclass(frozen=True)
class Record:
    topic: str
    offset: int
    key: bytes
    value: bytes


class InMemoryBroker:
    def __init__(self) -> None:
        self._logs: dict[str, list[Record]] = {}
        self._committed: dict[tuple[str, str], int] = {}
        self._waiters: list[asyncio.Future[None]] = []

    def records(self, topic: str) -> list[Record]:
        return list(self._logs.get(topic, []))

    def append_raw(self, topic: str, key: bytes, value: bytes) -> None:
        log: list[Record] = self._logs.setdefault(topic, [])
        log.append(Record(topic=topic, offset=len(log), key=key, value=value))
        waiters: list[asyncio.Future[None]] = self._waiters
        self._waiters = []
        waiter: asyncio.Future[None]
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    def _committed_offset(self, group: str, topic: str) -> int:
        return self._committed.get((group, topic), 0)

    def _commit(self, group: str, topic: str, offset: int) -> None:
        self._committed[(group, topic)] = offset

    async def _wait_for_append(self, timeout: float) -> None:
        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append(waiter)
        try:
            await asyncio.wait_for(waiter, timeout)
        except TimeoutError:
            pass
        finally:
            if waiter in self._waiters:
                self._waiters.remove(waiter)


class InMemoryPublisher:
    def __init__(self, broker: InMemoryBroker) -> None:
        self._broker: InMemoryBroker = broker
        self._closed: bool = False

    async def publish(self, events: Sequence[Event]) -> None:
        if self._closed:
            raise RuntimeError("publisher is closed")
        event: Event
        for event in events:
            self._broker.append_raw(topic_for(event), partition_key(event), encode(event))

    async def close(self) -> None:
        self._closed = True


class InMemorySubscriber:
    def __init__(self, broker: InMemoryBroker, group: str, topics: Sequence[str]) -> None:
        self._broker: InMemoryBroker = broker
        self._group: str = group
        self._topics: list[str] = list(topics)
        self._positions: dict[str, int] = {t: broker._committed_offset(group, t) for t in self._topics}
        self._closed: bool = False

    def _take(self, max_events: int) -> list[Event]:
        out: list[Event] = []
        topic: str
        for topic in self._topics:
            log: list[Record] = self._broker.records(topic)
            while len(out) < max_events and self._positions[topic] < len(log):
                record: Record = log[self._positions[topic]]
                self._positions[topic] += 1
                try:
                    out.append(decode(record.value))
                except ValueError:
                    logger.warning("skipping undecodable record: topic=%s offset=%d", topic, record.offset)
        return out

    async def poll(self, max_events: int = 500, timeout: float = 1.0) -> list[Event]:
        if self._closed:
            raise RuntimeError("subscriber is closed")
        deadline: float = time.monotonic() + timeout
        while True:
            events: list[Event] = self._take(max_events)
            if events:
                return events
            remaining: float = deadline - time.monotonic()
            if remaining <= 0:
                return []
            await self._broker._wait_for_append(remaining)
            if self._closed:
                raise RuntimeError("subscriber is closed")

    async def commit(self) -> None:
        if self._closed:
            raise RuntimeError("subscriber is closed")
        topic: str
        for topic in self._topics:
            self._broker._commit(self._group, topic, self._positions[topic])

    async def close(self) -> None:
        self._closed = True
