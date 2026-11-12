"""Spec checks 23-32: in-memory bus."""

import asyncio
import logging
import time
from datetime import UTC, datetime, timedelta

import pytest

from gamedev_trends.bus import (
    ERASURES_TOPIC,
    OBSERVATIONS_TOPIC,
    InMemoryBroker,
    InMemoryPublisher,
    InMemorySubscriber,
    Publisher,
    Record,
    Subscriber,
    encode,
    partition_key,
)
from gamedev_trends.contract import Erasure, Event, Observation
from gamedev_trends.kafka_bus import KafkaPublisher, KafkaSubscriber

T10: datetime = datetime(2026, 11, 12, 10, 0, tzinfo=UTC)
POST: str = "at://did:plc:abc/app.bsky.feed.post/3m5xyz"


def steam_obs(n: int = 0) -> Observation:
    return Observation.create(
        source="steam",
        entity_type="game",
        entity_id="730",
        metric="players_current",
        value=n,
        observed_at=T10 + timedelta(minutes=n),
    )


def bsky_obs() -> Observation:
    return Observation.create(
        source="bluesky",
        entity_type="post",
        entity_id=POST,
        metric="likes",
        value=42,
        window_start=T10,
        observed_at=T10 + timedelta(hours=1),
    )


def erasure() -> Erasure:
    return Erasure.create(source="bluesky", entity_type="post", entity_id=POST, requested_at=T10)


def test_23_publish_writes_records() -> None:
    async def run() -> InMemoryBroker:
        broker: InMemoryBroker = InMemoryBroker()
        pub: InMemoryPublisher = InMemoryPublisher(broker)
        await pub.publish([steam_obs(), bsky_obs(), erasure()])
        return broker

    broker: InMemoryBroker = asyncio.run(run())
    obs: list[Record] = broker.records(OBSERVATIONS_TOPIC)
    assert [r.offset for r in obs] == [0, 1]
    assert [r.topic for r in obs] == [OBSERVATIONS_TOPIC] * 2
    expected: list[Event] = [steam_obs(), bsky_obs()]
    assert [r.key for r in obs] == [partition_key(e) for e in expected]
    assert [r.value for r in obs] == [encode(e) for e in expected]
    ers: list[Record] = broker.records(ERASURES_TOPIC)
    assert len(ers) == 1
    assert ers[0].offset == 0
    assert ers[0].value == encode(erasure())


async def test_23_publish_empty_writes_nothing() -> None:
    broker: InMemoryBroker = InMemoryBroker()
    await InMemoryPublisher(broker).publish([])
    assert broker.records(OBSERVATIONS_TOPIC) == []
    assert broker.records(ERASURES_TOPIC) == []
    assert broker.records("no.such.topic") == []


async def test_23_records_returns_copy() -> None:
    broker: InMemoryBroker = InMemoryBroker()
    await InMemoryPublisher(broker).publish([steam_obs()])
    broker.records(OBSERVATIONS_TOPIC).clear()
    assert len(broker.records(OBSERVATIONS_TOPIC)) == 1


async def test_24_subscriber_reads_in_order() -> None:
    broker: InMemoryBroker = InMemoryBroker()
    await InMemoryPublisher(broker).publish([steam_obs(), bsky_obs()])
    sub: InMemorySubscriber = InMemorySubscriber(broker, "store", [OBSERVATIONS_TOPIC])
    assert await sub.poll() == [steam_obs(), bsky_obs()]
    assert await sub.poll(timeout=0.05) == []


async def test_25_max_events() -> None:
    broker: InMemoryBroker = InMemoryBroker()
    await InMemoryPublisher(broker).publish([steam_obs(i) for i in range(5)])
    sub: InMemorySubscriber = InMemorySubscriber(broker, "store", [OBSERVATIONS_TOPIC])
    sizes: list[int] = [len(await sub.poll(max_events=2)) for _ in range(3)]
    assert sizes == [2, 2, 1]


async def test_26_groups_are_independent() -> None:
    broker: InMemoryBroker = InMemoryBroker()
    await InMemoryPublisher(broker).publish([steam_obs(0), steam_obs(1)])
    a: InMemorySubscriber = InMemorySubscriber(broker, "store", [OBSERVATIONS_TOPIC])
    b: InMemorySubscriber = InMemorySubscriber(broker, "detector", [OBSERVATIONS_TOPIC])
    assert await a.poll() == [steam_obs(0), steam_obs(1)]
    assert await b.poll() == [steam_obs(0), steam_obs(1)]


async def test_27_at_least_once() -> None:
    broker: InMemoryBroker = InMemoryBroker()
    pub: InMemoryPublisher = InMemoryPublisher(broker)
    events: list[Event] = [steam_obs(i) for i in range(3)]
    await pub.publish(events)
    first: InMemorySubscriber = InMemorySubscriber(broker, "g", [OBSERVATIONS_TOPIC])
    assert await first.poll() == events
    # no commit, no close: a new subscriber of the group starts from the committed position (none)
    second: InMemorySubscriber = InMemorySubscriber(broker, "g", [OBSERVATIONS_TOPIC])
    assert await second.poll() == events
    await second.commit()
    await second.close()
    third: InMemorySubscriber = InMemorySubscriber(broker, "g", [OBSERVATIONS_TOPIC])
    assert await third.poll(timeout=0.05) == []
    await pub.publish([steam_obs(10)])
    assert await third.poll() == [steam_obs(10)]


async def test_27_subscriber_keeps_position_without_commit() -> None:
    broker: InMemoryBroker = InMemoryBroker()
    await InMemoryPublisher(broker).publish([steam_obs(0), steam_obs(1)])
    sub: InMemorySubscriber = InMemorySubscriber(broker, "g", [OBSERVATIONS_TOPIC])
    assert await sub.poll(max_events=1) == [steam_obs(0)]
    assert await sub.poll(max_events=1) == [steam_obs(1)]


async def test_28_empty_poll_waits_about_timeout() -> None:
    sub: InMemorySubscriber = InMemorySubscriber(InMemoryBroker(), "g", [OBSERVATIONS_TOPIC])
    start: float = time.monotonic()
    assert await sub.poll(timeout=0.05) == []
    elapsed: float = time.monotonic() - start
    assert 0.04 <= elapsed <= 1.0


async def test_28_poll_returns_at_once_when_data_present() -> None:
    broker: InMemoryBroker = InMemoryBroker()
    await InMemoryPublisher(broker).publish([steam_obs()])
    sub: InMemorySubscriber = InMemorySubscriber(broker, "g", [OBSERVATIONS_TOPIC])
    start: float = time.monotonic()
    assert await sub.poll(timeout=10) == [steam_obs()]
    assert time.monotonic() - start < 1.0


async def test_28_waiting_poll_wakes_on_publish() -> None:
    broker: InMemoryBroker = InMemoryBroker()
    pub: InMemoryPublisher = InMemoryPublisher(broker)
    sub: InMemorySubscriber = InMemorySubscriber(broker, "g", [OBSERVATIONS_TOPIC])
    task: asyncio.Task[list[Event]] = asyncio.create_task(sub.poll(timeout=10))
    await asyncio.sleep(0.1)
    assert not task.done()
    start: float = time.monotonic()
    await pub.publish([steam_obs()])
    got: list[Event] = await asyncio.wait_for(task, timeout=2)
    assert got == [steam_obs()]
    assert time.monotonic() - start < 1.0


async def test_29_topics_in_subscription_order() -> None:
    broker: InMemoryBroker = InMemoryBroker()
    await InMemoryPublisher(broker).publish([erasure(), steam_obs(), bsky_obs()])
    sub: InMemorySubscriber = InMemorySubscriber(broker, "g", [OBSERVATIONS_TOPIC, ERASURES_TOPIC])
    assert await sub.poll() == [steam_obs(), bsky_obs(), erasure()]


async def test_30_garbage_skipped_with_warning(caplog: pytest.LogCaptureFixture) -> None:
    broker: InMemoryBroker = InMemoryBroker()
    pub: InMemoryPublisher = InMemoryPublisher(broker)
    await pub.publish([steam_obs(0)])
    broker.append_raw(OBSERVATIONS_TOPIC, b"k", b"garbage")
    await pub.publish([steam_obs(1)])
    sub: InMemorySubscriber = InMemorySubscriber(broker, "g", [OBSERVATIONS_TOPIC])
    with caplog.at_level(logging.WARNING, logger="gamedev_trends.bus"):
        got: list[Event] = await sub.poll()
    assert got == [steam_obs(0), steam_obs(1)]
    msgs: list[str] = [
        r.getMessage()
        for r in caplog.records
        if r.name == "gamedev_trends.bus" and r.levelno == logging.WARNING
    ]
    assert any(OBSERVATIONS_TOPIC in m and "1" in m for m in msgs)
    assert await sub.poll(timeout=0.05) == []  # garbage position counts as read


async def test_31_closed_publisher_raises() -> None:
    pub: InMemoryPublisher = InMemoryPublisher(InMemoryBroker())
    await pub.close()
    with pytest.raises(RuntimeError):
        await pub.publish([steam_obs()])
    await pub.close()


async def test_31_closed_subscriber_raises() -> None:
    sub: InMemorySubscriber = InMemorySubscriber(InMemoryBroker(), "g", [OBSERVATIONS_TOPIC])
    await sub.close()
    with pytest.raises(RuntimeError):
        await sub.poll(timeout=0.01)
    with pytest.raises(RuntimeError):
        await sub.commit()
    await sub.close()


async def test_32_inmemory_satisfies_protocols() -> None:
    broker: InMemoryBroker = InMemoryBroker()
    pub: Publisher = InMemoryPublisher(broker)
    sub: Subscriber = InMemorySubscriber(broker, "g", [OBSERVATIONS_TOPIC])
    await pub.publish([steam_obs()])
    assert await sub.poll() == [steam_obs()]


async def kafka_satisfies_protocols() -> None:
    """Never called: mypy and ty check that the Kafka classes satisfy the protocols."""
    pub: Publisher = KafkaPublisher("localhost:9092")
    sub: Subscriber = KafkaSubscriber("localhost:9092", "g", [OBSERVATIONS_TOPIC])
    await pub.close()
    await sub.close()
