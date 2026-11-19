"""Spec checks 34-36: real Kafka on localhost:9092. Run with `uv run pytest -m integration`."""

import asyncio
import logging
import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin._metadata import ClusterMetadata

from gamedev_trends.bus import ERASURES_TOPIC, OBSERVATIONS_TOPIC, partition_key
from gamedev_trends.contract import Erasure, Event, Observation
from gamedev_trends.kafka_bus import KafkaPublisher, KafkaSubscriber

pytestmark: pytest.MarkDecorator = pytest.mark.integration

BOOTSTRAP: str = "localhost:9092"
T10: datetime = datetime(2026, 11, 12, 10, 0, tzinfo=UTC)


@pytest.fixture(autouse=True, scope="module")
def topics_exist() -> None:
    """Fail fast when kafka-init has not created the topics yet.

    A consumer subscribed to a topic that does not exist yet does not see it until the next metadata
    refresh, while the producer waits for the topic and delivers. Started during kafka-init, test 34
    then read 10 events of 11: the Erasure on the topic created second was never polled.
    """
    metadata: ClusterMetadata = AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(timeout=10)
    missing: list[str] = sorted({OBSERVATIONS_TOPIC, ERASURES_TOPIC} - set(metadata.topics))
    if missing:
        pytest.fail(f"topics missing: {', '.join(missing)}; run `docker compose up -d --wait kafka-init`")


def make_obs(entity_id: str, i: int) -> Observation:
    return Observation.create(
        source="steam",
        entity_type="game",
        entity_id=entity_id,
        metric="players_current",
        value=i,
        observed_at=T10 + timedelta(minutes=i),
    )


def make_batch() -> tuple[str, list[Event]]:
    entity: str = f"it-{uuid.uuid4()}"
    events: list[Event] = [make_obs(entity, i) for i in range(10)]
    events.append(
        Erasure.create(source="steam", entity_type="game", entity_id=f"{entity}-gone", requested_at=T10)
    )
    return entity, events


async def collect(sub: KafkaSubscriber, want: set[uuid.UUID], limit: float) -> list[Event]:
    got: list[Event] = []
    deadline: float = time.monotonic() + limit
    while time.monotonic() < deadline and not want <= {e.event_id for e in got}:
        got += [e for e in await sub.poll(timeout=1.0) if e.event_id in want]
    return got


async def test_34_publish_and_read_back() -> None:
    entity: str
    events: list[Event]
    entity, events = make_batch()
    group: str = f"it-{uuid.uuid4()}"
    sub: KafkaSubscriber = KafkaSubscriber(BOOTSTRAP, group, [OBSERVATIONS_TOPIC, ERASURES_TOPIC])
    pub: KafkaPublisher = KafkaPublisher(BOOTSTRAP)
    try:
        await pub.publish(events)
        got: list[Event] = await collect(sub, {e.event_id for e in events}, 30)
    finally:
        await pub.close()
        await sub.close()
    assert len(got) == 11
    assert {e.event_id: e for e in got} == {e.event_id: e for e in events}
    ours: list[Event] = [e for e in got if isinstance(e, Observation) and e.entity_id == entity]
    assert ours == events[:10]


async def test_35_at_least_once() -> None:
    events: list[Event]
    _: str
    _, events = make_batch()
    want: set[uuid.UUID] = {e.event_id for e in events}
    group: str = f"it-{uuid.uuid4()}"
    topics: list[str] = [OBSERVATIONS_TOPIC, ERASURES_TOPIC]
    pub: KafkaPublisher = KafkaPublisher(BOOTSTRAP)
    try:
        await pub.publish(events)
    finally:
        await pub.close()
    first: KafkaSubscriber = KafkaSubscriber(BOOTSTRAP, group, topics)
    try:
        assert len(await collect(first, want, 30)) == 11
    finally:
        await first.close()
    second: KafkaSubscriber = KafkaSubscriber(BOOTSTRAP, group, topics)
    try:
        assert len(await collect(second, want, 30)) == 11
        await second.commit()
    finally:
        await second.close()
    third: KafkaSubscriber = KafkaSubscriber(BOOTSTRAP, group, topics)
    try:
        assert await collect(third, want, 10) == []
    finally:
        await third.close()


async def test_36_garbage_is_skipped(caplog: pytest.LogCaptureFixture) -> None:
    obs: Observation = make_obs(f"it-{uuid.uuid4()}", 0)
    raw: Producer = Producer({"bootstrap.servers": BOOTSTRAP})
    raw.produce(OBSERVATIONS_TOPIC, value=b"garbage", key=partition_key(obs))
    raw.flush(10)
    pub: KafkaPublisher = KafkaPublisher(BOOTSTRAP)
    sub: KafkaSubscriber = KafkaSubscriber(BOOTSTRAP, f"it-{uuid.uuid4()}", [OBSERVATIONS_TOPIC])
    try:
        await pub.publish([obs])
        with caplog.at_level(logging.WARNING, logger="gamedev_trends.kafka_bus"):
            got: list[Event] = await collect(sub, {obs.event_id}, 30)
    finally:
        await pub.close()
        await sub.close()
    assert got == [obs]
    assert any(r.name == "gamedev_trends.kafka_bus" and r.levelno == logging.WARNING for r in caplog.records)
    await asyncio.sleep(0)
