"""Spec checks 1-5: the Excerpt event, its wire format, topic, partition key and in-memory delivery."""

import asyncio
import json
from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from gamedev_trends.bus import (
    OBSERVATIONS_TOPIC,
    InMemoryBroker,
    InMemoryPublisher,
    InMemorySubscriber,
    decode,
    encode,
    partition_key,
    topic_for,
)
from gamedev_trends.contract import EXCERPT_MAX_LENGTH, Erasure, Event, Excerpt, Observation, make_event_id

P1: str = "at://did:plc:a/app.bsky.feed.post/p1"
T: datetime = datetime(2026, 11, 15, 12, 0, 1, tzinfo=UTC)


def make(
    *, text: str = "Hello", posted_at: datetime = T, entity_id: str = P1, source: str = "bluesky"
) -> Excerpt:
    return Excerpt.create(
        source=source, entity_type="post", entity_id=entity_id, text=text, posted_at=posted_at
    )


def test_01_create_and_event_id() -> None:
    e: Excerpt = make()
    assert e.kind == "excerpt"
    assert e.event_id == make_event_id("excerpt", "bluesky", "post", P1)
    assert e.text == "Hello"
    assert e.posted_at == T


def test_01_event_id_ignores_text_and_time() -> None:
    other: Excerpt = make(text="Different", posted_at=T + timedelta(hours=3))
    assert other.event_id == make().event_id
    assert make(entity_id=P1 + "x").event_id != make().event_id


def test_02_limit_is_140_code_points() -> None:
    assert EXCERPT_MAX_LENGTH == 140
    assert len(make(text="\U0001f3ae" * 140).text) == 140


@pytest.mark.parametrize("text", ["\U0001f3ae" * 141, "x" * 141, "", "a\ud800b"])
def test_02_bad_text_is_rejected(text: str) -> None:
    with pytest.raises(ValidationError):
        make(text=text)


def test_02_naive_time_is_rejected() -> None:
    with pytest.raises(ValidationError):
        make(posted_at=datetime(2026, 11, 15, 12, 0, 1))


def test_02_other_zone_is_normalised_to_utc() -> None:
    e: Excerpt = make(posted_at=datetime(2026, 11, 15, 15, 0, 1, tzinfo=timezone(timedelta(hours=3))))
    assert e.posted_at == T
    assert e.posted_at.utcoffset() == timedelta(0)


def test_02_model_is_frozen() -> None:
    e: Excerpt = make()
    with pytest.raises(ValidationError):
        e.text = "changed"  # type: ignore[misc]  # ty: ignore[invalid-assignment]


def test_03_roundtrip() -> None:
    e: Excerpt = make(text="Привет \U0001f3ae")
    back: Event = decode(encode(e))
    assert type(back) is Excerpt
    assert back == e
    assert back.text == "Привет \U0001f3ae"


def test_03_json_key_order() -> None:
    data: dict[str, object] = json.loads(encode(make()))
    assert list(data) == ["kind", "event_id", "source", "entity_type", "entity_id", "text", "posted_at"]
    assert data["kind"] == "excerpt"


def test_03_old_types_still_decode_by_kind() -> None:
    obs: Observation = Observation.create(
        source="steam", entity_type="game", entity_id="730", metric="m", value=1, observed_at=T
    )
    gone: Erasure = Erasure.create(source="steam", entity_type="game", entity_id="730", requested_at=T)
    assert type(decode(encode(obs))) is Observation
    assert type(decode(encode(gone))) is Erasure


def test_04_topic_and_partition_key() -> None:
    e: Excerpt = make()
    assert topic_for(e) == "radar.observations.v1"
    assert partition_key(e) == f"bluesky|post|{P1}".encode()
    gone: Erasure = Erasure.create(source="bluesky", entity_type="post", entity_id=P1, requested_at=T)
    assert topic_for(gone) == "radar.erasures.v1"


async def test_05_in_memory_bus_carries_the_excerpt() -> None:
    broker: InMemoryBroker = InMemoryBroker()
    e: Excerpt = make()
    events: list[Event] = [e]
    await InMemoryPublisher(broker).publish(events)
    assert [r.topic for r in broker.records(OBSERVATIONS_TOPIC)] == [OBSERVATIONS_TOPIC]
    sub: InMemorySubscriber = InMemorySubscriber(broker, "store", [OBSERVATIONS_TOPIC])
    got: list[Event] = await asyncio.wait_for(sub.poll(timeout=0.5), 5)
    assert got == [e]
    assert type(got[0]) is Excerpt
