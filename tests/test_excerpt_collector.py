"""Spec checks 12-18: the collector publishes the excerpt of a tagged post with the window."""

import pytest
from bluesky_support import (
    OBS_TOPIC,
    P1,
    SEC,
    A,
    FlakyPublisher,
    Rig,
    dt,
    erasure_of,
    erasures,
    example_events,
    excerpts,
    make_rig,
    observations,
    other,
    post_obs,
    stream_obs,
    t,
)

from gamedev_trends.bluesky import BlueskyCollector, InMemoryCursorStore, InMemoryPostIndex
from gamedev_trends.bus import InMemoryBroker, InMemoryPublisher, PublishError, Record, decode
from gamedev_trends.contract import Event, Excerpt, Observation
from gamedev_trends.jetstream import AccountStatus, JetstreamEvent, PostCreated, PostDeleted

C: str = "did:plc:c"
P3: str = "at://did:plc:c/app.bsky.feed.post/p3"
P2: str = "at://did:plc:b/app.bsky.feed.post/p2"


def ex(uri: str, text: str, seconds: float) -> Excerpt:
    return Excerpt.create(
        source="bluesky", entity_type="post", entity_id=uri, text=text, posted_at=dt(seconds)
    )


def example() -> list[JetstreamEvent | None]:
    return [
        PostCreated(did=A, rkey="p1", time_us=t(1), tags=("gamedev",), excerpt="Hello #gamedev"),
        PostCreated(did="did:plc:b", rkey="p2", time_us=t(2), tags=("cats",), excerpt="no tag here"),
        PostCreated(did=C, rkey="p3", time_us=t(3), tags=("godot",), excerpt=""),
        other(61),
    ]


def topic(rig: Rig) -> list[Event]:
    records: list[Record] = rig.broker.records(OBS_TOPIC)
    return [decode(r.value) for r in records]


async def test_12_worked_example() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle(example())
    expected: list[Observation] = [
        *stream_obs(0, events=3, posts=3, likes=0, reposts=0),
        post_obs(P1, "tag_gamedev", 1),
        post_obs(P3, "tag_godot", 1),
    ]
    assert observations(rig.broker) == expected
    assert excerpts(rig.broker) == [ex(P1, "Hello #gamedev", 1)]
    published: list[Event] = topic(rig)
    assert len(published) == 7
    assert published[:6] == expected
    assert published[6] == ex(P1, "Hello #gamedev", 1)


async def test_13_created_and_deleted_in_one_window() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle(
        [
            PostCreated(did=A, rkey="p1", time_us=t(1), tags=("gamedev",), excerpt="Hello"),
            PostDeleted(did=A, rkey="p1", time_us=t(3)),
            other(61),
        ]
    )
    assert excerpts(rig.broker) == []
    assert erasures(rig.broker) == [erasure_of(P1, 3)]


async def test_14_account_deleted_in_the_same_window() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle(
        [
            PostCreated(did=A, rkey="p1", time_us=t(1), tags=("gamedev",), excerpt="one"),
            PostCreated(did=A, rkey="p2", time_us=t(2), tags=("gamedev",), excerpt="two"),
            AccountStatus(did=A, time_us=t(3), active=False, status="deleted"),
            other(61),
        ]
    )
    assert excerpts(rig.broker) == []
    assert len(erasures(rig.broker)) == 2


async def test_15_too_long_uri_gives_neither_tag_row_nor_excerpt() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle(
        [
            PostCreated(did=A, rkey="r" * 1100, time_us=t(1), tags=("gamedev",), excerpt="long uri"),
            other(61),
        ]
    )
    assert excerpts(rig.broker) == []
    assert observations(rig.broker) == stream_obs(0, events=1, posts=1, likes=0, reposts=0)


async def test_16_replay_after_a_break_repeats_the_same_excerpt() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle(example())
    rig.collector.start(t(0))
    await rig.collector.handle(example())
    got: list[Excerpt] = excerpts(rig.broker)
    assert len(got) == 2
    assert got[0].event_id == got[1].event_id
    assert got[0].text == got[1].text == "Hello #gamedev"


async def test_17_publish_failure_then_retry_gives_one_excerpt() -> None:
    broker: InMemoryBroker = InMemoryBroker()
    cursor: InMemoryCursorStore = InMemoryCursorStore()
    collector: BlueskyCollector = BlueskyCollector(
        InMemoryPostIndex(), FlakyPublisher(InMemoryPublisher(broker), 1), cursor
    )
    collector.start(None)
    with pytest.raises(PublishError):
        await collector.handle(example())
    assert excerpts(broker) == []
    collector.start(None)
    await collector.handle(example())
    assert excerpts(broker) == [ex(P1, "Hello #gamedev", 1)]
    assert cursor.value == t(0) + 60 * SEC


async def test_18_example_without_excerpts_gives_none() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle(example_events())
    assert len(observations(rig.broker)) == 11
    assert excerpts(rig.broker) == []
    assert [type(e) for e in topic(rig)] == [Observation] * 11


async def test_12_excerpts_follow_first_appearance_order() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle(
        [
            PostCreated(did=A, rkey="p1", time_us=t(1), tags=("gamedev",), excerpt="first"),
            PostCreated(did=C, rkey="p3", time_us=t(2), tags=("godot",), excerpt="second"),
            other(61),
        ]
    )
    assert [e.entity_id for e in excerpts(rig.broker)] == [P1, P3]
    assert isinstance(topic(rig)[-1], Excerpt)
