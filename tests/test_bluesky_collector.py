"""Spec checks 14-28: windows, what goes to Kafka, deletions, late events, restarts, validation."""

from typing import Any

import pytest
from bluesky_support import (
    P1,
    P2,
    SEC,
    A,
    FlakyPublisher,
    Rig,
    dt,
    erasure_of,
    erasures,
    example_events,
    make_rig,
    observations,
    other,
    post_obs,
    stream_obs,
    t,
)

from gamedev_trends.bluesky import (
    BlueskyCollector,
    InMemoryCursorStore,
    InMemoryPostIndex,
    Stats,
)
from gamedev_trends.bus import InMemoryBroker, InMemoryPublisher, PublishError
from gamedev_trends.contract import Observation
from gamedev_trends.jetstream import (
    AccountStatus,
    Engagement,
    JetstreamEvent,
    PostCreated,
    PostDeleted,
)

EXAMPLE_WINDOW_1: list[Observation] = [
    *stream_obs(0, events=6, posts=2, likes=3, reposts=1),
    post_obs(P1, "tag_gamedev", 1),
    post_obs(P1, "likes", 2),
    post_obs(P1, "reposts", 1),
]
EXAMPLE_WINDOW_2: list[Observation] = stream_obs(60, events=3, posts=0, likes=1, reposts=0)


async def test_14_worked_example() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle(example_events())
    assert observations(rig.broker) == EXAMPLE_WINDOW_1 + EXAMPLE_WINDOW_2
    assert erasures(rig.broker) == [erasure_of(P1, 62)]
    assert rig.cursor.value == t(120)
    assert rig.collector.window_start_us == t(120)
    assert rig.collector.stats == Stats(
        events=10,
        posts=2,
        likes=4,
        reposts=1,
        tagged_posts=1,
        engagements=3,
        erasures=1,
        windows=2,
        late=0,
        malformed=0,
    )


async def test_14_the_example_split_across_handle_calls_gives_the_same_result() -> None:
    rig: Rig = make_rig()
    event: JetstreamEvent | None
    for event in example_events():
        await rig.collector.handle([event])
    assert observations(rig.broker) == EXAMPLE_WINDOW_1 + EXAMPLE_WINDOW_2
    assert erasures(rig.broker) == [erasure_of(P1, 62)]


async def test_15_cursor_after_the_first_window_and_no_erasures_yet() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle(example_events()[:7])
    assert rig.cursor.value == t(60)
    assert erasures(rig.broker) == []
    assert observations(rig.broker) == EXAMPLE_WINDOW_1


async def test_15_nothing_is_published_while_the_window_is_open() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle(example_events()[:6])
    assert observations(rig.broker) == []
    assert rig.cursor.value is None
    assert rig.collector.window_start_us == t(0)


async def test_16_whitelist_is_case_insensitive() -> None:
    rig: Rig = make_rig(tags=("GameDev",))
    await rig.collector.handle([PostCreated(did=A, rkey="p1", time_us=t(1), tags=("gamedev",)), other(61)])
    assert post_obs(P1, "tag_gamedev", 1) in observations(rig.broker)


async def test_16_matching_tags_give_one_row_each_in_alphabetical_order() -> None:
    rig: Rig = make_rig()
    post: PostCreated = PostCreated(did=A, rkey="p1", time_us=t(1), tags=("pixelart", "gamedev", "cats"))
    await rig.collector.handle([post, other(61)])
    posts: list[Observation] = [o for o in observations(rig.broker) if o.entity_type == "post"]
    assert posts == [post_obs(P1, "tag_gamedev", 1), post_obs(P1, "tag_pixelart", 1)]


async def test_16_posts_without_a_whitelisted_tag_are_not_tracked() -> None:
    rig: Rig = make_rig()
    post: PostCreated = PostCreated(did=A, rkey="p1", time_us=t(1), tags=("cats",))
    like: Engagement = Engagement(kind="like", subject_uri=P1, time_us=t(2))
    await rig.collector.handle([post, like, other(61)])
    assert [o for o in observations(rig.broker) if o.entity_type == "post"] == []
    assert await rig.index.posted_at(P1) is None
    assert rig.collector.stats.tagged_posts == 0
    assert rig.collector.stats.engagements == 0


async def test_16_posts_are_listed_in_order_of_first_appearance() -> None:
    rig: Rig = make_rig()
    p2: PostCreated = PostCreated(did="did:plc:b", rkey="p2", time_us=t(1), tags=("godot",))
    p1: PostCreated = PostCreated(did=A, rkey="p1", time_us=t(2), tags=("gamedev",))
    await rig.collector.handle([p2, p1, other(61)])
    posts: list[Observation] = [o for o in observations(rig.broker) if o.entity_type == "post"]
    assert posts == [post_obs(P2, "tag_godot", 1), post_obs(P1, "tag_gamedev", 1)]


async def test_16_likes_are_listed_by_first_like_and_reposts_follow_the_likes() -> None:
    rig: Rig = make_rig()
    events: list[JetstreamEvent | None] = [
        PostCreated(did=A, rkey="p1", time_us=t(1), tags=("gamedev",)),
        PostCreated(did="did:plc:b", rkey="p2", time_us=t(2), tags=("gamedev",)),
        Engagement(kind="repost", subject_uri=P1, time_us=t(3)),
        Engagement(kind="like", subject_uri=P2, time_us=t(4)),
        Engagement(kind="like", subject_uri=P1, time_us=t(5)),
        Engagement(kind="like", subject_uri=P2, time_us=t(6)),
        other(61),
    ]
    await rig.collector.handle(events)
    assert observations(rig.broker) == [
        *stream_obs(0, events=6, posts=2, likes=3, reposts=1),
        post_obs(P1, "tag_gamedev", 1),
        post_obs(P2, "tag_gamedev", 1),
        post_obs(P2, "likes", 2),
        post_obs(P1, "likes", 1),
        post_obs(P1, "reposts", 1),
    ]


async def test_17_tracking_window_is_by_event_time() -> None:
    rig: Rig = make_rig()
    day: int = 24 * 3600 * SEC
    events: list[JetstreamEvent | None] = [
        PostCreated(did=A, rkey="p1", time_us=t(0), tags=("gamedev",)),
        Engagement(kind="like", subject_uri=P1, time_us=t(0) + day - 1),
        Engagement(kind="like", subject_uri=P1, time_us=t(0) + day),
        other(86400 + 60),
    ]
    await rig.collector.handle(events)
    likes: list[Observation] = [
        o for o in observations(rig.broker) if o.metric == "likes" and o.entity_type == "post"
    ]
    assert likes == [post_obs(P1, "likes", 1, window=86340)]
    assert rig.collector.stats.engagements == 1
    assert rig.collector.stats.likes == 2


async def test_17_track_hours_is_configurable() -> None:
    rig: Rig = make_rig(track_hours=1)
    hour: int = 3600 * SEC
    events: list[JetstreamEvent | None] = [
        PostCreated(did=A, rkey="p1", time_us=t(0), tags=("gamedev",)),
        Engagement(kind="like", subject_uri=P1, time_us=t(0) + hour - 1),
        Engagement(kind="like", subject_uri=P1, time_us=t(0) + hour),
        other(3600 + 60),
    ]
    await rig.collector.handle(events)
    assert rig.collector.stats.engagements == 1


async def test_18_post_created_and_deleted_in_one_window() -> None:
    rig: Rig = make_rig()
    events: list[JetstreamEvent | None] = [
        PostCreated(did=A, rkey="p1", time_us=t(1), tags=("gamedev",)),
        Engagement(kind="like", subject_uri=P1, time_us=t(2)),
        PostDeleted(did=A, rkey="p1", time_us=t(3)),
        other(61),
    ]
    await rig.collector.handle(events)
    assert observations(rig.broker) == stream_obs(0, events=3, posts=1, likes=1, reposts=0)
    assert erasures(rig.broker) == [erasure_of(P1, 3)]


async def test_19_deleting_an_untracked_post_gives_no_erasure() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle([PostDeleted(did=A, rkey="p9", time_us=t(1)), other(61)])
    assert erasures(rig.broker) == []
    assert rig.collector.stats.erasures == 0
    assert observations(rig.broker) == stream_obs(0, events=1, posts=0, likes=0, reposts=0)


async def test_19_deleting_the_same_post_twice_gives_one_erasure() -> None:
    rig: Rig = make_rig()
    events: list[JetstreamEvent | None] = [
        PostCreated(did=A, rkey="p1", time_us=t(1), tags=("gamedev",)),
        PostDeleted(did=A, rkey="p1", time_us=t(2)),
        PostDeleted(did=A, rkey="p1", time_us=t(3)),
        other(61),
    ]
    await rig.collector.handle(events)
    assert erasures(rig.broker) == [erasure_of(P1, 2)]


async def test_19_erasures_keep_the_order_of_deletion() -> None:
    rig: Rig = make_rig()
    events: list[JetstreamEvent | None] = [
        PostCreated(did=A, rkey="p1", time_us=t(1), tags=("gamedev",)),
        PostCreated(did="did:plc:b", rkey="p2", time_us=t(2), tags=("gamedev",)),
        PostDeleted(did="did:plc:b", rkey="p2", time_us=t(3)),
        PostDeleted(did=A, rkey="p1", time_us=t(4)),
        other(61),
    ]
    await rig.collector.handle(events)
    assert erasures(rig.broker) == [erasure_of(P2, 3), erasure_of(P1, 4)]


def _account_scenario(status: str | None, active: bool) -> list[JetstreamEvent | None]:
    return [
        PostCreated(did=A, rkey="p1", time_us=t(1), tags=("gamedev",)),
        PostCreated(did=A, rkey="p2", time_us=t(2), tags=("gamedev",)),
        other(61),
        Engagement(kind="like", subject_uri=P1, time_us=t(62)),
        AccountStatus(did=A, time_us=t(63), active=active, status=status),
        other(121),
    ]


@pytest.mark.parametrize("status", ["deleted", "deactivated", "takendown", "suspended"])
async def test_20_erasing_statuses_erase_every_post_of_the_account(status: str) -> None:
    rig: Rig = make_rig()
    await rig.collector.handle(_account_scenario(status, False))
    pa2: str = "at://did:plc:a/app.bsky.feed.post/p2"
    window_2: list[Observation] = [o for o in observations(rig.broker) if o.window_start == dt(60)]
    assert window_2 == stream_obs(60, events=3, posts=0, likes=1, reposts=0)
    assert erasures(rig.broker) == [erasure_of(P1, 63), erasure_of(pa2, 63)]
    assert await rig.index.posted_at(P1) is None
    assert await rig.index.posted_at(pa2) is None
    assert rig.collector.stats.erasures == 2


@pytest.mark.parametrize(
    ("status", "active"),
    [("desynchronized", False), (None, False), ("throttled", False), ("deleted", True)],
)
async def test_20_other_account_states_erase_nothing(status: str | None, active: bool) -> None:
    rig: Rig = make_rig()
    await rig.collector.handle(_account_scenario(status, active))
    window_2: list[Observation] = [o for o in observations(rig.broker) if o.window_start == dt(60)]
    assert window_2 == [*stream_obs(60, events=3, posts=0, likes=1, reposts=0), post_obs(P1, "likes", 1, 60)]
    assert erasures(rig.broker) == []
    assert await rig.index.posted_at(P1) == t(1)


async def test_20_account_erasure_leaves_other_authors_alone() -> None:
    rig: Rig = make_rig()
    events: list[JetstreamEvent | None] = [
        PostCreated(did=A, rkey="p1", time_us=t(1), tags=("gamedev",)),
        PostCreated(did="did:plc:b", rkey="p2", time_us=t(2), tags=("gamedev",)),
        AccountStatus(did=A, time_us=t(3), active=False, status="deleted"),
        other(61),
    ]
    await rig.collector.handle(events)
    assert erasures(rig.broker) == [erasure_of(P1, 3)]
    assert await rig.index.posted_at(P2) == t(2)


async def test_21_events_before_the_resume_point_are_late() -> None:
    rig: Rig = make_rig()
    rig.collector.start(t(60))
    await rig.collector.handle([PostCreated(did=A, rkey="p1", time_us=t(59), tags=("gamedev",))])
    assert rig.collector.stats.late == 1
    assert rig.collector.stats.events == 1
    assert rig.collector.stats.posts == 0
    assert await rig.index.posted_at(P1) is None
    assert rig.collector.window_start_us is None
    await rig.collector.handle([other(60)])
    assert rig.collector.window_start_us == t(60)
    assert rig.collector.stats.late == 1


async def test_21_events_before_the_open_window_are_late() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle([other(70)])
    assert rig.collector.window_start_us == t(60)
    await rig.collector.handle([PostCreated(did=A, rkey="p1", time_us=t(30), tags=("gamedev",))])
    assert rig.collector.stats.late == 1
    assert await rig.index.posted_at(P1) is None
    assert rig.collector.window_start_us == t(60)


async def test_22_malformed_frames_are_counted_and_open_no_window() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle([None])
    assert rig.collector.stats.malformed == 1
    assert rig.collector.stats.events == 0
    assert rig.collector.window_start_us is None
    assert observations(rig.broker) == []


async def test_23_replay_after_a_break_is_idempotent() -> None:
    rig: Rig = make_rig()
    first: list[JetstreamEvent | None] = example_events()[:7]
    await rig.collector.handle(first)
    rig.collector.start(t(0))
    await rig.collector.handle(first)
    published: list[Observation] = observations(rig.broker)
    assert len(published) == 14
    assert published[:7] == published[7:]
    assert [o.event_id for o in published[:7]] == [o.event_id for o in published[7:]]


async def test_24_a_break_in_the_middle_of_a_window_does_not_double_count() -> None:
    rig: Rig = make_rig()
    events: list[JetstreamEvent | None] = example_events()
    await rig.collector.handle(events[:6])
    rig.collector.start(t(0))
    await rig.collector.handle(events[:7])
    assert observations(rig.broker) == EXAMPLE_WINDOW_1
    assert rig.cursor.value == t(60)


async def test_25_publish_failure_keeps_the_cursor_and_the_window() -> None:
    broker: InMemoryBroker = InMemoryBroker()
    index: InMemoryPostIndex = InMemoryPostIndex()
    cursor: InMemoryCursorStore = InMemoryCursorStore()
    collector: BlueskyCollector = BlueskyCollector(
        index, FlakyPublisher(InMemoryPublisher(broker), 1), cursor
    )
    collector.start(None)
    with pytest.raises(PublishError):
        await collector.handle(example_events()[:7])
    assert cursor.value is None
    assert collector.stats.windows == 0
    assert collector.window_start_us == t(0)
    assert observations(broker) == []
    collector.start(None)
    await collector.handle(example_events()[:7])
    assert observations(broker) == EXAMPLE_WINDOW_1
    assert cursor.value == t(60)
    assert collector.stats.windows == 1


async def test_26_skipped_windows_are_not_published() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle([other(1), other(300)])
    assert observations(rig.broker) == stream_obs(0, events=1, posts=0, likes=0, reposts=0)
    assert rig.cursor.value == t(300)
    assert rig.collector.stats.windows == 1


async def test_27_a_too_long_uri_is_not_tracked() -> None:
    rig: Rig = make_rig()
    long_rkey: str = "k" * 1100
    post: PostCreated = PostCreated(did=A, rkey=long_rkey, time_us=t(1), tags=("gamedev",))
    await rig.collector.handle([post, other(61)])
    assert [o for o in observations(rig.broker) if o.entity_type == "post"] == []
    assert await rig.index.posted_at(post.uri) is None
    assert rig.collector.stats.tagged_posts == 0
    assert rig.collector.stats.posts == 1
    assert observations(rig.broker) == stream_obs(0, events=1, posts=1, likes=0, reposts=0)


async def test_27_a_uri_of_exactly_1024_characters_is_tracked() -> None:
    rig: Rig = make_rig()
    prefix: str = "at://did:plc:a/app.bsky.feed.post/"
    post: PostCreated = PostCreated(did=A, rkey="k" * (1024 - len(prefix)), time_us=t(1), tags=("gamedev",))
    assert len(post.uri) == 1024
    await rig.collector.handle([post])
    assert await rig.index.posted_at(post.uri) == t(1)
    assert rig.collector.stats.tagged_posts == 1


@pytest.mark.parametrize(
    "kwargs",
    [
        {"window_seconds": 7},
        {"window_seconds": 70},
        {"window_seconds": 9},
        {"track_hours": 0},
        {"track_hours": 169},
        {"tags": ()},
        {"tags": ("game dev",)},
        {"tags": ("1up",)},
        {"tags": ("a" * 29,)},
    ],
)
def test_28_invalid_constructor_arguments(kwargs: dict[str, Any]) -> None:
    broker: InMemoryBroker = InMemoryBroker()
    with pytest.raises(ValueError):
        BlueskyCollector(InMemoryPostIndex(), InMemoryPublisher(broker), InMemoryCursorStore(), **kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [{"window_seconds": 10}, {"tags": ("a" * 28,)}, {"track_hours": 1}, {"track_hours": 168}],
)
def test_28_valid_constructor_arguments(kwargs: dict[str, Any]) -> None:
    broker: InMemoryBroker = InMemoryBroker()
    BlueskyCollector(InMemoryPostIndex(), InMemoryPublisher(broker), InMemoryCursorStore(), **kwargs)


async def test_28_the_window_length_is_configurable() -> None:
    rig: Rig = make_rig(window_seconds=10)
    await rig.collector.handle([other(1), other(11)])
    expected: list[Observation] = []
    metric: str
    value: int
    for metric, value in zip(("events", "posts", "likes", "reposts"), (1, 0, 0, 0), strict=True):
        expected.append(
            Observation.create(
                source="bluesky",
                entity_type="stream",
                entity_id="jetstream",
                metric=metric,
                value=value,
                observed_at=dt(10),
                window_start=dt(0),
            )
        )
    assert observations(rig.broker) == expected
    assert rig.cursor.value == t(10)
