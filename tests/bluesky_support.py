"""Shared helpers for the Bluesky tests: synthetic Jetstream frames, event shortcuts, fakes."""

import asyncio
import json
from collections.abc import AsyncGenerator, Iterable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from gamedev_trends.bluesky import (
    DEFAULT_TAGS,
    BlueskyCollector,
    InMemoryCursorStore,
    InMemoryPostIndex,
    Stats,
)
from gamedev_trends.bus import InMemoryBroker, InMemoryPublisher, PublishError, Record, decode
from gamedev_trends.contract import Erasure, Event, Observation
from gamedev_trends.jetstream import (
    ConnectionLost,
    Engagement,
    FrameSource,
    JetstreamEvent,
    OtherEvent,
    PostCreated,
    PostDeleted,
)

SEC: int = 10**6
T0: int = 1794744000000000
T0_DT: datetime = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
A: str = "did:plc:a"
B: str = "did:plc:b"
P1: str = "at://did:plc:a/app.bsky.feed.post/p1"
P2: str = "at://did:plc:b/app.bsky.feed.post/p2"
OBS_TOPIC: str = "radar.observations.v1"
ERA_TOPIC: str = "radar.erasures.v1"
TAG_FACET: str = "app.bsky.richtext.facet#tag"


def t(seconds: float) -> int:
    """time_us of T0 plus the given seconds."""
    return T0 + round(seconds * SEC)


def dt(seconds: float) -> datetime:
    return T0_DT + timedelta(seconds=seconds)


def frame(obj: dict[str, Any]) -> str:
    return json.dumps(obj)


def commit_frame(
    did: str,
    time_us: int,
    operation: str,
    collection: str,
    rkey: str,
    record: dict[str, Any] | None = None,
) -> str:
    commit: dict[str, Any] = {"operation": operation, "collection": collection, "rkey": rkey}
    if record is not None:
        commit["record"] = record
    return frame({"did": did, "time_us": time_us, "kind": "commit", "commit": commit})


def tag_facet(tag: str) -> dict[str, Any]:
    return {"features": [{"$type": TAG_FACET, "tag": tag}]}


def post_frame(
    did: str = A,
    rkey: str = "p1",
    time_us: int = T0,
    *,
    facet_tags: Sequence[str] = (),
    tags: Sequence[str] | None = None,
) -> str:
    record: dict[str, Any] = {"text": "hello", "facets": [tag_facet(x) for x in facet_tags]}
    if tags is not None:
        record["tags"] = list(tags)
    return commit_frame(did, time_us, "create", "app.bsky.feed.post", rkey, record)


def like_frame(did: str, time_us: int, subject: str, collection: str = "app.bsky.feed.like") -> str:
    return commit_frame(did, time_us, "create", collection, "l1", {"subject": {"uri": subject, "cid": "c"}})


def account_frame(did: str, time_us: int, active: object, status: str | None = None) -> str:
    account: dict[str, Any] = {"active": active}
    if status is not None:
        account["status"] = status
    return frame({"did": did, "time_us": time_us, "kind": "account", "account": account})


def identity_frame(did: str = A, time_us: int = T0) -> str:
    return frame({"did": did, "time_us": time_us, "kind": "identity", "identity": {"did": did}})


def other(seconds: float, did: str = "did:plc:z") -> OtherEvent:
    return OtherEvent(did=did, time_us=t(seconds))


def example_events() -> list[JetstreamEvent | None]:
    """The ten events of the worked example in the spec."""
    return [
        PostCreated(did=A, rkey="p1", time_us=t(1), tags=("gamedev", "art")),
        PostCreated(did=B, rkey="p2", time_us=t(2), tags=()),
        Engagement(kind="like", subject_uri=P1, time_us=t(3)),
        Engagement(kind="like", subject_uri=P2, time_us=t(4)),
        Engagement(kind="repost", subject_uri=P1, time_us=t(5)),
        Engagement(kind="like", subject_uri=P1, time_us=t(6)),
        other(61),
        PostDeleted(did=A, rkey="p1", time_us=t(62)),
        Engagement(kind="like", subject_uri=P1, time_us=t(63)),
        other(121),
    ]


def stream_obs(window: int, events: int, posts: int, likes: int, reposts: int) -> list[Observation]:
    """The four stream observations of the window that starts `window` seconds after T0 (W = 60)."""
    out: list[Observation] = []
    metric: str
    value: int
    for metric, value in zip(
        ("events", "posts", "likes", "reposts"), (events, posts, likes, reposts), strict=True
    ):
        out.append(
            Observation.create(
                source="bluesky",
                entity_type="stream",
                entity_id="jetstream",
                metric=metric,
                value=value,
                observed_at=dt(window + 60),
                window_start=dt(window),
            )
        )
    return out


def post_obs(uri: str, metric: str, value: int, window: int = 0) -> Observation:
    return Observation.create(
        source="bluesky",
        entity_type="post",
        entity_id=uri,
        metric=metric,
        value=value,
        observed_at=dt(window + 60),
        window_start=dt(window),
    )


def erasure_of(uri: str, seconds: float) -> Erasure:
    return Erasure.create(source="bluesky", entity_type="post", entity_id=uri, requested_at=dt(seconds))


def observations(broker: InMemoryBroker) -> list[Observation]:
    out: list[Observation] = []
    record: Record
    for record in broker.records(OBS_TOPIC):
        event: Event = decode(record.value)
        assert isinstance(event, Observation)
        out.append(event)
    return out


def erasures(broker: InMemoryBroker) -> list[Erasure]:
    out: list[Erasure] = []
    record: Record
    for record in broker.records(ERA_TOPIC):
        event: Event = decode(record.value)
        assert isinstance(event, Erasure)
        out.append(event)
    return out


@dataclass
class Rig:
    collector: BlueskyCollector
    index: InMemoryPostIndex
    cursor: InMemoryCursorStore
    broker: InMemoryBroker


def make_rig(*, tags: Iterable[str] = DEFAULT_TAGS, window_seconds: int = 60, track_hours: int = 24) -> Rig:
    broker: InMemoryBroker = InMemoryBroker()
    index: InMemoryPostIndex = InMemoryPostIndex()
    cursor: InMemoryCursorStore = InMemoryCursorStore()
    collector: BlueskyCollector = BlueskyCollector(
        index,
        InMemoryPublisher(broker),
        cursor,
        tags=tags,
        window_seconds=window_seconds,
        track_hours=track_hours,
    )
    collector.start(None)
    return Rig(collector, index, cursor, broker)


class FlakyPublisher:
    """Raises PublishError on the first `fail_times` publish calls, then delegates."""

    def __init__(self, inner: InMemoryPublisher, fail_times: int) -> None:
        self.inner: InMemoryPublisher = inner
        self.fail_times: int = fail_times
        self.calls: int = 0

    async def publish(self, events: Sequence[Event]) -> None:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise PublishError("boom")
        await self.inner.publish(events)

    async def close(self) -> None:
        return None


class RecordingCollector(BlueskyCollector):
    """Stands in for the collector in session tests: records what run_session hands over."""

    def __init__(self, fail_on_call: int | None = None, block: asyncio.Event | None = None) -> None:
        self.stats: Stats = Stats()
        self.received: list[JetstreamEvent | None] = []
        self.calls: list[list[JetstreamEvent | None]] = []
        self.starts: list[int | None] = []
        self.fail_on_call: int | None = fail_on_call
        self.block: asyncio.Event | None = block

    def start(self, resume_us: int | None) -> None:
        self.starts.append(resume_us)

    async def handle(self, events: Sequence[JetstreamEvent | None]) -> None:
        self.calls.append(list(events))
        if self.block is not None:
            await self.block.wait()
        if self.fail_on_call is not None and len(self.calls) == self.fail_on_call:
            raise PublishError("boom")
        self.received.extend(events)


type Script = list[str | bytes] | Exception


class ScriptedConnect:
    """Connect: each call takes the next script. An Exception is raised on enter; a list of frames is
    served without awaiting and then ConnectionLost is raised."""

    def __init__(self, *scripts: Script) -> None:
        self.scripts: list[Script] = list(scripts)
        self.urls: list[str] = []

    def __call__(self, url: str) -> AbstractAsyncContextManager[FrameSource]:
        self.urls.append(url)
        script: Script = self.scripts.pop(0) if len(self.scripts) > 1 else self.scripts[0]
        return self._open(script)

    @asynccontextmanager
    async def _open(self, script: Script) -> AsyncGenerator[FrameSource]:
        if isinstance(script, Exception):
            raise script
        yield ListSource(script)


class ListSource:
    def __init__(self, frames: Sequence[str | bytes]) -> None:
        self.frames: list[str | bytes] = list(frames)

    async def recv(self) -> str | bytes:
        if not self.frames:
            raise ConnectionLost("closed")
        return self.frames.pop(0)


class FakeSleep:
    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
        await asyncio.sleep(0)


@dataclass
class Ticker:
    """monotonic() that grows by `step` on every call."""

    step: float
    now: float = 0.0
    calls: int = field(default=0)

    def __call__(self) -> float:
        self.calls += 1
        self.now += self.step
        return self.now


async def yield_loop(times: int) -> None:
    """Give the other tasks `times` chances to run."""
    done: int = 0
    while done < times:
        await asyncio.sleep(0)
        done += 1
