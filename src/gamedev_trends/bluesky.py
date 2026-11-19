"""Bluesky Jetstream collector."""

import argparse
import asyncio
import logging
import math
import os
import re
import signal
import sys
import time
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any, Literal, Protocol, cast

import redis
import redis.asyncio
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.client import Pipeline

from gamedev_trends.bus import Publisher
from gamedev_trends.contract import Erasure, Event, Excerpt, Observation
from gamedev_trends.jetstream import (
    DEFAULT_ENDPOINTS,
    JETSTREAM_COLLECTIONS,
    AccountStatus,
    Connect,
    ConnectionLost,
    Engagement,
    FrameSource,
    JetstreamEvent,
    PostCreated,
    PostDeleted,
    did_of,
    normalize_tag,
    parse_event,
    subscribe_url,
    us_to_datetime,
    websocket_connect,
)
from gamedev_trends.logs import setup_logging
from gamedev_trends.store import Sleep

logger: logging.Logger = logging.getLogger(__name__)

type Reconnect = Literal["replay", "live"]
RECONNECT_MODES: tuple[Reconnect, ...] = ("replay", "live")
SOURCE: str = "bluesky"
ENTITY_POST: str = "post"
ENTITY_STREAM: str = "stream"
STREAM_ID: str = "jetstream"
METRIC_LIKES: str = "likes"
METRIC_REPOSTS: str = "reposts"
STREAM_METRICS: tuple[str, ...] = ("events", "posts", "likes", "reposts")
TAG_METRIC_PREFIX: str = "tag_"
TAG_PATTERN: str = r"^[a-z][a-z0-9_]{0,27}$"
DEFAULT_TAGS: tuple[str, ...] = (
    "gamedev",
    "indiedev",
    "indiegame",
    "godot",
    "pixelart",
    "screenshotsaturday",
    "mobilegame",
)
ERASING_STATUSES: frozenset[str] = frozenset({"deleted", "deactivated", "takendown", "suspended"})
CURSOR_KEY: str = "bluesky:cursor"
POSTS_KEY_PREFIX: str = "bluesky:posts:"
MAX_URI_LENGTH: int = 1024
BATCH_MAX: int = 500


class PostIndex(Protocol):
    async def add(self, uri: str, did: str, time_us: int) -> None: ...
    async def posted_at(self, uri: str) -> int | None: ...
    async def remove(self, uri: str) -> bool: ...
    async def remove_author(self, did: str) -> list[str]: ...


class InMemoryPostIndex:
    def __init__(self) -> None:
        self._posts: dict[str, dict[str, int]] = {}

    async def add(self, uri: str, did: str, time_us: int) -> None:
        self._posts.setdefault(did, {})[uri] = time_us

    async def posted_at(self, uri: str) -> int | None:
        did: str | None = did_of(uri)
        if did is None:
            return None
        return self._posts.get(did, {}).get(uri)

    async def remove(self, uri: str) -> bool:
        did: str | None = did_of(uri)
        if did is None:
            return False
        author: dict[str, int] = self._posts.get(did, {})
        if uri not in author:
            return False
        del author[uri]
        if not author:
            del self._posts[did]
        return True

    async def remove_author(self, did: str) -> list[str]:
        author: dict[str, int] = self._posts.pop(did, {})
        return [uri for uri, _ in sorted(author.items(), key=lambda item: (item[1], item[0]))]


def _text(value: object) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


class RedisPostIndex:
    def __init__(self, redis: redis.asyncio.Redis, prefix: str = POSTS_KEY_PREFIX) -> None:
        self._redis: AsyncRedis = redis
        self._prefix: str = prefix

    async def add(self, uri: str, did: str, time_us: int) -> None:
        await cast(Any, self._redis).zadd(self._prefix + did, {uri: time_us})

    async def posted_at(self, uri: str) -> int | None:
        did: str | None = did_of(uri)
        if did is None:
            return None
        score: float | None = await cast(Any, self._redis).zscore(self._prefix + did, uri)
        return None if score is None else int(score)

    async def remove(self, uri: str) -> bool:
        did: str | None = did_of(uri)
        if did is None:
            return False
        removed: int = await cast(Any, self._redis).zrem(self._prefix + did, uri)
        return removed > 0

    async def remove_author(self, did: str) -> list[str]:
        key: str = self._prefix + did
        pipe: Pipeline = self._redis.pipeline(transaction=True)
        try:
            cast(Any, pipe).zrange(key, 0, -1)
            cast(Any, pipe).delete(key)
            results: list[Any] = await pipe.execute()
        finally:
            await pipe.aclose()
        return [_text(x) for x in results[0]]


class CursorStore(Protocol):
    async def load(self) -> int | None: ...
    async def save(self, time_us: int) -> None: ...


class InMemoryCursorStore:
    value: int | None

    def __init__(self, value: int | None = None) -> None:
        self.value: int | None = value

    async def load(self) -> int | None:
        return self.value

    async def save(self, time_us: int) -> None:
        self.value: int | None = time_us


class RedisCursorStore:
    def __init__(self, redis: redis.asyncio.Redis, key: str = CURSOR_KEY) -> None:
        self._redis: AsyncRedis = redis
        self._key: str = key

    async def load(self) -> int | None:
        raw: object = await cast(Any, self._redis).get(self._key)
        if raw is None:
            return None
        text: str = _text(raw)
        try:
            value: int = int(text)
        except ValueError:
            value = 0
        if value <= 0:
            logger.warning("ignoring a bad cursor value in redis: key=%s value=%r", self._key, text[:100])
            return None
        return value

    async def save(self, time_us: int) -> None:
        await cast(Any, self._redis).set(self._key, str(time_us))


@dataclass
class Stats:
    events: int = 0
    malformed: int = 0
    late: int = 0
    posts: int = 0
    likes: int = 0
    reposts: int = 0
    tagged_posts: int = 0
    engagements: int = 0
    erasures: int = 0
    windows: int = 0


@dataclass
class _Window:
    start_us: int
    events: int = 0
    posts: int = 0
    likes: int = 0
    reposts: int = 0
    tagged: dict[str, tuple[str, ...]] = field(default_factory=dict)
    like_counts: dict[str, int] = field(default_factory=dict)
    repost_counts: dict[str, int] = field(default_factory=dict)
    erased: dict[str, int] = field(default_factory=dict)
    excerpts: dict[str, tuple[str, int]] = field(default_factory=dict)


class BlueskyCollector:
    stats: Stats
    _position_us: int | None = None

    def __init__(
        self,
        index: PostIndex,
        publisher: Publisher,
        cursor: CursorStore,
        *,
        tags: Iterable[str] = DEFAULT_TAGS,
        window_seconds: int = 60,
        track_hours: int = 24,
    ) -> None:
        whitelist: list[str] = []
        raw: str
        for raw in tags:
            tag: str = normalize_tag(raw)
            if re.fullmatch(TAG_PATTERN, tag) is None:
                raise ValueError(f"tag must match {TAG_PATTERN}, got {raw!r}")
            if tag not in whitelist:
                whitelist.append(tag)
        if not whitelist:
            raise ValueError("at least one tag is required")
        if window_seconds < 10 or 3600 % window_seconds != 0:
            raise ValueError(f"window_seconds must be >= 10 and divide 3600, got {window_seconds}")
        if not 1 <= track_hours <= 168:
            raise ValueError(f"track_hours must be from 1 to 168, got {track_hours}")
        self._index: PostIndex = index
        self._publisher: Publisher = publisher
        self._cursor: CursorStore = cursor
        self._tags: frozenset[str] = frozenset(whitelist)
        self._window_us: int = window_seconds * 10**6
        self._track_us: int = track_hours * 3600 * 10**6
        self._resume_us: int | None = None
        self._window: _Window | None = None
        self._boundary: Counter[JetstreamEvent] = Counter()
        self._skip: Counter[JetstreamEvent] = Counter()
        self.stats: Stats = Stats()

    @property
    def window_start_us(self) -> int | None:
        return None if self._window is None else self._window.start_us

    @property
    def position_us(self) -> int | None:
        return self._position_us

    def resume(self, *, live: bool = False) -> int | None:
        """Reconnect inside the process: keep the window and continue from the last accepted event."""
        position: int | None = self._position_us
        if position is None:
            raise RuntimeError("resume() needs at least one accepted event")
        if live:
            self._resume_us = None
            self._skip = Counter()
            return None
        self._resume_us = position
        self._skip = Counter(self._boundary)
        return position

    def start(self, resume_us: int | None) -> None:
        self._window = None
        self._resume_us = resume_us
        self._position_us: int | None = None
        self._boundary = Counter()
        self._skip = Counter()

    async def handle(self, events: Sequence[JetstreamEvent | None]) -> None:
        event: JetstreamEvent | None
        for event in events:
            if event is None:
                self.stats.malformed += 1
                continue
            self.stats.events += 1
            window: _Window | None = self._window
            if (self._resume_us is not None and event.time_us < self._resume_us) or (
                window is not None and event.time_us < window.start_us
            ):
                self.stats.late += 1
                continue
            if self._resume_us == event.time_us and self._skip[event] > 0:
                self._skip[event] -= 1
                self.stats.late += 1
                continue
            if window is not None and event.time_us >= window.start_us + self._window_us:
                await self._close(window, event.time_us // self._window_us * self._window_us)
                window = None
            if window is None:
                window = _Window(start_us=event.time_us // self._window_us * self._window_us)
                self._window = window
            await self._apply(window, event)
            position: int | None = self._position_us
            if position is None or event.time_us > position:
                self._position_us: int | None = event.time_us
                self._boundary = Counter({event: 1})
            elif event.time_us == position:
                self._boundary[event] += 1

    async def _apply(self, window: _Window, event: JetstreamEvent) -> None:
        window.events += 1
        if isinstance(event, PostCreated):
            window.posts += 1
            self.stats.posts += 1
            matched: tuple[str, ...] = tuple(sorted(self._tags.intersection(event.tags)))
            uri: str = event.uri
            if matched and len(uri) <= MAX_URI_LENGTH:
                await self._index.add(uri, event.did, event.time_us)
                window.tagged[uri] = matched
                if event.excerpt:
                    window.excerpts[uri] = (event.excerpt, event.time_us)
                self.stats.tagged_posts += 1
        elif isinstance(event, PostDeleted):
            await self._erase(window, event.uri, event.time_us)
        elif isinstance(event, Engagement):
            counts: dict[str, int]
            if event.kind == "like":
                window.likes += 1
                self.stats.likes += 1
                counts = window.like_counts
            else:
                window.reposts += 1
                self.stats.reposts += 1
                counts = window.repost_counts
            posted: int | None = await self._index.posted_at(event.subject_uri)
            if posted is not None and event.time_us - posted < self._track_us:
                counts[event.subject_uri] = counts.get(event.subject_uri, 0) + 1
                self.stats.engagements += 1
        elif isinstance(event, AccountStatus):
            if not event.active and event.status in ERASING_STATUSES:
                gone: str
                for gone in await self._index.remove_author(event.did):
                    self._forget(window, gone, event.time_us)

    async def _erase(self, window: _Window, uri: str, time_us: int) -> None:
        if await self._index.remove(uri):
            self._forget(window, uri, time_us)

    @staticmethod
    def _forget(window: _Window, uri: str, time_us: int) -> None:
        window.tagged.pop(uri, None)
        window.excerpts.pop(uri, None)
        window.like_counts.pop(uri, None)
        window.repost_counts.pop(uri, None)
        window.erased.setdefault(uri, time_us)

    async def _close(self, window: _Window, next_start_us: int) -> None:
        start: datetime = us_to_datetime(window.start_us)
        end: datetime = us_to_datetime(window.start_us + self._window_us)
        out: list[Event] = []

        def observation(entity_type: str, entity_id: str, metric: str, value: int) -> Observation:
            return Observation.create(
                source=SOURCE,
                entity_type=entity_type,
                entity_id=entity_id,
                metric=metric,
                value=value,
                observed_at=end,
                window_start=start,
            )

        stream: tuple[int, ...] = (window.events, window.posts, window.likes, window.reposts)
        metric: str
        value: int
        for metric, value in zip(STREAM_METRICS, stream, strict=True):
            out.append(observation(ENTITY_STREAM, STREAM_ID, metric, value))
        uri: str
        matched: tuple[str, ...]
        for uri, matched in window.tagged.items():
            tag: str
            for tag in matched:
                out.append(observation(ENTITY_POST, uri, TAG_METRIC_PREFIX + tag, 1))
        for uri, value in window.like_counts.items():
            out.append(observation(ENTITY_POST, uri, METRIC_LIKES, value))
        for uri, value in window.repost_counts.items():
            out.append(observation(ENTITY_POST, uri, METRIC_REPOSTS, value))
        time_us: int
        text: str
        for uri in window.tagged:
            if uri in window.excerpts:
                text, time_us = window.excerpts[uri]
                out.append(
                    Excerpt.create(
                        source=SOURCE,
                        entity_type=ENTITY_POST,
                        entity_id=uri,
                        text=text,
                        posted_at=us_to_datetime(time_us),
                    )
                )
        for uri, time_us in window.erased.items():
            out.append(
                Erasure.create(
                    source=SOURCE,
                    entity_type=ENTITY_POST,
                    entity_id=uri,
                    requested_at=us_to_datetime(time_us),
                )
            )
        await self._publisher.publish(out)
        await self._cursor.save(next_start_us)
        self.stats.windows += 1
        self.stats.erasures += len(window.erased)
        self._window = None


@dataclass(frozen=True)
class _End:
    """Queue marker: the reader is done."""


_END: _End = _End()


def _format_stats(delta: Stats, seconds: float, queue: asyncio.Queue[str | bytes | _End], size: int) -> str:
    return (
        f"bluesky stats: rate={delta.events / seconds:.1f}/s events={delta.events} posts={delta.posts} "
        f"likes={delta.likes} reposts={delta.reposts} tagged_posts={delta.tagged_posts} "
        f"engagements={delta.engagements} erasures={delta.erasures} late={delta.late} "
        f"malformed={delta.malformed} queue={queue.qsize()}/{size}"
    )


def _delta(now: Stats, before: Stats) -> Stats:
    return Stats(
        events=now.events - before.events,
        malformed=now.malformed - before.malformed,
        late=now.late - before.late,
        posts=now.posts - before.posts,
        likes=now.likes - before.likes,
        reposts=now.reposts - before.reposts,
        tagged_posts=now.tagged_posts - before.tagged_posts,
        engagements=now.engagements - before.engagements,
        erasures=now.erasures - before.erasures,
        windows=now.windows - before.windows,
    )


async def _read(source: FrameSource, queue: asyncio.Queue[str | bytes | _End], idle_timeout: float) -> None:
    try:
        while True:
            frame: str | bytes
            async with asyncio.timeout(idle_timeout):
                frame = await source.recv()
            await queue.put(frame)
    except ConnectionLost as exc:
        logger.warning("jetstream connection lost: %s", exc)
    except TimeoutError:
        logger.warning("jetstream idle for %.1fs; reconnecting", idle_timeout)
    except Exception:
        logger.exception("jetstream reader failed")
    await queue.put(_END)


async def _drain(
    queue: asyncio.Queue[str | bytes | _End],
    collector: BlueskyCollector,
    *,
    queue_size: int,
    batch_max: int,
    stats_seconds: float,
    monotonic: Callable[[], float],
) -> int:
    handled: int = 0
    last: float = monotonic()
    snapshot: Stats = replace(collector.stats)
    finished: bool = False
    while not finished:
        frames: list[str | bytes] = []
        first: str | bytes | _End = await queue.get()
        item: str | bytes | _End = first
        while True:
            if isinstance(item, _End):
                finished = True
                break
            frames.append(item)
            if len(frames) >= batch_max or queue.empty():
                break
            item = queue.get_nowait()
        if not frames:
            break
        events: list[JetstreamEvent | None] = [parse_event(f) for f in frames]
        try:
            await collector.handle(events)
        except Exception:
            logger.exception("collector failed on a batch of %d frames", len(frames))
            return handled
        handled += len(frames)
        now: float = monotonic()
        if now - last >= stats_seconds:
            current: Stats = replace(collector.stats)
            logger.info(_format_stats(_delta(current, snapshot), now - last, queue, queue_size))
            snapshot = current
            last = now
    return handled


async def run_session(
    connect: Connect,
    url: str,
    collector: BlueskyCollector,
    *,
    queue_size: int = 10_000,
    batch_max: int = BATCH_MAX,
    idle_timeout: float = 60.0,
    stats_seconds: float = 60.0,
    monotonic: Callable[[], float] = time.monotonic,
) -> int:
    queue: asyncio.Queue[str | bytes | _End] = asyncio.Queue(maxsize=queue_size)
    handled: list[int] = [0]
    try:
        stack: AsyncExitStack = AsyncExitStack()
        async with stack:
            source: FrameSource = await stack.enter_async_context(connect(url))
            reader: asyncio.Task[None] = asyncio.create_task(_read(source, queue, idle_timeout))
            try:
                handled[0] = await _drain(
                    queue,
                    collector,
                    queue_size=queue_size,
                    batch_max=batch_max,
                    stats_seconds=stats_seconds,
                    monotonic=monotonic,
                )
            finally:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
    except Exception as exc:
        logger.warning("jetstream session failed: %r", exc)
    return handled[0]


async def run_forever(
    connect: Connect,
    collector: BlueskyCollector,
    cursor: CursorStore,
    *,
    endpoints: Sequence[str],
    queue_size: int = 10_000,
    idle_timeout: float = 60.0,
    stats_seconds: float = 60.0,
    backoff_base: float = 1.0,
    backoff_max: float = 30.0,
    max_replay_seconds: int | None = None,
    reconnect: Reconnect = "replay",
    sleep: Sleep = asyncio.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    clock: Callable[[], float] = time.time,
    max_sessions: int | None = None,
) -> None:
    if reconnect not in RECONNECT_MODES:
        raise ValueError(f"reconnect must be 'replay' or 'live', got {reconnect!r}")
    failures: int = 0
    sessions: int = 0
    i: int = 0
    loaded: bool = False
    initial: int | None = None
    last_live_to_us: int | None = None

    def too_old(from_us: int, now_us: int) -> bool:
        return max_replay_seconds is not None and now_us - from_us > max_replay_seconds * 1_000_000

    def log_gap(from_us: int, now_us: int) -> None:
        logger.warning(
            "bluesky gap: from=%d to=%d seconds=%d max_replay=%s; reading the live stream",
            from_us,
            now_us,
            (now_us - from_us) // 1_000_000,
            max_replay_seconds,
        )

    def log_reconnect_gap(position_us: int, now_us: int) -> None:
        from_us: int = position_us if last_live_to_us is None else max(position_us, last_live_to_us)
        logger.info(
            "bluesky gap: from=%d to=%d seconds=%.1f reconnect=live; reading the live stream",
            from_us,
            now_us,
            (now_us - from_us) / 1e6,
        )

    while True:
        handled: int = 0
        if not loaded:
            try:
                initial = await cursor.load()
                loaded = True
            except Exception:
                logger.exception("loading the cursor failed")
        if loaded:
            now_us: int = round(clock() * 1_000_000)
            url_cursor: int | None
            mode: str
            position: int | None = collector.position_us
            if position is None:
                if initial is not None and too_old(initial, now_us):
                    log_gap(initial, now_us)
                    initial = None
                collector.start(initial)
                url_cursor = initial
                mode = "live" if initial is None else "start"
            elif too_old(position, now_us):
                log_gap(position, now_us)
                collector.resume(live=True)
                url_cursor = None
                mode = "live"
            elif reconnect == "live":
                log_reconnect_gap(position, now_us)
                last_live_to_us = now_us
                collector.resume(live=True)
                url_cursor = None
                mode = "live"
            else:
                url_cursor = collector.resume()
                mode = "resume"
            endpoint: str = endpoints[i]
            lag: str = "none" if url_cursor is None else f"{(now_us - url_cursor) / 1e6:.1f}"
            logger.info(
                "bluesky session: endpoint=%s cursor=%s mode=%s lag=%s",
                endpoint,
                "none" if url_cursor is None else url_cursor,
                mode,
                lag,
            )
            handled = await run_session(
                connect,
                subscribe_url(endpoint, JETSTREAM_COLLECTIONS, url_cursor),
                collector,
                queue_size=queue_size,
                idle_timeout=idle_timeout,
                stats_seconds=stats_seconds,
                monotonic=monotonic,
            )
        sessions += 1
        if max_sessions is not None and sessions >= max_sessions:
            return
        if handled > 0:
            failures = 0
        else:
            failures += 1
            i = (i + 1) % len(endpoints)
        delay: float = backoff_base if failures == 0 else min(backoff_max, backoff_base * 2 ** (failures - 1))
        await sleep(delay)


@dataclass(frozen=True)
class BlueskyConfig:
    kafka_bootstrap_servers: str = "localhost:9092"
    redis_url: str = "redis://localhost:6379/0"
    endpoints: tuple[str, ...] = DEFAULT_ENDPOINTS
    tags: tuple[str, ...] = DEFAULT_TAGS
    window_seconds: int = 60
    track_hours: int = 24
    queue_size: int = 10_000
    idle_timeout_seconds: float = 60.0
    stats_seconds: float = 60.0
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 30.0
    max_replay_seconds: int = 300
    reconnect: Reconnect = "replay"

    @property
    def replay_limit_seconds(self) -> int:
        return max(self.max_replay_seconds, 2 * self.window_seconds)

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> BlueskyConfig:
        """Read the variables; absent or empty ones take the defaults, bad ones raise ValueError."""
        d: BlueskyConfig = cls()
        endpoints: tuple[str, ...] = d.endpoints
        raw_endpoints: str = env.get("BLUESKY_ENDPOINTS", "").strip()
        if raw_endpoints:
            parsed: list[str] = [x.strip() for x in raw_endpoints.split(",")]
            item: str
            for item in parsed:
                if not item.startswith(("wss://", "ws://")) or "?" in item:
                    raise ValueError(
                        f"BLUESKY_ENDPOINTS: each endpoint must start with wss:// or ws:// "
                        f"and have no query, got {item!r}"
                    )
            endpoints = tuple(parsed)
        tags: tuple[str, ...] = d.tags
        raw_tags: str = env.get("BLUESKY_TAGS", "").strip()
        if raw_tags:
            seen: dict[str, None] = {}
            raw: str
            for raw in raw_tags.split(","):
                tag: str = normalize_tag(raw)
                if re.fullmatch(TAG_PATTERN, tag) is None:
                    raise ValueError(f"BLUESKY_TAGS: each tag must match {TAG_PATTERN}, got {raw!r}")
                seen.setdefault(tag, None)
            tags = tuple(seen)
        window: int = _int(env, "BLUESKY_WINDOW_SECONDS", d.window_seconds)
        if window < 10 or 3600 % window != 0:
            raise ValueError(f"BLUESKY_WINDOW_SECONDS: must be >= 10 and divide 3600, got {window}")
        track: int = _int(env, "BLUESKY_TRACK_HOURS", d.track_hours)
        if not 1 <= track <= 168:
            raise ValueError(f"BLUESKY_TRACK_HOURS: must be from 1 to 168, got {track}")
        max_replay: int = _int(env, "BLUESKY_MAX_REPLAY_SECONDS", d.max_replay_seconds)
        if max_replay < 1:
            raise ValueError(f"BLUESKY_MAX_REPLAY_SECONDS: must be >= 1, got {max_replay}")
        queue: int = _int(env, "BLUESKY_QUEUE_SIZE", d.queue_size)
        if not 1 <= queue <= 1_000_000:
            raise ValueError(f"BLUESKY_QUEUE_SIZE: must be from 1 to 1000000, got {queue}")
        idle: float = _float(env, "BLUESKY_IDLE_TIMEOUT_SECONDS", d.idle_timeout_seconds)
        if idle <= 0:
            raise ValueError(f"BLUESKY_IDLE_TIMEOUT_SECONDS: must be > 0, got {idle}")
        stats: float = _float(env, "BLUESKY_STATS_SECONDS", d.stats_seconds)
        if stats <= 0:
            raise ValueError(f"BLUESKY_STATS_SECONDS: must be > 0, got {stats}")
        base: float = _float(env, "BLUESKY_BACKOFF_BASE_SECONDS", d.backoff_base_seconds)
        if base <= 0:
            raise ValueError(f"BLUESKY_BACKOFF_BASE_SECONDS: must be > 0, got {base}")
        cap: float = _float(env, "BLUESKY_BACKOFF_MAX_SECONDS", d.backoff_max_seconds)
        if cap < base:
            raise ValueError(f"BLUESKY_BACKOFF_MAX_SECONDS: must be >= {base} (the base delay), got {cap}")
        raw_reconnect: str = env.get("BLUESKY_RECONNECT", "").strip()
        reconnect: Reconnect = d.reconnect
        if raw_reconnect:
            mode: Reconnect | None = next((m for m in RECONNECT_MODES if m == raw_reconnect), None)
            if mode is None:
                raise ValueError(f"BLUESKY_RECONNECT: expected replay or live, got {raw_reconnect!r}")
            reconnect = mode
        return cls(
            kafka_bootstrap_servers=_str(env, "KAFKA_BOOTSTRAP_SERVERS", d.kafka_bootstrap_servers),
            redis_url=_str(env, "REDIS_URL", d.redis_url),
            endpoints=endpoints,
            tags=tags,
            window_seconds=window,
            track_hours=track,
            queue_size=queue,
            idle_timeout_seconds=idle,
            stats_seconds=stats,
            backoff_base_seconds=base,
            backoff_max_seconds=cap,
            max_replay_seconds=max_replay,
            reconnect=reconnect,
        )


def _str(env: Mapping[str, str], name: str, default: str) -> str:
    return env.get(name, "").strip() or default


def _int(env: Mapping[str, str], name: str, default: int) -> int:
    raw: str = env.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"{name}: expected an integer, got {raw!r}") from None


def _float(env: Mapping[str, str], name: str, default: float) -> float:
    raw: str = env.get(name, "").strip()
    if not raw:
        return default
    try:
        value: float = float(raw)
    except ValueError:
        raise ValueError(f"{name}: expected a number, got {raw!r}") from None
    if not math.isfinite(value):
        raise ValueError(f"{name}: expected a finite number, got {raw!r}")
    return value


def _log_summary(stats: Stats, seconds: float) -> None:
    rate: float = stats.events / seconds if seconds > 0 else 0.0
    logger.info(
        "bluesky summary: seconds=%.0f events=%d rate=%.1f/s tagged_posts=%d engagements=%d "
        "erasures=%d windows=%d",
        seconds,
        stats.events,
        rate,
        stats.tagged_posts,
        stats.engagements,
        stats.erasures,
        stats.windows,
    )


async def _serve(config: BlueskyConfig, duration: float | None) -> None:
    from gamedev_trends.kafka_bus import KafkaPublisher  # imports the Kafka client

    started: float = time.monotonic()
    redis_client: redis.asyncio.Redis = redis.asyncio.Redis.from_url(config.redis_url, decode_responses=True)
    publisher: KafkaPublisher | None = None
    stats: Stats = Stats()
    try:
        publisher = KafkaPublisher(config.kafka_bootstrap_servers)
        cursor: RedisCursorStore = RedisCursorStore(redis_client)
        collector: BlueskyCollector = BlueskyCollector(
            RedisPostIndex(redis_client),
            publisher,
            cursor,
            tags=config.tags,
            window_seconds=config.window_seconds,
            track_hours=config.track_hours,
        )
        stats = collector.stats
        logger.info("bluesky replay limit: %ds", config.replay_limit_seconds)
        logger.info("bluesky reconnect: %s", config.reconnect)
        try:
            async with asyncio.timeout(duration):
                await run_forever(
                    websocket_connect,
                    collector,
                    cursor,
                    endpoints=config.endpoints,
                    queue_size=config.queue_size,
                    idle_timeout=config.idle_timeout_seconds,
                    stats_seconds=config.stats_seconds,
                    backoff_base=config.backoff_base_seconds,
                    backoff_max=config.backoff_max_seconds,
                    max_replay_seconds=config.replay_limit_seconds,
                    reconnect=config.reconnect,
                )
        except TimeoutError:
            if duration is None:
                raise
    finally:
        _log_summary(stats, time.monotonic() - started)
        if publisher is not None:
            await publisher.close()
        await redis_client.aclose()


async def _run(config: BlueskyConfig, duration: float | None) -> int:
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    task: asyncio.Task[None] = asyncio.create_task(_serve(config, duration))
    sig: signal.Signals
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, task.cancel)
    try:
        await task
    except asyncio.CancelledError:
        pass
    return 0


def _positive_seconds(text: str) -> float:
    try:
        value: float = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a number of seconds, got {text!r}") from None
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError(f"must be a positive number of seconds, got {text!r}")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(prog="python -m gamedev_trends.bluesky")
    parser.add_argument(
        "--duration", type=_positive_seconds, default=None, help="stop after this many seconds"
    )
    args: argparse.Namespace = parser.parse_args(argv)
    config: BlueskyConfig
    try:
        config = BlueskyConfig.from_env(os.environ)
    except ValueError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    setup_logging()
    return asyncio.run(_run(config, args.duration))


if __name__ == "__main__":
    sys.exit(main())
