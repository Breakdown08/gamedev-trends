"""Spec checks 57-62: real Redis, Kafka, ClickHouse and Grafana.

Run with `uv run pytest -m integration` after `docker compose up -d --wait`. Redis keys use the prefix
`it-<hex>:`, DIDs `did:plc:it<hex>`, ClickHouse databases `radar_it_<hex>`. The Kafka test pins its
consumer group to the end of both topics before it publishes and does not read history.
"""

import logging
import time
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import httpx
import pytest
import pytest_asyncio
import redis.asyncio
from bluesky_dashboard_support import RATE_SQL, TAGS_SQL, TOP_SQL
from bluesky_integration_support import TeePublisher, pin_group_to_end, pump_until, purge_radar
from bluesky_support import SEC
from store_integration_support import BOOTSTRAP, new_client, wait_for_stack

from gamedev_trends.bluesky import (
    BlueskyCollector,
    RedisCursorStore,
    RedisPostIndex,
)
from gamedev_trends.bus import ERASURES_TOPIC, OBSERVATIONS_TOPIC
from gamedev_trends.clickhouse import (
    ClickHouseClient,
    ClickHouseErasureSink,
    ClickHouseExcerptSink,
    ClickHouseSink,
    ensure_erasures_schema,
    ensure_excerpts_schema,
    ensure_schema,
)
from gamedev_trends.contract import Erasure, Event, Excerpt, Observation
from gamedev_trends.jetstream import Engagement, JetstreamEvent, OtherEvent, PostCreated, PostDeleted
from gamedev_trends.kafka_bus import KafkaPublisher, KafkaSubscriber
from gamedev_trends.store import BatchReport, StoreConsumer, to_excerpt_row, to_row

pytestmark: pytest.MarkDecorator = pytest.mark.integration

REDIS_URL: str = "redis://localhost:6379/0"
GRAFANA: str = "http://localhost:3000"
type Db = tuple[ClickHouseClient, str]


@pytest.fixture(scope="session")
def stack() -> None:
    wait_for_stack()


@pytest_asyncio.fixture
async def database(stack: None) -> AsyncIterator[Db]:
    name: str = f"radar_it_{uuid.uuid4().hex[:12]}"
    http: httpx.AsyncClient
    async with httpx.AsyncClient(timeout=30.0) as http:
        client: ClickHouseClient = new_client(http)
        await ensure_schema(client, name)
        await ensure_erasures_schema(client, name)
        try:
            await ensure_excerpts_schema(client, name)
            yield client, name
        finally:
            await client.execute(f"DROP DATABASE IF EXISTS {name} SYNC")


@pytest_asyncio.fixture
async def redis_client(stack: None) -> AsyncIterator[redis.asyncio.Redis]:
    r: redis.asyncio.Redis = redis.asyncio.Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        yield r
    finally:
        await r.aclose()


@pytest.fixture
def token() -> str:
    return uuid.uuid4().hex[:12]


async def drop_keys(r: redis.asyncio.Redis, prefix: str) -> None:
    keys: list[str] = [k async for k in r.scan_iter(match=f"{prefix}*")]
    if keys:
        await cast(Any, r.delete(*keys))


async def test_57_redis_post_index(redis_client: redis.asyncio.Redis, token: str) -> None:
    prefix: str = f"it-{token}:"
    did_a: str = f"did:plc:it{token}a"
    did_b: str = f"did:plc:it{token}b"
    pa1: str = f"at://{did_a}/app.bsky.feed.post/p1"
    pa2: str = f"at://{did_a}/app.bsky.feed.post/p2"
    pb1: str = f"at://{did_b}/app.bsky.feed.post/p1"
    index: RedisPostIndex = RedisPostIndex(redis_client, prefix)
    try:
        await index.add(pa2, did_a, 1794744002000000)
        await index.add(pb1, did_b, 1794744003000000)
        await index.add(pa1, did_a, 1794744001234567)
        await index.add(pa1, did_a, 1794744001234567)
        assert await index.posted_at(pa1) == 1794744001234567
        assert await redis_client.zscore(prefix + did_a, pa1) == 1794744001234567.0
        assert await index.posted_at(f"at://{did_a}/app.bsky.feed.post/nope") is None
        assert await index.posted_at("https://x") is None
        assert await index.remove(pb1) is True
        assert await index.remove(pb1) is False
        assert await index.remove("https://x") is False
        await index.add(pb1, did_b, 1794744003000000)
        assert await index.remove_author(did_a) == [pa1, pa2]
        assert await cast(Any, redis_client.exists(prefix + did_a)) == 0
        assert await index.posted_at(pa1) is None
        assert await index.posted_at(pb1) == 1794744003000000
    finally:
        await drop_keys(redis_client, prefix)


async def test_58_redis_cursor_store(
    redis_client: redis.asyncio.Redis, token: str, caplog: pytest.LogCaptureFixture
) -> None:
    key: str = f"it-{token}:cursor"
    store: RedisCursorStore = RedisCursorStore(redis_client, key)
    try:
        assert await store.load() is None
        await store.save(1794744060000000)
        assert await store.load() == 1794744060000000
        assert await redis_client.get(key) == "1794744060000000"
        await cast(Any, redis_client.set(key, "garbage"))
        with caplog.at_level(logging.DEBUG):
            assert await store.load() is None
        assert any(r.levelno == logging.WARNING for r in caplog.records)
    finally:
        await cast(Any, redis_client.delete(key))


def entity_rows(rows: list[dict[str, object]]) -> list[tuple[object, object]]:
    return [(r["entity_id"], r["metric"]) for r in rows]


async def test_59_erasure_sink_in_clickhouse(database: Db) -> None:
    client: ClickHouseClient
    name: str
    client, name = database
    token: str = uuid.uuid4().hex[:10]
    window: datetime = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
    quoted: str = f"at://did:plc:it{token}/app.bsky.feed.post/it'q\\{token}"
    plain_one: str = f"at://did:plc:it{token}/app.bsky.feed.post/one"
    plain_two: str = f"at://did:plc:it{token}/app.bsky.feed.post/two"
    items: list[Observation] = [
        Observation.create(
            source="bluesky",
            entity_type="post",
            entity_id=entity,
            metric="tag_gamedev",
            value=1,
            observed_at=window + timedelta(minutes=1),
            window_start=window,
        )
        for entity in (quoted, plain_one, plain_two)
    ]
    ingested: datetime = window + timedelta(hours=1)
    await ClickHouseSink(client, name).write([to_row(o, ingested) for o in items])
    sink: ClickHouseErasureSink = ClickHouseErasureSink(client, name)
    erasures: list[Erasure] = [
        Erasure.create(source="bluesky", entity_type="post", entity_id=e, requested_at=ingested)
        for e in (quoted, plain_one)
    ]
    await sink.erase(erasures, ingested)
    left: list[dict[str, object]] = await client.select(
        "SELECT entity_id FROM observations FINAL ORDER BY entity_id", database=name
    )
    assert left == [{"entity_id": plain_two}]
    keys: list[tuple[str, str, str]] = [("bluesky", "post", e) for e in (quoted, plain_one, plain_two)]
    assert await sink.erased(keys) == {keys[0], keys[1]}
    count: list[dict[str, object]] = await client.select(
        "SELECT count() AS n FROM erasures FINAL", database=name
    )
    assert int(str(count[0]["n"])) == 2
    await sink.erase(erasures, ingested + timedelta(minutes=5))
    again: list[dict[str, object]] = await client.select(
        "SELECT count() AS n FROM erasures FINAL", database=name
    )
    assert int(str(again[0]["n"])) == 2
    await client.execute(f"SYSTEM REFRESH VIEW {name}.observations_hourly_mv")
    await client.execute(f"SYSTEM WAIT VIEW {name}.observations_hourly_mv")
    hourly: list[dict[str, object]] = await client.select(
        "SELECT entity_id FROM observations_hourly ORDER BY entity_id", database=name
    )
    assert hourly == [{"entity_id": plain_two}]


def base_time_us() -> int:
    """A window-aligned instant in 2030, random per run, so synthetic windows never meet real ones."""
    start: int = 1893456000
    return (start + (uuid.uuid4().int % 1000) * 3600) * SEC


async def test_60_collector_through_kafka_to_the_store(
    database: Db, redis_client: redis.asyncio.Redis, token: str
) -> None:
    client: ClickHouseClient
    name: str
    client, name = database
    prefix: str = f"it-{token}:"
    did_keep: str = f"did:plc:it{token}k"
    did_gone: str = f"did:plc:it{token}g"
    p_keep: str = f"at://{did_keep}/app.bsky.feed.post/keep"
    p_gone: str = f"at://{did_gone}/app.bsky.feed.post/gone"
    base: int = base_time_us()
    group: str = f"it-{uuid.uuid4()}"
    pin_group_to_end(group, [OBSERVATIONS_TOPIC, ERASURES_TOPIC])

    publisher: TeePublisher = TeePublisher(KafkaPublisher(BOOTSTRAP))
    collector: BlueskyCollector = BlueskyCollector(
        RedisPostIndex(redis_client, prefix),
        publisher,
        RedisCursorStore(redis_client, f"{prefix}cursor"),
    )
    collector.start(None)
    events: list[JetstreamEvent | None] = [
        PostCreated(did=did_keep, rkey="keep", time_us=base + 1 * SEC, tags=("gamedev",)),
        PostCreated(did=did_gone, rkey="gone", time_us=base + 2 * SEC, tags=("gamedev",)),
        Engagement(kind="like", subject_uri=p_keep, time_us=base + 3 * SEC),
        Engagement(kind="like", subject_uri=p_gone, time_us=base + 4 * SEC),
        OtherEvent(did=did_keep, time_us=base + 61 * SEC),
        PostDeleted(did=did_gone, rkey="gone", time_us=base + 62 * SEC),
        OtherEvent(did=did_keep, time_us=base + 121 * SEC),
    ]
    sub: KafkaSubscriber = KafkaSubscriber(BOOTSTRAP, group, [OBSERVATIONS_TOPIC, ERASURES_TOPIC])
    consumer: StoreConsumer = StoreConsumer(
        sub, ClickHouseSink(client, name), max_seconds=1.0, eraser=ClickHouseErasureSink(client, name)
    )
    try:
        await collector.handle(events)
        wanted: set[str] = {str(e.event_id) for e in publisher.events}
        assert wanted
        await pump_until(consumer, wanted)

        window_1: str = (datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=base)).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )
        rows: list[dict[str, object]] = await client.select(
            "SELECT entity_type, entity_id, metric, window_start FROM observations FINAL "
            "ORDER BY entity_type, entity_id, metric",
            database=name,
        )
        keep: list[object] = [r["metric"] for r in rows if r["entity_id"] == p_keep]
        assert keep == ["likes", "tag_gamedev"]
        assert [r for r in rows if r["entity_id"] == p_gone] == []
        stream: list[dict[str, object]] = [
            r for r in rows if r["entity_type"] == "stream" and r["window_start"] == window_1
        ]
        assert len(stream) == 4
        gone: list[dict[str, object]] = await client.select(
            "SELECT entity_id FROM erasures FINAL", database=name
        )
        assert gone == [{"entity_id": p_gone}]

        late: Observation = Observation.create(
            source="bluesky",
            entity_type="post",
            entity_id=p_gone,
            metric="likes",
            value=5,
            observed_at=datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=base + 60 * SEC),
            window_start=datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=base),
        )
        late_publisher: KafkaPublisher = KafkaPublisher(BOOTSTRAP)
        try:
            late_events: list[Event] = [late]
            await late_publisher.publish(late_events)
        finally:
            await late_publisher.close()
        reports: list[BatchReport] = await pump_until(consumer, {str(late.event_id)})
        assert sum(r.dropped for r in reports) >= 1
        after: list[dict[str, object]] = await client.select(
            f"SELECT count() AS n FROM observations FINAL WHERE entity_id = '{p_gone}'", database=name
        )
        assert int(str(after[0]["n"])) == 0
    finally:
        await sub.close()
        await publisher.close()
        await drop_keys(redis_client, prefix)
        await purge_radar(client, [p_keep, p_gone], base)


def hour_12(day_minute: int = 0) -> datetime:
    return datetime(2026, 11, 15, 12, day_minute, tzinfo=UTC)


def post_observation(entity: str, metric: str, value: float, minute: int) -> Observation:
    return Observation.create(
        source="bluesky",
        entity_type="post",
        entity_id=entity,
        metric=metric,
        value=value,
        observed_at=hour_12(minute + 1),
        window_start=hour_12(minute),
    )


def with_range(sql: str) -> str:
    return sql.replace("$__fromTime", "toDateTime64('2026-11-15 00:00:00', 6, 'UTC')").replace(
        "$__toTime", "toDateTime64('2026-11-15 23:59:59', 6, 'UTC')"
    )


async def test_61_dashboard_queries_on_real_data(database: Db) -> None:
    client: ClickHouseClient
    name: str
    client, name = database
    x: str = "at://did:plc:itx/app.bsky.feed.post/rx"
    y: str = "at://did:plc:ity/app.bsky.feed.post/ry"
    z: str = "at://did:plc:itz/app.bsky.feed.post/rz"
    items: list[Observation] = [
        post_observation(x, "tag_gamedev", 1, 0),
        post_observation(y, "tag_gamedev", 1, 0),
        post_observation(z, "tag_pixelart", 1, 0),
        post_observation(x, "likes", 3, 0),
        post_observation(x, "likes", 2, 1),
        post_observation(x, "reposts", 1, 0),
        post_observation(y, "likes", 1, 0),
        Observation.create(
            source="bluesky",
            entity_type="stream",
            entity_id="jetstream",
            metric="events",
            value=14100,
            observed_at=hour_12(1),
            window_start=hour_12(0),
        ),
    ]
    await ClickHouseSink(client, name).write([to_row(o, hour_12(5)) for o in items])
    w: str = "at://did:plc:itw/app.bsky.feed.post/rw"
    await ClickHouseExcerptSink(client, name).write_excerpts(
        [
            to_excerpt_row(
                Excerpt.create(
                    source="bluesky",
                    entity_type="post",
                    entity_id=entity,
                    text=text,
                    posted_at=hour_12(0),
                ),
                hour_12(5),
            )
            for entity, text in ((x, "Hello #gamedev world"), (w, "orphan, not in observations"))
        ]
    )

    tags: list[dict[str, object]] = await client.select(with_range(TAGS_SQL), database=name)
    assert [(str(r["time"])[:19], r["tag"], int(str(r["posts"]))) for r in tags] == [
        ("2026-11-15 12:00:00", "gamedev", 2),
        ("2026-11-15 12:00:00", "pixelart", 1),
    ]

    rate: list[dict[str, object]] = await client.select(with_range(RATE_SQL), database=name)
    assert [(r["metric"], float(str(r["per_second"]))) for r in rate] == [("events", 235.0)]

    top: list[dict[str, object]] = await client.select(with_range(TOP_SQL), database=name)
    assert len(top) == 3
    assert [r["about"] for r in top] == ["Hello #gamedev world", "", ""]
    assert (float(str(top[0]["likes"])), float(str(top[0]["reposts"])), top[0]["tags"]) == (
        5.0,
        1.0,
        "gamedev",
    )
    assert top[0]["link"] == "https://bsky.app/profile/did:plc:itx/post/rx"
    assert (float(str(top[1]["likes"])), top[1]["link"]) == (
        1.0,
        "https://bsky.app/profile/did:plc:ity/post/ry",
    )
    assert top[2]["link"] == "https://bsky.app/profile/did:plc:itz/post/rz"
    assert top[2]["tags"] == "pixelart"


async def test_34_excerpt_sink_in_clickhouse(database: Db) -> None:
    client: ClickHouseClient
    name: str
    client, name = database
    token: str = uuid.uuid4().hex[:10]
    odd: str = f"at://did:plc:it{token}/app.bsky.feed.post/it'q\\{token}"
    plain: str = f"at://did:plc:it{token}/app.bsky.feed.post/plain"
    odd_text: str = "It's \\ \u041f\u0440\u0438\u0432\u0435\u0442 \U0001f3ae"
    items: list[Excerpt] = [
        Excerpt.create(source="bluesky", entity_type="post", entity_id=e, text=text, posted_at=hour_12(0))
        for e, text in ((odd, odd_text), (plain, "plain text"))
    ]
    rows: list[dict[str, str | float | None]] = [to_excerpt_row(x, hour_12(5)) for x in items]
    sink: ClickHouseExcerptSink = ClickHouseExcerptSink(client, name)
    await sink.write_excerpts(rows)
    got: list[dict[str, object]] = await client.select(
        "SELECT entity_id, text FROM excerpts FINAL ORDER BY entity_id", database=name
    )
    assert {(r["entity_id"], r["text"]) for r in got} == {(odd, odd_text), (plain, "plain text")}
    assert len(got) == 2
    await sink.write_excerpts([to_excerpt_row(x, hour_12(9)) for x in items])
    again: list[dict[str, object]] = await client.select(
        "SELECT count() AS n FROM excerpts FINAL", database=name
    )
    assert int(str(again[0]["n"])) == 2
    await sink.erase_excerpts([("bluesky", "post", odd)])
    left: list[dict[str, object]] = await client.select(
        "SELECT entity_id, text FROM excerpts FINAL", database=name
    )
    assert left == [{"entity_id": plain, "text": "plain text"}]


async def test_35_excerpts_through_kafka_and_erasure(
    database: Db, redis_client: redis.asyncio.Redis, token: str
) -> None:
    client: ClickHouseClient
    name: str
    client, name = database
    prefix: str = f"it-{token}:"
    did_keep: str = f"did:plc:it{token}k"
    did_gone: str = f"did:plc:it{token}g"
    p_keep: str = f"at://{did_keep}/app.bsky.feed.post/keep"
    p_gone: str = f"at://{did_gone}/app.bsky.feed.post/gone"
    base: int = base_time_us()
    group: str = f"it-{uuid.uuid4()}"
    pin_group_to_end(group, [OBSERVATIONS_TOPIC, ERASURES_TOPIC])

    publisher: TeePublisher = TeePublisher(KafkaPublisher(BOOTSTRAP))
    collector: BlueskyCollector = BlueskyCollector(
        RedisPostIndex(redis_client, prefix),
        publisher,
        RedisCursorStore(redis_client, f"{prefix}cursor"),
    )
    collector.start(None)
    events: list[JetstreamEvent | None] = [
        PostCreated(did=did_keep, rkey="keep", time_us=base + 1 * SEC, tags=("gamedev",), excerpt="keep me"),
        PostCreated(
            did=did_gone, rkey="gone", time_us=base + 2 * SEC, tags=("gamedev",), excerpt="gone soon"
        ),
        OtherEvent(did=did_keep, time_us=base + 61 * SEC),
        PostDeleted(did=did_gone, rkey="gone", time_us=base + 62 * SEC),
        OtherEvent(did=did_keep, time_us=base + 121 * SEC),
    ]
    sub: KafkaSubscriber = KafkaSubscriber(BOOTSTRAP, group, [OBSERVATIONS_TOPIC, ERASURES_TOPIC])
    consumer: StoreConsumer = StoreConsumer(
        sub,
        ClickHouseSink(client, name),
        max_seconds=1.0,
        eraser=ClickHouseErasureSink(client, name),
        excerpts=ClickHouseExcerptSink(client, name),
    )
    try:
        await collector.handle(events)
        wanted: set[str] = {str(e.event_id) for e in publisher.events}
        assert wanted
        await pump_until(consumer, wanted)
        rows: list[dict[str, object]] = await client.select(
            "SELECT entity_id, text FROM excerpts FINAL", database=name
        )
        assert rows == [{"entity_id": p_keep, "text": "keep me"}]

        late: Excerpt = Excerpt.create(
            source="bluesky",
            entity_type="post",
            entity_id=p_gone,
            text="gone soon",
            posted_at=datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=base + 2 * SEC),
        )
        late_publisher: KafkaPublisher = KafkaPublisher(BOOTSTRAP)
        try:
            late_events: list[Event] = [late]
            await late_publisher.publish(late_events)
        finally:
            await late_publisher.close()
        reports: list[BatchReport] = await pump_until(consumer, {str(late.event_id)})
        assert sum(r.dropped for r in reports) >= 1
        after: list[dict[str, object]] = await client.select(
            "SELECT entity_id, text FROM excerpts FINAL", database=name
        )
        assert after == [{"entity_id": p_keep, "text": "keep me"}]
    finally:
        await sub.close()
        await publisher.close()
        await drop_keys(redis_client, prefix)
        await purge_radar(client, [p_keep, p_gone], base)


def test_62_dashboard_is_provisioned_in_grafana(stack: None) -> None:
    deadline: float = time.monotonic() + 60.0
    response: httpx.Response | None = None
    while time.monotonic() < deadline:
        try:
            response = httpx.get(f"{GRAFANA}/api/dashboards/uid/radar-bluesky", timeout=10.0)
            if response.status_code == 200:
                break
        except httpx.TransportError:
            pass
        time.sleep(1.0)
    assert response is not None
    assert response.status_code == 200, "Grafana did not serve radar-bluesky; docker compose restart grafana"
    body: dict[str, Any] = response.json()
    assert body["dashboard"]["title"] == "Bluesky radar"
    assert len(body["dashboard"]["panels"]) == 3
