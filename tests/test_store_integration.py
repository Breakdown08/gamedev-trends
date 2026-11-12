"""Spec checks 41-46: real Kafka (localhost:9092) and ClickHouse (localhost:8123).

Run with `uv run pytest -m integration` after `docker compose up -d --wait`.
"""

import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import pytest_asyncio
from store_integration_support import BOOTSTRAP, new_client, wait_for_stack

from gamedev_trends.bus import OBSERVATIONS_TOPIC
from gamedev_trends.clickhouse import ClickHouseClient, ClickHouseSink, ensure_schema
from gamedev_trends.contract import Event, Observation
from gamedev_trends.kafka_bus import KafkaPublisher, KafkaSubscriber
from gamedev_trends.store import BatchReport, Row, StoreConsumer, to_row

pytestmark: pytest.MarkDecorator = pytest.mark.integration

T10: datetime = datetime(2026, 11, 12, 10, 0, tzinfo=UTC)
DEADLINE_SECONDS: float = 60.0


@pytest.fixture(scope="session")
def stack() -> None:
    wait_for_stack()


type Db = tuple[ClickHouseClient, str]


@pytest_asyncio.fixture
async def database(stack: None) -> AsyncIterator[Db]:
    name: str = f"radar_it_{uuid.uuid4().hex[:12]}"
    http: httpx.AsyncClient
    async with httpx.AsyncClient(timeout=30.0) as http:
        client: ClickHouseClient = new_client(http)
        await ensure_schema(client, name)
        try:
            yield client, name
        finally:
            await client.execute(f"DROP DATABASE IF EXISTS {name} SYNC")


def game_obs(entity: str, i: int) -> Observation:
    return Observation.create(
        source="steam",
        entity_type="game",
        entity_id=entity,
        metric="players_current",
        value=100 + i,
        observed_at=T10 + timedelta(minutes=i),
    )


async def publish(events: list[Event]) -> None:
    publisher: KafkaPublisher = KafkaPublisher(BOOTSTRAP)
    try:
        await publisher.publish(events)
    finally:
        await publisher.close()


def new_consumer(client: ClickHouseClient, name: str, group: str) -> tuple[StoreConsumer, KafkaSubscriber]:
    sub: KafkaSubscriber = KafkaSubscriber(BOOTSTRAP, group, [OBSERVATIONS_TOPIC])
    return StoreConsumer(sub, ClickHouseSink(client, name), max_seconds=1.0), sub


def ids_of(events: list[Event]) -> set[str]:
    return {str(e.event_id) for e in events}


async def pump(consumer: StoreConsumer, wanted: set[str], *, do_flush: bool = True) -> list[BatchReport]:
    """Run next_batch (and flush) until every wanted event_id has been seen, within 60 s."""
    deadline: float = time.monotonic() + DEADLINE_SECONDS
    seen: set[str] = set()
    reports: list[BatchReport] = []
    while not wanted <= seen:
        left: float = deadline - time.monotonic()
        assert left > 0, f"missing {len(wanted - seen)} of {len(wanted)} events"
        batch: list[Event] = await asyncio.wait_for(consumer.next_batch(), left)
        seen |= ids_of(batch)
        if do_flush:
            reports.append(await consumer.flush(batch))
    return reports


async def final_rows(client: ClickHouseClient, name: str, entity: str) -> list[dict[str, object]]:
    return await client.select(
        "SELECT toString(event_id) AS event_id, value, toString(observed_at) AS observed_at "
        f"FROM observations FINAL WHERE entity_id = '{entity}' ORDER BY observed_at",
        database=name,
    )


async def test_41_schema_is_idempotent(database: Db) -> None:
    client: ClickHouseClient
    name: str
    client, name = database
    await ensure_schema(client, name)
    rows: list[dict[str, object]] = await client.select(
        f"SELECT name, engine FROM system.tables WHERE database = '{name}'"
    )
    engines: dict[object, object] = {r["name"]: r["engine"] for r in rows}
    assert engines["observations"] == "ReplacingMergeTree"
    assert engines["observations_hourly"] == "MergeTree"
    assert engines["observations_hourly_mv"] == "MaterializedView"


async def test_42_sink_deduplicates(database: Db) -> None:
    client: ClickHouseClient
    name: str
    client, name = database
    entity: str = f"it-{uuid.uuid4()}"
    sink: ClickHouseSink = ClickHouseSink(client, name)
    base: list[Row] = [to_row(game_obs(entity, i), T10 + timedelta(hours=1)) for i in range(3)]
    await sink.write(base)
    await sink.write(base)
    fixed: Row = {**base[0], "value": 999.0, "ingested_at": "2026-11-12 12:00:00.000000"}
    await sink.write([fixed])
    count: list[dict[str, object]] = await client.select(
        f"SELECT count() AS n FROM observations FINAL WHERE entity_id = '{entity}'", database=name
    )
    assert int(str(count[0]["n"])) == 3
    rows: list[dict[str, object]] = await final_rows(client, name, entity)
    assert rows[0]["value"] == 999.0


async def test_43_hourly_aggregate(database: Db) -> None:
    client: ClickHouseClient
    name: str
    client, name = database
    game: str = f"it-{uuid.uuid4()}"
    tag: str = f"it-{uuid.uuid4()}"
    ingested: datetime = T10 + timedelta(hours=2)
    second: Observation = Observation.create(
        source="steam",
        entity_type="game",
        entity_id=game,
        metric="players_current",
        value=200,
        observed_at=T10 + timedelta(minutes=30),
    )
    first: Observation = Observation.create(
        source="steam",
        entity_type="game",
        entity_id=game,
        metric="players_current",
        value=100,
        observed_at=T10,
    )
    posts: Observation = Observation.create(
        source="bluesky",
        entity_type="tag",
        entity_id=tag,
        metric="posts",
        value=7,
        observed_at=T10 + timedelta(hours=1),
        window_start=T10,
    )
    sink: ClickHouseSink = ClickHouseSink(client, name)
    await sink.write([to_row(o, ingested) for o in (first, second, second, posts)])
    await client.execute(f"SYSTEM REFRESH VIEW {name}.observations_hourly_mv")
    await client.execute(f"SYSTEM WAIT VIEW {name}.observations_hourly_mv")
    g: list[dict[str, object]] = await client.select(
        f"SELECT * FROM observations_hourly WHERE entity_id = '{game}'", database=name
    )
    assert len(g) == 1
    assert g[0]["hour"] == "2026-11-12 10:00:00"
    assert int(str(g[0]["samples"])) == 2
    assert (g[0]["value_sum"], g[0]["value_min"], g[0]["value_max"]) == (300, 100, 200)
    assert (g[0]["value_avg"], g[0]["value_last"]) == (150, 200)
    t: list[dict[str, object]] = await client.select(
        f"SELECT * FROM observations_hourly WHERE entity_id = '{tag}'", database=name
    )
    assert len(t) == 1
    assert t[0]["hour"] == "2026-11-12 10:00:00"
    assert t[0]["value_sum"] == 7


async def test_44_kafka_to_clickhouse(database: Db) -> None:
    client: ClickHouseClient
    name: str
    client, name = database
    entity: str = f"it-{uuid.uuid4()}"
    events: list[Event] = [game_obs(entity, i) for i in range(20)]
    await publish(events)
    consumer: StoreConsumer
    sub: KafkaSubscriber
    consumer, sub = new_consumer(client, name, f"it-{uuid.uuid4()}")
    try:
        await pump(consumer, ids_of(events))
    finally:
        await sub.close()
    rows: list[dict[str, object]] = await final_rows(client, name, entity)
    assert [r["event_id"] for r in rows] == [str(e.event_id) for e in events]
    assert [r["value"] for r in rows] == [100.0 + i for i in range(20)]
    assert [r["observed_at"] for r in rows] == [
        (T10 + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S.%f") for i in range(20)
    ]


async def test_45_redelivery_does_not_duplicate(database: Db) -> None:
    client: ClickHouseClient
    name: str
    client, name = database
    entity: str = f"it-{uuid.uuid4()}"
    events: list[Event] = [game_obs(entity, i) for i in range(20)]
    await publish(events)
    consumer: StoreConsumer
    sub: KafkaSubscriber
    consumer, sub = new_consumer(client, name, f"it-{uuid.uuid4()}")
    try:
        await pump(consumer, ids_of(events))
    finally:
        await sub.close()
    await publish(events)
    consumer, sub = new_consumer(client, name, f"it-{uuid.uuid4()}")
    try:
        await pump(consumer, ids_of(events))
    finally:
        await sub.close()
    count: list[dict[str, object]] = await client.select(
        f"SELECT count() AS n FROM observations FINAL WHERE entity_id = '{entity}'", database=name
    )
    assert int(str(count[0]["n"])) == 20


async def test_46_at_least_once_on_kafka(database: Db) -> None:
    client: ClickHouseClient
    name: str
    client, name = database
    entity: str = f"it-{uuid.uuid4()}"
    group: str = f"it-{uuid.uuid4()}"
    events: list[Event] = [game_obs(entity, i) for i in range(5)]
    wanted: set[str] = ids_of(events)
    await publish(events)

    consumer: StoreConsumer
    sub: KafkaSubscriber
    consumer, sub = new_consumer(client, name, group)
    try:
        await pump(consumer, wanted, do_flush=False)  # crash before the write
    finally:
        await sub.close()

    consumer, sub = new_consumer(client, name, group)
    try:
        reports: list[BatchReport] = await pump(consumer, wanted)
    finally:
        await sub.close()
    assert reports
    assert all(r.committed for r in reports)

    consumer, sub = new_consumer(client, name, group)
    try:
        end: float = time.monotonic() + 10.0
        while time.monotonic() < end:
            try:
                batch: list[Event] = await asyncio.wait_for(consumer.next_batch(), end - time.monotonic())
            except TimeoutError:
                break
            assert not (ids_of(batch) & wanted)
    finally:
        await sub.close()
