"""Spec checks 33-43: real MongoDB (localhost:27017) and ClickHouse (localhost:8123).

Run with `uv run pytest -m integration` after `docker compose up -d --wait`.
"""

import uuid
from collections.abc import AsyncIterator, MutableMapping
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import httpx
import pytest
import pytest_asyncio
from catalog_integration_support import MONGO_URL, T1, T2, game, metric_row, wait_for_catalog_stack
from catalog_support import FakeSource, as_source
from store_integration_support import new_client

from gamedev_trends.catalog import (
    Game,
    GrowingGame,
    RefreshReport,
    StoredGame,
    growing_games,
    radar_appids,
    refresh,
)
from gamedev_trends.clickhouse import ClickHouseClient, ensure_schema
from gamedev_trends.mongo_catalog import (
    APPID_INDEX,
    COLLECTION,
    GAMES_VALIDATOR,
    TAGS_INDEX,
    IndieShare,
    MongoCatalog,
    NicheStats,
    TagGrowth,
    UpsertResult,
    indie_share_pipeline,
    niche_pipeline,
)

if TYPE_CHECKING:
    from pymongo.asynchronous.database import AsyncDatabase

pytestmark: pytest.MarkDecorator = pytest.mark.integration

type ClickHouseDb = tuple[ClickHouseClient, str]


@pytest.fixture(scope="session")
def stack() -> None:
    wait_for_catalog_stack()


@pytest_asyncio.fixture
async def db(stack: None) -> AsyncIterator[AsyncDatabase[dict[str, Any]]]:
    from pymongo import AsyncMongoClient

    client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(MONGO_URL, tz_aware=True)
    name: str = f"radar_it_{uuid.uuid4().hex[:12]}"
    try:
        yield client[name]
    finally:
        await client.drop_database(name)
        await client.close()


@pytest_asyncio.fixture
async def catalog(db: AsyncDatabase[dict[str, Any]]) -> MongoCatalog:
    result: MongoCatalog = MongoCatalog(db)
    await result.ensure_schema()
    return result


@pytest_asyncio.fixture
async def ch(stack: None) -> AsyncIterator[ClickHouseDb]:
    name: str = f"radar_it_{uuid.uuid4().hex[:12]}"
    http: httpx.AsyncClient
    async with httpx.AsyncClient(timeout=30.0) as http:
        client: ClickHouseClient = new_client(http)
        await ensure_schema(client, name)
        try:
            yield client, name
        finally:
            await client.execute(f"DROP DATABASE IF EXISTS {name} SYNC")


def find_stage(plan: object, stage: str, index: str) -> bool:
    """Recursive walk: the plan shape depends on the engine (classic or SBE)."""
    if isinstance(plan, dict):
        if plan.get("stage") == stage and plan.get("indexName") == index:
            return True
        return any(find_stage(v, stage, index) for v in plan.values())
    if isinstance(plan, list):
        return any(find_stage(v, stage, index) for v in plan)
    return False


async def seed_for_explain(catalog: MongoCatalog) -> None:
    games: list[Game] = [
        game(500 + i, tags=[("it-tag-a" if i < 3 else f"it-other-{i}", 100 + i)]) for i in range(20)
    ]
    await catalog.upsert(games, T1)


async def test_33_ensure_schema_idempotent(db: AsyncDatabase[dict[str, Any]]) -> None:
    catalog: MongoCatalog = MongoCatalog(db)
    await catalog.ensure_schema()
    await catalog.ensure_schema()
    cursor: Any = await db[COLLECTION].list_indexes()
    indexes: dict[str, dict[str, Any]] = {i["name"]: i async for i in cursor}
    assert set(indexes) == {"_id_", APPID_INDEX, TAGS_INDEX}
    assert indexes[APPID_INDEX]["unique"] is True
    assert dict(indexes[APPID_INDEX]["key"]) == {"appid": 1}
    assert dict(indexes[TAGS_INDEX]["key"]) == {"tags.name": 1}
    options: MutableMapping[str, Any] = await db[COLLECTION].options()
    assert options["validator"] == GAMES_VALIDATOR


async def test_33_ensure_schema_updates_validator_of_existing_collection(
    db: AsyncDatabase[dict[str, Any]],
) -> None:
    await db.create_collection(COLLECTION)
    catalog: MongoCatalog = MongoCatalog(db)
    await catalog.ensure_schema()
    options: MutableMapping[str, Any] = await db[COLLECTION].options()
    assert options["validator"] == GAMES_VALIDATOR


async def test_34_upsert_insert_then_update(catalog: MongoCatalog) -> None:
    first: list[Game] = [game(1), game(2), game(3)]
    assert await catalog.upsert(first, T1) == UpsertResult(3, 0)
    assert await catalog.count() == 3
    second: list[Game] = [game(1, name="Renamed", now=T2), game(2, now=T2), game(3, now=T2)]
    assert await catalog.upsert(second, T2) == UpsertResult(0, 3)
    assert await catalog.count() == 3
    stored: StoredGame | None = await catalog.get(1)
    assert stored is not None
    assert stored.name == "Renamed"
    assert stored.updated_at == T2
    assert stored.created_at == T1
    assert stored.updated_at.tzinfo is not None
    assert stored.created_at.utcoffset() == timedelta(0)
    assert await catalog.get(999) is None


async def test_34_upsert_empty_and_naive_now(catalog: MongoCatalog) -> None:
    assert await catalog.upsert([], T1) == UpsertResult(0, 0)
    with pytest.raises(ValueError):
        await catalog.upsert([game(1)], datetime(2026, 11, 18, 10, 0))


async def test_35_duplicate_appid_rejected(db: AsyncDatabase[dict[str, Any]], catalog: MongoCatalog) -> None:
    from pymongo.errors import DuplicateKeyError

    await catalog.upsert([game(7)], T1)
    doc: dict[str, Any] = game(7).model_dump() | {"created_at": T1}
    with pytest.raises(DuplicateKeyError):
        await db[COLLECTION].insert_one(doc)


async def test_36_validator_rejects_bad_documents(
    db: AsyncDatabase[dict[str, Any]], catalog: MongoCatalog
) -> None:
    from pymongo.errors import WriteError

    good: dict[str, Any] = game(8).model_dump() | {"created_at": T1}
    no_appid: dict[str, Any] = {k: v for k, v in good.items() if k != "appid"}
    bad_tag: dict[str, Any] = good | {"tags": [{"tagid": "x", "name": "a", "weight": 1}]}
    doc: dict[str, Any]
    info: pytest.ExceptionInfo[WriteError]
    for doc in (no_appid, bad_tag):
        with pytest.raises(WriteError) as info:
            await db[COLLECTION].insert_one(dict(doc))
        assert info.value.code == 121
    await db[COLLECTION].insert_one(dict(good))
    assert await catalog.count() == 1


async def test_37_explain_shows_index_use(db: AsyncDatabase[dict[str, Any]], catalog: MongoCatalog) -> None:
    await seed_for_explain(catalog)
    pipeline: list[dict[str, Any]]
    for pipeline in (niche_pipeline("it-tag-a"), indie_share_pipeline("it-tag-a")):
        plan: dict[str, Any] = await db.command(
            {
                "explain": {"aggregate": COLLECTION, "pipeline": pipeline, "cursor": {}},
                "verbosity": "queryPlanner",
            }
        )
        assert find_stage(plan, "IXSCAN", TAGS_INDEX)
    by_appid: dict[str, Any] = await db.command(
        {
            "explain": {"find": COLLECTION, "filter": {"appid": 505}},
            "verbosity": "queryPlanner",
        }
    )
    assert find_stage(by_appid, "IXSCAN", APPID_INDEX) or find_stage(by_appid, "EXPRESS_IXSCAN", APPID_INDEX)


async def test_38_niche(catalog: MongoCatalog) -> None:
    games: list[Game] = [
        game(1, tags=[("it-tag-a", 100)]),
        game(2, tags=[("it-tag-a", 200), ("it-tag-x", 5)]),
        game(3, tags=[("it-tag-a", 300)]),
        game(4, tags=[("it-tag-x", 50)]),
        game(5, tags=[]),
        game(6, tags=[("it-tag-a", 900)], steam_type=6),
    ]
    await catalog.upsert(games, T1)
    assert await catalog.niche("it-tag-a") == NicheStats("it-tag-a", 3, 200.0)
    assert await catalog.niche("it-none") == NicheStats("it-none", 0, None)


async def test_39_indie_share(catalog: MongoCatalog) -> None:
    tag: list[tuple[str, int]] = [("it-tag-b", 10)]
    games: list[Game] = [
        game(1, tags=tag, developers=["A"], publishers=["A"]),
        game(2, tags=tag, developers=["A", "B"], publishers=["B", "A"]),
        game(3, tags=tag, developers=["A"], publishers=["P"]),
        game(4, tags=tag, developers=["A"], publishers=["A", "P"]),
        game(5, tags=tag, developers=[], publishers=["P"]),
        game(6, tags=tag, developers=["A"], publishers=["A"], steam_type=2),
    ]
    await catalog.upsert(games, T1)
    assert await catalog.indie_share("it-tag-b") == IndieShare("it-tag-b", 4, 2, 0.5)
    assert await catalog.indie_share("it-none") == IndieShare("it-none", 0, 0, None)


async def test_40_growing_tags(catalog: MongoCatalog) -> None:
    games: list[Game] = [
        game(1, tags=[("it-tag-a", 10), ("it-tag-c", 9)]),
        game(2, tags=[("it-tag-a", 10)]),
        game(3, tags=[("it-tag-a", 10)]),
        game(4, tags=[("it-tag-a", 10), ("it-tag-c", 9)], steam_type=6),
        game(5, tags=[("it-tag-d", 10)]),
    ]
    await catalog.upsert(games, T1)
    assert await catalog.growing_tags([1, 2]) == [TagGrowth("it-tag-a", 2, 3), TagGrowth("it-tag-c", 1, 1)]
    assert await catalog.growing_tags([4]) == []
    assert await catalog.growing_tags([]) == []
    assert await catalog.growing_tags([1, 2], limit=1) == [TagGrowth("it-tag-a", 2, 3)]


NOW_G: datetime = datetime(2026, 11, 20, tzinfo=UTC)


def hourly(entity: str, start: datetime, count: int, value: float) -> list[dict[str, str | float | None]]:
    return [metric_row(entity, start + timedelta(hours=i), value) for i in range(count)]


async def seed_growth(client: ClickHouseClient, name: str) -> None:
    recent: datetime = NOW_G - timedelta(hours=24)
    before: datetime = NOW_G - timedelta(hours=168 + 24)
    rows: list[dict[str, str | float | None]] = []
    rows += hourly("101", before, 24, 100.0) + hourly("101", recent, 24, 150.0)
    rows += hourly("102", before, 24, 100.0) + hourly("102", recent, 24, 110.0)
    rows += hourly("103", before, 6, 100.0) + hourly("103", recent, 24, 200.0)
    rows += hourly("it-x", before, 24, 100.0) + hourly("it-x", recent, 24, 500.0)
    rows.append(metric_row("101", NOW_G, 100000.0))  # exactly at now: outside the window
    await client.insert_rows("observations", rows, database=name)


async def test_41_growing_games(ch: ClickHouseDb) -> None:
    client: ClickHouseClient
    name: str
    client, name = ch
    await seed_growth(client, name)
    assert await growing_games(client, name, now=NOW_G) == [GrowingGame(101, 100.0, 150.0, 0.5)]
    relaxed: list[GrowingGame] = await growing_games(client, name, now=NOW_G, min_growth=0.05)
    assert [g.appid for g in relaxed] == [101, 102]
    assert relaxed[1].before == 100.0
    assert relaxed[1].recent == 110.0
    assert relaxed[1].growth == pytest.approx(0.1)


async def test_42_radar_appids(ch: ClickHouseDb) -> None:
    client: ClickHouseClient
    name: str
    client, name = ch
    rows: list[dict[str, str | float | None]] = [
        metric_row("730", T1, 1.0),
        metric_row("it-x", T1, 1.0),
        metric_row("440", T1, 1.0),
        metric_row("999", T1, 1.0, source="bluesky"),
    ]
    await client.insert_rows("observations", rows, database=name)
    assert await radar_appids(client, name) == [440, 730]


async def test_43_refresh_twice_against_mongo(catalog: MongoCatalog) -> None:
    ids: list[int] = [413150, 730, 227300, 1422450, 1281930, 2379780, 1329410, 999999999]
    first_now: datetime = datetime(2026, 11, 18, 10, 0, tzinfo=UTC)
    second_now: datetime = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)
    first: RefreshReport = await refresh(as_source(FakeSource()), catalog, ids, now=first_now)
    assert await catalog.count() == 6
    assert first.inserted == 6
    second: RefreshReport = await refresh(as_source(FakeSource()), catalog, ids, now=second_now)
    assert await catalog.count() == 6
    assert second.inserted == 0
    assert second.matched == 6
    assert second.unavailable == (1329410, 999999999)
    stored: StoredGame | None = await catalog.get(413150)
    assert stored is not None
    assert stored.created_at == first_now
    assert stored.updated_at == second_now
