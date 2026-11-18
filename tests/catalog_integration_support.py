"""Fixtures for the catalog integration tests: stack readiness, throw-away databases, game builders."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import httpx
from store_integration_support import new_client, poll_until

from gamedev_trends.catalog import Game, Tag
from gamedev_trends.clickhouse import ClickHouseClient, ClickHouseError
from gamedev_trends.store import format_datetime

if TYPE_CHECKING:
    pass

MONGO_URL: str = "mongodb://localhost:27017"
T1: datetime = datetime(2026, 11, 18, 10, 0, tzinfo=UTC)
T2: datetime = datetime(2026, 11, 18, 11, 0, tzinfo=UTC)


def mongo_ready() -> bool:
    import pymongo

    client: pymongo.MongoClient[dict[str, Any]] = pymongo.MongoClient(
        MONGO_URL, serverSelectionTimeoutMS=2000
    )
    try:
        return client.admin.command("ping").get("ok") == 1.0
    finally:
        client.close()


def wait_for_catalog_stack() -> None:
    def clickhouse_ready() -> bool:
        async def probe() -> bool:
            http: httpx.AsyncClient
            async with httpx.AsyncClient(timeout=5.0) as http:
                client: ClickHouseClient = new_client(http)
                return await client.ping() and await client.select("SELECT 1 AS x") == [{"x": 1}]

        try:
            return asyncio.run(probe())
        except ClickHouseError:
            return False

    poll_until("MongoDB on localhost:27017", mongo_ready)
    poll_until("ClickHouse on localhost:8123", clickhouse_ready)


def game(
    appid: int,
    *,
    name: str | None = None,
    tags: list[tuple[str, int]] | None = None,
    developers: list[str] | None = None,
    publishers: list[str] | None = None,
    steam_type: int = 0,
    now: datetime = T1,
) -> Game:
    tag_list: list[tuple[str, int]] = [] if tags is None else tags
    return Game(
        appid=appid,
        name=f"Game {appid}" if name is None else name,
        steam_type=steam_type,
        tags=[Tag(tagid=1000 + i, name=n, weight=w) for i, (n, w) in enumerate(tag_list)],
        categories=[],
        developers=["Dev"] if developers is None else developers,
        publishers=["Pub"] if publishers is None else publishers,
        is_free=False,
        price_usd_cents=999,
        discount_pct=0,
        release_date=datetime(2020, 1, 2, 3, 4, 5, tzinfo=UTC),
        coming_soon=False,
        early_access=False,
        updated_at=now,
    )


def metric_row(
    entity: str, observed_at: datetime, value: float, *, source: str = "steam"
) -> dict[str, str | float | None]:
    return {
        "event_id": str(uuid.uuid4()),
        "source": source,
        "entity_type": "game",
        "entity_id": entity,
        "metric": "players_current",
        "value": value,
        "observed_at": format_datetime(observed_at),
        "window_start": None,
        "ingested_at": format_datetime(observed_at + timedelta(seconds=1)),
    }
