"""Spec checks 16-27: dashboard SQL on a throw-away ClickHouse database and queries through Grafana.

Run with `uv run pytest -m integration` after `docker compose up -d --wait`. Part A never touches the
`radar` database: it creates and drops `radar_it_*`.
"""

import os
import re
import time
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_asyncio
from dashboards_support import ROOT, all_sql, compose_block, panels, substitute, target_of, unix
from store_integration_support import new_client, wait_for_stack

from gamedev_trends.clickhouse import ClickHouseClient, ClickHouseSink, ensure_schema
from gamedev_trends.contract import Observation
from gamedev_trends.store import Row, to_row

pytestmark: pytest.MarkDecorator = pytest.mark.integration

GRAFANA: str = "http://localhost:3000"
INGESTED: datetime = datetime(2026, 11, 12, 12, 0, tzinfo=UTC)
LATER: datetime = datetime(2026, 11, 12, 13, 0, tzinfo=UTC)

type Db = tuple[ClickHouseClient, str]


def at(day: int, hour: int = 0) -> datetime:
    return datetime(2026, 11, day, hour, tzinfo=UTC)


def obs(entity: str, metric: str, value: float, when: datetime, source: str = "steam") -> Observation:
    return Observation.create(
        source=source, entity_type="game", entity_id=entity, metric=metric, value=value, observed_at=when
    )


def chart(day: int, ranking: list[str]) -> list[Observation]:
    return [obs(appid, "chart_rank", float(i + 1), at(day)) for i, appid in enumerate(ranking)]


def rows_of(items: list[Observation], ingested: datetime = INGESTED) -> list[Row]:
    return [to_row(o, ingested) for o in items]


@pytest.fixture(scope="session")
def stack() -> None:
    wait_for_stack()


async def make_db(stack_ready: None) -> AsyncIterator[Db]:
    name: str = f"radar_it_{uuid.uuid4().hex[:12]}"
    http: httpx.AsyncClient
    async with httpx.AsyncClient(timeout=30.0) as http:
        client: ClickHouseClient = new_client(http)
        await ensure_schema(client, name)
        try:
            yield client, name
        finally:
            await client.execute(f"DROP DATABASE IF EXISTS {name} SYNC")


@pytest_asyncio.fixture
async def empty_db(stack: None) -> AsyncIterator[Db]:
    db: Db
    async for db in make_db(stack):
        yield db


@pytest_asyncio.fixture
async def database(empty_db: Db) -> Db:
    client: ClickHouseClient
    name: str
    client, name = empty_db
    items: list[Observation] = [
        *chart(11, ["10", "20", "30", "50"]),
        *chart(12, ["20", "10", "30", "40"]),
    ]
    hour: int
    for hour in (9, 10, 11):
        items.append(obs("10", "players_current", 1000.0 + hour, at(12, hour)))
        items.append(obs("20", "players_current", 2000.0 + hour, at(12, hour)))
    items.append(obs("30", "players_current", 3010.0, at(12, 10)))
    items.append(obs("10", "players_current", 9999.0, at(12, 10), source="other"))
    await ClickHouseSink(client, name).write(rows_of(items))
    return client, name


async def run(
    db: Db, sql: str, *, from_ts: int = 0, to_ts: int = 0, games: str = "'730','570'", period: int = 1
) -> list[dict[str, object]]:
    client: ClickHouseClient
    name: str
    client, name = db
    return await client.select(
        substitute(sql, from_ts=from_ts, to_ts=to_ts, games=games, period=period), database=name
    )


def ts(day: int, hour: int = 0) -> int:
    return unix(2026, 11, day, hour)


async def test_16_game_sql(database: Db) -> None:
    result: list[dict[str, object]] = await run(database, all_sql()["game"])
    assert result == [{"game": g} for g in ("20", "10", "30", "40", "50")]


async def test_17_players_sql(database: Db) -> None:
    result: list[dict[str, object]] = await run(
        database, all_sql()["players"], from_ts=ts(12, 10), to_ts=ts(12, 11), games="'10','20'"
    )
    assert [list(r) for r in result] == [["time", "game", "players"]] * 4
    assert [(r["time"], r["game"], r["players"]) for r in result] == [
        ("2026-11-12 10:00:00.000000", "10", 1010.0),
        ("2026-11-12 10:00:00.000000", "20", 2010.0),
        ("2026-11-12 11:00:00.000000", "10", 1011.0),
        ("2026-11-12 11:00:00.000000", "20", 2011.0),
    ]


async def test_18_rank_sql(database: Db) -> None:
    result: list[dict[str, object]] = await run(
        database, all_sql()["rank"], from_ts=ts(11), to_ts=ts(12), games="'10'"
    )
    assert result == [
        {"time": "2026-11-11 00:00:00.000000", "game": "10", "rank": 1},
        {"time": "2026-11-12 00:00:00.000000", "game": "10", "rank": 2},
    ]


def movement(
    game: str, now: int | None, then: int | None, delta: int | None, status: str, then_day: str = "2026-11-11"
) -> dict[str, object]:
    return {
        "game": game,
        "rank_now": now,
        "rank_then": then,
        "delta": delta,
        "status": status,
        "day_now": "2026-11-12",
        "day_then": then_day,
    }


async def test_19_movements_period_1(database: Db) -> None:
    result: list[dict[str, object]] = await run(database, all_sql()["movements"], period=1)
    assert [list(r) for r in result] == [
        ["game", "rank_now", "rank_then", "delta", "status", "day_now", "day_then"]
    ] * 5
    assert result == [
        movement("40", 4, None, None, "new"),
        movement("20", 1, 2, 1, "up"),
        movement("10", 2, 1, -1, "down"),
        movement("50", None, 4, None, "dropped"),
        movement("30", 3, 3, 0, "same"),
    ]


async def test_20_movements_empty(database: Db, stack: None) -> None:
    assert await run(database, all_sql()["movements"], period=7) == []
    fresh: Db
    async for fresh in make_db(stack):  # database is built on empty_db, so it is not empty: take a fresh one
        assert await run(fresh, all_sql()["movements"], period=1) == []


async def test_21_skipped_day(empty_db: Db) -> None:
    client: ClickHouseClient
    name: str
    client, name = empty_db
    await ClickHouseSink(client, name).write(
        rows_of([obs("10", "chart_rank", 1.0, at(10)), obs("10", "chart_rank", 3.0, at(12))])
    )
    result: list[dict[str, object]] = await run(empty_db, all_sql()["movements"], period=1)
    assert result == [movement("10", 3, 1, -2, "down", then_day="2026-11-10")]


async def test_22_deduplication(database: Db) -> None:
    client: ClickHouseClient
    name: str
    client, name = database
    again: Observation = obs("20", "chart_rank", 5.0, at(12))
    await ClickHouseSink(client, name).write(rows_of([again], LATER))
    moves: list[dict[str, object]] = await run(database, all_sql()["movements"], period=1)
    twenty: list[dict[str, object]] = [r for r in moves if r["game"] == "20"]
    assert len(twenty) == 1
    assert twenty[0]["rank_now"] == 5
    assert twenty[0]["status"] == "down"
    games: list[dict[str, object]] = await run(database, all_sql()["game"])
    names: list[object] = [r["game"] for r in games]
    assert len(names) == len(set(names))


# Part B: through Grafana


def grafana_password() -> str:
    value: str | None = os.environ.get("GRAFANA_ADMIN_PASSWORD")
    if value:
        return value
    env_file: Path = ROOT / ".env"
    if env_file.exists():
        line: str
        for line in env_file.read_text().splitlines():
            if line.startswith("GRAFANA_ADMIN_PASSWORD="):
                return line.split("=", 1)[1].strip()
    pytest.fail("GRAFANA_ADMIN_PASSWORD: add it to .env (see .env.example)")


@pytest.fixture(scope="session")
def grafana() -> None:
    deadline: float = time.monotonic() + 60.0
    while time.monotonic() < deadline:
        try:
            if httpx.get(f"{GRAFANA}/api/health", timeout=5.0).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(1.0)
    pytest.fail("Grafana not ready on localhost:3000 (60 s). Try: docker compose up -d --wait")


def admin() -> httpx.BasicAuth:
    return httpx.BasicAuth("admin", grafana_password())


def test_23_health(grafana: None) -> None:
    assert httpx.get(f"{GRAFANA}/api/health", timeout=10.0).status_code == 200


def test_24_dashboard_anonymous(grafana: None) -> None:
    response: httpx.Response = httpx.get(f"{GRAFANA}/api/dashboards/uid/radar-steam", timeout=10.0)
    assert response.status_code == 200
    body: dict[str, Any] = response.json()
    assert body["dashboard"]["title"] == "Steam radar"
    assert [p["id"] for p in body["dashboard"]["panels"]] == [1, 2, 3]
    assert body["meta"]["folderTitle"] == "Radar"


def test_25_datasource_health(grafana: None) -> None:
    response: httpx.Response = httpx.get(
        f"{GRAFANA}/api/datasources/uid/clickhouse/health", auth=admin(), timeout=30.0
    )
    assert response.status_code == 200
    assert response.json()["status"] == "OK"


def test_26_plugin_version(grafana: None) -> None:
    match: re.Match[str] | None = re.search(
        r"grafana-clickhouse-datasource@(\d+\.\d+\.\d+)", compose_block("grafana")
    )
    assert match is not None
    response: httpx.Response = httpx.get(
        f"{GRAFANA}/api/plugins/grafana-clickhouse-datasource/settings", auth=admin(), timeout=10.0
    )
    assert response.status_code == 200
    assert response.json()["info"]["version"] == match.group(1)


@pytest.mark.parametrize("index", [0, 1, 2])
def test_27_panel_query_through_grafana(grafana: None, index: int) -> None:
    panel: dict[str, Any] = panels()[index]
    target: dict[str, Any] = target_of(panel)
    raw: str = str(target["rawSql"]).replace("${game:singlequote}", "'730'").replace("${period}", "1")
    query: dict[str, Any] = {**target, "datasource": panel["datasource"], "rawSql": raw, "refId": "A"}
    response: httpx.Response = httpx.post(
        f"{GRAFANA}/api/ds/query",
        json={"queries": [query], "from": "now-7d", "to": "now"},
        auth=admin(),
        timeout=60.0,
    )
    assert response.status_code == 200, response.text
    result: dict[str, Any] = response.json()["results"]["A"]
    assert "error" not in result
    assert result.get("status", 200) == 200
