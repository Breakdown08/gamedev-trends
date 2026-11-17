"""Spec checks 17-22: the alerts_v1 interface on a live ClickHouse (localhost:8123).

Run with `uv run pytest -m integration` after `docker compose up -d --wait`.
"""

import os
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from alerts_interface_support import EXPECTED_COLUMNS
from store_integration_support import CLICKHOUSE_URL, HINT, ROOT, new_client, poll_until

from gamedev_trends.clickhouse import (
    CONTRACT_VIEWS,
    ClickHouseClient,
    ClickHouseError,
    Scalar,
    ensure_contract,
    ensure_schema,
)

pytestmark: pytest.MarkDecorator = pytest.mark.integration

VAR: str = "RADAR_ALERTS_CLICKHOUSE_PASSWORD"
INGESTED: str = "2026-11-17 12:00:00.000000"
LATER: str = "2026-11-17 12:05:00.000000"

type Pair = tuple[ClickHouseClient, ClickHouseClient]


def alerts_password() -> str:
    value: str | None = os.environ.get(VAR)
    if value:
        return value
    env_file: Path = ROOT / ".env"
    if env_file.exists():
        line: str
        for line in env_file.read_text().splitlines():
            if line.startswith(f"{VAR}="):
                found: str = line.split("=", 1)[1].strip()
                if found:
                    return found
    pytest.fail(f"{VAR} is not set: add the line to .env and run `{HINT}`")


@pytest.fixture(scope="session")
def clickhouse_up() -> None:
    import asyncio

    def ready() -> bool:
        async def probe() -> bool:
            http: httpx.AsyncClient
            async with httpx.AsyncClient(timeout=5.0) as http:
                client: ClickHouseClient = new_client(http)
                return await client.ping() and await client.select("SELECT 1 AS x") == [{"x": 1}]

        try:
            return asyncio.run(probe())
        except ClickHouseError:
            return False

    poll_until("ClickHouse on localhost:8123", ready)


@pytest_asyncio.fixture
async def radar(clickhouse_up: None) -> AsyncIterator[ClickHouseClient]:
    http: httpx.AsyncClient
    async with httpx.AsyncClient(timeout=30.0) as http:
        yield new_client(http)


@pytest_asyncio.fixture
async def clients(clickhouse_up: None) -> AsyncIterator[Pair]:
    password: str = alerts_password()
    http: httpx.AsyncClient
    async with httpx.AsyncClient(timeout=30.0) as http:
        yield (
            new_client(http),
            ClickHouseClient(http, url=CLICKHOUSE_URL, user="radar_alerts", password=password),
        )


def names() -> tuple[str, str]:
    source: str = f"radar_it_{uuid.uuid4().hex[:12]}"
    return source, source + "_v1"


async def drop(client: ClickHouseClient, source: str, contract: str) -> None:
    await client.execute(f"DROP DATABASE IF EXISTS {contract} SYNC")
    await client.execute(f"DROP DATABASE IF EXISTS {source} SYNC")


def row(
    event: str,
    source: str,
    entity_type: str,
    entity_id: str,
    metric: str,
    value: float,
    observed_at: str,
    window_start: str | None = None,
    ingested_at: str = INGESTED,
) -> dict[str, Scalar]:
    return {
        "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, event)),
        "source": source,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "metric": metric,
        "value": value,
        "observed_at": observed_at,
        "window_start": window_start,
        "ingested_at": ingested_at,
    }


def rows() -> list[dict[str, Scalar]]:
    d: str = "2026-11-17 "
    return [
        row("pl", "steam", "game", "730", "players_current", 1234.0, d + "10:00:00.000000"),
        row("pl", "steam", "game", "730", "players_current", 1234.0, d + "10:00:00.000000", None, LATER),
        row("ch", "steam", "game", "730", "chart_rank", 5.0, d + "00:00:00.000000"),
        row(
            "p1", "bluesky", "post", "at://p1", "tag_godot", 1.0, d + "10:01:00.000000", d + "10:00:00.000000"
        ),
        row(
            "p1",
            "bluesky",
            "post",
            "at://p1",
            "tag_godot",
            1.0,
            d + "10:01:00.000000",
            d + "10:00:00.000000",
            LATER,
        ),
        row(
            "p2", "bluesky", "post", "at://p2", "tag_godot", 1.0, d + "11:00:00.000000", d + "10:59:00.000000"
        ),
        row(
            "p3", "bluesky", "post", "at://p3", "tag_godot", 1.0, d + "11:01:00.000000", d + "11:00:00.000000"
        ),
        row("lk", "bluesky", "post", "at://p1", "likes", 3.0, d + "10:01:00.000000", d + "10:00:00.000000"),
        row("p4", "bluesky", "post", "at://p4", "tag_godot", 1.0, d + "10:30:00.000000", None),
        row("st", "steam", "stream", "570", "players_current", 9.0, d + "10:00:00.000000"),
    ]


async def describe(client: ClickHouseClient, table: str) -> list[tuple[str, str]]:
    result: list[dict[str, object]] = await client.select(f"DESCRIBE TABLE {table}")
    return [(str(r["name"]), str(r["type"])) for r in result]


async def test_17_views_mean_what_the_contract_says(radar: ClickHouseClient) -> None:
    source: str
    contract: str
    source, contract = names()
    try:
        await ensure_schema(radar, source)
        await ensure_contract(radar, source, contract)
        await radar.insert_rows("observations", rows(), database=source)
        players: list[dict[str, object]] = await radar.select(
            f"SELECT appid, toString(observed_at) AS t, players FROM {contract}.steam_players"
        )
        assert [(r["appid"], r["t"], int(str(r["players"]))) for r in players] == [
            ("730", "2026-11-17 10:00:00.000000", 1234)
        ]
        chart: list[dict[str, object]] = await radar.select(
            f"SELECT appid, toString(chart_day) AS t, rank FROM {contract}.steam_chart"
        )
        assert [(r["appid"], r["t"], int(str(r["rank"]))) for r in chart] == [
            ("730", "2026-11-17 00:00:00.000000", 5)
        ]
        tags: list[dict[str, object]] = await radar.select(
            f"SELECT tag, toString(hour) AS h, posts FROM {contract}.bluesky_tag_hourly ORDER BY hour"
        )
        assert [(r["tag"], r["h"], int(str(r["posts"]))) for r in tags] == [
            ("godot", "2026-11-17 10:00:00", 2),
            ("godot", "2026-11-17 11:00:00", 1),
        ]
    finally:
        await drop(radar, source, contract)


async def test_18_view_types(radar: ClickHouseClient) -> None:
    source: str
    contract: str
    source, contract = names()
    try:
        await ensure_schema(radar, source)
        await ensure_contract(radar, source, contract)
        view: str
        for view in CONTRACT_VIEWS:
            assert await describe(radar, f"{contract}.{view}") == EXPECTED_COLUMNS[view], view
    finally:
        await drop(radar, source, contract)


async def test_19_idempotent_and_v1_is_not_rewritten(radar: ClickHouseClient) -> None:
    source: str
    contract: str
    source, contract = names()
    source2: str
    contract2: str
    source2, contract2 = names()
    try:
        await ensure_schema(radar, source)
        await ensure_contract(radar, source, contract)
        await ensure_contract(radar, source, contract)
        tables: list[dict[str, object]] = await radar.select(
            f"SELECT name, engine FROM system.tables WHERE database = '{contract}' ORDER BY name"
        )
        assert sorted((str(t["name"]), str(t["engine"])) for t in tables) == sorted(
            (v, "View") for v in CONTRACT_VIEWS
        )
        await ensure_schema(radar, source2)
        await radar.execute(f"CREATE DATABASE {contract2}")
        await radar.execute(f"CREATE VIEW {contract2}.steam_players AS SELECT 1 AS x")
        await ensure_contract(radar, source2, contract2)
        assert await describe(radar, f"{contract2}.steam_players") == [("x", "UInt8")]
    finally:
        await drop(radar, source, contract)
        await drop(radar, source2, contract2)


async def test_20_alerts_user_reads_the_live_interface(clients: Pair) -> None:
    radar: ClickHouseClient
    alerts: ClickHouseClient
    radar, alerts = clients
    await ensure_schema(radar, "radar")
    await ensure_contract(radar, "radar")
    view: str
    for view in CONTRACT_VIEWS:
        counted: list[dict[str, object]] = await alerts.select(f"SELECT count() AS n FROM alerts_v1.{view}")
        assert len(counted) == 1
        assert "n" in counted[0]
        assert await describe(alerts, f"alerts_v1.{view}") == EXPECTED_COLUMNS[view], view


async def test_21_alerts_user_cannot_read_the_raw_table(clients: Pair) -> None:
    alerts: ClickHouseClient = clients[1]
    info: pytest.ExceptionInfo[ClickHouseError]
    with pytest.raises(ClickHouseError) as info:
        await alerts.select("SELECT count() FROM radar.observations")
    assert "ACCESS_DENIED" in str(info.value)


async def test_22_alerts_user_cannot_run_ddl(clients: Pair) -> None:
    radar: ClickHouseClient
    alerts: ClickHouseClient
    radar, alerts = clients
    await ensure_schema(radar, "radar")
    await ensure_contract(radar, "radar")
    created: bool = False
    try:
        try:
            await alerts.execute("CREATE TABLE alerts_v1.it_forbidden (a Int8) ENGINE = Memory")
        except ClickHouseError as exc:
            assert "ACCESS_DENIED" in str(exc)
        else:
            created = True
    finally:
        await radar.execute("DROP TABLE IF EXISTS alerts_v1.it_forbidden")
    assert not created, "radar_alerts managed to CREATE TABLE in alerts_v1"
    with pytest.raises(ClickHouseError):
        await alerts.execute("DROP VIEW alerts_v1.steam_players")
    exists: list[dict[str, object]] = await radar.select("EXISTS TABLE alerts_v1.steam_players")
    assert [int(str(v)) for v in exists[0].values()] == [1]
