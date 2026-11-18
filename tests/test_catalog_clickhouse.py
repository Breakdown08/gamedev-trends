"""Spec checks 22-23: radar_appids and growing_games parameter checks over MockTransport."""

from collections.abc import Awaitable
from datetime import UTC, datetime

import httpx
import pytest

from gamedev_trends.catalog import GrowingGame, growing_games, radar_appids
from gamedev_trends.clickhouse import ClickHouseClient

NOW: datetime = datetime(2026, 11, 20, tzinfo=UTC)


def client_with(requests: list[httpx.Request], body: str) -> ClickHouseClient:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, text=body)

    http: httpx.AsyncClient = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return ClickHouseClient(http, url="http://ch.test:8123", user="radar", password="x")


async def test_22_radar_appids() -> None:
    requests: list[httpx.Request] = []
    body: str = "".join(f'{{"entity_id": "{x}"}}\n' for x in ("730", "it-0b1c", "440", "0", "٣", ""))
    result: list[int] = await radar_appids(client_with(requests, body), "radar")
    assert result == [440, 730]
    assert len(requests) == 1
    sql: str = requests[0].content.decode()
    assert "FINAL" in sql
    assert "source = 'steam'" in sql
    assert "entity_type = 'game'" in sql


async def test_22_bad_database_name_makes_no_request() -> None:
    requests: list[httpx.Request] = []
    with pytest.raises(ValueError):
        await radar_appids(client_with(requests, ""), "Bad-Name")
    assert requests == []


async def test_23_growing_games_bad_parameters() -> None:
    requests: list[httpx.Request] = []
    client: ClickHouseClient = client_with(requests, "")
    calls: list[Awaitable[list[GrowingGame]]] = [
        growing_games(client, "radar", now=datetime(2026, 11, 20)),
        growing_games(client, "radar", now=NOW, window_hours=0),
        growing_games(client, "radar", now=NOW, lag_hours=10, window_hours=24),
        growing_games(client, "radar", now=NOW, min_growth=-0.1),
        growing_games(client, "radar", now=NOW, min_samples=0),
    ]
    call: Awaitable[list[GrowingGame]]
    for call in calls:
        with pytest.raises(ValueError):
            await call
    assert requests == []


async def test_23_growing_games_bad_database_name() -> None:
    requests: list[httpx.Request] = []
    with pytest.raises(ValueError):
        await growing_games(client_with(requests, ""), "Bad-Name", now=NOW)
    assert requests == []
