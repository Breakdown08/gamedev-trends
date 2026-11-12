"""Shared fakes for the Steam collector tests: scripted HTTP, limiter, sleep, clock, publisher."""

import json
from collections.abc import AsyncGenerator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from gamedev_trends.bus import InMemoryBroker, PublishError, Record, decode
from gamedev_trends.contract import Event, Observation
from gamedev_trends.steam import SteamClient

T10: datetime = datetime(2026, 11, 12, 10, 0, tzinfo=UTC)
ROLLUP: datetime = datetime(2026, 11, 11, tzinfo=UTC)
ROLLUP_UNIX: int = 1794355200
BASE: str = "https://api.steampowered.com"
CHART_URL: str = BASE + "/ISteamChartsService/GetMostPlayedGames/v1/"
PLAYERS_URL: str = BASE + "/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
DEFAULT_RANKS: list[tuple[int, int, int]] = [(1, 730, 1217867), (2, 570, 716944), (3, 578080, 728370)]
DEFAULT_ONLINE: dict[int, int] = {730: 747936, 570: 500000, 578080: 300000}


def chart_body(ranks: Sequence[tuple[int, int, int]] = DEFAULT_RANKS) -> dict[str, Any]:
    return {
        "response": {
            "rollup_date": ROLLUP_UNIX,
            "ranks": [{"rank": r, "appid": a, "last_week_rank": -1, "peak_in_game": p} for r, a, p in ranks],
        }
    }


def players_body(count: int) -> dict[str, Any]:
    return {"response": {"player_count": count, "result": 1}}


def resp(
    status: int = 200,
    body: object = None,
    *,
    text: str | None = None,
    headers: Mapping[str, str] | None = None,
) -> httpx.Response:
    if text is not None:
        return httpx.Response(status, text=text, headers=dict(headers or {}))
    return httpx.Response(status, content=json.dumps(body).encode(), headers=dict(headers or {}))


def no_data_404() -> httpx.Response:
    return resp(404, {"response": {"result": 42}})


type Item = httpx.Response | Exception


class Script:
    """HTTP handler that plays a fixed list of responses/exceptions; the last one repeats."""

    def __init__(self, *items: Item, on_request: Callable[[], None] | None = None) -> None:
        self.items: list[Item] = list(items)
        self.requests: list[httpx.Request] = []
        self.on_request: Callable[[], None] | None = on_request

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.on_request is not None:
            self.on_request()
        item: Item = self.items.pop(0) if len(self.items) > 1 else self.items[0]
        if isinstance(item, Exception):
            raise item
        return item


class Router:
    """Routes chart and per-appid requests; per-appid handlers are lists of Items (last repeats)."""

    def __init__(
        self,
        chart: Item | None = None,
        online: Mapping[int, int | list[Item]] | None = None,
    ) -> None:
        self.chart: Item = chart if chart is not None else resp(200, chart_body())
        self.online: dict[int, int | list[Item]] = dict(DEFAULT_ONLINE if online is None else online)
        self.requests: list[httpx.Request] = []

    def asked_appids(self) -> list[int]:
        return [int(r.url.params["appid"]) for r in self.requests if "appid" in r.url.params]

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path.endswith("GetMostPlayedGames/v1/"):
            if isinstance(self.chart, Exception):
                raise self.chart
            return self.chart
        appid: int = int(request.url.params["appid"])
        value: int | list[Item] = self.online[appid]
        if isinstance(value, int):
            return resp(200, players_body(value))
        item: Item = value.pop(0) if len(value) > 1 else value[0]
        if isinstance(item, Exception):
            raise item
        return item


class FakeSleep:
    def __init__(self, clock: FakeClock | None = None) -> None:
        self.calls: list[float] = []
        self.clock: FakeClock | None = clock

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
        if self.clock is not None:
            self.clock.advance(seconds)


class FakeClock:
    def __init__(self, start: datetime) -> None:
        self.current: datetime = start

    def now(self) -> datetime:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)


class FakeMonotonic:
    def __init__(self, start: float = 0.0) -> None:
        self.t: float = start

    def __call__(self) -> float:
        return self.t


class ScriptLimiter:
    """RateLimiter returning scripted values (float) or raising scripted exceptions; last repeats."""

    def __init__(self, *items: float | Exception) -> None:
        self.items: list[float | Exception] = list(items) if items else [0.0]
        self.calls: int = 0

    async def try_acquire(self) -> float:
        self.calls += 1
        item: float | Exception = self.items.pop(0) if len(self.items) > 1 else self.items[0]
        if isinstance(item, Exception):
            raise item
        return item


@dataclass
class Rig:
    client: SteamClient
    sleep: FakeSleep
    limiter: ScriptLimiter
    handler: Script | Router


@asynccontextmanager
async def rig(
    handler: Script | Router,
    *,
    limiter: ScriptLimiter | None = None,
    sleep: FakeSleep | None = None,
    base_url: str = BASE,
    max_attempts: int = 4,
    backoff_base: float = 1.0,
    backoff_max: float = 30.0,
) -> AsyncGenerator[Rig]:
    the_sleep: FakeSleep = sleep if sleep is not None else FakeSleep()
    the_limiter: ScriptLimiter = limiter if limiter is not None else ScriptLimiter()
    http: httpx.AsyncClient
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client: SteamClient = SteamClient(
            http,
            the_limiter,
            sleep=the_sleep,
            base_url=base_url,
            max_attempts=max_attempts,
            backoff_base=backoff_base,
            backoff_max=backoff_max,
        )
        yield Rig(client, the_sleep, the_limiter, handler)


@dataclass
class CountingPublisher:
    fail: bool = False
    calls: list[list[Event]] = field(default_factory=list)

    async def publish(self, events: Sequence[Event]) -> None:
        self.calls.append(list(events))
        if self.fail:
            raise PublishError("boom")

    async def close(self) -> None:
        return None


def observations(broker: InMemoryBroker) -> list[Observation]:
    out: list[Observation] = []
    record: Record
    for record in broker.records("radar.observations.v1"):
        event: Event = decode(record.value)
        assert isinstance(event, Observation)
        out.append(event)
    return out
