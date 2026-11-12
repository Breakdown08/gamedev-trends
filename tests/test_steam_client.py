"""Spec checks 10-23: SteamClient over httpx.MockTransport, fake sleep and limiter."""

from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from steam_support import (
    CHART_URL,
    PLAYERS_URL,
    ROLLUP,
    ROLLUP_UNIX,
    Rig,
    Script,
    ScriptLimiter,
    chart_body,
    no_data_404,
    players_body,
    resp,
    rig,
)

from gamedev_trends.ratelimit import RateLimiterError
from gamedev_trends.steam import Chart, ChartEntry, SteamError


def ok(n: int = 5) -> httpx.Response:
    return resp(200, players_body(n))


async def test_10_most_played_parses_chart() -> None:
    r: Rig
    async with rig(Script(resp(200, chart_body()))) as r:
        chart: Chart = await r.client.most_played()
    assert chart == Chart(
        rollup_date=datetime(2026, 11, 11, tzinfo=UTC),
        entries=(ChartEntry(1, 730, 1217867), ChartEntry(2, 570, 716944), ChartEntry(3, 578080, 728370)),
    )
    assert chart.rollup_date.utcoffset() is not None
    request: httpx.Request = r.handler.requests[0]
    assert request.method == "GET"
    assert str(request.url) == CHART_URL
    assert request.url.query == b""


async def test_11_chart_sorted_by_rank_and_extra_fields_ignored() -> None:
    body: dict[str, Any] = {
        "response": {
            "rollup_date": ROLLUP_UNIX,
            "foo": 1,
            "ranks": [
                {"rank": 3, "appid": 578080, "last_week_rank": -1, "peak_in_game": 728370, "foo": 1},
                {"rank": 1, "appid": 730, "last_week_rank": 1, "peak_in_game": 1217867},
                {"rank": 2, "appid": 570, "last_week_rank": 2, "peak_in_game": 716944},
            ],
        }
    }
    r: Rig
    async with rig(Script(resp(200, body))) as r:
        chart: Chart = await r.client.most_played()
    assert chart.rollup_date == ROLLUP
    assert [e.rank for e in chart.entries] == [1, 2, 3]
    assert [e.appid for e in chart.entries] == [730, 570, 578080]


async def test_12_current_players_request_and_result() -> None:
    r: Rig
    async with rig(Script(resp(200, players_body(747936)))) as r:
        assert await r.client.current_players(730) == 747936
    request: httpx.Request = r.handler.requests[0]
    assert request.method == "GET"
    assert str(request.url) == PLAYERS_URL + "?appid=730"
    assert dict(request.url.params) == {"appid": "730"}


async def test_12_base_url_trailing_slash_has_no_double_slash() -> None:
    r: Rig
    async with rig(Script(ok()), base_url="http://steam.test/") as r:
        await r.client.current_players(730)
    assert str(r.handler.requests[0].url).startswith("http://steam.test/ISteamUserStats/")
    assert "//ISteam" not in str(r.handler.requests[0].url)


async def test_13_zero_players_is_zero_not_none() -> None:
    r: Rig
    async with rig(Script(resp(200, players_body(0)))) as r:
        result: int | None = await r.client.current_players(1)
    assert result == 0
    assert result is not None


@pytest.mark.parametrize("response", [no_data_404(), resp(200, {"response": {"result": 42}})])
async def test_14_result_42_is_none_without_retry(response: httpx.Response) -> None:
    r: Rig
    async with rig(Script(response)) as r:
        assert await r.client.current_players(1) is None
    assert len(r.handler.requests) == 1
    assert r.sleep.calls == []


async def test_15_retries_503_then_ok() -> None:
    r: Rig
    async with rig(Script(resp(503), resp(503), ok(77))) as r:
        assert await r.client.current_players(730) == 77
    assert len(r.handler.requests) == 3
    assert r.sleep.calls == [1.0, 2.0]


async def test_16_exhausted_attempts_raise_steam_error() -> None:
    r: Rig
    async with rig(Script(resp(500)), max_attempts=4) as r:
        with pytest.raises(SteamError, match="500"):
            await r.client.current_players(730)
    assert len(r.handler.requests) == 4
    assert r.sleep.calls == [1.0, 2.0, 4.0]


async def test_17_backoff_capped_by_backoff_max() -> None:
    r: Rig
    async with rig(Script(resp(502)), max_attempts=4, backoff_base=10, backoff_max=15) as r:
        with pytest.raises(SteamError):
            await r.client.current_players(730)
    assert r.sleep.calls == [10.0, 15.0, 15.0]


@pytest.mark.parametrize(
    ("headers", "backoff_max", "expected"),
    [
        ({"Retry-After": "7"}, 30.0, [7.0]),
        ({"Retry-After": "100"}, 30.0, [30.0]),
        ({"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}, 30.0, [1.0]),
        ({"Retry-After": "soon"}, 30.0, [1.0]),
        ({}, 30.0, [1.0]),
    ],
)
async def test_18_429_retry_after(headers: dict[str, str], backoff_max: float, expected: list[float]) -> None:
    r: Rig
    async with rig(Script(resp(429, headers=headers), ok()), backoff_max=backoff_max) as r:
        assert await r.client.current_players(730) == 5
    assert r.sleep.calls == expected


async def test_19_timeout_then_ok() -> None:
    r: Rig
    async with rig(Script(httpx.ReadTimeout("slow"), ok(9))) as r:
        assert await r.client.current_players(730) == 9
    assert r.sleep.calls == [1.0]


async def test_19_connect_error_everywhere_keeps_cause() -> None:
    r: Rig
    async with rig(Script(httpx.ConnectError("down")), max_attempts=4) as r:
        info: pytest.ExceptionInfo[SteamError]
        with pytest.raises(SteamError) as info:
            await r.client.current_players(730)
    assert isinstance(info.value.__cause__, httpx.ConnectError)
    assert len(r.handler.requests) == 4


async def test_19_remote_protocol_error_is_retried() -> None:
    r: Rig
    async with rig(Script(httpx.RemoteProtocolError("cut"), ok(3))) as r:
        assert await r.client.current_players(730) == 3


@pytest.mark.parametrize(
    "response",
    [
        resp(400, text="<html>bad</html>"),
        resp(403, text="<html>no</html>"),
        resp(404, text="<html>none</html>"),
        resp(404, {"response": {"result": 2}}),
    ],
)
async def test_20_other_4xx_fail_at_once(response: httpx.Response) -> None:
    r: Rig
    async with rig(Script(response)) as r:
        with pytest.raises(SteamError):
            await r.client.current_players(730)
    assert len(r.handler.requests) == 1
    assert r.sleep.calls == []


@pytest.mark.parametrize(
    "response",
    [
        resp(200, text="not json"),
        resp(200, {}),
        resp(200, {"response": {}}),
        resp(200, {"response": {"player_count": "many", "result": 1}}),
        resp(200, {"response": {"player_count": -1, "result": 1}}),
        resp(200, {"response": {"player_count": True, "result": 1}}),
        resp(200, {"response": {"player_count": 5, "result": 2}}),
    ],
)
async def test_21_bad_player_body_fails_at_once(response: httpx.Response) -> None:
    r: Rig
    async with rig(Script(response)) as r:
        with pytest.raises(SteamError):
            await r.client.current_players(730)
    assert len(r.handler.requests) == 1
    assert r.sleep.calls == []


def _bad_chart_bodies() -> list[Any]:
    good: dict[str, Any] = {"rank": 1, "appid": 730, "peak_in_game": 10}
    return [
        {"response": {"rollup_date": ROLLUP_UNIX, "ranks": "nope"}},
        {"response": {"rollup_date": ROLLUP_UNIX, "ranks": [{"rank": 1, "peak_in_game": 10}]}},
        {"response": {"rollup_date": ROLLUP_UNIX, "ranks": [{**good, "rank": 0}]}},
        {"response": {"ranks": [good]}},
    ]


@pytest.mark.parametrize("body", _bad_chart_bodies())
async def test_21_bad_chart_body_fails_at_once(body: object) -> None:
    r: Rig
    async with rig(Script(resp(200, body))) as r:
        with pytest.raises(SteamError):
            await r.client.most_played()
    assert len(r.handler.requests) == 1
    assert r.sleep.calls == []


async def test_22_every_attempt_takes_a_token() -> None:
    limiter: ScriptLimiter = ScriptLimiter()
    r: Rig
    async with rig(Script(resp(503), ok()), limiter=limiter) as r:
        await r.client.current_players(730)
    assert limiter.calls == 2


async def test_22_token_wait_happens_before_the_request() -> None:
    seen: list[list[float]] = []
    limiter: ScriptLimiter = ScriptLimiter(0.5, 0.0)
    holder: list[Rig] = []
    script: Script = Script(ok(), on_request=lambda: seen.append(list(holder[0].sleep.calls)))
    r: Rig
    async with rig(script, limiter=limiter) as r:
        holder.append(r)
        await r.client.current_players(730)
    assert seen == [[0.5]]


async def test_23_limiter_error_once_is_retried_with_backoff() -> None:
    limiter: ScriptLimiter = ScriptLimiter(RateLimiterError("redis down"), 0.0)
    r: Rig
    async with rig(Script(ok(8)), limiter=limiter) as r:
        assert await r.client.current_players(730) == 8
    assert 1.0 in r.sleep.calls
    assert len(r.handler.requests) == 1


async def test_23_limiter_error_everywhere_means_no_requests() -> None:
    limiter: ScriptLimiter = ScriptLimiter(RateLimiterError("redis down"))
    r: Rig
    async with rig(Script(ok()), limiter=limiter, max_attempts=4) as r:
        with pytest.raises(SteamError):
            await r.client.current_players(730)
    assert r.handler.requests == []
