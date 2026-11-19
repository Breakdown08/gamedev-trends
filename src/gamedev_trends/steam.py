"""Steam collector: polls the most-played chart and current players, publishes Observations.

Every HTTP attempt takes a token from a shared rate limiter; 429, 5xx and transport errors are
retried with exponential backoff. A game whose request finally fails only loses its own
`players_current` measurement.
"""

import argparse
import asyncio
import json
import logging
import math
import os
import signal
import sys
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx
import redis.asyncio

from gamedev_trends.bus import Publisher, PublishError
from gamedev_trends.contract import Event, Observation
from gamedev_trends.entry import run_main
from gamedev_trends.kafka_bus import KafkaPublisher
from gamedev_trends.logs import setup_logging
from gamedev_trends.ratelimit import RateLimiter, RateLimiterError, RedisTokenBucket, Sleep, acquire

type Now = Callable[[], datetime]

logger: logging.Logger = logging.getLogger(__name__)

SOURCE: str = "steam"
ENTITY_TYPE: str = "game"
METRIC_PLAYERS: str = "players_current"
METRIC_RANK: str = "chart_rank"
METRIC_PEAK: str = "chart_peak_players"
STEAM_RATE_LIMIT_KEY: str = "ratelimit:steam"
NO_DATA_RESULT: int = 42
CHART_PATH: str = "/ISteamChartsService/GetMostPlayedGames/v1/"
PLAYERS_PATH: str = "/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
USER_AGENT: str = "gamedev-trends/0.1"

_DAY_SECONDS: int = 86400


def _text(env: Mapping[str, str], name: str, default: str) -> str:
    value: str = env.get(name, "").strip()
    return value or default


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


@dataclass(frozen=True)
class SteamConfig:
    kafka_bootstrap_servers: str = "localhost:9092"
    redis_url: str = "redis://localhost:6379/0"
    api_base: str = "https://api.steampowered.com"
    poll_interval_seconds: int = 3600
    top_n: int = 100
    rate_per_second: float = 1.0
    burst: float = 10.0
    http_timeout_seconds: float = 10.0
    max_attempts: int = 4
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 30.0

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> SteamConfig:
        """Read the STEAM_* variables; absent or empty ones take the defaults, bad ones raise ValueError."""
        d: SteamConfig = cls()
        interval: int = _int(env, "STEAM_POLL_INTERVAL_SECONDS", d.poll_interval_seconds)
        if interval < 60 or _DAY_SECONDS % interval != 0:
            raise ValueError(f"STEAM_POLL_INTERVAL_SECONDS: must be >= 60 and divide 86400, got {interval}")
        top_n: int = _int(env, "STEAM_TOP_N", d.top_n)
        if not 1 <= top_n <= 100:
            raise ValueError(f"STEAM_TOP_N: must be from 1 to 100, got {top_n}")
        rate: float = _float(env, "STEAM_RATE_PER_SECOND", d.rate_per_second)
        if rate <= 0:
            raise ValueError(f"STEAM_RATE_PER_SECOND: must be > 0, got {rate}")
        burst: float = _float(env, "STEAM_BURST", d.burst)
        if burst < 1:
            raise ValueError(f"STEAM_BURST: must be >= 1, got {burst}")
        timeout: float = _float(env, "STEAM_HTTP_TIMEOUT_SECONDS", d.http_timeout_seconds)
        if timeout <= 0:
            raise ValueError(f"STEAM_HTTP_TIMEOUT_SECONDS: must be > 0, got {timeout}")
        attempts: int = _int(env, "STEAM_MAX_ATTEMPTS", d.max_attempts)
        if attempts < 1:
            raise ValueError(f"STEAM_MAX_ATTEMPTS: must be >= 1, got {attempts}")
        base: float = _float(env, "STEAM_BACKOFF_BASE_SECONDS", d.backoff_base_seconds)
        if base <= 0:
            raise ValueError(f"STEAM_BACKOFF_BASE_SECONDS: must be > 0, got {base}")
        backoff_max: float = _float(env, "STEAM_BACKOFF_MAX_SECONDS", d.backoff_max_seconds)
        if backoff_max < base:
            raise ValueError(
                f"STEAM_BACKOFF_MAX_SECONDS: must be >= {base} (the base delay), got {backoff_max}"
            )
        return cls(
            kafka_bootstrap_servers=_text(env, "KAFKA_BOOTSTRAP_SERVERS", d.kafka_bootstrap_servers),
            redis_url=_text(env, "REDIS_URL", d.redis_url),
            api_base=_text(env, "STEAM_API_BASE", d.api_base).rstrip("/"),
            poll_interval_seconds=interval,
            top_n=top_n,
            rate_per_second=rate,
            burst=burst,
            http_timeout_seconds=timeout,
            max_attempts=attempts,
            backoff_base_seconds=base,
            backoff_max_seconds=backoff_max,
        )


@dataclass(frozen=True)
class ChartEntry:
    rank: int
    appid: int
    peak_in_game: int


@dataclass(frozen=True)
class Chart:
    rollup_date: datetime
    entries: tuple[ChartEntry, ...]


class SteamError(Exception):
    """A Steam request finally failed: attempts exhausted or the error is not retryable."""


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _response_object(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise SteamError("malformed response: not a JSON object")
    inner: object = payload.get("response")
    if not isinstance(inner, dict):
        raise SteamError("malformed response: no 'response' object")
    return inner


def _parse_chart(payload: object) -> Chart:
    body: dict[str, object] = _response_object(payload)
    rollup: object = body.get("rollup_date")
    ranks: object = body.get("ranks")
    if not isinstance(rollup, int) or isinstance(rollup, bool) or rollup < 0:
        raise SteamError("malformed chart: bad rollup_date")
    if not isinstance(ranks, list):
        raise SteamError("malformed chart: 'ranks' is not a list")
    entries: list[ChartEntry] = []
    item: object
    for item in ranks:
        if not isinstance(item, dict):
            raise SteamError("malformed chart: rank entry is not an object")
        rank: object = item.get("rank")
        appid: object = item.get("appid")
        peak: object = item.get("peak_in_game")
        if (
            not (isinstance(rank, int) and not isinstance(rank, bool) and rank >= 1)
            or not (isinstance(appid, int) and not isinstance(appid, bool) and appid >= 1)
            or not (isinstance(peak, int) and not isinstance(peak, bool) and peak >= 0)
        ):
            raise SteamError("malformed chart: bad rank entry")
        entries.append(ChartEntry(rank, appid, peak))
    try:
        rollup_date: datetime = datetime.fromtimestamp(rollup, UTC)
    except OverflowError, OSError, ValueError:
        raise SteamError("malformed chart: rollup_date out of range") from None
    entries.sort(key=lambda e: e.rank)
    return Chart(rollup_date, tuple(entries))


def _no_data(payload: object) -> bool:
    if not isinstance(payload, dict):
        return False
    inner: object = payload.get("response")
    return isinstance(inner, dict) and _is_int(inner.get("result")) and inner.get("result") == NO_DATA_RESULT


def _parse_players(payload: object) -> int | None:
    body: dict[str, object] = _response_object(payload)
    result: object = body.get("result")
    if _is_int(result) and result == NO_DATA_RESULT:
        return None
    count: object = body.get("player_count")
    if not (isinstance(count, int) and not isinstance(count, bool)) or count < 0:
        raise SteamError("malformed players response: bad player_count")
    if not _is_int(result) or result != 1:
        raise SteamError(f"unexpected result in players response: {result!r}")
    return count


def _json(response: httpx.Response) -> object | None:
    try:
        payload: object = json.loads(response.content)
    except ValueError:
        return None
    return payload


class SteamClient:
    def __init__(
        self,
        http: httpx.AsyncClient,
        limiter: RateLimiter,
        *,
        sleep: Sleep = asyncio.sleep,
        base_url: str = "https://api.steampowered.com",
        max_attempts: int = 4,
        backoff_base: float = 1.0,
        backoff_max: float = 30.0,
    ) -> None:
        self._http: httpx.AsyncClient = http
        self._limiter: RateLimiter = limiter
        self._sleep: Sleep = sleep
        self._base_url: str = base_url.rstrip("/")
        self._max_attempts: int = max_attempts
        self._backoff_base: float = backoff_base
        self._backoff_max: float = backoff_max

    async def most_played(self) -> Chart:
        response: httpx.Response = await self._get(CHART_PATH, None)
        payload: object | None = _json(response)
        if payload is None:
            raise SteamError("malformed chart: body is not JSON")
        return _parse_chart(payload)

    async def current_players(self, appid: int) -> int | None:
        """Current players. None: the game has no player data (result 42)."""
        response: httpx.Response = await self._get(PLAYERS_PATH, appid)
        payload: object | None = _json(response)
        if response.status_code == 404:
            if _no_data(payload):
                return None
            raise SteamError(f"{PLAYERS_PATH} appid={appid}: HTTP 404")
        if payload is None:
            raise SteamError("malformed players response: body is not JSON")
        return _parse_players(payload)

    async def _get(self, path: str, appid: int | None) -> httpx.Response:
        """One logical request with retries; returns a 200 response or a 404 with result 42."""
        url: str = self._base_url + path
        params: dict[str, int] | None = None if appid is None else {"appid": appid}
        attempt: int = 0
        while True:
            attempt += 1
            reason: str
            cause: Exception
            retry_after: float | None = None
            try:
                await acquire(self._limiter, self._sleep)
                response: httpx.Response = await self._http.get(url, params=params)
            except RateLimiterError as exc:
                reason, cause = f"rate limiter failed: {exc}", exc
            except httpx.TransportError as exc:
                reason, cause = f"{type(exc).__name__}: {exc}", exc
            else:
                status: int = response.status_code
                if status == 200:
                    return response
                if status == 404 and appid is not None and _no_data(_json(response)):
                    return response
                if status != 429 and not 500 <= status <= 599:
                    raise SteamError(f"{path} appid={appid}: HTTP {status}")
                reason = f"HTTP {status}"
                cause = SteamError(reason)
                if status == 429:
                    retry_after = self._retry_after(response)
            if attempt >= self._max_attempts:
                raise SteamError(f"{path} appid={appid}: failed after {attempt} attempts: {reason}") from (
                    None if isinstance(cause, SteamError) else cause
                )
            delay: float = (
                min(self._backoff_max, retry_after)
                if retry_after is not None
                else min(self._backoff_max, self._backoff_base * 2 ** (attempt - 1))
            )
            logger.warning(
                "steam request retry: path=%s appid=%s reason=%s delay=%s", path, appid, reason, delay
            )
            await self._sleep(delay)

    @staticmethod
    def _retry_after(response: httpx.Response) -> float | None:
        raw: str | None = response.headers.get("Retry-After")
        if raw is None:
            return None
        text: str = raw.strip()
        if not text.isascii() or not text.isdigit():
            return None
        return float(int(text))


@dataclass(frozen=True)
class CycleReport:
    slot: datetime
    chart_ok: bool
    games: int  # number of unique games taken from the chart (<= top_n)
    no_data: tuple[int, ...]
    failed: tuple[int, ...]
    published: int
    publish_ok: bool


class SteamCollector:
    def __init__(self, client: SteamClient, publisher: Publisher, top_n: int = 100) -> None:
        self._client: SteamClient = client
        self._publisher: Publisher = publisher
        self._top_n: int = top_n

    async def run_slot(self, slot: datetime) -> CycleReport:
        """Collect one slot.

        The first `top_n` chart entries are taken first, then repeated appids are dropped keeping the
        first entry; `CycleReport.games` is therefore the number of unique games.
        """
        if slot.tzinfo is None or slot.utcoffset() is None:
            raise ValueError("slot must be timezone-aware")
        chart: Chart
        try:
            chart = await self._client.most_played()
        except SteamError:
            logger.exception("steam chart request failed: slot=%s", slot.isoformat())
            return CycleReport(slot, False, 0, (), (), 0, True)
        entries: list[ChartEntry] = []
        seen: set[int] = set()
        entry: ChartEntry
        for entry in chart.entries[: self._top_n]:
            if entry.appid not in seen:
                seen.add(entry.appid)
                entries.append(entry)
        if not entries:
            logger.warning("steam chart is empty: slot=%s", slot.isoformat())
        events: list[Event] = []
        no_data: list[int] = []
        failed: list[int] = []
        for entry in entries:
            events.append(self._observation(entry.appid, METRIC_RANK, entry.rank, chart.rollup_date))
            events.append(self._observation(entry.appid, METRIC_PEAK, entry.peak_in_game, chart.rollup_date))
            try:
                players: int | None = await self._client.current_players(entry.appid)
            except Exception as exc:
                logger.warning("steam players request failed: appid=%d error=%r", entry.appid, exc)
                failed.append(entry.appid)
                continue
            if players is None:
                no_data.append(entry.appid)
            else:
                events.append(self._observation(entry.appid, METRIC_PLAYERS, players, slot))
        published: int = 0
        publish_ok: bool = True
        if events:
            try:
                await self._publisher.publish(events)
                published = len(events)
            except PublishError:
                logger.exception("publishing failed: slot=%s events=%d", slot.isoformat(), len(events))
                publish_ok = False
        report: CycleReport = CycleReport(
            slot, True, len(entries), tuple(no_data), tuple(failed), published, publish_ok
        )
        logger.info(
            "steam cycle done: slot=%s chart_ok=%s games=%d no_data=%d failed=%d published=%d publish_ok=%s",
            slot.isoformat(),
            report.chart_ok,
            report.games,
            len(report.no_data),
            len(report.failed),
            report.published,
            report.publish_ok,
        )
        return report

    @staticmethod
    def _observation(appid: int, metric: str, value: int, observed_at: datetime) -> Observation:
        return Observation.create(
            source=SOURCE,
            entity_type=ENTITY_TYPE,
            entity_id=str(appid),
            metric=metric,
            value=value,
            observed_at=observed_at,
        )


def slot_start(now: datetime, interval_seconds: int) -> datetime:
    """Start of the slot containing now (aware UTC). A naive now raises ValueError."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    seconds: int = math.floor(now.timestamp() / interval_seconds) * interval_seconds
    return datetime.fromtimestamp(seconds, UTC)


async def run_forever(
    run_slot: Callable[[datetime], Awaitable[CycleReport]],
    *,
    interval_seconds: int,
    now: Now,
    sleep: Sleep,
    max_slots: int | None = None,
) -> None:
    interval: timedelta = timedelta(seconds=interval_seconds)
    slot: datetime = slot_start(now(), interval_seconds)
    runs: int = 0
    while True:
        try:
            await run_slot(slot)
        except Exception:
            logger.exception("steam slot failed: slot=%s", slot.isoformat())
        runs += 1
        if max_slots is not None and runs >= max_slots:
            return
        following: datetime = slot + interval
        current: datetime = now()
        if current < following:
            await sleep((following - current).total_seconds())
            slot = following
        elif current >= following + interval:
            slot = slot_start(current, interval_seconds)
            logger.warning("steam cycle overran: skipped %d slot(s)", (slot - following) // interval)
        else:
            slot = following


async def _serve(config: SteamConfig, once: bool) -> int:
    http: httpx.AsyncClient = httpx.AsyncClient(
        timeout=config.http_timeout_seconds, headers={"User-Agent": USER_AGENT}
    )
    redis_client: redis.asyncio.Redis = redis.asyncio.Redis.from_url(config.redis_url)
    publisher: KafkaPublisher | None = None
    try:
        publisher = KafkaPublisher(config.kafka_bootstrap_servers)
        limiter: RedisTokenBucket = RedisTokenBucket(
            redis_client, STEAM_RATE_LIMIT_KEY, config.burst, config.rate_per_second
        )
        client: SteamClient = SteamClient(
            http,
            limiter,
            base_url=config.api_base,
            max_attempts=config.max_attempts,
            backoff_base=config.backoff_base_seconds,
            backoff_max=config.backoff_max_seconds,
        )
        collector: SteamCollector = SteamCollector(client, publisher, config.top_n)
        if once:
            report: CycleReport = await collector.run_slot(
                slot_start(datetime.now(UTC), config.poll_interval_seconds)
            )
            return 0 if report.chart_ok and report.publish_ok else 1
        await run_forever(
            collector.run_slot,
            interval_seconds=config.poll_interval_seconds,
            now=lambda: datetime.now(UTC),
            sleep=asyncio.sleep,
        )
        return 0
    finally:
        if publisher is not None:
            await publisher.close()
        await http.aclose()
        await redis_client.aclose()


async def _run(config: SteamConfig, once: bool) -> int:
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    task: asyncio.Task[int] = asyncio.create_task(_serve(config, once))
    sig: signal.Signals
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, task.cancel)
    try:
        return await task
    except asyncio.CancelledError:
        return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(prog="python -m gamedev_trends.steam")
    parser.add_argument("--once", action="store_true", help="collect the current slot once and exit")
    args: argparse.Namespace = parser.parse_args(argv)
    config: SteamConfig
    try:
        config = SteamConfig.from_env(os.environ)
    except ValueError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    setup_logging()
    return asyncio.run(_run(config, bool(args.once)))


if __name__ == "__main__":
    run_main(__spec__)
