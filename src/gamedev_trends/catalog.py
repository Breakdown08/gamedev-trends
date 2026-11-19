"""Steam game catalog: Steam responses -> pydantic models -> MongoDB.

CLI: `python -m gamedev_trends.catalog refresh | niche TAG | indie TAG | growing`.
"""

import argparse
import asyncio
import json
import logging
import math
import os
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Protocol

import httpx
from pydantic import BaseModel, ConfigDict, Field

from gamedev_trends.clickhouse import DATABASE_PATTERN, ClickHouseClient
from gamedev_trends.entry import run_main
from gamedev_trends.logs import setup_logging
from gamedev_trends.ratelimit import Sleep
from gamedev_trends.steam import USER_AGENT
from gamedev_trends.store import format_datetime

if TYPE_CHECKING:
    from gamedev_trends.mongo_catalog import UpsertResult

logger: logging.Logger = logging.getLogger(__name__)

STEAM_LANGUAGE: str = "english"
STEAM_COUNTRY: str = "US"
TAG_COUNT: int = 20
CHUNK_SIZE: int = 50
ITEMS_PATH: str = "/IStoreBrowseService/GetItems/v1/"
TAGS_PATH: str = "/IStoreService/GetTagList/v1/"
CATEGORIES_PATH: str = "/IStoreBrowseService/GetStoreCategories/v1/"


class Tag(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")
    tagid: int
    name: str | None
    weight: int


class Category(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")
    categoryid: int
    name: str | None


class Game(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")
    appid: int = Field(ge=1)
    name: str
    steam_type: int
    tags: list[Tag]
    categories: list[Category]
    developers: list[str]
    publishers: list[str]
    is_free: bool
    price_usd_cents: int | None
    discount_pct: int = Field(ge=0, le=100)
    release_date: datetime | None
    coming_soon: bool
    early_access: bool
    updated_at: datetime


class StoredGame(Game):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")
    created_at: datetime


@dataclass(frozen=True)
class ParsedItems:
    games: tuple[Game, ...]
    unavailable: tuple[int, ...]
    malformed: tuple[int, ...]


class CatalogError(Exception):
    """Steam answered with an error or a body that cannot be parsed; or attempts are exhausted."""


def items_request(appids: Sequence[int]) -> str:
    payload: dict[str, Any] = {
        "ids": [{"appid": a} for a in appids],
        "context": {"language": STEAM_LANGUAGE, "country_code": STEAM_COUNTRY},
        "data_request": {"include_tag_count": TAG_COUNT, "include_basic_info": True, "include_release": True},
    }
    return json.dumps(payload, separators=(",", ":"))


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _dictionary(payload: object, key: str, name_key: str, id_key: str) -> dict[int, str]:
    response: object = payload.get("response") if isinstance(payload, dict) else None
    entries: object = response.get(key) if isinstance(response, dict) else None
    if not isinstance(entries, list):
        raise CatalogError(f"response.{key} is missing or not a list")
    result: dict[int, str] = {}
    entry: object
    for entry in entries:
        if not isinstance(entry, dict):
            raise CatalogError(f"{key} entry is not an object: {entry!r}")
        ident: object = entry.get(id_key)
        name: object = entry.get(name_key)
        if not _is_int(ident) or not isinstance(name, str):
            raise CatalogError(f"{key} entry without int {id_key} and str {name_key}: {entry!r}")
        result[int(str(ident))] = name
    return result


def parse_tag_list(payload: object) -> dict[int, str]:
    return _dictionary(payload, "tags", "name", "tagid")


def parse_categories(payload: object) -> dict[int, str]:
    return _dictionary(payload, "categories", "display_name", "categoryid")


def _names(basic: dict[str, Any], key: str) -> list[str]:
    entries: Any = basic.get(key)
    if entries is None:
        return []
    return [str(e["name"]) for e in entries]


def _price(option: dict[str, Any] | None) -> tuple[int | None, int]:
    if option is None:
        return None, 0
    cents: Any = option.get("final_price_in_cents")
    return (None if cents is None else int(cents)), int(option.get("discount_pct", 0))


def _game(
    item: dict[str, Any], tags: Mapping[int, str], categories: Mapping[int, str], now: datetime
) -> Game:
    ident: Any = item["id"]
    name: Any = item.get("name")
    steam_type: Any = item.get("type")
    if not isinstance(name, str) or not _is_int(steam_type):
        raise ValueError("name must be a string and type an integer")
    raw_tags: list[dict[str, Any]] = item.get("tags") or []
    ordered: list[dict[str, Any]] = sorted(raw_tags, key=lambda t: (-t["weight"], t["tagid"]))
    tag_models: list[Tag] = [
        Tag(tagid=t["tagid"], name=tags.get(t["tagid"]), weight=t["weight"]) for t in ordered
    ]
    category_ids: list[int] = []
    block: dict[str, Any] = item.get("categories") or {}
    key: str
    ids: list[int]
    for key, ids in block.items():
        if not key.endswith("_categoryids"):
            continue
        cid: int
        for cid in ids:
            if cid not in category_ids:
                category_ids.append(cid)
    basic: dict[str, Any] = item.get("basic_info") or {}
    release: dict[str, Any] = item.get("release") or {}
    price: int | None
    discount: int
    price, discount = _price(item.get("best_purchase_option"))
    released: Any = release.get("steam_release_date")
    return Game(
        appid=ident,
        name=name,
        steam_type=steam_type,
        tags=tag_models,
        categories=[Category(categoryid=c, name=categories.get(c)) for c in category_ids],
        developers=_names(basic, "developers"),
        publishers=_names(basic, "publishers"),
        is_free=item.get("is_free") is True,
        price_usd_cents=price,
        discount_pct=discount,
        release_date=None if released is None else datetime.fromtimestamp(int(released), UTC),
        coming_soon=release.get("is_coming_soon") is True,
        early_access=release.get("is_early_access") is True,
        updated_at=now,
    )


def parse_items(
    payload: object,
    appids: Sequence[int],
    tags: Mapping[int, str],
    categories: Mapping[int, str],
    now: datetime,
) -> ParsedItems:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    response: object = payload.get("response") if isinstance(payload, dict) else None
    if not isinstance(response, dict):
        raise CatalogError("response is missing or not an object")
    store_items: object = response.get("store_items", [])
    if not isinstance(store_items, list):
        raise CatalogError("response.store_items is not a list")
    requested: list[int] = list(dict.fromkeys(appids))
    wanted: set[int] = set(requested)
    found: dict[int, dict[str, Any]] = {}
    item: object
    for item in store_items:
        if not isinstance(item, dict) or not _is_int(item.get("id")):
            continue
        if item["id"] in wanted and item["id"] not in found:
            found[item["id"]] = item
    games: list[Game] = []
    malformed: list[int] = []
    ident: int
    entry: dict[str, Any]
    for ident, entry in found.items():
        if entry.get("success") != 1:
            continue
        try:
            games.append(_game(entry, tags, categories, now))
        except ValueError, TypeError, KeyError, AttributeError, OverflowError, OSError:
            malformed.append(ident)
    unavailable: tuple[int, ...] = tuple(
        a for a in requested if a not in found or found[a].get("success") != 1
    )
    return ParsedItems(games=tuple(games), unavailable=unavailable, malformed=tuple(malformed))


class StoreSource(Protocol):
    async def tag_names(self) -> dict[int, str]: ...
    async def category_names(self) -> dict[int, str]: ...
    async def items(self, appids: Sequence[int]) -> object: ...


class StoreClient:
    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        base_url: str = "https://api.steampowered.com",
        max_attempts: int = 3,
        backoff_base: float = 1.0,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self._http: httpx.AsyncClient = http
        self._base: str = base_url.rstrip("/")
        self._max_attempts: int = max_attempts
        self._backoff_base: float = backoff_base
        self._sleep: Sleep = sleep

    async def _get(self, path: str, params: Mapping[str, str]) -> object:
        attempt: int
        for attempt in range(1, self._max_attempts + 1):
            if attempt > 1:
                await self._sleep(self._backoff_base * 2 ** (attempt - 2))
            try:
                response: httpx.Response = await self._http.get(f"{self._base}{path}", params=params)
            except httpx.TransportError as exc:
                logger.warning("steam %s attempt %d failed: %r", path, attempt, exc)
                continue
            if response.status_code == 429 or response.status_code >= 500:
                logger.warning("steam %s attempt %d answered %d", path, attempt, response.status_code)
                continue
            if response.status_code != 200:
                raise CatalogError(f"steam {path} answered {response.status_code}")
            try:
                return json.loads(response.text)
            except ValueError as exc:
                raise CatalogError(f"steam {path} answered a body that is not JSON") from exc
        raise CatalogError(f"steam {path}: {self._max_attempts} attempts exhausted")

    async def tag_names(self) -> dict[int, str]:
        return parse_tag_list(await self._get(TAGS_PATH, {"language": STEAM_LANGUAGE}))

    async def category_names(self) -> dict[int, str]:
        payload: object = await self._get(
            CATEGORIES_PATH, {"input_json": json.dumps({"language": STEAM_LANGUAGE}, separators=(",", ":"))}
        )
        return parse_categories(payload)

    async def items(self, appids: Sequence[int]) -> object:
        return await self._get(ITEMS_PATH, {"input_json": items_request(appids)})


def _check_database(database: str) -> None:
    if re.fullmatch(DATABASE_PATTERN, database) is None:
        raise ValueError(f"database name must match {DATABASE_PATTERN}, got {database!r}")


async def radar_appids(client: ClickHouseClient, database: str) -> list[int]:
    _check_database(database)
    rows: list[dict[str, object]] = await client.select(
        f"SELECT DISTINCT entity_id FROM {database}.observations FINAL "
        "WHERE source = 'steam' AND entity_type = 'game'"
    )
    found: set[int] = set()
    row: dict[str, object]
    for row in rows:
        raw: str = str(row.get("entity_id", ""))
        if raw.isascii() and raw.isdigit() and int(raw) >= 1:
            found.add(int(raw))
    return sorted(found)


@dataclass(frozen=True)
class GrowingGame:
    appid: int
    before: float
    recent: float
    growth: float


async def growing_games(
    client: ClickHouseClient,
    database: str,
    *,
    now: datetime,
    window_hours: int = 24,
    lag_hours: int = 168,
    min_growth: float = 0.2,
    min_samples: int = 12,
) -> list[GrowingGame]:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if window_hours < 1:
        raise ValueError("window_hours must be >= 1")
    if lag_hours < window_hours:
        raise ValueError("lag_hours must be >= window_hours")
    if min_growth < 0:
        raise ValueError("min_growth must be >= 0")
    if min_samples < 1:
        raise ValueError("min_samples must be >= 1")
    _check_database(database)
    recent_from: str = format_datetime(now - timedelta(hours=window_hours))
    before_to: str = format_datetime(now - timedelta(hours=lag_hours))
    before_from: str = format_datetime(now - timedelta(hours=lag_hours + window_hours))
    end: str = format_datetime(now)

    def at(value: str) -> str:
        return f"toDateTime64('{value}', 6, 'UTC')"

    in_recent: str = f"observed_at >= {at(recent_from)} AND observed_at < {at(end)}"
    in_before: str = f"observed_at >= {at(before_from)} AND observed_at < {at(before_to)}"
    rows: list[dict[str, object]] = await client.select(
        f"SELECT entity_id, avgIf(value, {in_before}) AS before, avgIf(value, {in_recent}) AS recent\n"
        f"FROM {database}.observations FINAL\n"
        "WHERE source = 'steam' AND entity_type = 'game' AND metric = 'players_current'\n"
        f"AND match(entity_id, '^[0-9]+$') AND observed_at >= {at(before_from)} AND observed_at < {at(end)}\n"
        "GROUP BY entity_id\n"
        f"HAVING countIf({in_before}) >= {min_samples} AND countIf({in_recent}) >= {min_samples}"
    )
    result: list[GrowingGame] = []
    row: dict[str, object]
    for row in rows:
        before: float = float(str(row["before"]))
        recent: float = float(str(row["recent"]))
        if before <= 0:
            continue
        growth: float = recent / before - 1
        if growth >= min_growth:
            result.append(GrowingGame(int(str(row["entity_id"])), before, recent, growth))
    result.sort(key=lambda g: (-g.growth, g.appid))
    return result


class CatalogSink(Protocol):
    async def upsert(self, games: Sequence[Game], now: datetime) -> UpsertResult: ...


@dataclass(frozen=True)
class RefreshReport:
    requested: int
    games: int
    inserted: int
    matched: int
    unavailable: tuple[int, ...]
    malformed: tuple[int, ...]
    failed: tuple[int, ...]


async def refresh(
    source: StoreSource,
    sink: CatalogSink,
    appids: Sequence[int],
    *,
    now: datetime,
    chunk_size: int = CHUNK_SIZE,
) -> RefreshReport:
    if chunk_size < 1:
        raise ValueError("chunk_size must be >= 1")
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    unique: list[int] = list(dict.fromkeys(appids))
    tags: dict[int, str] = await source.tag_names()
    categories: dict[int, str] = await source.category_names()
    games: int = 0
    inserted: int = 0
    matched: int = 0
    unavailable: list[int] = []
    malformed: list[int] = []
    failed: list[int] = []
    start: int
    for start in range(0, len(unique), chunk_size):
        chunk: list[int] = unique[start : start + chunk_size]
        try:
            payload: object = await source.items(chunk)
        except CatalogError as exc:
            logger.warning("catalog refresh: chunk of %d appids failed: %s", len(chunk), exc)
            failed.extend(chunk)
            continue
        parsed: ParsedItems = parse_items(payload, chunk, tags, categories, now)
        unavailable.extend(parsed.unavailable)
        malformed.extend(parsed.malformed)
        if parsed.games:
            written: UpsertResult = await sink.upsert(parsed.games, now)
            games += len(parsed.games)
            inserted += written.inserted
            matched += written.matched
    return RefreshReport(
        requested=len(unique),
        games=games,
        inserted=inserted,
        matched=matched,
        unavailable=tuple(unavailable),
        malformed=tuple(malformed),
        failed=tuple(failed),
    )


def _text(env: Mapping[str, str], name: str, default: str) -> str:
    return env.get(name, "").strip() or default


@dataclass(frozen=True)
class CatalogConfig:
    mongodb_url: str = "mongodb://localhost:27017"
    mongodb_database: str = "radar"
    api_base: str = "https://api.steampowered.com"
    http_timeout_seconds: float = 10.0
    clickhouse_url: str = "http://localhost:8123"
    clickhouse_user: str = "radar"
    clickhouse_password: str | None = None
    clickhouse_database: str = "radar"

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> CatalogConfig:
        defaults: CatalogConfig = cls()
        mongodb_url: str = _text(env, "MONGODB_URL", defaults.mongodb_url)
        if not mongodb_url.startswith(("mongodb://", "mongodb+srv://")):
            raise ValueError(
                f"MONGODB_URL: must start with mongodb:// or mongodb+srv://, got {mongodb_url!r}"
            )
        databases: dict[str, str] = {}
        name: str
        default: str
        for name, default in (
            ("MONGODB_DATABASE", defaults.mongodb_database),
            ("CLICKHOUSE_DATABASE", defaults.clickhouse_database),
        ):
            databases[name] = _text(env, name, default)
            if re.fullmatch(DATABASE_PATTERN, databases[name]) is None:
                raise ValueError(f"{name}: must match {DATABASE_PATTERN}, got {databases[name]!r}")
        timeout: float = defaults.http_timeout_seconds
        raw: str = env.get("CATALOG_HTTP_TIMEOUT_SECONDS", "").strip()
        if raw:
            try:
                timeout = float(raw)
            except ValueError:
                raise ValueError(f"CATALOG_HTTP_TIMEOUT_SECONDS: expected a number, got {raw!r}") from None
            if not math.isfinite(timeout) or timeout <= 0:
                raise ValueError(f"CATALOG_HTTP_TIMEOUT_SECONDS: expected a finite number > 0, got {raw!r}")
        return cls(
            mongodb_url=mongodb_url,
            mongodb_database=databases["MONGODB_DATABASE"],
            api_base=_text(env, "STEAM_API_BASE", defaults.api_base).rstrip("/"),
            http_timeout_seconds=timeout,
            clickhouse_url=_text(env, "CLICKHOUSE_URL", defaults.clickhouse_url).rstrip("/"),
            clickhouse_user=_text(env, "CLICKHOUSE_USER", defaults.clickhouse_user),
            clickhouse_password=env.get("CLICKHOUSE_PASSWORD", "").strip() or None,
            clickhouse_database=databases["CLICKHOUSE_DATABASE"],
        )


def _parser() -> argparse.ArgumentParser:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(prog="python -m gamedev_trends.catalog")
    commands: Any = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("refresh", help="load the radar's games from Steam into MongoDB")
    niche: argparse.ArgumentParser = commands.add_parser("niche", help="how crowded a tag is")
    niche.add_argument("tag")
    indie: argparse.ArgumentParser = commands.add_parser("indie", help="share of self-published games")
    indie.add_argument("tag")
    growing: argparse.ArgumentParser = commands.add_parser("growing", help="tags of growing games")
    growing.add_argument("--window-hours", type=int, default=24)
    growing.add_argument("--lag-hours", type=int, default=168)
    growing.add_argument("--min-growth", type=float, default=0.2)
    growing.add_argument("--limit", type=int, default=20)
    return parser


async def _run(args: argparse.Namespace, config: CatalogConfig) -> int:
    from pymongo import AsyncMongoClient

    from gamedev_trends.mongo_catalog import MongoCatalog, TagGrowth

    mongo: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(config.mongodb_url, tz_aware=True)
    catalog: MongoCatalog = MongoCatalog(mongo[config.mongodb_database])
    http: httpx.AsyncClient
    try:
        async with httpx.AsyncClient(
            timeout=config.http_timeout_seconds, headers={"User-Agent": USER_AGENT}
        ) as http:
            if args.command == "niche":
                await catalog.ensure_schema()
                stats: Any = await catalog.niche(args.tag)
                weight: str = "none" if stats.avg_weight is None else str(stats.avg_weight)
                print(f"tag={stats.tag} games={stats.games} avg_weight={weight}")
                return 0
            if args.command == "indie":
                await catalog.ensure_schema()
                share: Any = await catalog.indie_share(args.tag)
                value: str = "none" if share.share is None else f"{share.share:.2f}"
                print(
                    f"tag={share.tag} games={share.games} self_published={share.self_published} share={value}"
                )
                return 0
            password: str = config.clickhouse_password or ""
            clickhouse: ClickHouseClient = ClickHouseClient(
                http, url=config.clickhouse_url, user=config.clickhouse_user, password=password
            )
            now: datetime = datetime.now(UTC)
            if args.command == "refresh":
                await catalog.ensure_schema()
                appids: list[int] = await radar_appids(clickhouse, config.clickhouse_database)
                source: StoreClient = StoreClient(http, base_url=config.api_base)
                report: RefreshReport = await refresh(source, catalog, appids, now=now)
                print(
                    f"catalog refresh: requested={report.requested} games={report.games} "
                    f"inserted={report.inserted} updated={report.matched} "
                    f"unavailable={len(report.unavailable)} malformed={len(report.malformed)} "
                    f"failed={len(report.failed)}"
                )
                return 1 if report.failed else 0
            rising: list[GrowingGame] = await growing_games(
                clickhouse,
                config.clickhouse_database,
                now=now,
                window_hours=args.window_hours,
                lag_hours=args.lag_hours,
                min_growth=args.min_growth,
            )
            print(
                f"growing games: {len(rising)} "
                f"(window={args.window_hours}h lag={args.lag_hours}h min_growth={args.min_growth})"
            )
            item: TagGrowth
            for item in await catalog.growing_tags([g.appid for g in rising], args.limit):
                print(f"{item.tag}\t{item.growing}\t{item.total}")
            return 0
    finally:
        await mongo.close()


def main(argv: Sequence[str] | None = None) -> int:
    args: argparse.Namespace = _parser().parse_args(argv)
    config: CatalogConfig
    try:
        config = CatalogConfig.from_env(os.environ)
        if args.command in ("refresh", "growing") and config.clickhouse_password is None:
            raise ValueError("CLICKHOUSE_PASSWORD: required for this command")
    except ValueError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    setup_logging()
    try:
        return asyncio.run(_run(args, config))
    except Exception:
        logger.exception("catalog %s failed", args.command)
        return 1


if __name__ == "__main__":
    run_main(__spec__)
