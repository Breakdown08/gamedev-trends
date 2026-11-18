"""Shared helpers for the catalog tests: recorded Steam responses, fakes for source and sink."""

import copy
import json
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from gamedev_trends.catalog import CatalogError, Game, ParsedItems, StoreSource, parse_items
from gamedev_trends.mongo_catalog import UpsertResult

type Json = Any

ROOT: Path = Path(__file__).resolve().parents[1]
FIXTURES: Path = ROOT / "tests" / "fixtures" / "catalog"
T: datetime = datetime(2026, 11, 18, 10, 0, tzinfo=UTC)
FIXTURE_IDS: list[int] = [413150, 730, 227300, 1422450, 1281930, 2379780, 1329410, 999999999]
PARSED_IDS: list[int] = [413150, 730, 227300, 1422450, 1281930, 2379780]
STARDEW: int = 413150
BALATRO: int = 2379780


def load(name: str) -> Json:
    return json.loads((FIXTURES / name).read_text())


def getitems() -> Json:
    return load("getitems.json")


def taglist() -> Json:
    return load("taglist.json")


def categories() -> Json:
    return load("categories.json")


def raw_item(appid: int, payload: Json = None) -> dict[str, Any]:
    """The raw store_items element for appid from the fixture (or from payload)."""
    source: Json = getitems() if payload is None else payload
    item: dict[str, Any]
    for item in source["response"]["store_items"]:
        if item["id"] == appid:
            return item
    raise KeyError(appid)


def tag_dict() -> dict[int, str]:
    return {int(x["tagid"]): str(x["name"]) for x in taglist()["response"]["tags"]}


def category_dict() -> dict[int, str]:
    return {int(x["categoryid"]): str(x["display_name"]) for x in categories()["response"]["categories"]}


def parsed_games(
    payload: Json = None, tags: dict[int, str] | None = None, cats: dict[int, str] | None = None
) -> dict[int, Game]:
    """Parse the fixture; fail with a plain assertion when the parser does not return all games."""
    result: ParsedItems = parse_items(
        getitems() if payload is None else payload,
        FIXTURE_IDS,
        tag_dict() if tags is None else tags,
        category_dict() if cats is None else cats,
        T,
    )
    return {g.appid: g for g in result.games}


def fixture_copy() -> Json:
    return copy.deepcopy(getitems())


class FakeSource:
    """StoreSource over recorded answers; records every items() call."""

    def __init__(
        self,
        *,
        items_payload: Json = None,
        tags_error: bool = False,
        fail_chunks: Sequence[int] = (),
    ) -> None:
        self.items_payload: Json = items_payload
        self.tags_error: bool = tags_error
        self.fail_chunks: set[int] = set(fail_chunks)
        self.item_calls: list[list[int]] = []
        self.dictionary_calls: int = 0

    async def tag_names(self) -> dict[int, str]:
        self.dictionary_calls += 1
        if self.tags_error:
            raise CatalogError("tags down")
        return tag_dict()

    async def category_names(self) -> dict[int, str]:
        self.dictionary_calls += 1
        return category_dict()

    async def items(self, appids: Sequence[int]) -> object:
        index: int = len(self.item_calls)
        self.item_calls.append(list(appids))
        if index in self.fail_chunks:
            raise CatalogError(f"chunk {index} failed")
        if self.items_payload is not None:
            return self.items_payload
        recorded: dict[int, dict[str, Any]] = {x["id"]: x for x in getitems()["response"]["store_items"]}
        return {
            "response": {
                "store_items": [
                    recorded.get(a, {"id": a, "success": 1, "name": f"Game {a}", "type": 0}) for a in appids
                ]
            }
        }


class RecordingSink:
    """CatalogSink that records calls and answers UpsertResult(inserted=len, matched=1)."""

    def __init__(self) -> None:
        self.calls: list[tuple[list[Game], datetime]] = []

    async def upsert(self, games: Sequence[Game], now: datetime) -> UpsertResult:
        self.calls.append((list(games), now))
        return UpsertResult(inserted=len(games), matched=1)


def as_source(source: FakeSource) -> StoreSource:
    return source


class Recorder:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
