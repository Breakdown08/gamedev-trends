"""MongoDB catalog: schema validator, indexes, aggregation pipelines."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pymongo import UpdateOne
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.command_cursor import AsyncCommandCursor
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import CollectionInvalid
from pymongo.results import BulkWriteResult

from gamedev_trends.catalog import Game, StoredGame

COLLECTION: str = "games"
APPID_INDEX: str = "appid_unique"
TAGS_INDEX: str = "tags_name"
GAME_TYPE: int = 0

_INTEGER: list[str] = ["int", "long"]
GAMES_VALIDATOR: dict[str, Any] = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [
            "appid",
            "name",
            "steam_type",
            "tags",
            "categories",
            "developers",
            "publishers",
            "is_free",
            "price_usd_cents",
            "discount_pct",
            "release_date",
            "coming_soon",
            "early_access",
            "updated_at",
            "created_at",
        ],
        "properties": {
            "appid": {"bsonType": _INTEGER, "minimum": 1},
            "name": {"bsonType": "string"},
            "steam_type": {"bsonType": _INTEGER},
            "tags": {
                "bsonType": "array",
                "items": {
                    "bsonType": "object",
                    "required": ["tagid", "name", "weight"],
                    "properties": {
                        "tagid": {"bsonType": _INTEGER},
                        "name": {"bsonType": ["string", "null"]},
                        "weight": {"bsonType": _INTEGER},
                    },
                },
            },
            "categories": {
                "bsonType": "array",
                "items": {
                    "bsonType": "object",
                    "required": ["categoryid", "name"],
                    "properties": {
                        "categoryid": {"bsonType": _INTEGER},
                        "name": {"bsonType": ["string", "null"]},
                    },
                },
            },
            "developers": {"bsonType": "array", "items": {"bsonType": "string"}},
            "publishers": {"bsonType": "array", "items": {"bsonType": "string"}},
            "is_free": {"bsonType": "bool"},
            "price_usd_cents": {"bsonType": ["int", "long", "null"]},
            "discount_pct": {"bsonType": _INTEGER, "minimum": 0, "maximum": 100},
            "release_date": {"bsonType": ["date", "null"]},
            "coming_soon": {"bsonType": "bool"},
            "early_access": {"bsonType": "bool"},
            "updated_at": {"bsonType": "date"},
            "created_at": {"bsonType": "date"},
        },
    }
}


@dataclass(frozen=True)
class UpsertResult:
    inserted: int
    matched: int


@dataclass(frozen=True)
class NicheStats:
    tag: str
    games: int
    avg_weight: float | None


@dataclass(frozen=True)
class IndieShare:
    tag: str
    games: int
    self_published: int
    share: float | None


@dataclass(frozen=True)
class TagGrowth:
    tag: str
    growing: int
    total: int


def niche_pipeline(tag: str) -> list[dict[str, Any]]:
    return [
        {"$match": {"tags.name": tag, "steam_type": GAME_TYPE}},
        {"$unwind": "$tags"},
        {"$match": {"tags.name": tag}},
        {"$group": {"_id": None, "games": {"$sum": 1}, "avg_weight": {"$avg": "$tags.weight"}}},
        {"$project": {"_id": 0, "games": 1, "avg_weight": 1}},
    ]


def indie_share_pipeline(tag: str) -> list[dict[str, Any]]:
    return [
        {
            "$match": {
                "tags.name": tag,
                "steam_type": GAME_TYPE,
                "developers.0": {"$exists": True},
                "publishers.0": {"$exists": True},
            }
        },
        {"$project": {"self": {"$setEquals": ["$developers", "$publishers"]}}},
        {
            "$group": {
                "_id": None,
                "games": {"$sum": 1},
                "self_published": {"$sum": {"$cond": ["$self", 1, 0]}},
            }
        },
        {"$project": {"_id": 0, "games": 1, "self_published": 1}},
    ]


def growing_tags_pipeline(appids: Sequence[int], limit: int = 20) -> list[dict[str, Any]]:
    if limit < 1:
        raise ValueError("limit must be >= 1")
    return [
        {"$match": {"steam_type": GAME_TYPE}},
        {"$unwind": "$tags"},
        {"$match": {"tags.name": {"$ne": None}}},
        {
            "$group": {
                "_id": "$tags.name",
                "total": {"$sum": 1},
                "growing": {"$sum": {"$cond": [{"$in": ["$appid", list(appids)]}, 1, 0]}},
            }
        },
        {"$match": {"growing": {"$gt": 0}}},
        {"$sort": {"growing": -1, "total": 1, "_id": 1}},
        {"$limit": limit},
        {"$project": {"_id": 0, "tag": "$_id", "growing": 1, "total": 1}},
    ]


class MongoCatalog:
    def __init__(self, db: AsyncDatabase[dict[str, Any]]) -> None:
        self._db: AsyncDatabase[dict[str, Any]] = db

    async def ensure_schema(self) -> None:
        exists: bool = bool(await self._db.list_collection_names(filter={"name": COLLECTION}))
        if not exists:
            try:
                await self._db.create_collection(
                    COLLECTION,
                    validator=GAMES_VALIDATOR,
                    validationLevel="strict",
                    validationAction="error",
                )
            except CollectionInvalid:  # created by a concurrent run between the check and now
                exists = True
        if exists:
            await self._db.command(
                {
                    "collMod": COLLECTION,
                    "validator": GAMES_VALIDATOR,
                    "validationLevel": "strict",
                    "validationAction": "error",
                }
            )
        games: AsyncCollection[dict[str, Any]] = self._db[COLLECTION]
        await games.create_index([("appid", 1)], name=APPID_INDEX, unique=True)
        await games.create_index([("tags.name", 1)], name=TAGS_INDEX)

    async def upsert(self, games: Sequence[Game], now: datetime) -> UpsertResult:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        if not games:
            return UpsertResult(0, 0)
        operations: list[UpdateOne] = [
            UpdateOne(
                {"appid": g.appid},
                {"$set": g.model_dump(), "$setOnInsert": {"created_at": now}},
                upsert=True,
            )
            for g in games
        ]
        result: BulkWriteResult = await self._db[COLLECTION].bulk_write(operations, ordered=False)
        return UpsertResult(inserted=result.upserted_count, matched=result.matched_count)

    async def get(self, appid: int) -> StoredGame | None:
        document: dict[str, Any] | None = await self._db[COLLECTION].find_one({"appid": appid}, {"_id": 0})
        return None if document is None else StoredGame.model_validate(document)

    async def count(self) -> int:
        return await self._db[COLLECTION].count_documents({})

    async def _aggregate(self, pipeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
        cursor: AsyncCommandCursor[dict[str, Any]] = await self._db[COLLECTION].aggregate(pipeline)
        return await cursor.to_list()

    async def niche(self, tag: str) -> NicheStats:
        rows: list[dict[str, Any]] = await self._aggregate(niche_pipeline(tag))
        if not rows:
            return NicheStats(tag, 0, None)
        return NicheStats(tag, int(rows[0]["games"]), float(rows[0]["avg_weight"]))

    async def indie_share(self, tag: str) -> IndieShare:
        rows: list[dict[str, Any]] = await self._aggregate(indie_share_pipeline(tag))
        if not rows:
            return IndieShare(tag, 0, 0, None)
        games: int = int(rows[0]["games"])
        own: int = int(rows[0]["self_published"])
        return IndieShare(tag, games, own, own / games)

    async def growing_tags(self, appids: Sequence[int], limit: int = 20) -> list[TagGrowth]:
        pipeline: list[dict[str, Any]] = growing_tags_pipeline(appids, limit)
        if not appids:
            return []
        rows: list[dict[str, Any]] = await self._aggregate(pipeline)
        return [TagGrowth(str(r["tag"]), int(r["growing"]), int(r["total"])) for r in rows]
