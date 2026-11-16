"""ClickHouse HTTP client, schema and the observation sink."""

import json
import logging
import re
from collections.abc import Mapping, Sequence
from datetime import datetime

import httpx

from gamedev_trends.contract import Erasure
from gamedev_trends.store import EntityKey, Row, SinkError, format_datetime

logger: logging.Logger = logging.getLogger(__name__)

OBSERVATIONS_TABLE: str = "observations"
ERASURES_TABLE: str = "erasures"
ERASED_CHUNK: int = 500
HOURLY_TABLE: str = "observations_hourly"
HOURLY_VIEW: str = "observations_hourly_mv"
HOURLY_REFRESH: str = "EVERY 5 MINUTE"
DATABASE_PATTERN: str = r"^[a-z_][a-z0-9_]{0,63}$"

_ERROR_BODY_CHARS: int = 1000

type Scalar = str | float | None


class ClickHouseError(Exception):
    """ClickHouse answered with a non-200 status or the request did not arrive."""


def schema_sql(database: str) -> list[str]:
    """Four DDL statements in order: database, raw table, hourly table, refreshable view.

    A database name that does not match DATABASE_PATTERN raises ValueError (it is put into SQL as is).
    """
    if re.fullmatch(DATABASE_PATTERN, database) is None:
        raise ValueError(f"database name must match {DATABASE_PATTERN}, got {database!r}")
    raw: str = f"{database}.{OBSERVATIONS_TABLE}"
    hourly: str = f"{database}.{HOURLY_TABLE}"
    view: str = f"{database}.{HOURLY_VIEW}"
    return [
        f"CREATE DATABASE IF NOT EXISTS {database}",
        f"""CREATE TABLE IF NOT EXISTS {raw} (
    event_id     UUID,
    source       LowCardinality(String),
    entity_type  LowCardinality(String),
    entity_id    String,
    metric       LowCardinality(String),
    value        Float64,
    observed_at  DateTime64(6, 'UTC'),
    window_start Nullable(DateTime64(6, 'UTC')),
    ingested_at  DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(observed_at)
ORDER BY (source, entity_type, entity_id, metric, observed_at, event_id)""",
        f"""CREATE TABLE IF NOT EXISTS {hourly} (
    hour        DateTime('UTC'),
    source      LowCardinality(String),
    entity_type LowCardinality(String),
    entity_id   String,
    metric      LowCardinality(String),
    samples     UInt64,
    value_sum   Float64,
    value_min   Float64,
    value_max   Float64,
    value_avg   Float64,
    value_last  Float64
) ENGINE = MergeTree
ORDER BY (source, entity_type, entity_id, metric, hour)""",
        f"""CREATE MATERIALIZED VIEW IF NOT EXISTS {view}
REFRESH {HOURLY_REFRESH} TO {hourly} AS
SELECT
    toDateTime(toStartOfHour(coalesce(window_start, observed_at)), 'UTC') AS hour,
    source, entity_type, entity_id, metric,
    count()                       AS samples,
    sum(value)                    AS value_sum,
    min(value)                    AS value_min,
    max(value)                    AS value_max,
    avg(value)                    AS value_avg,
    argMax(value, observed_at)    AS value_last
FROM {raw} FINAL
GROUP BY hour, source, entity_type, entity_id, metric""",
    ]


class ClickHouseClient:
    def __init__(self, http: httpx.AsyncClient, *, url: str, user: str, password: str) -> None:
        """url without a trailing slash (one is stripped). Timeouts belong to the owner of http."""
        self._http: httpx.AsyncClient = http
        self._url: str = url.rstrip("/")
        self._user: str = user
        self._password: str = password

    def _headers(self, database: str | None) -> dict[str, str]:
        headers: dict[str, str] = {"X-ClickHouse-User": self._user, "X-ClickHouse-Key": self._password}
        if database is not None:
            headers["X-ClickHouse-Database"] = database
        return headers

    async def _post(
        self, content: bytes, *, database: str | None, params: Mapping[str, str] | None = None
    ) -> str:
        try:
            response: httpx.Response = await self._http.post(
                f"{self._url}/", content=content, params=params, headers=self._headers(database)
            )
        except httpx.TransportError as exc:
            raise ClickHouseError(f"request to ClickHouse failed: {exc!r}") from exc
        if response.status_code != 200:
            raise ClickHouseError(
                f"ClickHouse answered {response.status_code}: {response.text[:_ERROR_BODY_CHARS]}"
            )
        return response.text

    async def ping(self) -> bool:
        """GET /ping: True only for 200 with the body "Ok.\\n"."""
        try:
            response: httpx.Response = await self._http.get(f"{self._url}/ping")
        except httpx.TransportError:
            return False
        return response.status_code == 200 and response.text == "Ok.\n"

    async def execute(self, sql: str, *, database: str | None = None) -> str:
        return await self._post(sql.encode("utf-8"), database=database)

    async def insert_rows(self, table: str, rows: Sequence[Mapping[str, Scalar]], *, database: str) -> None:
        if not rows:
            return
        body: str = "".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n" for row in rows)
        await self._post(
            body.encode("utf-8"),
            database=database,
            params={"query": f"INSERT INTO {table} FORMAT JSONEachRow", "async_insert": "0"},
        )

    async def select(self, sql: str, *, database: str | None = None) -> list[dict[str, object]]:
        text: str = await self.execute(f"{sql}\nFORMAT JSONEachRow", database=database)
        result: list[dict[str, object]] = []
        line: str
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                parsed: object = json.loads(line)
            except ValueError as exc:
                raise ClickHouseError(f"unparseable row from ClickHouse: {line[:200]!r}") from exc
            if not isinstance(parsed, dict):
                raise ClickHouseError(f"row from ClickHouse is not an object: {line[:200]!r}")
            result.append(parsed)
        return result


async def ensure_schema(client: ClickHouseClient, database: str) -> None:
    """Run schema_sql(database) in order. Idempotent."""
    statement: str
    for statement in schema_sql(database):
        await client.execute(statement)


class ClickHouseSink:
    def __init__(self, client: ClickHouseClient, database: str) -> None:
        self._client: ClickHouseClient = client
        self._database: str = database

    async def write(self, rows: Sequence[Row]) -> None:
        try:
            await self._client.insert_rows(OBSERVATIONS_TABLE, rows, database=self._database)
        except ClickHouseError as exc:
            raise SinkError(str(exc)) from exc


def sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def erasures_schema_sql(database: str) -> list[str]:
    """One DDL statement for the erasures table. A bad database name raises ValueError."""
    if re.fullmatch(DATABASE_PATTERN, database) is None:
        raise ValueError(f"database name must match {DATABASE_PATTERN}, got {database!r}")
    return [
        f"""CREATE TABLE IF NOT EXISTS {database}.{ERASURES_TABLE} (
    event_id     UUID,
    source       LowCardinality(String),
    entity_type  LowCardinality(String),
    entity_id    String,
    requested_at DateTime64(6, 'UTC'),
    ingested_at  DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (source, entity_type, entity_id)"""
    ]


async def ensure_erasures_schema(client: ClickHouseClient, database: str) -> None:
    """Create the erasures table. Idempotent."""
    statement: str
    for statement in erasures_schema_sql(database):
        await client.execute(statement)


def _tuples(keys: Sequence[EntityKey]) -> str:
    return ", ".join("(" + ", ".join(sql_string(part) for part in key) + ")" for key in keys)


def _chunks(keys: Sequence[EntityKey]) -> list[Sequence[EntityKey]]:
    return [keys[i : i + ERASED_CHUNK] for i in range(0, len(keys), ERASED_CHUNK)]


class ClickHouseErasureSink:
    def __init__(self, client: ClickHouseClient, database: str) -> None:
        self._client: ClickHouseClient = client
        self._database: str = database

    async def erase(self, erasures: Sequence[Erasure], ingested_at: datetime) -> None:
        rows: list[Row] = [
            {
                "event_id": str(e.event_id),
                "source": e.source,
                "entity_type": e.entity_type,
                "entity_id": e.entity_id,
                "requested_at": format_datetime(e.requested_at),
                "ingested_at": format_datetime(ingested_at),
            }
            for e in erasures
        ]
        keys: list[EntityKey] = list(dict.fromkeys((e.source, e.entity_type, e.entity_id) for e in erasures))
        try:
            await self._client.insert_rows(ERASURES_TABLE, rows, database=self._database)
            chunk: Sequence[EntityKey]
            for chunk in _chunks(keys):
                await self._client.execute(
                    f"DELETE FROM {self._database}.{OBSERVATIONS_TABLE} "
                    f"WHERE (source, entity_type, entity_id) IN ({_tuples(chunk)})"
                )
        except ClickHouseError as exc:
            raise SinkError(str(exc)) from exc

    async def erased(self, keys: Sequence[EntityKey]) -> set[EntityKey]:
        found: set[EntityKey] = set()
        try:
            chunk: Sequence[EntityKey]
            for chunk in _chunks(keys):
                result: list[dict[str, object]] = await self._client.select(
                    "SELECT source, entity_type, entity_id "
                    f"FROM {self._database}.{ERASURES_TABLE} "
                    f"WHERE (source, entity_type, entity_id) IN ({_tuples(chunk)})"
                )
                row: dict[str, object]
                for row in result:
                    found.add((str(row["source"]), str(row["entity_type"]), str(row["entity_id"])))
        except ClickHouseError as exc:
            raise SinkError(str(exc)) from exc
        return found


EXCERPTS_TABLE: str = "excerpts"


def excerpts_schema_sql(database: str) -> list[str]:
    """One DDL statement: the excerpts table (no TTL). A bad database name raises ValueError."""
    if re.fullmatch(DATABASE_PATTERN, database) is None:
        raise ValueError(f"database name must match {DATABASE_PATTERN}, got {database!r}")
    return [
        f"""CREATE TABLE IF NOT EXISTS {database}.{EXCERPTS_TABLE} (
    event_id    UUID,
    source      LowCardinality(String),
    entity_type LowCardinality(String),
    entity_id   String,
    text        String,
    posted_at   DateTime64(6, 'UTC'),
    ingested_at DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (source, entity_type, entity_id)"""
    ]


async def ensure_excerpts_schema(client: ClickHouseClient, database: str) -> None:
    """Create the excerpts table. Idempotent."""
    statement: str
    for statement in excerpts_schema_sql(database):
        await client.execute(statement)


class ClickHouseExcerptSink:
    def __init__(self, client: ClickHouseClient, database: str) -> None:
        self._client: ClickHouseClient = client
        self._database: str = database

    async def write_excerpts(self, rows: Sequence[Row]) -> None:
        try:
            await self._client.insert_rows(EXCERPTS_TABLE, rows, database=self._database)
        except ClickHouseError as exc:
            raise SinkError(str(exc)) from exc

    async def erase_excerpts(self, keys: Sequence[EntityKey]) -> None:
        distinct: list[EntityKey] = list(dict.fromkeys(keys))
        try:
            chunk: Sequence[EntityKey]
            for chunk in _chunks(distinct):
                await self._client.execute(
                    f"DELETE FROM {self._database}.{EXCERPTS_TABLE} "
                    f"WHERE (source, entity_type, entity_id) IN ({_tuples(chunk)})"
                )
        except ClickHouseError as exc:
            raise SinkError(str(exc)) from exc
