"""Spec checks 46-49: sql_string, the erasures table and ClickHouseErasureSink over MockTransport."""

import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from store_support import NOW, Recorder, make_client

from gamedev_trends.clickhouse import (
    ERASED_CHUNK,
    ClickHouseClient,
    ClickHouseErasureSink,
    ensure_erasures_schema,
    erasures_schema_sql,
    sql_string,
)
from gamedev_trends.contract import Erasure
from gamedev_trends.store import EntityKey, SinkError, format_datetime

REQUESTED: datetime = datetime(2026, 11, 15, 12, 1, 2, 345678, tzinfo=UTC)
ERASURES_DDL: str = (
    "CREATE TABLE IF NOT EXISTS radar.erasures ( event_id UUID, source LowCardinality(String), "
    "entity_type LowCardinality(String), entity_id String, requested_at DateTime64(6, 'UTC'), "
    "ingested_at DateTime64(6, 'UTC') ) ENGINE = ReplacingMergeTree(ingested_at) "
    "ORDER BY (source, entity_type, entity_id)"
)


def collapse(sql: str) -> str:
    return " ".join(sql.split())


def post_erasure(entity: str, at: datetime = REQUESTED) -> Erasure:
    return Erasure.create(source="bluesky", entity_type="post", entity_id=entity, requested_at=at)


def ok(_: httpx.Request) -> httpx.Response:
    return httpx.Response(200, text="")


def test_46_sql_string() -> None:
    assert sql_string("a'b\\c") == "'a\\'b\\\\c'"
    assert sql_string("") == "''"
    assert sql_string("plain") == "'plain'"


def test_47_erasures_schema() -> None:
    statements: list[str] = erasures_schema_sql("radar")
    assert len(statements) == 1
    assert collapse(statements[0]) == ERASURES_DDL


def test_47_the_database_name_is_used_and_checked() -> None:
    assert "other_db.erasures" in erasures_schema_sql("other_db")[0]
    bad: str
    for bad in ("Radar", "a;b", "", "x y", "drop table"):
        with pytest.raises(ValueError):
            erasures_schema_sql(bad)


async def test_47_ensure_erasures_schema_sends_exactly_that_statement() -> None:
    rec: Recorder = Recorder()
    client: ClickHouseClient = make_client(ok, recorder=rec)
    await ensure_erasures_schema(client, "radar")
    assert [r.content.decode("utf-8") for r in rec.requests] == erasures_schema_sql("radar")


async def test_48_erase_inserts_rows_then_deletes_the_distinct_entities() -> None:
    rec: Recorder = Recorder()
    client: ClickHouseClient = make_client(ok, recorder=rec)
    sink: ClickHouseErasureSink = ClickHouseErasureSink(client, "radar")
    first: Erasure = post_erasure("at://did:plc:a/app.bsky.feed.post/p1")
    again: Erasure = post_erasure("at://did:plc:a/app.bsky.feed.post/p1", REQUESTED + timedelta(seconds=5))
    second: Erasure = post_erasure("at://did:plc:a/app.bsky.feed.post/p2")
    await sink.erase([first, again, second], NOW)
    assert len(rec.requests) == 2
    insert: httpx.Request = rec.requests[0]
    assert dict(insert.url.params)["query"] == "INSERT INTO erasures FORMAT JSONEachRow"
    assert insert.headers["X-ClickHouse-Database"] == "radar"
    lines: list[str] = insert.content.decode("utf-8").splitlines()
    rows: list[dict[str, str]] = [json.loads(line) for line in lines]
    assert len(rows) == 3
    assert list(rows[0]) == ["event_id", "source", "entity_type", "entity_id", "requested_at", "ingested_at"]
    assert rows[0] == {
        "event_id": str(first.event_id),
        "source": "bluesky",
        "entity_type": "post",
        "entity_id": "at://did:plc:a/app.bsky.feed.post/p1",
        "requested_at": format_datetime(REQUESTED),
        "ingested_at": format_datetime(NOW),
    }
    assert [r["requested_at"] for r in rows] == [
        format_datetime(REQUESTED),
        format_datetime(REQUESTED + timedelta(seconds=5)),
        format_datetime(REQUESTED),
    ]
    assert rows[2]["entity_id"] == "at://did:plc:a/app.bsky.feed.post/p2"
    delete: str = rec.requests[1].content.decode("utf-8")
    assert delete == (
        "DELETE FROM radar.observations WHERE (source, entity_type, entity_id) IN "
        "(('bluesky', 'post', 'at://did:plc:a/app.bsky.feed.post/p1'), "
        "('bluesky', 'post', 'at://did:plc:a/app.bsky.feed.post/p2'))"
    )


async def test_48_quotes_and_backslashes_are_escaped_in_the_delete() -> None:
    rec: Recorder = Recorder()
    sink: ClickHouseErasureSink = ClickHouseErasureSink(make_client(ok, recorder=rec), "radar")
    await sink.erase([post_erasure("a'b\\c")], NOW)
    assert rec.requests[1].content.decode("utf-8").endswith("IN (('bluesky', 'post', 'a\\'b\\\\c'))")


async def test_48_the_delete_is_cut_into_chunks() -> None:
    assert ERASED_CHUNK == 500
    rec: Recorder = Recorder()
    sink: ClickHouseErasureSink = ClickHouseErasureSink(make_client(ok, recorder=rec), "radar")
    erasures: list[Erasure] = [post_erasure(f"at://did:plc:a/app.bsky.feed.post/p{i}") for i in range(501)]
    await sink.erase(erasures, NOW)
    assert len(rec.requests) == 3
    deletes: list[str] = [r.content.decode("utf-8") for r in rec.requests[1:]]
    assert all(d.startswith("DELETE FROM radar.observations WHERE") for d in deletes)
    assert [d.count("('bluesky', 'post', ") for d in deletes] == [500, 1]
    assert deletes[0].count("p0'") == 1
    assert deletes[1].endswith("p500'))")


async def test_48_http_500_becomes_sink_error() -> None:
    sink: ClickHouseErasureSink = ClickHouseErasureSink(
        make_client(lambda _: httpx.Response(500, text="Code: 241")), "radar"
    )
    with pytest.raises(SinkError):
        await sink.erase([post_erasure("at://did:plc:a/app.bsky.feed.post/p1")], NOW)


async def test_48_a_failing_delete_becomes_sink_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.content.startswith(b"DELETE"):
            return httpx.Response(500, text="Code: 62")
        return httpx.Response(200, text="")

    sink: ClickHouseErasureSink = ClickHouseErasureSink(make_client(handler), "radar")
    with pytest.raises(SinkError):
        await sink.erase([post_erasure("at://did:plc:a/app.bsky.feed.post/p1")], NOW)


async def test_49_erased_of_nothing_makes_no_request() -> None:
    rec: Recorder = Recorder()
    sink: ClickHouseErasureSink = ClickHouseErasureSink(make_client(ok, recorder=rec), "radar")
    assert await sink.erased([]) == set()
    assert rec.requests == []


async def test_49_erased_selects_the_matching_keys() -> None:
    rec: Recorder = Recorder()
    body: str = json.dumps({"source": "bluesky", "entity_type": "post", "entity_id": "a"}) + "\n"
    sink: ClickHouseErasureSink = ClickHouseErasureSink(
        make_client(lambda _: httpx.Response(200, text=body), recorder=rec), "radar"
    )
    keys: list[EntityKey] = [("bluesky", "post", "a"), ("bluesky", "post", "b")]
    assert await sink.erased(keys) == {("bluesky", "post", "a")}
    assert len(rec.requests) == 1
    sql: str = rec.requests[0].content.decode("utf-8")
    assert sql.startswith(
        "SELECT source, entity_type, entity_id FROM radar.erasures WHERE "
        "(source, entity_type, entity_id) IN (('bluesky', 'post', 'a'), ('bluesky', 'post', 'b'))"
    )
    assert sql.rstrip().endswith("FORMAT JSONEachRow")


async def test_49_erased_is_cut_into_chunks_and_merges_the_answers() -> None:
    rec: Recorder = Recorder()

    def handler(request: httpx.Request) -> httpx.Response:
        sql: str = request.content.decode("utf-8")
        if "'k0'" in sql:
            return httpx.Response(
                200, text=json.dumps({"source": "s", "entity_type": "t", "entity_id": "k0"}) + "\n"
            )
        return httpx.Response(
            200, text=json.dumps({"source": "s", "entity_type": "t", "entity_id": "k500"}) + "\n"
        )

    sink: ClickHouseErasureSink = ClickHouseErasureSink(make_client(handler, recorder=rec), "radar")
    keys: list[EntityKey] = [("s", "t", f"k{i}") for i in range(501)]
    assert await sink.erased(keys) == {("s", "t", "k0"), ("s", "t", "k500")}
    assert len(rec.requests) == 2


async def test_49_http_500_becomes_sink_error() -> None:
    sink: ClickHouseErasureSink = ClickHouseErasureSink(
        make_client(lambda _: httpx.Response(500, text="Code: 241")), "radar"
    )
    with pytest.raises(SinkError):
        await sink.erased([("bluesky", "post", "a")])
