"""Spec checks 19, 28-30: the excerpts table and ClickHouseExcerptSink over MockTransport."""

import httpx
import pytest
from store_support import NOW, T10, Recorder, dumps, make_client

from gamedev_trends.clickhouse import (
    ERASED_CHUNK,
    EXCERPTS_TABLE,
    ClickHouseClient,
    ClickHouseExcerptSink,
    ensure_excerpts_schema,
    erasures_schema_sql,
    excerpts_schema_sql,
    schema_sql,
)
from gamedev_trends.contract import Excerpt
from gamedev_trends.store import EntityKey, Row, SinkError, to_excerpt_row

PA: str = "at://did:plc:a/app.bsky.feed.post/pa"
PB: str = "at://did:plc:b/app.bsky.feed.post/pb"
EXCERPTS_DDL: str = (
    "CREATE TABLE IF NOT EXISTS radar.excerpts ( event_id UUID, source LowCardinality(String), "
    "entity_type LowCardinality(String), entity_id String, text String, "
    "posted_at DateTime64(6, 'UTC'), ingested_at DateTime64(6, 'UTC') ) "
    "ENGINE = ReplacingMergeTree(ingested_at) ORDER BY (source, entity_type, entity_id)"
)


def collapse(sql: str) -> str:
    return " ".join(sql.split())


def ok(_: httpx.Request) -> httpx.Response:
    return httpx.Response(200, text="")


def row(entity: str, text: str) -> Row:
    return to_excerpt_row(
        Excerpt.create(source="bluesky", entity_type="post", entity_id=entity, text=text, posted_at=T10),
        NOW,
    )


def test_19_schema() -> None:
    assert EXCERPTS_TABLE == "excerpts"
    statements: list[str] = excerpts_schema_sql("radar")
    assert len(statements) == 1
    assert collapse(statements[0]) == EXCERPTS_DDL
    assert "TTL" not in statements[0]


def test_19_database_name_is_used_and_checked() -> None:
    assert "other_db.excerpts" in excerpts_schema_sql("other_db")[0]
    bad: str
    for bad in ("Bad-Name", "Radar", "a;b", "", "x y"):
        with pytest.raises(ValueError):
            excerpts_schema_sql(bad)


async def test_19_ensure_sends_exactly_that_statement() -> None:
    rec: Recorder = Recorder()
    client: ClickHouseClient = make_client(ok, recorder=rec)
    await ensure_excerpts_schema(client, "radar")
    assert [r.content.decode("utf-8") for r in rec.requests] == excerpts_schema_sql("radar")


def test_19_the_other_schemas_are_unchanged() -> None:
    assert len(schema_sql("radar")) == 4
    assert len(erasures_schema_sql("radar")) == 1


async def test_28_write_excerpts_inserts_rows_in_order() -> None:
    rec: Recorder = Recorder()
    sink: ClickHouseExcerptSink = ClickHouseExcerptSink(make_client(ok, recorder=rec), "radar")
    rows: list[Row] = [row(PA, "Привет \U0001f3ae 'q' \\"), row(PB, "plain")]
    await sink.write_excerpts(rows)
    assert len(rec.requests) == 1
    req: httpx.Request = rec.requests[0]
    assert dict(req.url.params)["query"] == "INSERT INTO excerpts FORMAT JSONEachRow"
    assert req.headers["X-ClickHouse-Database"] == "radar"
    assert req.content.decode("utf-8") == dumps(rows[0]) + dumps(rows[1])


async def test_28_no_rows_no_request() -> None:
    rec: Recorder = Recorder()
    sink: ClickHouseExcerptSink = ClickHouseExcerptSink(make_client(ok, recorder=rec), "radar")
    await sink.write_excerpts([])
    assert rec.requests == []


async def test_29_erase_excerpts_one_delete_for_distinct_keys() -> None:
    rec: Recorder = Recorder()
    sink: ClickHouseExcerptSink = ClickHouseExcerptSink(make_client(ok, recorder=rec), "radar")
    keys: list[EntityKey] = [("bluesky", "post", PA), ("bluesky", "post", PB), ("bluesky", "post", PA)]
    await sink.erase_excerpts(keys)
    assert [r.content.decode("utf-8") for r in rec.requests] == [
        "DELETE FROM radar.excerpts WHERE (source, entity_type, entity_id) IN "
        f"(('bluesky', 'post', '{PA}'), ('bluesky', 'post', '{PB}'))"
    ]


async def test_29_quotes_and_backslashes_are_escaped() -> None:
    rec: Recorder = Recorder()
    sink: ClickHouseExcerptSink = ClickHouseExcerptSink(make_client(ok, recorder=rec), "radar")
    await sink.erase_excerpts([("bluesky", "post", "a'b\\c")])
    assert rec.requests[0].content.decode("utf-8").endswith("IN (('bluesky', 'post', 'a\\'b\\\\c'))")


async def test_29_delete_is_cut_into_chunks() -> None:
    rec: Recorder = Recorder()
    sink: ClickHouseExcerptSink = ClickHouseExcerptSink(make_client(ok, recorder=rec), "radar")
    keys: list[EntityKey] = [("bluesky", "post", f"p{i}") for i in range(ERASED_CHUNK + 1)]
    await sink.erase_excerpts(keys)
    deletes: list[str] = [r.content.decode("utf-8") for r in rec.requests]
    assert len(deletes) == 2
    assert all(d.startswith("DELETE FROM radar.excerpts WHERE") for d in deletes)
    assert [d.count("('bluesky', 'post', ") for d in deletes] == [500, 1]


async def test_29_no_keys_no_request() -> None:
    rec: Recorder = Recorder()
    sink: ClickHouseExcerptSink = ClickHouseExcerptSink(make_client(ok, recorder=rec), "radar")
    await sink.erase_excerpts([])
    assert rec.requests == []


async def test_30_http_500_becomes_sink_error() -> None:
    sink: ClickHouseExcerptSink = ClickHouseExcerptSink(
        make_client(lambda _: httpx.Response(500, text="Code: 241")), "radar"
    )
    with pytest.raises(SinkError):
        await sink.write_excerpts([row(PA, "x")])
    with pytest.raises(SinkError):
        await sink.erase_excerpts([("bluesky", "post", PA)])
