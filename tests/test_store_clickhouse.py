"""Spec checks 26-33, 34-36: ClickHouse client over MockTransport, schema statements."""

import json

import httpx
import pytest
from store_support import Recorder, dumps, make_client

from gamedev_trends.clickhouse import (
    ClickHouseClient,
    ClickHouseError,
    ClickHouseSink,
    ensure_schema,
    schema_sql,
)
from gamedev_trends.store import ObservationSink, Row, SinkError

R1: Row = {"event_id": "a", "source": "steam", "value": 1.5, "window_start": None}
R2: Row = {"event_id": "b", "source": "стим", "value": 2.0, "window_start": "2026-11-12 10:00:00.000000"}


def ok(_: httpx.Request) -> httpx.Response:
    return httpx.Response(200, text="")


async def test_26_insert_rows_request_shape() -> None:
    rec: Recorder = Recorder()
    client: ClickHouseClient = make_client(ok, url="http://ch.test:8123/", recorder=rec)
    await client.insert_rows("observations", [R1, R2], database="radar_x")
    assert len(rec.requests) == 1
    req: httpx.Request = rec.requests[0]
    assert req.method == "POST"
    assert str(req.url).split("?")[0] == "http://ch.test:8123/"
    assert dict(req.url.params) == {
        "query": "INSERT INTO observations FORMAT JSONEachRow",
        "async_insert": "0",
    }
    assert req.headers["X-ClickHouse-User"] == "radar"
    assert req.headers["X-ClickHouse-Key"] == "s3cret"
    assert req.headers["X-ClickHouse-Database"] == "radar_x"
    assert req.content == (dumps(R1) + dumps(R2)).encode("utf-8")
    assert "s3cret" not in str(req.url)


async def test_27_insert_empty_makes_no_request() -> None:
    rec: Recorder = Recorder()
    client: ClickHouseClient = make_client(ok, recorder=rec)
    await client.insert_rows("observations", [], database="radar")
    assert rec.requests == []


async def test_28_error_status_raises() -> None:
    server: ClickHouseClient = make_client(
        lambda _: httpx.Response(500, text="Code: 241. DB::Exception: Memory limit")
    )
    info: pytest.ExceptionInfo[ClickHouseError]
    with pytest.raises(ClickHouseError) as info:
        await server.insert_rows("t", [R1], database="radar")
    assert "500" in str(info.value)
    assert "Code: 241" in str(info.value)
    denied: ClickHouseClient = make_client(lambda _: httpx.Response(403, text="Code: 516. auth failed"))
    info2: pytest.ExceptionInfo[ClickHouseError]
    with pytest.raises(ClickHouseError) as info2:
        await denied.execute("SELECT 1")
    assert "403" in str(info2.value)


async def test_28_error_body_is_truncated() -> None:
    client: ClickHouseClient = make_client(lambda _: httpx.Response(500, text="x" * 5000))
    info: pytest.ExceptionInfo[ClickHouseError]
    with pytest.raises(ClickHouseError) as info:
        await client.execute("SELECT 1")
    assert len(str(info.value)) < 1500


async def test_29_transport_error_is_wrapped() -> None:
    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    client: ClickHouseClient = make_client(boom)
    info: pytest.ExceptionInfo[ClickHouseError]
    with pytest.raises(ClickHouseError) as info:
        await client.execute("SELECT 1")
    assert isinstance(info.value.__cause__, httpx.ConnectError)


async def test_30_execute() -> None:
    rec: Recorder = Recorder()
    client: ClickHouseClient = make_client(lambda _: httpx.Response(200, text="done\n"), recorder=rec)
    out: str = await client.execute("CREATE DATABASE IF NOT EXISTS x")
    assert out == "done\n"
    req: httpx.Request = rec.requests[0]
    assert req.method == "POST"
    assert req.content.decode("utf-8") == "CREATE DATABASE IF NOT EXISTS x"
    assert "X-ClickHouse-Database" not in req.headers
    assert req.headers["X-ClickHouse-User"] == "radar"
    assert req.headers["X-ClickHouse-Key"] == "s3cret"


async def test_31_select() -> None:
    rec: Recorder = Recorder()
    client: ClickHouseClient = make_client(
        lambda _: httpx.Response(200, text='{"x":1}\n{"x":2}\n'), recorder=rec
    )
    assert await client.select("SELECT 1 AS x") == [{"x": 1}, {"x": 2}]
    assert rec.requests[0].content.decode("utf-8").rstrip().endswith("FORMAT JSONEachRow")
    empty: ClickHouseClient = make_client(lambda _: httpx.Response(200, text=""))
    assert await empty.select("SELECT 1 AS x WHERE 0") == []
    bad: ClickHouseClient = make_client(lambda _: httpx.Response(200, text="[1]\n"))
    with pytest.raises(ClickHouseError):
        await bad.select("SELECT 1")


async def test_32_ping() -> None:
    rec: Recorder = Recorder()
    good: ClickHouseClient = make_client(lambda _: httpx.Response(200, text="Ok.\n"), recorder=rec)
    assert await good.ping() is True
    assert rec.requests[0].method == "GET"
    assert str(rec.requests[0].url) == "http://ch.test:8123/ping"
    assert await make_client(lambda _: httpx.Response(500, text="Ok.\n")).ping() is False
    assert await make_client(lambda _: httpx.Response(200, text="nope")).ping() is False

    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    assert await make_client(boom).ping() is False


async def test_33_sink_wraps_clickhouse_error() -> None:
    client: ClickHouseClient = make_client(lambda _: httpx.Response(500, text="Code: 241"))
    sink: ObservationSink = ClickHouseSink(client, "radar")
    info: pytest.ExceptionInfo[SinkError]
    with pytest.raises(SinkError) as info:
        await sink.write([R1])
    assert isinstance(info.value.__cause__, ClickHouseError)


async def test_33_sink_inserts_into_observations() -> None:
    rec: Recorder = Recorder()
    client: ClickHouseClient = make_client(ok, recorder=rec)
    await ClickHouseSink(client, "radar_x").write([R1])
    req: httpx.Request = rec.requests[0]
    assert req.url.params["query"] == "INSERT INTO observations FORMAT JSONEachRow"
    assert req.headers["X-ClickHouse-Database"] == "radar_x"
    assert json.loads(req.content.decode("utf-8")) == R1


def norm(sql: str) -> str:
    return " ".join(sql.split())


def test_34_schema_statements() -> None:
    stmts: list[str] = [norm(s) for s in schema_sql("radar")]
    assert len(stmts) == 4
    assert all("IF NOT EXISTS" in s for s in stmts)
    assert "CREATE DATABASE" in stmts[0]
    assert "radar.observations" in stmts[1]
    part: str
    for part in (
        "ReplacingMergeTree(ingested_at)",
        "PARTITION BY toYYYYMM(observed_at)",
        "ORDER BY (source, entity_type, entity_id, metric, observed_at, event_id)",
        "window_start Nullable(DateTime64(6, 'UTC'))",
    ):
        assert part in stmts[1]
    assert "radar.observations_hourly" in stmts[2]
    assert "ENGINE = MergeTree" in stmts[2]
    assert "ORDER BY (source, entity_type, entity_id, metric, hour)" in stmts[2]
    for part in (
        "MATERIALIZED VIEW",
        "radar.observations_hourly_mv",
        "REFRESH EVERY 5 MINUTE",
        "TO radar.observations_hourly",
        "FROM radar.observations FINAL",
        "coalesce(window_start, observed_at)",
    ):
        assert part in stmts[3]


def test_34_schema_uses_given_database() -> None:
    joined: str = " ".join(schema_sql("radar_it_0123abcd"))
    assert "radar_it_0123abcd.observations_hourly_mv" in joined
    assert "radar." not in joined.replace("radar_it_0123abcd.", "")


@pytest.mark.parametrize("name", ["", "radar-x", "1db", "Radar", "a; DROP TABLE x", "a" * 65])
def test_35_bad_database_name(name: str) -> None:
    with pytest.raises(ValueError):
        schema_sql(name)


def test_35_good_database_name() -> None:
    assert len(schema_sql("radar_it_0123abcd")) == 4


async def test_36_ensure_schema_sends_statements_in_order() -> None:
    rec: Recorder = Recorder()
    client: ClickHouseClient = make_client(ok, recorder=rec)
    await ensure_schema(client, "radar")
    assert [r.content.decode("utf-8") for r in rec.requests] == schema_sql("radar")
