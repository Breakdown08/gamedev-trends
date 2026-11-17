"""Spec checks 1-10: contract_sql, ensure_contract and the start of the store (no Docker)."""

import logging

import httpx
import pytest
from alerts_interface_support import EXPECTED_SQL
from store_support import Recorder, make_client

import gamedev_trends.clickhouse as ch
import gamedev_trends.kafka_bus as kafka_bus
from gamedev_trends import store
from gamedev_trends.clickhouse import (
    CONTRACT_DATABASE,
    CONTRACT_VIEWS,
    DATABASE_PATTERN,
    ClickHouseClient,
    ClickHouseError,
    contract_sql,
    ensure_contract,
)
from gamedev_trends.store import StoreConfig

BAD_NAMES: list[str] = ["", "Radar", "alerts-v1", "a b", "radar; DROP TABLE x", "1radar"]


def test_01_contract_sql_is_the_four_statements() -> None:
    assert contract_sql("radar") == EXPECTED_SQL


def test_02_defaults_and_constants() -> None:
    assert CONTRACT_DATABASE == "alerts_v1"
    assert CONTRACT_VIEWS == ("steam_players", "steam_chart", "bluesky_tag_hourly")
    assert contract_sql("radar") == contract_sql("radar", "alerts_v1")
    assert len(contract_sql("radar")) == 4


def test_03_other_names_are_substituted() -> None:
    sql: list[str] = contract_sql("radar_it_ab", "radar_it_ab_v1")
    assert len(sql) == 4
    assert sql[0] == "CREATE DATABASE IF NOT EXISTS radar_it_ab_v1"
    i: int
    view: str
    for i, view in enumerate(CONTRACT_VIEWS, start=1):
        assert sql[i].count("FROM radar_it_ab.observations FINAL") == 1
        assert sql[i].startswith(f"CREATE VIEW IF NOT EXISTS radar_it_ab_v1.{view}\n")
    statement: str
    for statement in sql:
        assert "radar." not in statement
        assert "alerts_v1" not in statement


def test_04_idempotent_and_definer() -> None:
    sql: list[str] = contract_sql("radar")
    assert len(sql) == 4
    statement: str
    forbidden: str
    for statement in sql:
        assert "IF NOT EXISTS" in statement
        for forbidden in ("OR REPLACE", "DROP", "ALTER"):
            assert forbidden not in statement
    for statement in sql[1:]:
        assert "DEFINER = CURRENT_USER SQL SECURITY DEFINER" in statement


@pytest.mark.parametrize("bad", BAD_NAMES)
def test_05_bad_source_raises(bad: str) -> None:
    info: pytest.ExceptionInfo[ValueError]
    with pytest.raises(ValueError, match="database name must match") as info:
        contract_sql(bad)
    assert DATABASE_PATTERN in str(info.value)


@pytest.mark.parametrize("bad", BAD_NAMES)
def test_05_bad_contract_raises(bad: str) -> None:
    info: pytest.ExceptionInfo[ValueError]
    with pytest.raises(ValueError, match="database name must match") as info:
        contract_sql("radar", bad)
    assert DATABASE_PATTERN in str(info.value)


def test_05_source_is_checked_before_contract() -> None:
    info: pytest.ExceptionInfo[ValueError]
    with pytest.raises(ValueError) as info:
        contract_sql("Bad_Source", "Bad_Contract")
    assert "Bad_Source" in str(info.value)


def ok(_: httpx.Request) -> httpx.Response:
    return httpx.Response(200, text="")


async def test_06_ensure_contract_posts_four_statements() -> None:
    recorder: Recorder = Recorder()
    client: ClickHouseClient = make_client(ok, recorder=recorder)
    await ensure_contract(client, "radar")
    assert [r.method for r in recorder.requests] == ["POST"] * 4
    assert [r.content.decode() for r in recorder.requests] == EXPECTED_SQL


async def test_07_ensure_contract_with_other_names() -> None:
    recorder: Recorder = Recorder()
    client: ClickHouseClient = make_client(ok, recorder=recorder)
    await ensure_contract(client, "radar", "radar_it_x_v1")
    assert [r.content.decode() for r in recorder.requests] == contract_sql("radar", "radar_it_x_v1")


async def test_08_failure_stops_the_run() -> None:
    recorder: Recorder = Recorder()

    def handler(_: httpx.Request) -> httpx.Response:
        if len(recorder.requests) == 2:
            return httpx.Response(500, text="boom")
        return httpx.Response(200, text="")

    client: ClickHouseClient = make_client(handler, recorder=recorder)
    with pytest.raises(ClickHouseError):
        await ensure_contract(client, "radar")
    assert len(recorder.requests) == 2


class Marker(Exception):
    """Raised by the fake KafkaSubscriber: the schema phase is over."""


class Fakes:
    def __init__(self, contract_failures: int = 0) -> None:
        self.calls: list[str] = []
        self.contract_args: list[tuple[str, str]] = []
        self.subscribers: int = 0
        self.contract_failures: int = contract_failures

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fakes: Fakes = self

        async def ensure_schema(client: ClickHouseClient, database: str) -> None:
            fakes.calls.append("ensure_schema")

        async def ensure_erasures_schema(client: ClickHouseClient, database: str) -> None:
            fakes.calls.append("ensure_erasures_schema")

        async def ensure_excerpts_schema(client: ClickHouseClient, database: str) -> None:
            fakes.calls.append("ensure_excerpts_schema")

        async def ensure_contract(
            client: ClickHouseClient, source: str, contract: str = CONTRACT_DATABASE
        ) -> None:
            fakes.calls.append("ensure_contract")
            fakes.contract_args.append((source, contract))
            if fakes.contract_failures > 0:
                fakes.contract_failures -= 1
                raise ClickHouseError("boom")

        class FakeSubscriber:
            def __init__(self, *args: object, **kwargs: object) -> None:
                fakes.subscribers += 1
                raise Marker

        monkeypatch.setattr(ch, "ensure_schema", ensure_schema)
        monkeypatch.setattr(ch, "ensure_erasures_schema", ensure_erasures_schema)
        monkeypatch.setattr(ch, "ensure_excerpts_schema", ensure_excerpts_schema)
        monkeypatch.setattr(ch, "ensure_contract", ensure_contract)
        monkeypatch.setattr(kafka_bus, "KafkaSubscriber", FakeSubscriber)


ORDER: list[str] = ["ensure_schema", "ensure_erasures_schema", "ensure_excerpts_schema", "ensure_contract"]


async def test_09_store_start_creates_the_interface(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    fakes: Fakes = Fakes()
    fakes.install(monkeypatch)
    with caplog.at_level(logging.INFO, logger="gamedev_trends.store"):
        with pytest.raises(Marker):
            await store._serve(StoreConfig(clickhouse_password="x"))
    assert fakes.calls == ORDER
    assert fakes.contract_args == [("radar", "alerts_v1")]
    infos: list[str] = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    assert "schema ready: database=radar interface=alerts_v1" in infos


async def test_10_interface_failure_retries_everything(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    fakes: Fakes = Fakes(contract_failures=1)
    fakes.install(monkeypatch)
    config: StoreConfig = StoreConfig(
        clickhouse_password="x", retry_base_seconds=0.001, retry_max_seconds=0.001
    )
    with caplog.at_level(logging.INFO, logger="gamedev_trends.store"):
        with pytest.raises(Marker):
            await store._serve(config)
    assert fakes.calls == ORDER + ORDER
    warnings: list[str] = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("schema attempt 1 failed: boom" in m for m in warnings)
    assert fakes.subscribers == 1
