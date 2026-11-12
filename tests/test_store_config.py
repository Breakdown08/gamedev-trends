"""Spec checks 37-40: StoreConfig.from_env and main."""

import pytest

from gamedev_trends.store import StoreConfig, main


def test_37_defaults() -> None:
    assert StoreConfig.from_env({"CLICKHOUSE_PASSWORD": "pw"}) == StoreConfig(clickhouse_password="pw")
    cfg: StoreConfig = StoreConfig.from_env({"CLICKHOUSE_PASSWORD": "pw", "CLICKHOUSE_URL": "http://x:8123/"})
    assert cfg.clickhouse_url == "http://x:8123"


def test_37_overrides_empty_and_unknown() -> None:
    cfg: StoreConfig = StoreConfig.from_env(
        {
            "CLICKHOUSE_PASSWORD": "pw",
            "KAFKA_BOOTSTRAP_SERVERS": "k:1",
            "CLICKHOUSE_USER": "u",
            "CLICKHOUSE_DATABASE": "db_1",
            "STORE_GROUP": "g",
            "STORE_BATCH_MAX_EVENTS": "100000",
            "STORE_BATCH_MAX_SECONDS": "0.5",
            "STORE_HTTP_TIMEOUT_SECONDS": "7",
            "STORE_RETRY_BASE_SECONDS": "2",
            "STORE_RETRY_MAX_SECONDS": "60",
            "STORE_UNKNOWN": "x",
        }
    )
    assert cfg == StoreConfig(
        clickhouse_password="pw",
        kafka_bootstrap_servers="k:1",
        clickhouse_user="u",
        clickhouse_database="db_1",
        group="g",
        batch_max_events=100000,
        batch_max_seconds=0.5,
        http_timeout_seconds=7.0,
        retry_base_seconds=2.0,
        retry_max_seconds=60.0,
    )
    empty: StoreConfig = StoreConfig.from_env(
        {"CLICKHOUSE_PASSWORD": "pw", "STORE_GROUP": "", "CLICKHOUSE_URL": ""}
    )
    assert empty == StoreConfig(clickhouse_password="pw")


@pytest.mark.parametrize(
    ("env", "name"),
    [
        ({}, "CLICKHOUSE_PASSWORD"),
        ({"CLICKHOUSE_PASSWORD": ""}, "CLICKHOUSE_PASSWORD"),
        ({"CLICKHOUSE_DATABASE": "radar-x"}, "CLICKHOUSE_DATABASE"),
        ({"STORE_BATCH_MAX_EVENTS": "0"}, "STORE_BATCH_MAX_EVENTS"),
        ({"STORE_BATCH_MAX_EVENTS": "100001"}, "STORE_BATCH_MAX_EVENTS"),
        ({"STORE_BATCH_MAX_EVENTS": "abc"}, "STORE_BATCH_MAX_EVENTS"),
        ({"STORE_BATCH_MAX_SECONDS": "0"}, "STORE_BATCH_MAX_SECONDS"),
        ({"STORE_HTTP_TIMEOUT_SECONDS": "0"}, "STORE_HTTP_TIMEOUT_SECONDS"),
        ({"STORE_RETRY_BASE_SECONDS": "0"}, "STORE_RETRY_BASE_SECONDS"),
        ({"STORE_RETRY_MAX_SECONDS": "0.5"}, "STORE_RETRY_MAX_SECONDS"),
    ],
)
def test_38_invalid_values(env: dict[str, str], name: str) -> None:
    full: dict[str, str] = {"CLICKHOUSE_PASSWORD": "pw", **env}
    if name == "CLICKHOUSE_PASSWORD":
        full = dict(env)
    with pytest.raises(ValueError, match=name):
        StoreConfig.from_env(full)


def test_39_main_without_password(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("CLICKHOUSE_PASSWORD", raising=False)
    assert main([]) == 2
    assert "CLICKHOUSE_PASSWORD" in capsys.readouterr().err
