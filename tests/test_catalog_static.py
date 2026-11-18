"""Spec checks 24-27: pipelines, validator, configuration, CLI argument handling."""

from typing import Any

import pytest
from catalog_support import STARDEW, parsed_games

from gamedev_trends.catalog import CatalogConfig, main
from gamedev_trends.mongo_catalog import (
    GAMES_VALIDATOR,
    growing_tags_pipeline,
    indie_share_pipeline,
    niche_pipeline,
)


def test_24_pipelines() -> None:
    assert niche_pipeline("Roguelike")[0] == {"$match": {"tags.name": "Roguelike", "steam_type": 0}}
    assert indie_share_pipeline("Indie")[0]["$match"]["tags.name"] == "Indie"
    assert {"$limit": 5} in growing_tags_pipeline([1, 2], limit=5)
    with pytest.raises(ValueError):
        growing_tags_pipeline([1], limit=0)


def test_25_validator_required_matches_model() -> None:
    schema: dict[str, Any] = GAMES_VALIDATOR["$jsonSchema"]
    assert schema["bsonType"] == "object"
    assert set(schema["required"]) == set(parsed_games()[STARDEW].model_dump()) | {"created_at"}


def test_26_config_defaults() -> None:
    assert CatalogConfig.from_env({}) == CatalogConfig(
        mongodb_url="mongodb://localhost:27017",
        mongodb_database="radar",
        api_base="https://api.steampowered.com",
        http_timeout_seconds=10.0,
        clickhouse_url="http://localhost:8123",
        clickhouse_user="radar",
        clickhouse_password=None,
        clickhouse_database="radar",
    )


def test_26_config_values_and_empty_strings() -> None:
    config: CatalogConfig = CatalogConfig.from_env(
        {
            "MONGODB_URL": "mongodb+srv://example.test/x",
            "MONGODB_DATABASE": "radar_x",
            "STEAM_API_BASE": "https://steam.test/",
            "CATALOG_HTTP_TIMEOUT_SECONDS": "2.5",
            "CLICKHOUSE_URL": "http://ch.test:8123/",
            "CLICKHOUSE_USER": "u",
            "CLICKHOUSE_PASSWORD": "pw",
            "CLICKHOUSE_DATABASE": "db_x",
        }
    )
    assert config.mongodb_url == "mongodb+srv://example.test/x"
    assert config.mongodb_database == "radar_x"
    assert config.api_base == "https://steam.test"
    assert config.http_timeout_seconds == 2.5
    assert config.clickhouse_url == "http://ch.test:8123"
    assert config.clickhouse_user == "u"
    assert config.clickhouse_password == "pw"
    assert config.clickhouse_database == "db_x"
    assert CatalogConfig.from_env({"CLICKHOUSE_PASSWORD": ""}).clickhouse_password is None
    assert CatalogConfig.from_env({"MONGODB_DATABASE": ""}).mongodb_database == "radar"


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("MONGODB_URL", "redis://x"),
        ("MONGODB_DATABASE", "Bad-Name"),
        ("CATALOG_HTTP_TIMEOUT_SECONDS", "0"),
        ("CATALOG_HTTP_TIMEOUT_SECONDS", "nan"),
    ],
)
def test_26_config_bad_values(name: str, value: str) -> None:
    with pytest.raises(ValueError, match=name):
        CatalogConfig.from_env({name: value})


def test_27_refresh_without_clickhouse_password(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("CLICKHOUSE_PASSWORD", raising=False)
    assert main(["refresh"]) == 2
    assert "configuration error:" in capsys.readouterr().err


def test_27_unknown_command() -> None:
    code: int | str | None
    try:
        code = main(["nosuch"])  # argparse may exit instead of returning
    except SystemExit as exc:
        code = exc.code
    assert code == 2
