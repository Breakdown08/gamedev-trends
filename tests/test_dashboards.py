"""Spec checks 1-14: static checks of the Grafana dashboard, provisioning, compose and README."""

import json
import re
from typing import Any

import pytest
from dashboards_support import (
    ALLOWED_PLACEHOLDERS,
    DATASOURCE,
    DATASOURCE_PATH,
    PROVIDER_PATH,
    ROOT,
    all_sql,
    compose_block,
    dashboard_text,
    find_datasources,
    grep_key,
    load_dashboard,
    panels,
    substitute,
    target_of,
    variables,
)


def test_01_json_roundtrip_is_stable() -> None:
    text: str = dashboard_text()
    obj: Any = json.loads(text)
    assert text == json.dumps(obj, indent=2, ensure_ascii=False) + "\n"


def test_02_root() -> None:
    d: dict[str, Any] = load_dashboard()
    assert d["uid"] == "radar-steam"
    assert d["title"] == "Steam radar"
    assert d.get("id") is None
    assert "__inputs" not in d
    assert d["time"] == {"from": "now-7d", "to": "now"}
    assert d["refresh"] == "5m"
    assert isinstance(d["schemaVersion"], int)
    assert not isinstance(d["schemaVersion"], bool)


def test_03_panels() -> None:
    p: list[dict[str, Any]] = panels()
    assert [(x["id"], x["type"], x["title"]) for x in p] == [
        (1, "timeseries", "Players online"),
        (2, "timeseries", "Chart rank"),
        (3, "table", "Chart movements"),
    ]
    panel: dict[str, Any]
    for panel in p:
        target: dict[str, Any] = target_of(panel)
        assert target["refId"] == "A"
        assert target["editorType"] == "sql"
        assert isinstance(target["rawSql"], str)
        assert target["rawSql"].strip() != ""


def test_04_datasource_everywhere() -> None:
    found: list[object] = find_datasources(load_dashboard())
    assert len(found) >= 7  # 3 panels + 3 targets + variable game
    assert all(x == DATASOURCE for x in found)
    assert "${DS_" not in dashboard_text()


def test_05_variables() -> None:
    v: list[dict[str, Any]] = variables()
    assert [x["name"] for x in v] == ["game", "period"]
    game: dict[str, Any] = v[0]
    assert game["type"] == "query"
    assert game["multi"] is True
    assert game["includeAll"] is True
    assert game["refresh"] == 1
    assert "FINAL" in all_sql()["game"]
    period: dict[str, Any] = v[1]
    assert period["type"] == "custom"
    assert period["query"] == "1,7,30"
    assert period["current"]["value"] == "1"
    assert not period["multi"]


def test_06_datasource_provisioning() -> None:
    text: str = DATASOURCE_PATH.read_text()
    pair: tuple[str, str]
    for pair in [
        ("apiVersion", "1"),
        ("type", "grafana-clickhouse-datasource"),
        ("uid", "clickhouse"),
        ("host", "clickhouse"),
        ("port", "8123"),
        ("protocol", "http"),
        ("username", "radar"),
        ("defaultDatabase", "radar"),
    ]:
        assert grep_key(text, *pair), pair
    assert grep_key(text, "password", "$CLICKHOUSE_PASSWORD") or grep_key(
        text, "password", "${CLICKHOUSE_PASSWORD}"
    )
    assert "change-me" not in text


def test_07_dashboard_provider() -> None:
    text: str = PROVIDER_PATH.read_text()
    pair: tuple[str, str]
    for pair in [
        ("apiVersion", "1"),
        ("type", "file"),
        ("folder", "Radar"),
        ("allowUiUpdates", "false"),
        ("disableDeletion", "true"),
        ("path", "/var/lib/grafana/dashboards"),
    ]:
        assert grep_key(text, *pair), pair


@pytest.mark.parametrize("name", ["players", "rank", "movements", "game"])
def test_08_sql_rules(name: str) -> None:
    sql: str = all_sql()[name]
    assert re.search(r"\bobservations\b", sql), "must read observations"
    assert re.findall(r"\bobservations\b(?!\s+FINAL\b)", sql) == []
    assert "observations_hourly" not in sql
    assert "radar." not in sql
    assert "`radar`." not in sql
    assert re.search(r"source\s*=\s*'steam'", sql)
    assert re.search(r"entity_type\s*=\s*'game'", sql)
    rest: str = sql
    placeholder: str
    for placeholder in ALLOWED_PLACEHOLDERS:
        rest = rest.replace(placeholder, "")
    assert "$" not in rest
    assert "$" not in substitute(sql, from_ts=1, to_ts=2)
    assert not sql.rstrip().endswith(";")
    assert "FORMAT" not in sql.upper()


def test_09_placeholders_per_query() -> None:
    s: dict[str, str] = all_sql()
    name: str
    for name in ("players", "rank"):
        assert "${game:singlequote}" in s[name]
        assert "$__fromTime" in s[name]
        assert "$__toTime" in s[name]
        assert "${period}" not in s[name]
    assert "${period}" in s["movements"]
    assert "$__fromTime" not in s["movements"]
    assert "$__toTime" not in s["movements"]
    assert "${game" not in s["movements"]
    assert "$" not in s["game"]


def test_10_steam_link() -> None:
    assert "https://store.steampowered.com/app/${__value.raw}" in dashboard_text()


def test_11_compose_grafana_block() -> None:
    block: str = compose_block("grafana")
    assert re.search(r"^\s*image:\s*grafana/grafana:\d+\.\d+\.\d+\s*$", block, re.MULTILINE)
    assert re.search(r"grafana-clickhouse-datasource@\d+\.\d+\.\d+", block)
    needle: str
    for needle in [
        '"127.0.0.1:3000:3000"',
        'GF_AUTH_ANONYMOUS_ENABLED: "true"',
        "GF_AUTH_ANONYMOUS_ORG_ROLE: Viewer",
        "GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_ADMIN_PASSWORD:?",
        "CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD:?",
        "./grafana/provisioning:/etc/grafana/provisioning:ro",
        "./grafana/dashboards:/var/lib/grafana/dashboards:ro",
        "service_healthy",
        "healthcheck:",
        "/api/health",
        "restart: unless-stopped",
    ]:
        assert needle in block, needle
    assert "build:" not in block


def test_12_compose_static_with_grafana() -> None:
    text: str = (ROOT / "compose.yaml").read_text()
    images: set[str] = set(re.findall(r"^\s*image:\s*(\S+)", text, re.MULTILINE))
    grafana_image: re.Match[str] | None = re.search(
        r"^\s*image:\s*(grafana/grafana:\d+\.\d+\.\d+)\s*$", compose_block("grafana"), re.MULTILINE
    )
    assert grafana_image is not None
    assert images == {
        "apache/kafka:4.3.2",
        "clickhouse/clickhouse-server:26.8.36.3",
        "redis:8.8.4-alpine",
        grafana_image.group(1),
    }
    ports: list[str] = re.findall(r"^\s*-\s*[\"']?(\d+(?::\d+)?)[\"']?\s*$", text, re.MULTILINE)
    assert ports == []
    assert text.count("127.0.0.1:") >= 5


def test_13_env_example() -> None:
    assert (ROOT / ".env.example").read_text().strip() == (
        "CLICKHOUSE_PASSWORD=change-me\n"
        "GRAFANA_ADMIN_PASSWORD=change-me\n"
        "RADAR_ALERTS_CLICKHOUSE_PASSWORD=change-me"
    )
    lines: set[str] = {x.strip() for x in (ROOT / ".gitignore").read_text().splitlines()}
    assert ".env" in lines


def test_14_readme() -> None:
    text: str = (ROOT / "README.md").read_text()
    assert re.search(r"^## Dashboards\s*$", text, re.MULTILINE)
    needle: str
    for needle in [
        "http://localhost:3000",
        "GRAFANA_ADMIN_PASSWORD",
        "appid",
        "grafana/dashboards/steam.json",
        "docker compose restart grafana",
    ]:
        assert needle in text, needle
    status: list[str] = [x for x in text.splitlines() if x.startswith("Status:")]
    assert len(status) == 1
    assert "Grafana" in status[0]
