"""Spec checks 50-55: dependency, compose, dashboard file and README (51 lives in test_store_project)."""

import json
import re
import tomllib
from typing import Any

from bluesky_dashboard_support import (
    RATE_SQL,
    TAGS_SQL,
    TOP_SQL,
    bluesky_dashboard,
    bluesky_panels,
    bluesky_sql,
    bluesky_text,
    collapse,
)
from dashboards_support import DATASOURCE, ROOT, compose_block, find_datasources


def test_50_websockets_dependency() -> None:
    data: dict[str, Any] = tomllib.loads((ROOT / "pyproject.toml").read_text())
    assert "websockets>=17.4,<18" in data["project"]["dependencies"]
    assert 'name = "websockets"' in (ROOT / "uv.lock").read_text()


def _profiles(block: str) -> list[str] | None:
    inline: re.Match[str] | None = re.search(r"^\s*profiles:\s*\[(.*)\]\s*$", block, re.MULTILINE)
    if inline is not None:
        return [x.strip().strip("'\"") for x in inline.group(1).split(",") if x.strip()]
    listed: re.Match[str] | None = re.search(r"^\s*profiles:\s*\n((?:\s+-\s*\S+\s*\n)+)", block, re.MULTILINE)
    if listed is not None:
        return [x.strip().strip("'\"") for x in re.findall(r"-\s*(\S+)", listed.group(1))]
    return None


def test_52_bluesky_collector_block() -> None:
    block: str = compose_block("bluesky-collector")
    needle: str
    for needle in [
        "build: .",
        "gamedev_trends.bluesky",
        "KAFKA_BOOTSTRAP_SERVERS: kafka:29092",
        "REDIS_URL: redis://redis:6379/0",
        "service_completed_successfully",
        "service_healthy",
        "restart: unless-stopped",
    ]:
        assert needle in block, needle
    assert "image:" not in block
    assert "ports:" not in block
    assert _profiles(block) == ["collectors"]


def test_52_only_the_bluesky_collector_has_a_profile() -> None:
    text: str = (ROOT / "compose.yaml").read_text()
    assert len(re.findall(r"^\s*profiles:", text, re.MULTILINE)) == 1
    assert "profiles" not in compose_block("steam-collector")


def test_53_redis_persists_to_a_named_volume() -> None:
    block: str = compose_block("redis")
    assert "--appendonly" in block
    assert "redis-data:/data" in block
    text: str = (ROOT / "compose.yaml").read_text()
    top: re.Match[str] | None = re.search(r"^volumes:\n((?:[ ]+.*\n?)+)", text, re.MULTILINE)
    assert top is not None
    assert re.search(r"^\s+redis-data:\s*$", top.group(1), re.MULTILINE)


def test_54_json_roundtrip_is_stable() -> None:
    text: str = bluesky_text()
    obj: Any = json.loads(text)
    assert text == json.dumps(obj, indent=2, ensure_ascii=False) + "\n"


def test_54_root() -> None:
    d: dict[str, Any] = bluesky_dashboard()
    assert d["uid"] == "radar-bluesky"
    assert d["title"] == "Bluesky radar"
    assert d.get("id") is None
    assert "__inputs" not in d
    assert d["time"] == {"from": "now-24h", "to": "now"}
    assert d["refresh"] == "1m"
    assert isinstance(d["schemaVersion"], int)
    assert not isinstance(d["schemaVersion"], bool)
    assert d["templating"]["list"] == []


def test_54_panels_and_targets() -> None:
    panels: list[dict[str, Any]] = bluesky_panels()
    assert [(p["id"], p["type"], p["title"]) for p in panels] == [
        (1, "timeseries", "Tag frequency"),
        (2, "timeseries", "Stream rate"),
        (3, "table", "Top posts"),
    ]
    panel: dict[str, Any]
    for panel in panels:
        assert panel["datasource"] == DATASOURCE
        assert len(panel["targets"]) == 1
        target: dict[str, Any] = panel["targets"][0]
        assert target["refId"] == "A"
        assert target["editorType"] == "sql"
        assert target["datasource"] == DATASOURCE


def test_54_datasource_everywhere() -> None:
    found: list[object] = find_datasources(bluesky_dashboard())
    assert len(found) >= 6
    assert all(x == DATASOURCE for x in found)
    assert "${DS_" not in bluesky_text()


def test_54_queries() -> None:
    sql: dict[str, str] = bluesky_sql()
    assert collapse(sql["tags"]) == collapse(TAGS_SQL)
    assert collapse(sql["rate"]) == collapse(RATE_SQL)
    assert collapse(sql["top"]) == collapse(TOP_SQL)


def test_54_only_time_range_placeholders() -> None:
    allowed: set[str] = {"$__fromTime", "$__toTime"}
    query: str
    for query in bluesky_sql().values():
        assert set(re.findall(r"\$\{?\w+\}?", query)) <= allowed


def test_54_link_column_uses_the_raw_value() -> None:
    assert "${__value.raw}" in bluesky_text()


def test_55_readme_bluesky_section() -> None:
    text: str = (ROOT / "README.md").read_text()
    assert re.search(r"^## Bluesky collector\s*$", text, re.MULTILINE)
    needle: str
    for needle in [
        "uv run python -m gamedev_trends.bluesky",
        "docker compose --profile collectors up -d --wait",
        "BLUESKY_TAGS",
        "BLUESKY_WINDOW_SECONDS",
        "bluesky stats",
        "radar.erasures.v1",
        "Bluesky radar",
        "grafana/dashboards/bluesky.json",
    ]:
        assert needle in text, needle
    assert "Not covered: erasures" not in text


def test_55_status_line() -> None:
    text: str = (ROOT / "README.md").read_text()
    status: list[str] = [x for x in text.splitlines() if x.startswith("Status:")]
    assert len(status) == 1
    assert "Grafana" in status[0]
    assert "Bluesky" in status[0]
