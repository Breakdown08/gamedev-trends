"""Helpers for the dashboard tests: loading steam.json, extracting SQL, substituting placeholders."""

import json
import re
from pathlib import Path
from typing import Any

ROOT: Path = Path(__file__).resolve().parents[1]
DASHBOARD_PATH: Path = ROOT / "grafana" / "dashboards" / "steam.json"
DATASOURCE_PATH: Path = ROOT / "grafana" / "provisioning" / "datasources" / "clickhouse.yaml"
PROVIDER_PATH: Path = ROOT / "grafana" / "provisioning" / "dashboards" / "radar.yaml"
DATASOURCE: dict[str, str] = {"type": "grafana-clickhouse-datasource", "uid": "clickhouse"}
ALLOWED_PLACEHOLDERS: tuple[str, ...] = ("$__fromTime", "$__toTime", "${game:singlequote}", "${period}")
SQL_NAMES: tuple[str, ...] = ("players", "rank", "movements", "game")


def dashboard_text() -> str:
    return DASHBOARD_PATH.read_text()


def load_dashboard() -> dict[str, Any]:
    result: dict[str, Any] = json.loads(dashboard_text())
    return result


def panels() -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = load_dashboard()["panels"]
    return result


def variables() -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = load_dashboard()["templating"]["list"]
    return result


def target_of(panel: dict[str, Any]) -> dict[str, Any]:
    targets: list[dict[str, Any]] = panel["targets"]
    assert len(targets) == 1
    return targets[0]


def game_sql() -> str:
    query: Any = variables()[0]["query"]
    if isinstance(query, dict):
        return str(query["rawSql"])
    assert isinstance(query, str)
    return query


def all_sql() -> dict[str, str]:
    """The four queries by name: players, rank, movements, game."""
    p: list[dict[str, Any]] = panels()
    return {
        "players": str(target_of(p[0])["rawSql"]),
        "rank": str(target_of(p[1])["rawSql"]),
        "movements": str(target_of(p[2])["rawSql"]),
        "game": game_sql(),
    }


def substitute(
    sql: str, *, from_ts: int = 0, to_ts: int = 0, games: str = "'730','570'", period: int = 1
) -> str:
    """Expand the Grafana placeholders the way the spec prescribes for tests."""
    out: str = sql.replace("$__fromTime", f"toDateTime({from_ts}, 'UTC')")
    out = out.replace("$__toTime", f"toDateTime({to_ts}, 'UTC')")
    out = out.replace("${game:singlequote}", games)
    out = out.replace("${period}", str(period))
    return out


def unix(year: int, month: int, day: int, hour: int = 0) -> int:
    from datetime import UTC, datetime

    return int(datetime(year, month, day, hour, tzinfo=UTC).timestamp())


def find_datasources(node: object) -> list[object]:
    """Every value stored under a 'datasource' key anywhere in the tree."""
    found: list[object] = []
    if isinstance(node, dict):
        key: str
        value: object
        for key, value in node.items():
            if key == "datasource":
                found.append(value)
            found += find_datasources(value)
    elif isinstance(node, list):
        item: object
        for item in node:
            found += find_datasources(item)
    return found


def compose_block(name: str) -> str:
    text: str = (ROOT / "compose.yaml").read_text()
    match: re.Match[str] | None = re.search(rf"^  {name}:\n((?:    .*\n|\n)+)", text, re.MULTILINE)
    assert match is not None, f"no {name} block in compose.yaml"
    return match.group(1)


def grep_key(text: str, key: str, value: str) -> bool:
    """A line `key: value` with any spaces around the colon."""
    return (
        re.search(rf"^\s*-?\s*{re.escape(key)}\s*:\s*{re.escape(value)}\s*$", text, re.MULTILINE) is not None
    )
