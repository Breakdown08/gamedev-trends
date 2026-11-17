"""Shared data for the alerts_v1 interface tests: the consumer's DDL and columns, kept as our own copy."""

import re
from pathlib import Path

ROOT: Path = Path(__file__).resolve().parents[1]

EXPECTED_SQL: list[str] = [
    "CREATE DATABASE IF NOT EXISTS alerts_v1",
    """CREATE VIEW IF NOT EXISTS alerts_v1.steam_players
DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
SELECT entity_id AS appid, observed_at, toUInt32(value) AS players
FROM radar.observations FINAL
WHERE source = 'steam' AND entity_type = 'game' AND metric = 'players_current'""",
    """CREATE VIEW IF NOT EXISTS alerts_v1.steam_chart
DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
SELECT entity_id AS appid, observed_at AS chart_day, toUInt16(value) AS rank
FROM radar.observations FINAL
WHERE source = 'steam' AND entity_type = 'game' AND metric = 'chart_rank'""",
    """CREATE VIEW IF NOT EXISTS alerts_v1.bluesky_tag_hourly
DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
SELECT substring(metric, 5) AS tag, toStartOfHour(assumeNotNull(window_start)) AS hour, count() AS posts
FROM radar.observations FINAL
WHERE source = 'bluesky' AND entity_type = 'post' AND startsWith(metric, 'tag_') AND window_start IS NOT NULL
GROUP BY tag, hour""",
]

EXPECTED_COLUMNS: dict[str, list[tuple[str, str]]] = {
    "steam_players": [
        ("appid", "String"),
        ("observed_at", "DateTime64(6, 'UTC')"),
        ("players", "UInt32"),
    ],
    "steam_chart": [
        ("appid", "String"),
        ("chart_day", "DateTime64(6, 'UTC')"),
        ("rank", "UInt16"),
    ],
    "bluesky_tag_hourly": [
        ("tag", "LowCardinality(String)"),
        ("hour", "DateTime('UTC')"),
        ("posts", "UInt64"),
    ],
}


def readme_section(text: str, heading: str) -> str:
    """Text from the heading line (regex) to the next `## ` heading or the end of the file."""
    match: re.Match[str] | None = re.search(heading, text, re.MULTILINE)
    assert match is not None, f"no heading {heading!r}"
    rest: str = text[match.end() :]
    nxt: re.Match[str] | None = re.search(r"^## ", rest, re.MULTILINE)
    return rest if nxt is None else rest[: nxt.start()]
