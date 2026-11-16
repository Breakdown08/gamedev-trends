"""The Bluesky dashboard file and the three queries the spec prescribes for its panels."""

import json
from pathlib import Path
from typing import Any

from dashboards_support import ROOT

BLUESKY_PATH: Path = ROOT / "grafana" / "dashboards" / "bluesky.json"

TAGS_SQL: str = """
SELECT toStartOfHour(window_start) AS time, substring(metric, 5) AS tag, count() AS posts
FROM observations FINAL
WHERE source = 'bluesky' AND entity_type = 'post' AND startsWith(metric, 'tag_')
  AND window_start >= $__fromTime AND window_start <= $__toTime
GROUP BY time, tag
ORDER BY time, tag
"""

RATE_SQL: str = """
SELECT window_start AS time, metric, value / dateDiff('second', window_start, observed_at) AS per_second
FROM observations FINAL
WHERE source = 'bluesky' AND entity_type = 'stream' AND entity_id = 'jetstream'
  AND window_start >= $__fromTime AND window_start <= $__toTime
ORDER BY time, metric
"""

TOP_SQL: str = "\n".join(
    [
        "SELECT",
        "    concat('https://bsky.app/profile/', splitByChar('/', entity_id)[3], '/post/',"
        " splitByChar('/', entity_id)[5]) AS link,",
        "    sumIf(value, metric = 'likes') AS likes,",
        "    sumIf(value, metric = 'reposts') AS reposts,",
        "    arrayStringConcat(arraySort(groupUniqArrayIf(substring(metric, 5),"
        " startsWith(metric, 'tag_'))), ' ') AS tags",
        "FROM observations FINAL",
        "WHERE source = 'bluesky' AND entity_type = 'post'",
        "  AND window_start >= $__fromTime AND window_start <= $__toTime",
        "GROUP BY entity_id",
        "HAVING tags != ''",
        "ORDER BY likes + reposts DESC, link",
        "LIMIT 20",
    ]
)


def collapse(sql: str) -> str:
    return " ".join(sql.split())


def bluesky_text() -> str:
    return BLUESKY_PATH.read_text()


def bluesky_dashboard() -> dict[str, Any]:
    result: dict[str, Any] = json.loads(bluesky_text())
    return result


def bluesky_panels() -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = bluesky_dashboard()["panels"]
    return result


def bluesky_sql() -> dict[str, str]:
    """rawSql of the panels by role: tags, rate, top."""
    p: list[dict[str, Any]] = bluesky_panels()
    return {
        "tags": str(p[0]["targets"][0]["rawSql"]),
        "rate": str(p[1]["targets"][0]["rawSql"]),
        "top": str(p[2]["targets"][0]["rawSql"]),
    }
