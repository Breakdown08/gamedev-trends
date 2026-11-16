"""Spec checks 31-32: the Top posts query and the README."""

import re

from bluesky_dashboard_support import bluesky_sql, bluesky_text, collapse
from dashboards_support import ROOT


def section(text: str, heading: str) -> str:
    match: re.Match[str] | None = re.search(
        rf"^## {re.escape(heading)}\s*$(.*?)(?=^## |\Z)", text, re.MULTILINE | re.DOTALL
    )
    assert match is not None, heading
    return match.group(1)


def test_31_top_query_columns() -> None:
    sql: str = collapse(bluesky_sql()["top"])
    outer: re.Match[str] | None = re.search(r"\) SELECT (.*?) FROM ranked\b", sql)
    assert outer is not None
    assert re.findall(r"\bAS (\w+)", outer.group(1)) == ["link", "about", "likes", "reposts", "tags"]


def test_31_top_query_has_the_join_and_only_time_placeholders() -> None:
    sql: str = collapse(bluesky_sql()["top"])
    assert "LEFT JOIN" in sql
    assert "FROM excerpts FINAL" in sql
    assert set(re.findall(r"\$\{?\w+\}?", sql)) <= {"$__fromTime", "$__toTime"}


def test_31_about_has_no_display_name_override() -> None:
    assert "О чём" not in bluesky_text()


def test_32_readme() -> None:
    text: str = (ROOT / "README.md").read_text()
    assert "radar.excerpts" in text
    assert "140" in section(text, "Bluesky collector")
    assert re.search(r"\babout\b", section(text, "Dashboards"))
    assert "Not stored: post texts" not in text
