"""Spec checks 6-8: datetime formatting and event to row mapping."""

from datetime import datetime, timedelta, timezone

import pytest
from store_support import T10

from gamedev_trends.contract import Observation
from gamedev_trends.store import Row, format_datetime, to_row

PLUS7: timezone = timezone(timedelta(hours=7))
KEYS: list[str] = [
    "event_id",
    "source",
    "entity_type",
    "entity_id",
    "metric",
    "value",
    "observed_at",
    "window_start",
    "ingested_at",
]


def test_06_format_datetime() -> None:
    assert (
        format_datetime(datetime(2026, 11, 12, 17, 0, 5, 123456, tzinfo=PLUS7))
        == "2026-11-12 10:00:05.123456"
    )
    assert format_datetime(T10) == "2026-11-12 10:00:00.000000"
    with pytest.raises(ValueError):
        format_datetime(datetime(2026, 11, 12, 10, 0))


def test_07_to_row_example() -> None:
    o: Observation = Observation.create(
        source="steam",
        entity_type="game",
        entity_id="730",
        metric="players_current",
        value=638896,
        observed_at=T10,
    )
    row: Row = to_row(o, datetime(2026, 11, 12, 17, 0, 5, 123456, tzinfo=PLUS7))
    assert row == {
        "event_id": str(o.event_id),
        "source": "steam",
        "entity_type": "game",
        "entity_id": "730",
        "metric": "players_current",
        "value": 638896.0,
        "observed_at": "2026-11-12 10:00:00.000000",
        "window_start": None,
        "ingested_at": "2026-11-12 10:00:05.123456",
    }
    assert list(row) == KEYS
    assert isinstance(row["event_id"], str)


def test_08_windowed_observation() -> None:
    o: Observation = Observation.create(
        source="bluesky",
        entity_type="tag",
        entity_id="indiedev",
        metric="posts",
        value=42,
        observed_at=T10 + timedelta(hours=1),
        window_start=T10,
    )
    row: Row = to_row(o, T10 + timedelta(hours=2))
    assert row["window_start"] == "2026-11-12 10:00:00.000000"
    assert row["observed_at"] == "2026-11-12 11:00:00.000000"
    assert row["value"] == 42.0
    assert isinstance(row["value"], float)
    assert list(row) == KEYS
