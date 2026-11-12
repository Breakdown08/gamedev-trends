"""Spec checks 6-16: the measurement contract."""

import math
import uuid
from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from gamedev_trends.contract import EVENT_ID_NAMESPACE, Erasure, Observation, erases, make_event_id

T10: datetime = datetime(2026, 11, 12, 10, 0, tzinfo=UTC)
T11: datetime = datetime(2026, 11, 12, 11, 0, tzinfo=UTC)
BAD_NAMES: list[str] = ["", "Steam", "steam|x", "1steam", "a" * 33]


def steam(
    *,
    source: str = "steam",
    entity_type: str = "game",
    entity_id: str = "730",
    metric: str = "players_current",
    value: float = 638896,
    observed_at: datetime = T10,
    window_start: datetime | None = None,
) -> Observation:
    return Observation.create(
        source=source,
        entity_type=entity_type,
        entity_id=entity_id,
        metric=metric,
        value=value,
        observed_at=observed_at,
        window_start=window_start,
    )


def with_name(field: str, text: str) -> Observation:
    return steam(
        source=text if field == "source" else "steam",
        entity_type=text if field == "entity_type" else "game",
        metric=text if field == "metric" else "players_current",
    )


def erasure(
    *,
    source: str = "bluesky",
    entity_type: str = "post",
    entity_id: str = "at://did:plc:abc/app.bsky.feed.post/3m5xyz",
    requested_at: datetime = T11,
) -> Erasure:
    return Erasure.create(
        source=source, entity_type=entity_type, entity_id=entity_id, requested_at=requested_at
    )


def test_06_steam_observation_fields_and_event_id() -> None:
    obs: Observation = steam()
    assert obs.kind == "observation"
    assert (obs.source, obs.entity_type, obs.entity_id, obs.metric) == (
        "steam",
        "game",
        "730",
        "players_current",
    )
    assert obs.value == 638896
    assert obs.observed_at == T10
    assert obs.window_start is None
    expected: uuid.UUID = make_event_id(
        "observation", "steam", "game", "730", "players_current", "", "2026-11-12T10:00:00+00:00"
    )
    assert obs.event_id == expected
    assert expected == uuid.uuid5(
        EVENT_ID_NAMESPACE, "observation|steam|game|730|players_current||2026-11-12T10:00:00+00:00"
    )


def test_06_namespace_and_make_event_id() -> None:
    assert EVENT_ID_NAMESPACE == uuid.uuid5(uuid.NAMESPACE_URL, "gamedev-trends:event")
    assert make_event_id("a", "b") == uuid.uuid5(EVENT_ID_NAMESPACE, "a|b")


def test_06_windowed_event_id_contains_window_start() -> None:
    obs: Observation = Observation.create(
        source="bluesky",
        entity_type="post",
        entity_id="at://did:plc:abc/app.bsky.feed.post/3m5xyz",
        metric="likes",
        value=42,
        window_start=T10,
        observed_at=T11,
    )
    assert obs.window_start == T10
    assert obs.event_id == make_event_id(
        "observation",
        "bluesky",
        "post",
        "at://did:plc:abc/app.bsky.feed.post/3m5xyz",
        "likes",
        "2026-11-12T10:00:00+00:00",
        "2026-11-12T11:00:00+00:00",
    )


def test_07_event_id_is_deterministic_and_keyed_on_natural_key() -> None:
    base: uuid.UUID = steam().event_id
    assert steam().event_id == base
    assert steam(value=1).event_id == base
    assert steam(observed_at=T10 + timedelta(microseconds=1)).event_id != base
    assert steam(metric="chart_rank").event_id != base
    assert steam(entity_id="731").event_id != base


def test_08_timezone_is_normalised_to_utc() -> None:
    local: datetime = datetime(2026, 11, 12, 17, 0, tzinfo=timezone(timedelta(hours=7)))
    obs: Observation = steam(observed_at=local)
    assert obs.observed_at == T10
    assert obs.observed_at.utcoffset() == timedelta(0)
    assert obs.event_id == steam(observed_at=T10).event_id


def test_09_naive_times_rejected() -> None:
    with pytest.raises(ValidationError):
        steam(observed_at=datetime(2026, 11, 12, 10, 0))
    with pytest.raises(ValidationError):
        steam(window_start=datetime(2026, 11, 12, 9, 0))


@pytest.mark.parametrize("start", [T11, T11 + timedelta(seconds=1)])
def test_10_window_start_not_before_observed_at_rejected(start: datetime) -> None:
    with pytest.raises(ValidationError):
        steam(window_start=start, observed_at=T11)


def test_10_window_start_an_hour_earlier_passes() -> None:
    obs: Observation = steam(window_start=T10, observed_at=T11)
    assert obs.window_start == T10


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_11_non_finite_value_rejected(bad: float) -> None:
    with pytest.raises(ValidationError):
        steam(value=bad)


@pytest.mark.parametrize("ok", [0, -5, 638896])
def test_11_finite_value_passes(ok: float) -> None:
    obs: Observation = steam(value=ok)
    assert obs.value == ok
    assert math.isfinite(obs.value)


@pytest.mark.parametrize("field", ["source", "entity_type", "metric"])
@pytest.mark.parametrize("bad", BAD_NAMES)
def test_12_bad_name_rejected(field: str, bad: str) -> None:
    with pytest.raises(ValidationError):
        with_name(field, bad)


@pytest.mark.parametrize("field", ["source", "entity_type", "metric"])
def test_12_good_name_passes(field: str) -> None:
    obs: Observation = with_name(field, "app_store")
    assert getattr(obs, field) == "app_store"


def test_12_name_of_32_chars_passes() -> None:
    assert steam(source="a" * 32).source == "a" * 32


@pytest.mark.parametrize("bad", BAD_NAMES)
def test_12_erasure_names_validated(bad: str) -> None:
    with pytest.raises(ValidationError):
        erasure(source=bad)
    with pytest.raises(ValidationError):
        erasure(entity_type=bad)


@pytest.mark.parametrize("bad", ["", "a" * 1025])
def test_13_bad_entity_id_rejected(bad: str) -> None:
    with pytest.raises(ValidationError):
        steam(entity_id=bad)


@pytest.mark.parametrize("ok", ["a" * 1024, "a|b"])
def test_13_good_entity_id_passes(ok: str) -> None:
    assert steam(entity_id=ok).entity_id == ok


def test_14_model_is_frozen() -> None:
    obs: Observation = steam()
    with pytest.raises(ValidationError):
        obs.value = 1.0  # type: ignore[misc]  # ty: ignore[invalid-assignment]


def test_14_erasure_is_frozen() -> None:
    er: Erasure = erasure()
    with pytest.raises(ValidationError):
        er.entity_id = "x"  # type: ignore[misc]  # ty: ignore[invalid-assignment]


def test_15_erasure_event_id() -> None:
    er: Erasure = erasure()
    assert er.kind == "erasure"
    assert er.event_id == make_event_id(
        "erasure", "bluesky", "post", "at://did:plc:abc/app.bsky.feed.post/3m5xyz"
    )
    assert erasure(requested_at=T11 + timedelta(days=1)).event_id == er.event_id


def test_15_naive_requested_at_rejected() -> None:
    with pytest.raises(ValidationError):
        erasure(requested_at=datetime(2026, 11, 12, 12, 0))


def test_15_requested_at_normalised_to_utc() -> None:
    er: Erasure = erasure(requested_at=datetime(2026, 11, 12, 18, 0, tzinfo=timezone(timedelta(hours=7))))
    assert er.requested_at == T11
    assert er.requested_at.utcoffset() == timedelta(0)


def test_16_erases_matches_on_full_entity() -> None:
    obs: Observation = Observation.create(
        source="bluesky",
        entity_type="post",
        entity_id="p1",
        metric="likes",
        value=1,
        observed_at=T11,
    )

    def er(**over: str) -> Erasure:
        args: dict[str, str] = {"source": "bluesky", "entity_type": "post", "entity_id": "p1"}
        args.update(over)
        return Erasure.create(
            source=args["source"],
            entity_type=args["entity_type"],
            entity_id=args["entity_id"],
            requested_at=T11,
        )

    assert erases(er(), obs) is True
    assert erases(er(source="steam"), obs) is False
    assert erases(er(entity_type="tag"), obs) is False
    assert erases(er(entity_id="p2"), obs) is False
