"""Spec checks 17-22: encoding, decoding, topics, keys."""

import json
from collections.abc import Callable
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from gamedev_trends.bus import decode, encode, partition_key, topic_for
from gamedev_trends.contract import Erasure, Event, Observation

T10: datetime = datetime(2026, 11, 12, 10, 0, tzinfo=UTC)
T11: datetime = datetime(2026, 11, 12, 11, 0, tzinfo=UTC)
POST: str = "at://did:plc:abc/app.bsky.feed.post/3m5xyz"


def steam_obs(metric: str = "players_current", observed_at: datetime = T10) -> Observation:
    return Observation.create(
        source="steam",
        entity_type="game",
        entity_id="730",
        metric=metric,
        value=638896,
        observed_at=observed_at,
    )


def bsky_obs() -> Observation:
    return Observation.create(
        source="bluesky",
        entity_type="post",
        entity_id=POST,
        metric="likes",
        value=42,
        window_start=T10,
        observed_at=T11,
    )


def post_erasure() -> Erasure:
    return Erasure.create(source="bluesky", entity_type="post", entity_id=POST, requested_at=T11)


@pytest.mark.parametrize("make", [steam_obs, bsky_obs, post_erasure])
def test_17_roundtrip(make: Callable[[], Event]) -> None:
    event: Event = make()
    back: Event = decode(encode(event))
    assert back == event
    assert type(back) is type(event)


def test_18_observation_json_keys() -> None:
    data: dict[str, object] = json.loads(encode(steam_obs()))
    assert list(data) == [
        "kind",
        "event_id",
        "source",
        "entity_type",
        "entity_id",
        "metric",
        "value",
        "observed_at",
        "window_start",
    ]
    assert data["kind"] == "observation"
    assert data["window_start"] is None


def test_18_erasure_json_keys() -> None:
    data: dict[str, object] = json.loads(encode(post_erasure()))
    assert list(data) == ["kind", "event_id", "source", "entity_type", "entity_id", "requested_at"]
    assert data["kind"] == "erasure"


def test_19_extra_field_ignored() -> None:
    data: dict[str, object] = json.loads(encode(steam_obs()))
    data["extra"] = 1
    back: Event = decode(json.dumps(data).encode())
    assert isinstance(back, Observation)
    assert back == steam_obs()


def test_20_garbage_rejected() -> None:
    with pytest.raises(ValidationError):
        decode(b"not json")
    with pytest.raises(ValidationError):
        decode(b'{"kind":"unknown"}')


def test_20_observation_without_event_id_rejected() -> None:
    data: dict[str, object] = json.loads(encode(steam_obs()))
    del data["event_id"]
    with pytest.raises(ValidationError):
        decode(json.dumps(data).encode())


def test_21_topics() -> None:
    assert topic_for(steam_obs()) == "radar.observations.v1"
    assert topic_for(post_erasure()) == "radar.erasures.v1"


def test_22_partition_key() -> None:
    assert partition_key(steam_obs()) == b"steam|game|730"
    assert partition_key(steam_obs("chart_rank", T11)) == partition_key(steam_obs())
    er: Erasure = Erasure.create(source="steam", entity_type="game", entity_id="730", requested_at=T11)
    assert partition_key(er) == b"steam|game|730"
