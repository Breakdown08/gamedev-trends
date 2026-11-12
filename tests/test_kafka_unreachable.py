"""Spec check 33: no broker listening."""

import time
from datetime import UTC, datetime

import pytest

from gamedev_trends.bus import PublishError
from gamedev_trends.contract import Observation
from gamedev_trends.kafka_bus import KafkaPublisher


async def test_33_publish_without_broker_raises_publish_error() -> None:
    obs: Observation = Observation.create(
        source="steam",
        entity_type="game",
        entity_id="730",
        metric="players_current",
        value=1,
        observed_at=datetime(2026, 11, 12, 10, 0, tzinfo=UTC),
    )
    pub: KafkaPublisher = KafkaPublisher("127.0.0.1:1", delivery_timeout=2.0)
    start: float = time.monotonic()
    try:
        with pytest.raises(PublishError):
            await pub.publish([obs])
    finally:
        await pub.close()
    assert time.monotonic() - start <= 15.0
