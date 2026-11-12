"""Fixtures for the store integration tests: stack readiness and a throw-away database."""

import asyncio
import os
import time
import uuid
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin._metadata import ClusterMetadata, TopicMetadata

from gamedev_trends.bus import OBSERVATIONS_TOPIC
from gamedev_trends.clickhouse import ClickHouseClient, ClickHouseError

ROOT: Path = Path(__file__).resolve().parents[1]
BOOTSTRAP: str = "localhost:9092"
CLICKHOUSE_URL: str = "http://localhost:8123"
STEP_SECONDS: float = 60.0
HINT: str = "docker compose up -d --wait"


def clickhouse_password() -> str:
    value: str | None = os.environ.get("CLICKHOUSE_PASSWORD")
    if value:
        return value
    env_file: Path = ROOT / ".env"
    if env_file.exists():
        line: str
        for line in env_file.read_text().splitlines():
            if line.startswith("CLICKHOUSE_PASSWORD="):
                return line.split("=", 1)[1].strip()
    pytest.fail("CLICKHOUSE_PASSWORD: cp .env.example .env")


def new_client(http: httpx.AsyncClient) -> ClickHouseClient:
    return ClickHouseClient(http, url=CLICKHOUSE_URL, user="radar", password=clickhouse_password())


def poll_until(step: str, probe: Callable[[], bool]) -> None:
    deadline: float = time.monotonic() + STEP_SECONDS
    while time.monotonic() < deadline:
        try:
            if probe():
                return
        except Exception:
            pass
        time.sleep(0.5)
    pytest.fail(f"stack not ready: {step} (60 s). Try: {HINT}")


def kafka_ready() -> bool:
    meta: ClusterMetadata = AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(timeout=5)
    topic: TopicMetadata | None = meta.topics.get(OBSERVATIONS_TOPIC)
    if topic is None or topic.error is not None or len(topic.partitions) != 6:
        return False
    return all(part.leader >= 0 and part.error is None for part in topic.partitions.values())


def warm_up_group_coordinator() -> bool:
    consumer: Consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"it-warmup-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    try:
        consumer.subscribe([OBSERVATIONS_TOPIC])
        deadline: float = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            consumer.poll(0.5)
            if consumer.assignment():
                return True
        return False
    finally:
        consumer.close()


def wait_for_stack() -> None:
    def clickhouse_ready() -> bool:
        async def probe() -> bool:
            http: httpx.AsyncClient
            async with httpx.AsyncClient(timeout=5.0) as http:
                client: ClickHouseClient = new_client(http)
                return await client.ping() and await client.select("SELECT 1 AS x") == [{"x": 1}]

        try:
            return asyncio.run(probe())
        except ClickHouseError:
            return False

    poll_until("ClickHouse on localhost:8123", clickhouse_ready)
    poll_until(
        "Kafka on localhost:9092 (topic radar.observations.v1, 6 partitions with leaders)", kafka_ready
    )
    poll_until("Kafka group coordinator", warm_up_group_coordinator)
