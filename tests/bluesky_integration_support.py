"""Helpers for the Bluesky integration tests: Kafka group positions, a recording publisher, pumping."""

import asyncio
import time
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin._metadata import ClusterMetadata
from store_integration_support import BOOTSTRAP

from gamedev_trends.bus import Publisher
from gamedev_trends.clickhouse import ClickHouseClient, sql_string
from gamedev_trends.contract import Event
from gamedev_trends.store import BatchReport, StoreConsumer

DEADLINE_SECONDS: float = 60.0


def pin_group_to_end(group: str, topics: Sequence[str]) -> None:
    """Commit the group's offsets at the high watermark of every partition: it will not read history."""
    consumer: Consumer = Consumer(
        {"bootstrap.servers": BOOTSTRAP, "group.id": group, "enable.auto.commit": False}
    )
    try:
        offsets: list[TopicPartition] = []
        topic: str
        for topic in topics:
            metadata: ClusterMetadata = consumer.list_topics(topic, timeout=10)
            partition: int
            for partition in metadata.topics[topic].partitions:
                low: int
                high: int
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
                offsets.append(TopicPartition(topic, partition, high))
        consumer.commit(offsets=offsets, asynchronous=False)
    finally:
        consumer.close()


class TeePublisher:
    """Publishes through `inner` and remembers every event handed to it."""

    def __init__(self, inner: Publisher) -> None:
        self.inner: Publisher = inner
        self.events: list[Event] = []

    async def publish(self, events: Sequence[Event]) -> None:
        await self.inner.publish(events)
        self.events.extend(events)

    async def close(self) -> None:
        await self.inner.close()


async def pump_until(consumer: StoreConsumer, wanted: set[str]) -> list[BatchReport]:
    """next_batch and flush until every wanted event_id has been seen, within 60 s."""
    deadline: float = time.monotonic() + DEADLINE_SECONDS
    seen: set[str] = set()
    reports: list[BatchReport] = []
    while not wanted <= seen:
        left: float = deadline - time.monotonic()
        assert left > 0, f"missing {len(wanted - seen)} of {len(wanted)} events"
        batch: list[Event] = await asyncio.wait_for(consumer.next_batch(), left)
        seen |= {str(e.event_id) for e in batch}
        reports.append(await consumer.flush(batch))
    return reports


def _us_literal(time_us: int) -> str:
    moment: datetime = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=time_us)
    return f"toDateTime64('{moment.strftime('%Y-%m-%d %H:%M:%S.%f')}', 6, 'UTC')"


async def purge_radar(
    client: ClickHouseClient, entity_ids: Sequence[str], base_us: int, database: str = "radar"
) -> None:
    """Remove what a synthetic run left in the shared database: its posts, its erasures, its stream windows.

    The running store service ingests the test's events into `radar` too. Waits (up to 30 s, best effort)
    until the last erasure has arrived, then deletes synchronously, observations first so that a refresh
    of the hourly view cannot bring the rows back.
    """
    ids: str = ", ".join(sql_string(e) for e in entity_ids)
    deadline: float = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        seen: list[dict[str, object]] = await client.select(
            f"SELECT count() AS n FROM {database}.erasures WHERE entity_id IN ({ids})"
        )
        if int(str(seen[0]["n"])) >= 1:
            break
        await asyncio.sleep(1.0)
    start: str = _us_literal(base_us)
    end: str = _us_literal(base_us + 3600 * 10**6)
    await client.execute(
        f"DELETE FROM {database}.observations WHERE source = 'bluesky' AND (entity_id IN ({ids}) "
        f"OR (entity_type = 'stream' AND entity_id = 'jetstream' AND window_start >= {start} "
        f"AND window_start < {end}))"
    )
    await client.execute(
        f"DELETE FROM {database}.observations_hourly WHERE source = 'bluesky' AND (entity_id IN ({ids}) "
        f"OR (entity_type = 'stream' AND entity_id = 'jetstream' AND hour >= {start} AND hour < {end}))"
    )
    await client.execute(f"DELETE FROM {database}.erasures WHERE source = 'bluesky' AND entity_id IN ({ids})")
    exists: list[dict[str, object]] = await client.select(
        f"SELECT count() AS e FROM system.tables WHERE database = '{database}' AND name = 'excerpts'"
    )
    if int(str(exists[0]["e"])) == 1:
        await client.execute(
            f"DELETE FROM {database}.excerpts WHERE source = 'bluesky' AND entity_id IN ({ids})"
        )
