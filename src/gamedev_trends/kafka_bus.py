"""Kafka implementation of the bus on confluent-kafka."""

import asyncio
import logging
import time
from collections.abc import Sequence

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from confluent_kafka.aio import AIOProducer

from gamedev_trends.bus import PublishError, decode, encode, partition_key, topic_for
from gamedev_trends.contract import Event

logger: logging.Logger = logging.getLogger(__name__)


class KafkaPublisher:
    def __init__(self, bootstrap_servers: str, delivery_timeout: float = 30.0) -> None:
        """Create inside a running event loop (AIOProducer requires it)."""
        self._producer: AIOProducer = AIOProducer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "message.timeout.ms": int(delivery_timeout * 1000),
            }
        )
        self._closed: bool = False

    async def publish(self, events: Sequence[Event]) -> None:
        if self._closed:
            raise RuntimeError("publisher is closed")
        if not events:
            return
        futures: list[asyncio.Future[object]] = []
        failed: int = 0
        event: Event
        for event in events:
            try:
                futures.append(
                    await self._producer.produce(
                        topic_for(event), value=encode(event), key=partition_key(event)
                    )
                )
            except KafkaException, BufferError:
                failed += 1
        try:
            await self._producer.flush()
        except KafkaException:
            pass
        results: list[object] = await asyncio.gather(*futures, return_exceptions=True)
        failed += sum(1 for r in results if isinstance(r, BaseException))
        if failed:
            raise PublishError(f"{failed} of {len(events)} events were not delivered")

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._producer.close()


class KafkaSubscriber:
    def __init__(self, bootstrap_servers: str, group: str, topics: Sequence[str]) -> None:
        self._consumer: Consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self._consumer.subscribe(list(topics))
        self._closed: bool = False

    def _convert(self, message: Message) -> Event | None:
        error: KafkaError | None = message.error()
        if error is not None:
            if error.fatal():
                raise KafkaException(error)
            logger.warning("consumer error: %s", error)
            return None
        value: bytes | None = message.value()
        try:
            return decode(value if value is not None else b"")
        except ValueError:
            logger.warning(
                "skipping undecodable record: topic=%s partition=%s offset=%s",
                message.topic(),
                message.partition(),
                message.offset(),
            )
            return None

    def _poll_blocking(self, max_events: int, timeout: float) -> list[Event]:
        events: list[Event] = []
        first: Message | None = self._consumer.poll(timeout)
        if first is None:
            return events
        messages: list[Message] = [first]
        if max_events > 1:
            messages += self._consumer.consume(max_events - 1, 0)
        message: Message
        for message in messages:
            event: Event | None = self._convert(message)
            if event is not None:
                events.append(event)
        return events

    async def poll(self, max_events: int = 500, timeout: float = 1.0) -> list[Event]:
        if self._closed:
            raise RuntimeError("subscriber is closed")
        deadline: float = time.monotonic() + timeout
        while True:
            remaining: float = max(deadline - time.monotonic(), 0.0)
            events: list[Event] = await asyncio.to_thread(self._poll_blocking, max_events, remaining)
            if events or time.monotonic() >= deadline:
                return events

    async def commit(self) -> None:
        if self._closed:
            raise RuntimeError("subscriber is closed")
        try:
            await asyncio.to_thread(self._consumer.commit, asynchronous=False)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError._NO_OFFSET:
                raise

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await asyncio.to_thread(self._consumer.close)
