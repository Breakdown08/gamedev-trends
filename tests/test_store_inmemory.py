"""Spec checks 23-25: at-least-once on the in-memory bus."""

import asyncio

from store_support import FakeSink, FakeSleep, obs, wait_until

from gamedev_trends.bus import OBSERVATIONS_TOPIC, InMemoryBroker, InMemoryPublisher, InMemorySubscriber
from gamedev_trends.contract import Event
from gamedev_trends.store import StoreConsumer


def subscriber(broker: InMemoryBroker) -> InMemorySubscriber:
    return InMemorySubscriber(broker, "store", [OBSERVATIONS_TOPIC])


async def test_23_24_crash_before_write_then_recover() -> None:
    broker: InMemoryBroker = InMemoryBroker()
    await InMemoryPublisher(broker).publish([obs(i) for i in range(5)])

    failing: FakeSink = FakeSink(failures=-1)
    first: InMemorySubscriber = subscriber(broker)
    consumer: StoreConsumer = StoreConsumer(first, failing, max_seconds=0.05, sleep=FakeSleep())
    task: asyncio.Task[None] = asyncio.create_task(consumer.run())
    await wait_until(lambda: len(failing.calls) >= 2, limit=100000)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await first.close()

    probe: InMemorySubscriber = subscriber(broker)
    seen: list[Event] = await probe.poll(timeout=0.05)
    assert seen == [obs(i) for i in range(5)]
    await probe.close()

    good: FakeSink = FakeSink()
    second: InMemorySubscriber = subscriber(broker)
    await StoreConsumer(second, good, max_seconds=0.05).run(max_batches=1)
    assert len(good.calls) == 1
    assert len(good.calls[0]) == 5
    await second.close()

    third: InMemorySubscriber = subscriber(broker)
    assert await third.poll(timeout=0.05) == []
    await third.close()
    await InMemoryPublisher(broker).publish([obs(5)])
    fourth: InMemorySubscriber = subscriber(broker)
    assert await fourth.poll(timeout=0.05) == [obs(5)]


async def test_25_redelivery_reaches_sink_as_is() -> None:
    broker: InMemoryBroker = InMemoryBroker()
    await InMemoryPublisher(broker).publish([obs(0), obs(0)])
    sink: FakeSink = FakeSink()
    await StoreConsumer(subscriber(broker), sink, max_seconds=0.05).run(max_batches=1)
    assert len(sink.calls[0]) == 2
    assert sink.calls[0][0]["event_id"] == sink.calls[0][1]["event_id"]
