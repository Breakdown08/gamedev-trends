"""Spec checks 9-13: next_batch."""

import pytest
from store_support import Clock, FakeSink, FakeSubscriber, Step, obs

from gamedev_trends.store import StoreConsumer


def consumer(
    sub: FakeSubscriber,
    clock: Clock,
    *,
    max_events: int = 5000,
    max_seconds: float = 5.0,
    idle_timeout: float = 1.0,
) -> StoreConsumer:
    return StoreConsumer(
        sub,
        FakeSink(),
        max_events=max_events,
        max_seconds=max_seconds,
        idle_timeout=idle_timeout,
        monotonic=clock,
    )


async def test_09_batch_closes_by_size() -> None:
    clock: Clock = Clock()
    sub: FakeSubscriber = FakeSubscriber([Step([obs(0), obs(1)]), Step([obs(2), obs(3)])], clock)
    c: StoreConsumer = consumer(sub, clock, max_events=4, max_seconds=5.0, idle_timeout=2.0)
    assert await c.next_batch() == [obs(0), obs(1), obs(2), obs(3)]
    assert [(p.max_events, p.timeout) for p in sub.polls] == [(4, 2.0), (2, 5.0)]


async def test_10_batch_closes_by_time() -> None:
    clock: Clock = Clock()
    sub: FakeSubscriber = FakeSubscriber([Step([obs(0)]), Step([])], clock)
    c: StoreConsumer = consumer(sub, clock, max_events=100, max_seconds=5.0)
    assert await c.next_batch() == [obs(0)]
    assert len(sub.polls) == 2
    assert sub.polls[1].timeout == 5.0


async def test_11_waits_for_first_event_then_starts_timer() -> None:
    clock: Clock = Clock()
    sub: FakeSubscriber = FakeSubscriber([Step([]), Step([]), Step([obs(0)]), Step([])], clock)
    c: StoreConsumer = consumer(sub, clock, max_events=100, max_seconds=5.0, idle_timeout=2.0)
    assert await c.next_batch() == [obs(0)]
    assert [(p.max_events, p.timeout) for p in sub.polls] == [(100, 2.0), (100, 2.0), (100, 2.0), (99, 5.0)]


async def test_12_partial_time_left() -> None:
    clock: Clock = Clock()
    sub: FakeSubscriber = FakeSubscriber([Step([obs(0)]), Step([obs(1)], advance=2.0), Step([])], clock)
    c: StoreConsumer = consumer(sub, clock, max_events=100, max_seconds=5.0)
    assert await c.next_batch() == [obs(0), obs(1)]
    assert [p.timeout for p in sub.polls] == [1.0, 5.0, 3.0]
    assert [p.max_events for p in sub.polls] == [100, 99, 98]


@pytest.mark.parametrize(
    ("max_events", "max_seconds", "idle_timeout", "retry_base", "retry_max"),
    [
        (0, 5.0, 1.0, 1.0, 30.0),
        (10, 0.0, 1.0, 1.0, 30.0),
        (10, 5.0, 0.0, 1.0, 30.0),
        (10, 5.0, 1.0, 0.0, 30.0),
        (10, 5.0, 1.0, 1.0, 0.5),
    ],
)
def test_13_constructor_validation(
    max_events: int, max_seconds: float, idle_timeout: float, retry_base: float, retry_max: float
) -> None:
    with pytest.raises(ValueError):
        StoreConsumer(
            FakeSubscriber([]),
            FakeSink(),
            max_events=max_events,
            max_seconds=max_seconds,
            idle_timeout=idle_timeout,
            retry_base=retry_base,
            retry_max=retry_max,
        )
