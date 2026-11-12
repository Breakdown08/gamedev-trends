"""Spec checks 4-9 and the Redis-script contract without a server: in-memory token bucket."""

from typing import cast
from unittest.mock import MagicMock

import pytest
import redis
import redis.asyncio
from steam_support import FakeMonotonic, FakeSleep, ScriptLimiter

from gamedev_trends.ratelimit import (
    InMemoryTokenBucket,
    RateLimiter,
    RateLimiterError,
    RedisTokenBucket,
    acquire,
)


async def test_04_bucket_drains_and_refills() -> None:
    clock: FakeMonotonic = FakeMonotonic()
    bucket: InMemoryTokenBucket = InMemoryTokenBucket(capacity=2, refill_per_second=1, monotonic=clock)
    assert await bucket.try_acquire() == 0.0
    assert await bucket.try_acquire() == 0.0
    assert await bucket.try_acquire() == pytest.approx(1.0)
    clock.t = 0.5
    assert await bucket.try_acquire() == pytest.approx(0.5)
    clock.t = 1.0
    assert await bucket.try_acquire() == 0.0


async def test_05_refusal_does_not_spend_a_token() -> None:
    clock: FakeMonotonic = FakeMonotonic()
    bucket: InMemoryTokenBucket = InMemoryTokenBucket(capacity=2, refill_per_second=1, monotonic=clock)
    await bucket.try_acquire()
    await bucket.try_acquire()
    assert await bucket.try_acquire() == pytest.approx(1.0)
    assert await bucket.try_acquire() == pytest.approx(1.0)


async def test_06_bucket_does_not_overflow() -> None:
    clock: FakeMonotonic = FakeMonotonic()
    bucket: InMemoryTokenBucket = InMemoryTokenBucket(capacity=2, refill_per_second=1, monotonic=clock)
    clock.t = 100.0
    assert await bucket.try_acquire() == 0.0
    assert await bucket.try_acquire() == 0.0
    assert await bucket.try_acquire() > 0


@pytest.mark.parametrize(("capacity", "refill"), [(0.5, 1.0), (2.0, 0.0), (0.0, 1.0), (2.0, -1.0)])
def test_07_invalid_parameters_raise_value_error(capacity: float, refill: float) -> None:
    with pytest.raises(ValueError):
        InMemoryTokenBucket(capacity=capacity, refill_per_second=refill)
    with pytest.raises(ValueError):
        RedisTokenBucket(MagicMock(), "k", capacity=capacity, refill_per_second=refill)


async def test_08_acquire_sleeps_until_token_taken() -> None:
    limiter: ScriptLimiter = ScriptLimiter(0.3, 0.2, 0.0)
    sleep: FakeSleep = FakeSleep()
    await acquire(limiter, sleep)
    assert sleep.calls == [0.3, 0.2]
    assert limiter.calls == 3


async def test_09_in_memory_bucket_satisfies_protocol() -> None:
    limiter: RateLimiter = InMemoryTokenBucket(capacity=1, refill_per_second=1)
    assert await limiter.try_acquire() == 0.0


def _redis_bucket_is_a_rate_limiter(client: redis.asyncio.Redis) -> None:
    """Never called: mypy and ty check that RedisTokenBucket satisfies RateLimiter."""
    limiter: RateLimiter = RedisTokenBucket(client, "k", capacity=1, refill_per_second=1)
    assert limiter is not None


class FakeScriptRedis:
    """Stand-in for the Redis client: register_script returns a callable giving a scripted result."""

    def __init__(self, result: object) -> None:
        self.result: object = result

    def register_script(self, script: str) -> FakeScriptRedis:
        return self

    async def __call__(self, *args: object, **kwargs: object) -> object:
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def fake(result: object) -> redis.asyncio.Redis:
    return cast(redis.asyncio.Redis, FakeScriptRedis(result))


@pytest.mark.parametrize(("raw", "expected"), [("0", 0.0), (b"0", 0.0), ("0.25", 0.25), (b"0.05", 0.05)])
async def test_redis_bucket_parses_script_string_result(raw: object, expected: float) -> None:
    bucket: RedisTokenBucket = RedisTokenBucket(
        fake(raw),
        "k",
        capacity=1,
        refill_per_second=1,
    )
    assert await bucket.try_acquire() == pytest.approx(expected)


@pytest.mark.parametrize("raw", [None, 5, [1], {"a": 1}])
async def test_redis_bucket_garbage_result_is_limiter_error(raw: object) -> None:
    bucket: RedisTokenBucket = RedisTokenBucket(
        fake(raw),
        "k",
        capacity=1,
        refill_per_second=1,
    )
    with pytest.raises(RateLimiterError):
        await bucket.try_acquire()


@pytest.mark.parametrize(
    "error", [redis.ConnectionError("down"), redis.TimeoutError("slow"), redis.RedisError("other")]
)
async def test_redis_bucket_redis_error_becomes_limiter_error(error: redis.RedisError) -> None:
    bucket: RedisTokenBucket = RedisTokenBucket(
        fake(error),
        "k",
        capacity=1,
        refill_per_second=1,
    )
    info: pytest.ExceptionInfo[RateLimiterError]
    with pytest.raises(RateLimiterError) as info:
        await bucket.try_acquire()
    assert info.value.__cause__ is error
