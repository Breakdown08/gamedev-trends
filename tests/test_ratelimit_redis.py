"""Spec checks 40-43: Redis token bucket. 40-42 need Redis on localhost:6379 (marker integration)."""

import asyncio
import math
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any, cast

import pytest
import redis.asyncio

from gamedev_trends.ratelimit import RateLimiterError, RedisTokenBucket

REDIS_URL: str = "redis://localhost:6379/0"


@pytest.fixture
async def client() -> AsyncIterator[redis.asyncio.Redis]:
    r: redis.asyncio.Redis = redis.asyncio.Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        yield r
    finally:
        await r.aclose()


@pytest.fixture
async def key(client: redis.asyncio.Redis) -> AsyncIterator[str]:
    k: str = f"it-ratelimit-{uuid.uuid4()}"
    try:
        yield k
    finally:
        await cast(Any, client.delete(k))


@pytest.mark.integration
async def test_40_three_tokens_then_wait_then_one_more(client: redis.asyncio.Redis, key: str) -> None:
    bucket: RedisTokenBucket = RedisTokenBucket(client, key, capacity=3, refill_per_second=10)
    assert [await bucket.try_acquire() for _ in range(3)] == [0.0, 0.0, 0.0]
    wait: float = await bucket.try_acquire()
    assert 0 < wait <= 0.1
    await asyncio.sleep(wait + 0.02)
    assert await bucket.try_acquire() == 0.0


@pytest.mark.integration
async def test_41_atomic_under_concurrency(client: redis.asyncio.Redis, key: str) -> None:
    bucket: RedisTokenBucket = RedisTokenBucket(client, key, capacity=5, refill_per_second=0.001)
    results: list[float] = await asyncio.gather(*[bucket.try_acquire() for _ in range(20)])
    assert results.count(0.0) == 5


@pytest.mark.integration
async def test_42_ttl_and_hash_fields(client: redis.asyncio.Redis, key: str) -> None:
    bucket: RedisTokenBucket = RedisTokenBucket(client, key, capacity=5, refill_per_second=0.001)
    await bucket.try_acquire()
    ttl: int = await cast(Any, client.ttl(key))
    assert 0 < ttl <= math.ceil(5 / 0.001) + 60
    fields: dict[str, str] = await cast(Any, client.hgetall(key))
    assert "tokens" in fields
    assert "ts" in fields


async def test_43_unreachable_redis_raises_limiter_error() -> None:
    r: redis.asyncio.Redis = redis.asyncio.Redis.from_url("redis://127.0.0.1:1/0")
    bucket: RedisTokenBucket = RedisTokenBucket(r, f"it-ratelimit-{uuid.uuid4()}", 1, 1)
    start: float = time.monotonic()
    try:
        info: pytest.ExceptionInfo[RateLimiterError]
        with pytest.raises(RateLimiterError) as info:
            await bucket.try_acquire()
    finally:
        await r.aclose()
    assert isinstance(info.value.__cause__, redis.RedisError)
    assert time.monotonic() - start <= 10.0
