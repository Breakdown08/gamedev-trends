"""Token bucket rate limiter: in-memory and Redis (atomic Lua script) implementations."""

import math
import time
from collections.abc import Awaitable, Callable
from typing import Protocol

import redis
import redis.asyncio
from redis.commands.core import AsyncScript

type Sleep = Callable[[float], Awaitable[None]]


class RateLimiterError(Exception):
    """The limiter storage is unavailable or answered garbage."""


class RateLimiter(Protocol):
    async def try_acquire(self) -> float:
        """Take one token. 0.0: taken. > 0: none available, nothing taken; seconds until one appears.

        Storage failure raises RateLimiterError.
        """
        ...


async def acquire(limiter: RateLimiter, sleep: Sleep) -> None:
    """Retry try_acquire, sleeping the returned time, until a token is taken."""
    wait: float = await limiter.try_acquire()
    while wait > 0:
        await sleep(wait)
        wait = await limiter.try_acquire()


def _check(capacity: float, refill_per_second: float) -> None:
    if not capacity >= 1:
        raise ValueError(f"capacity must be >= 1, got {capacity}")
    if not refill_per_second > 0:
        raise ValueError(f"refill_per_second must be > 0, got {refill_per_second}")


class InMemoryTokenBucket:
    def __init__(
        self, capacity: float, refill_per_second: float, monotonic: Callable[[], float] = time.monotonic
    ) -> None:
        _check(capacity, refill_per_second)
        self._capacity: float = capacity
        self._rate: float = refill_per_second
        self._monotonic: Callable[[], float] = monotonic
        self._tokens: float = capacity
        self._ts: float | None = None

    async def try_acquire(self) -> float:
        now: float = self._monotonic()
        ts: float = now if self._ts is None else self._ts
        tokens: float = min(self._capacity, self._tokens + max(0.0, now - ts) * self._rate)
        wait: float = 0.0
        if tokens >= 1:
            tokens -= 1
        else:
            wait = (1 - tokens) / self._rate
        self._tokens = tokens
        self._ts = now
        return wait


_SCRIPT: str = """
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end
tokens = math.min(capacity, tokens + math.max(0, now - ts) * rate)
local wait = 0
if tokens >= 1 then
  tokens = tokens - 1
else
  wait = (1 - tokens) / rate
end
redis.call('HSET', KEYS[1], 'tokens', string.format('%.9f', tokens), 'ts', string.format('%.6f', now))
redis.call('EXPIRE', KEYS[1], ttl)
return tostring(wait)
"""


class RedisTokenBucket:
    def __init__(
        self, redis: redis.asyncio.Redis, key: str, capacity: float, refill_per_second: float
    ) -> None:
        _check(capacity, refill_per_second)
        self._key: str = key
        self._capacity: float = capacity
        self._rate: float = refill_per_second
        self._ttl: int = math.ceil(capacity / refill_per_second) + 60
        self._script: AsyncScript = redis.register_script(_SCRIPT)

    async def try_acquire(self) -> float:
        raw: object
        try:
            raw = await self._script(keys=[self._key], args=[self._capacity, self._rate, self._ttl])
        except redis.RedisError as exc:
            raise RateLimiterError(f"redis token bucket failed: {exc!r}") from exc
        if not isinstance(raw, bytes | str):
            raise RateLimiterError(f"unexpected script result: {raw!r}")
        try:
            wait: float = float(raw)
        except ValueError as exc:
            raise RateLimiterError(f"unexpected script result: {raw!r}") from exc
        if not math.isfinite(wait) or wait < 0:
            raise RateLimiterError(f"unexpected script result: {raw!r}")
        return wait
