"""Shared fakes for the store tests: scripted subscriber, sink, clock, sleep, HTTP client."""

import asyncio
import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import httpx

from gamedev_trends.clickhouse import ClickHouseClient
from gamedev_trends.contract import Erasure, Event, Observation
from gamedev_trends.store import EntityKey, Row, SinkError

T10: datetime = datetime(2026, 11, 12, 10, 0, tzinfo=UTC)
NOW: datetime = datetime(2026, 11, 12, 10, 0, 7, tzinfo=UTC)


def obs(i: int) -> Observation:
    return Observation.create(
        source="steam",
        entity_type="game",
        entity_id="730",
        metric="players_current",
        value=i,
        observed_at=T10 + timedelta(minutes=i),
    )


def erasure() -> Erasure:
    return Erasure.create(source="steam", entity_type="game", entity_id="730", requested_at=T10)


class Clock:
    def __init__(self) -> None:
        self.t: float = 100.0

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@dataclass
class PollCall:
    max_events: int
    timeout: float


@dataclass
class Step:
    """One scripted poll answer. advance=None on an empty answer means "advance by the timeout"."""

    events: list[Event]
    advance: float | None = None


class FakeSubscriber:
    def __init__(
        self, steps: Sequence[Step], clock: Clock | None = None, journal: list[str] | None = None
    ) -> None:
        self.steps: list[Step] = list(steps)
        self.clock: Clock = clock if clock is not None else Clock()
        self.journal: list[str] = journal if journal is not None else []
        self.polls: list[PollCall] = []
        self.commits: int = 0
        self.commit_error: Exception | None = None

    async def poll(self, max_events: int = 500, timeout: float = 1.0) -> list[Event]:
        self.polls.append(PollCall(max_events, timeout))
        if len(self.polls) > len(self.steps):
            raise AssertionError(f"unexpected poll #{len(self.polls)}")
        step: Step = self.steps[len(self.polls) - 1]
        advance: float = step.advance if step.advance is not None else (0.0 if step.events else timeout)
        self.clock.advance(advance)
        return list(step.events)

    async def commit(self) -> None:
        self.commits += 1
        self.journal.append("commit")
        if self.commit_error is not None:
            raise self.commit_error

    async def close(self) -> None:
        return None


class FakeSink:
    """Fails with `failures` SinkErrors (or `error` if given), then accepts. failures=-1: always fails."""

    def __init__(
        self, failures: int = 0, journal: list[str] | None = None, error: Exception | None = None
    ) -> None:
        self.failures: int = failures
        self.error: Exception | None = error
        self.journal: list[str] = journal if journal is not None else []
        self.calls: list[list[Row]] = []

    async def write(self, rows: Sequence[Row]) -> None:
        self.calls.append([dict(r) for r in rows])
        self.journal.append("write")
        if self.error is not None:
            raise self.error
        if self.failures < 0 or len(self.calls) <= self.failures:
            raise SinkError("down")


class FakeSleep:
    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
        await asyncio.sleep(0)


async def wait_until(predicate: Callable[[], bool], limit: int = 1000) -> None:
    spins: int = 0
    while spins < limit:
        if predicate():
            return
        spins += 1
        await asyncio.sleep(0)
    raise AssertionError("condition not reached")


@dataclass
class Recorder:
    requests: list[httpx.Request] = field(default_factory=list)


def make_client(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    url: str = "http://ch.test:8123",
    recorder: Recorder | None = None,
) -> ClickHouseClient:
    def wrapped(request: httpx.Request) -> httpx.Response:
        if recorder is not None:
            recorder.requests.append(request)
        return handler(request)

    http: httpx.AsyncClient = httpx.AsyncClient(transport=httpx.MockTransport(wrapped))
    return ClickHouseClient(http, url=url, user="radar", password="s3cret")


def dumps(row: Row) -> str:
    return json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"


class FakeEraser:
    """ErasureSink double. A failure counter of -1 means "always fails"."""

    def __init__(
        self,
        journal: list[str] | None = None,
        *,
        erase_failures: int = 0,
        erased_failures: int = 0,
        erase_error: Exception | None = None,
        erased_keys: set[EntityKey] | None = None,
    ) -> None:
        self.journal: list[str] = journal if journal is not None else []
        self.erase_failures: int = erase_failures
        self.erased_failures: int = erased_failures
        self.erase_error: Exception | None = erase_error
        self.erased_keys: set[EntityKey] = erased_keys if erased_keys is not None else set()
        self.erase_calls: list[tuple[list[Erasure], datetime]] = []
        self.erased_calls: list[list[EntityKey]] = []

    async def erase(self, erasures: Sequence[Erasure], ingested_at: datetime) -> None:
        self.erase_calls.append((list(erasures), ingested_at))
        self.journal.append("erase")
        if self.erase_error is not None:
            raise self.erase_error
        if self.erase_failures < 0 or len(self.erase_calls) <= self.erase_failures:
            raise SinkError("down")

    async def erased(self, keys: Sequence[EntityKey]) -> set[EntityKey]:
        self.erased_calls.append(list(keys))
        self.journal.append("erased")
        if self.erased_failures < 0 or len(self.erased_calls) <= self.erased_failures:
            raise SinkError("down")
        return set(self.erased_keys)
