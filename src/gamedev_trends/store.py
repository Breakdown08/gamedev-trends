"""Kafka to ClickHouse consumer: batches observations, writes them, then commits."""

import argparse
import asyncio
import logging
import math
import os
import re
import signal
import sys
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

import httpx

from gamedev_trends.bus import ERASURES_TOPIC, OBSERVATIONS_TOPIC, Subscriber
from gamedev_trends.contract import Erasure, Event, Excerpt, Observation
from gamedev_trends.entry import run_main
from gamedev_trends.logs import setup_logging

logger: logging.Logger = logging.getLogger("gamedev_trends.store")

type Row = dict[str, str | float | None]
type Sleep = Callable[[float], Awaitable[None]]
type Now = Callable[[], datetime]

DEFAULT_GROUP: str = "radar-store"

_MAX_BATCH_EVENTS_LIMIT: int = 100_000
_MAX_EXPONENT: int = 60


def retry_delay(attempt: int, base: float, cap: float) -> float:
    """Delay after the attempt-th failure (1, 2, ...): base * 2 ** (attempt - 1), at most cap."""
    factor: int = 2 ** min(attempt - 1, _MAX_EXPONENT)
    return min(cap, base * factor)


def format_datetime(dt: datetime) -> str:
    """Aware datetime in UTC with exactly six fractional digits. A naive one raises ValueError."""
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")


def to_row(obs: Observation, ingested_at: datetime) -> Row:
    """A row of the observations table."""
    return {
        "event_id": str(obs.event_id),
        "source": obs.source,
        "entity_type": obs.entity_type,
        "entity_id": obs.entity_id,
        "metric": obs.metric,
        "value": float(obs.value),
        "observed_at": format_datetime(obs.observed_at),
        "window_start": None if obs.window_start is None else format_datetime(obs.window_start),
        "ingested_at": format_datetime(ingested_at),
    }


def to_excerpt_row(excerpt: Excerpt, ingested_at: datetime) -> Row:
    """A row of the excerpts table."""
    return {
        "event_id": str(excerpt.event_id),
        "source": excerpt.source,
        "entity_type": excerpt.entity_type,
        "entity_id": excerpt.entity_id,
        "text": excerpt.text,
        "posted_at": format_datetime(excerpt.posted_at),
        "ingested_at": format_datetime(ingested_at),
    }


class SinkError(Exception):
    """The batch was not written; the write can be retried."""


class ObservationSink(Protocol):
    async def write(self, rows: Sequence[Row]) -> None:
        """Write all rows or raise SinkError. After a return the rows are visible in the store."""
        ...


type EntityKey = tuple[str, str, str]


class ErasureSink(Protocol):
    async def erase(self, erasures: Sequence[Erasure], ingested_at: datetime) -> None:
        """Record the erasures and delete the observations of their entities, or raise SinkError."""
        ...

    async def erased(self, keys: Sequence[EntityKey]) -> set[EntityKey]:
        """Which of the keys are erased, or raise SinkError."""
        ...


class ExcerptSink(Protocol):
    async def write_excerpts(self, rows: Sequence[Row]) -> None:
        """Write all rows or raise SinkError."""
        ...

    async def erase_excerpts(self, keys: Sequence[EntityKey]) -> None:
        """Delete the excerpts of these entities or raise SinkError."""
        ...


@dataclass(frozen=True)
class BatchReport:
    events: int
    rows: int
    attempts: int
    committed: bool
    erasures: int = 0
    dropped: int = 0
    excerpts: int = 0


class StoreConsumer:
    def __init__(
        self,
        subscriber: Subscriber,
        sink: ObservationSink,
        *,
        max_events: int = 5000,
        max_seconds: float = 5.0,
        idle_timeout: float = 1.0,
        retry_base: float = 1.0,
        retry_max: float = 30.0,
        sleep: Sleep = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        now: Now = lambda: datetime.now(UTC),
        eraser: ErasureSink | None = None,
        excerpts: ExcerptSink | None = None,
    ) -> None:
        if max_events < 1:
            raise ValueError(f"max_events must be >= 1, got {max_events}")
        if max_seconds <= 0:
            raise ValueError(f"max_seconds must be > 0, got {max_seconds}")
        if idle_timeout <= 0:
            raise ValueError(f"idle_timeout must be > 0, got {idle_timeout}")
        if retry_base <= 0:
            raise ValueError(f"retry_base must be > 0, got {retry_base}")
        if retry_max < retry_base:
            raise ValueError(f"retry_max must be >= retry_base ({retry_base}), got {retry_max}")
        self._subscriber: Subscriber = subscriber
        self._sink: ObservationSink = sink
        self._max_events: int = max_events
        self._max_seconds: float = max_seconds
        self._idle_timeout: float = idle_timeout
        self._retry_base: float = retry_base
        self._retry_max: float = retry_max
        self._sleep: Sleep = sleep
        self._monotonic: Callable[[], float] = monotonic
        self._now: Now = now
        self._eraser: ErasureSink | None = eraser
        self._excerpts: ExcerptSink | None = excerpts

    async def next_batch(self) -> list[Event]:
        """One batch: closes by size or max_seconds after the first event, whichever comes first."""
        batch: list[Event] = []
        while not batch:
            batch = await self._subscriber.poll(max_events=self._max_events, timeout=self._idle_timeout)
        t0: float = self._monotonic()
        while len(batch) < self._max_events:
            remaining: float = self._max_seconds - (self._monotonic() - t0)
            if remaining <= 0:
                break
            batch.extend(
                await self._subscriber.poll(max_events=self._max_events - len(batch), timeout=remaining)
            )
        return batch

    async def _retry[T](self, step: str, call: Callable[[], Awaitable[T]]) -> T:
        """Run call until it stops raising SinkError, sleeping retry_delay between attempts."""
        attempt: int = 0
        while True:
            attempt += 1
            try:
                return await call()
            except SinkError as exc:
                delay: float = retry_delay(attempt, self._retry_base, self._retry_max)
                logger.warning("%s attempt %d failed: %s; retrying in %.1fs", step, attempt, exc, delay)
                await self._sleep(delay)

    async def flush(self, events: Sequence[Event]) -> BatchReport:
        """Write the observations of the batch (retrying until it succeeds), then commit."""
        observations: list[Observation] = []
        erasures: list[Erasure] = []
        excerpts: list[Excerpt] = []
        event: Event
        for event in events:
            if isinstance(event, Observation):
                observations.append(event)
            elif isinstance(event, Erasure) and self._eraser is not None:
                erasures.append(event)
            elif isinstance(event, Excerpt) and self._excerpts is not None:
                excerpts.append(event)
            else:
                logger.warning("skipping non-observation event: event_id=%s", event.event_id)
        ingested_at: datetime = self._now()
        dropped: int = 0
        if self._eraser is not None:
            eraser: ErasureSink = self._eraser
            if erasures:
                await self._retry("erase", lambda: eraser.erase(erasures, ingested_at))
                if self._excerpts is not None:
                    excerpt_sink: ExcerptSink = self._excerpts
                    gone: list[EntityKey] = list(
                        dict.fromkeys((e.source, e.entity_type, e.entity_id) for e in erasures)
                    )
                    await self._retry("erase_excerpts", lambda: excerpt_sink.erase_excerpts(gone))
            if observations or excerpts:
                keys: list[EntityKey] = list(
                    dict.fromkeys(
                        (e.source, e.entity_type, e.entity_id)
                        for e in events
                        if isinstance(e, Observation)
                        or (isinstance(e, Excerpt) and self._excerpts is not None)
                    )
                )
                erased: set[EntityKey] = await self._retry("erased", lambda: eraser.erased(keys))
                if erased:
                    kept: list[Observation] = [
                        o for o in observations if (o.source, o.entity_type, o.entity_id) not in erased
                    ]
                    kept_excerpts: list[Excerpt] = [
                        x for x in excerpts if (x.source, x.entity_type, x.entity_id) not in erased
                    ]
                    dropped = len(observations) - len(kept) + len(excerpts) - len(kept_excerpts)
                    logger.info("dropping %d event(s) of erased entities", dropped)
                    observations = kept
                    excerpts = kept_excerpts
        rows: list[Row] = [to_row(o, ingested_at) for o in observations]
        attempts: int = 0
        if rows:
            while True:
                attempts += 1
                try:
                    await self._sink.write(rows)
                    break
                except SinkError as exc:
                    delay: float = retry_delay(attempts, self._retry_base, self._retry_max)
                    logger.warning("write attempt %d failed: %s; retrying in %.1fs", attempts, exc, delay)
                    await self._sleep(delay)
        written: int = 0
        if excerpts and self._excerpts is not None:
            writer: ExcerptSink = self._excerpts
            excerpt_rows: list[Row] = [to_excerpt_row(x, ingested_at) for x in excerpts]
            await self._retry("write_excerpts", lambda: writer.write_excerpts(excerpt_rows))
            written = len(excerpt_rows)
        committed: bool = True
        try:
            await self._subscriber.commit()
        except Exception:
            committed = False
            logger.exception("commit failed; the next commit will cover this position")
        report: BatchReport = BatchReport(
            events=len(events),
            rows=len(rows),
            attempts=attempts,
            committed=committed,
            erasures=len(erasures),
            dropped=dropped,
            excerpts=written,
        )
        logger.info(
            "batch: events=%d rows=%d attempts=%d committed=%s",
            report.events,
            report.rows,
            report.attempts,
            report.committed,
        )
        return report

    async def run(self, max_batches: int | None = None) -> None:
        batches: int = 0
        while max_batches is None or batches < max_batches:
            await self.flush(await self.next_batch())
            batches += 1


def _text(env: Mapping[str, str], name: str, default: str) -> str:
    value: str = env.get(name, "").strip()
    return value or default


def _int(env: Mapping[str, str], name: str, default: int) -> int:
    raw: str = env.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"{name}: expected an integer, got {raw!r}") from None


def _float(env: Mapping[str, str], name: str, default: float) -> float:
    raw: str = env.get(name, "").strip()
    if not raw:
        return default
    try:
        value: float = float(raw)
    except ValueError:
        raise ValueError(f"{name}: expected a number, got {raw!r}") from None
    if not math.isfinite(value):
        raise ValueError(f"{name}: expected a finite number, got {raw!r}")
    return value


@dataclass(frozen=True)
class StoreConfig:
    clickhouse_password: str
    kafka_bootstrap_servers: str = "localhost:9092"
    clickhouse_url: str = "http://localhost:8123"
    clickhouse_user: str = "radar"
    clickhouse_database: str = "radar"
    group: str = DEFAULT_GROUP
    batch_max_events: int = 5000
    batch_max_seconds: float = 5.0
    http_timeout_seconds: float = 30.0
    retry_base_seconds: float = 1.0
    retry_max_seconds: float = 30.0

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> StoreConfig:
        """Read the variables; absent or empty ones take the defaults, bad ones raise ValueError."""
        password: str = env.get("CLICKHOUSE_PASSWORD", "").strip()
        if not password:
            raise ValueError("CLICKHOUSE_PASSWORD: required, set it in .env")
        d: StoreConfig = cls(clickhouse_password=password)
        from gamedev_trends.clickhouse import DATABASE_PATTERN  # circular at module level

        database: str = _text(env, "CLICKHOUSE_DATABASE", d.clickhouse_database)
        if re.fullmatch(DATABASE_PATTERN, database) is None:
            raise ValueError(f"CLICKHOUSE_DATABASE: must match {DATABASE_PATTERN}, got {database!r}")
        max_events: int = _int(env, "STORE_BATCH_MAX_EVENTS", d.batch_max_events)
        if not 1 <= max_events <= _MAX_BATCH_EVENTS_LIMIT:
            raise ValueError(
                f"STORE_BATCH_MAX_EVENTS: must be from 1 to {_MAX_BATCH_EVENTS_LIMIT}, got {max_events}"
            )
        max_seconds: float = _float(env, "STORE_BATCH_MAX_SECONDS", d.batch_max_seconds)
        if max_seconds <= 0:
            raise ValueError(f"STORE_BATCH_MAX_SECONDS: must be > 0, got {max_seconds}")
        timeout: float = _float(env, "STORE_HTTP_TIMEOUT_SECONDS", d.http_timeout_seconds)
        if timeout <= 0:
            raise ValueError(f"STORE_HTTP_TIMEOUT_SECONDS: must be > 0, got {timeout}")
        base: float = _float(env, "STORE_RETRY_BASE_SECONDS", d.retry_base_seconds)
        if base <= 0:
            raise ValueError(f"STORE_RETRY_BASE_SECONDS: must be > 0, got {base}")
        retry_max: float = _float(env, "STORE_RETRY_MAX_SECONDS", d.retry_max_seconds)
        if retry_max < base:
            raise ValueError(f"STORE_RETRY_MAX_SECONDS: must be >= {base} (the base delay), got {retry_max}")
        return cls(
            clickhouse_password=password,
            kafka_bootstrap_servers=_text(env, "KAFKA_BOOTSTRAP_SERVERS", d.kafka_bootstrap_servers),
            clickhouse_url=_text(env, "CLICKHOUSE_URL", d.clickhouse_url).rstrip("/"),
            clickhouse_user=_text(env, "CLICKHOUSE_USER", d.clickhouse_user),
            clickhouse_database=database,
            group=_text(env, "STORE_GROUP", d.group),
            batch_max_events=max_events,
            batch_max_seconds=max_seconds,
            http_timeout_seconds=timeout,
            retry_base_seconds=base,
            retry_max_seconds=retry_max,
        )


async def _serve(config: StoreConfig) -> None:
    # Imported here: clickhouse.py imports Row and SinkError from this module.
    from gamedev_trends.clickhouse import (
        CONTRACT_DATABASE,
        ClickHouseClient,
        ClickHouseErasureSink,
        ClickHouseError,
        ClickHouseExcerptSink,
        ClickHouseSink,
        ensure_contract,
        ensure_erasures_schema,
        ensure_excerpts_schema,
        ensure_schema,
    )
    from gamedev_trends.kafka_bus import KafkaSubscriber

    http: httpx.AsyncClient = httpx.AsyncClient(timeout=config.http_timeout_seconds)
    subscriber: KafkaSubscriber | None = None
    try:
        client: ClickHouseClient = ClickHouseClient(
            http, url=config.clickhouse_url, user=config.clickhouse_user, password=config.clickhouse_password
        )
        attempt: int = 0
        while True:
            try:
                await ensure_schema(client, config.clickhouse_database)
                await ensure_erasures_schema(client, config.clickhouse_database)
                await ensure_excerpts_schema(client, config.clickhouse_database)
                await ensure_contract(client, config.clickhouse_database)
                break
            except ClickHouseError as exc:
                attempt += 1
                delay: float = retry_delay(attempt, config.retry_base_seconds, config.retry_max_seconds)
                logger.warning("schema attempt %d failed: %s; retrying in %.1fs", attempt, exc, delay)
                await asyncio.sleep(delay)
        logger.info("schema ready: database=%s interface=%s", config.clickhouse_database, CONTRACT_DATABASE)
        subscriber = KafkaSubscriber(
            config.kafka_bootstrap_servers, config.group, [OBSERVATIONS_TOPIC, ERASURES_TOPIC]
        )
        consumer: StoreConsumer = StoreConsumer(
            subscriber,
            ClickHouseSink(client, config.clickhouse_database),
            eraser=ClickHouseErasureSink(client, config.clickhouse_database),
            excerpts=ClickHouseExcerptSink(client, config.clickhouse_database),
            max_events=config.batch_max_events,
            max_seconds=config.batch_max_seconds,
            retry_base=config.retry_base_seconds,
            retry_max=config.retry_max_seconds,
        )
        await consumer.run()
    finally:
        if subscriber is not None:
            await subscriber.close()
        await http.aclose()


async def _run(config: StoreConfig) -> int:
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    task: asyncio.Task[None] = asyncio.create_task(_serve(config))
    sig: signal.Signals
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, task.cancel)
    try:
        await task
    except asyncio.CancelledError:
        pass
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(prog="python -m gamedev_trends.store")
    parser.parse_args(argv)
    config: StoreConfig
    try:
        config = StoreConfig.from_env(os.environ)
    except ValueError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    setup_logging()
    return asyncio.run(_run(config))


if __name__ == "__main__":
    run_main(__spec__)
