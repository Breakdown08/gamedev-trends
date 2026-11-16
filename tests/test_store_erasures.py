"""Spec checks 42-45: StoreConsumer.flush with an eraser (the checks without one are tests 14-22)."""

import asyncio
import logging
from datetime import datetime

import pytest
from store_support import NOW, T10, FakeEraser, FakeSink, FakeSleep, FakeSubscriber, wait_until

from gamedev_trends.contract import Erasure, Observation
from gamedev_trends.store import BatchReport, EntityKey, StoreConsumer, to_row

KEY_A: EntityKey = ("steam", "game", "a")
KEY_B: EntityKey = ("steam", "game", "b")


def game_obs(entity: str, value: int = 1) -> Observation:
    return Observation.create(
        source="steam",
        entity_type="game",
        entity_id=entity,
        metric="players_current",
        value=value,
        observed_at=T10,
    )


def game_erasure(entity: str) -> Erasure:
    return Erasure.create(source="steam", entity_type="game", entity_id=entity, requested_at=T10)


def make(
    sink: FakeSink,
    eraser: FakeEraser,
    journal: list[str],
    sleep: FakeSleep,
    now_calls: list[int] | None = None,
) -> tuple[StoreConsumer, FakeSubscriber]:
    sub: FakeSubscriber = FakeSubscriber([], journal=journal)

    def now() -> datetime:
        if now_calls is not None:
            now_calls.append(1)
        return NOW

    return StoreConsumer(sub, sink, sleep=sleep, now=now, eraser=eraser), sub


async def test_42_erase_then_check_then_write_then_commit() -> None:
    journal: list[str] = []
    sink: FakeSink = FakeSink(journal=journal)
    eraser: FakeEraser = FakeEraser(journal, erased_keys={KEY_B})
    c: StoreConsumer
    sub: FakeSubscriber
    c, sub = make(sink, eraser, journal, FakeSleep())
    erasure_b: Erasure = game_erasure("b")
    report: BatchReport = await c.flush([game_obs("a"), erasure_b, game_obs("b")])
    assert journal == ["erase", "erased", "write", "commit"]
    assert eraser.erase_calls == [([erasure_b], NOW)]
    assert eraser.erased_calls == [[KEY_A, KEY_B]]
    assert sink.calls == [[to_row(game_obs("a"), NOW)]]
    assert report == BatchReport(events=3, rows=1, attempts=1, committed=True, erasures=1, dropped=1)


async def test_42_nothing_is_dropped_when_nothing_is_erased() -> None:
    journal: list[str] = []
    sink: FakeSink = FakeSink(journal=journal)
    eraser: FakeEraser = FakeEraser(journal)
    c: StoreConsumer
    sub: FakeSubscriber
    c, sub = make(sink, eraser, journal, FakeSleep())
    report: BatchReport = await c.flush([game_obs("a"), game_obs("b")])
    assert journal == ["erased", "write", "commit"]
    assert report == BatchReport(events=2, rows=2, attempts=1, committed=True, erasures=0, dropped=0)


async def test_42_dropped_counts_measurements_and_keys_are_distinct_in_order_of_appearance() -> None:
    journal: list[str] = []
    sink: FakeSink = FakeSink(journal=journal)
    eraser: FakeEraser = FakeEraser(journal, erased_keys={KEY_B})
    c: StoreConsumer
    sub: FakeSubscriber
    c, sub = make(sink, eraser, journal, FakeSleep())
    report: BatchReport = await c.flush([game_obs("b", 1), game_obs("a"), game_obs("b", 2)])
    assert eraser.erased_calls == [[KEY_B, KEY_A]]
    assert report.rows == 1
    assert report.dropped == 2
    assert sink.calls == [[to_row(game_obs("a"), NOW)]]


async def test_42_the_clock_is_read_once() -> None:
    journal: list[str] = []
    calls: list[int] = []
    sink: FakeSink = FakeSink(failures=1, journal=journal)
    eraser: FakeEraser = FakeEraser(journal, erase_failures=1)
    c: StoreConsumer
    sub: FakeSubscriber
    c, sub = make(sink, eraser, journal, FakeSleep(), calls)
    await c.flush([game_erasure("b"), game_obs("a")])
    assert len(calls) == 1
    assert len(eraser.erase_calls) == 2
    assert all(ingested == NOW for _, ingested in eraser.erase_calls)
    assert all(row["ingested_at"] == "2026-11-12 10:00:07.000000" for call in sink.calls for row in call)


async def test_43_only_erasures() -> None:
    journal: list[str] = []
    sink: FakeSink = FakeSink(journal=journal)
    eraser: FakeEraser = FakeEraser(journal)
    c: StoreConsumer
    sub: FakeSubscriber
    c, sub = make(sink, eraser, journal, FakeSleep())
    report: BatchReport = await c.flush([game_erasure("b")])
    assert journal == ["erase", "commit"]
    assert sink.calls == []
    assert report == BatchReport(events=1, rows=0, attempts=0, committed=True, erasures=1, dropped=0)


async def test_44_all_measurements_erased() -> None:
    journal: list[str] = []
    sink: FakeSink = FakeSink(journal=journal)
    eraser: FakeEraser = FakeEraser(journal, erased_keys={KEY_B})
    c: StoreConsumer
    sub: FakeSubscriber
    c, sub = make(sink, eraser, journal, FakeSleep())
    report: BatchReport = await c.flush([game_obs("b")])
    assert journal == ["erased", "commit"]
    assert sink.calls == []
    assert report.rows == 0
    assert report.attempts == 0
    assert report.dropped == 1
    assert report.committed is True


async def test_45_erase_is_retried_with_backoff(caplog: pytest.LogCaptureFixture) -> None:
    journal: list[str] = []
    sink: FakeSink = FakeSink(journal=journal)
    eraser: FakeEraser = FakeEraser(journal, erase_failures=2)
    sleep: FakeSleep = FakeSleep()
    c: StoreConsumer
    sub: FakeSubscriber
    c, sub = make(sink, eraser, journal, sleep)
    with caplog.at_level(logging.DEBUG):
        report: BatchReport = await c.flush([game_erasure("b"), game_obs("a")])
    assert journal == ["erase", "erase", "erase", "erased", "write", "commit"]
    assert sleep.calls == [1.0, 2.0]
    assert report.attempts == 1
    assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 2


async def test_45_every_step_counts_its_own_attempts() -> None:
    journal: list[str] = []
    sink: FakeSink = FakeSink(failures=1, journal=journal)
    eraser: FakeEraser = FakeEraser(journal, erase_failures=1, erased_failures=1)
    sleep: FakeSleep = FakeSleep()
    c: StoreConsumer
    sub: FakeSubscriber
    c, sub = make(sink, eraser, journal, sleep)
    report: BatchReport = await c.flush([game_erasure("b"), game_obs("a")])
    assert journal == ["erase", "erase", "erased", "erased", "write", "write", "commit"]
    assert sleep.calls == [1.0, 1.0, 1.0]
    assert report.attempts == 2


async def test_45_erased_failing_forever_never_commits() -> None:
    journal: list[str] = []
    sink: FakeSink = FakeSink(journal=journal)
    eraser: FakeEraser = FakeEraser(journal, erased_failures=-1)
    c: StoreConsumer
    sub: FakeSubscriber
    c, sub = make(sink, eraser, journal, FakeSleep())
    task: asyncio.Task[BatchReport] = asyncio.create_task(c.flush([game_obs("a")]))
    await wait_until(lambda: len(eraser.erased_calls) >= 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert sub.commits == 0
    assert "commit" not in journal
    assert sink.calls == []


async def test_45_erase_failing_forever_never_commits() -> None:
    journal: list[str] = []
    eraser: FakeEraser = FakeEraser(journal, erase_failures=-1)
    c: StoreConsumer
    sub: FakeSubscriber
    c, sub = make(FakeSink(journal=journal), eraser, journal, FakeSleep())
    task: asyncio.Task[BatchReport] = asyncio.create_task(c.flush([game_erasure("b")]))
    await wait_until(lambda: len(eraser.erase_calls) >= 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert sub.commits == 0


async def test_45_other_exceptions_propagate_without_a_commit() -> None:
    journal: list[str] = []
    eraser: FakeEraser = FakeEraser(journal, erase_error=RuntimeError("boom"))
    sleep: FakeSleep = FakeSleep()
    c: StoreConsumer
    sub: FakeSubscriber
    c, sub = make(FakeSink(journal=journal), eraser, journal, sleep)
    with pytest.raises(RuntimeError):
        await c.flush([game_erasure("b"), game_obs("a")])
    assert sub.commits == 0
    assert sleep.calls == []
