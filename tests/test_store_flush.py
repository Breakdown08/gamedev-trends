"""Spec checks 14-22: flush, retry and commit ordering."""

import asyncio
import logging
from datetime import datetime

import pytest
from store_support import NOW, FakeSink, FakeSleep, FakeSubscriber, Step, erasure, obs, wait_until

from gamedev_trends.contract import Erasure
from gamedev_trends.store import BatchReport, StoreConsumer, to_row


def make(
    sink: FakeSink,
    journal: list[str],
    sleep: FakeSleep,
    *,
    now_calls: list[int] | None = None,
    retry_base: float = 1.0,
    retry_max: float = 30.0,
) -> tuple[StoreConsumer, FakeSubscriber]:
    sub: FakeSubscriber = FakeSubscriber([], journal=journal)

    def now() -> datetime:
        if now_calls is not None:
            now_calls.append(1)
        return NOW

    return StoreConsumer(sub, sink, sleep=sleep, now=now, retry_base=retry_base, retry_max=retry_max), sub


async def test_14_success_writes_then_commits() -> None:
    journal: list[str] = []
    sink: FakeSink = FakeSink(journal=journal)
    c: StoreConsumer
    sub: FakeSubscriber
    c, sub = make(sink, journal, FakeSleep())
    report: BatchReport = await c.flush([obs(0), obs(1), obs(2)])
    assert journal == ["write", "commit"]
    assert sink.calls == [[to_row(obs(i), NOW) for i in range(3)]]
    assert report == BatchReport(events=3, rows=3, attempts=1, committed=True)


async def test_15_retry_same_rows_with_backoff(caplog: pytest.LogCaptureFixture) -> None:
    journal: list[str] = []
    sink: FakeSink = FakeSink(failures=3, journal=journal)
    sleep: FakeSleep = FakeSleep()
    c: StoreConsumer
    sub: FakeSubscriber
    c, sub = make(sink, journal, sleep)
    with caplog.at_level(logging.DEBUG):
        report: BatchReport = await c.flush([obs(0), obs(1)])
    assert journal == ["write"] * 4 + ["commit"]
    assert all(call == sink.calls[0] for call in sink.calls)
    assert len(sink.calls[0]) == 2
    assert sleep.calls == [1.0, 2.0, 4.0]
    assert report.attempts == 4
    assert report.committed is True
    warnings: list[logging.LogRecord] = [
        r for r in caplog.records if r.name == "gamedev_trends.store" and r.levelno == logging.WARNING
    ]
    assert len(warnings) == 3


async def test_16_delay_is_capped() -> None:
    journal: list[str] = []
    sink: FakeSink = FakeSink(failures=4, journal=journal)
    sleep: FakeSleep = FakeSleep()
    c: StoreConsumer
    sub: FakeSubscriber
    c, sub = make(sink, journal, sleep, retry_base=10.0, retry_max=15.0)
    report: BatchReport = await c.flush([obs(0)])
    assert sleep.calls == [10.0, 15.0, 15.0, 15.0]
    assert report.attempts == 5


async def test_17_no_commit_without_write() -> None:
    journal: list[str] = []
    sink: FakeSink = FakeSink(failures=-1, journal=journal)
    c: StoreConsumer
    sub: FakeSubscriber
    c, sub = make(sink, journal, FakeSleep())
    task: asyncio.Task[BatchReport] = asyncio.create_task(c.flush([obs(0)]))
    await wait_until(lambda: len(sink.calls) >= 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert sub.commits == 0
    assert "commit" not in journal


async def test_18_other_exception_propagates() -> None:
    journal: list[str] = []
    sink: FakeSink = FakeSink(journal=journal, error=RuntimeError("boom"))
    sleep: FakeSleep = FakeSleep()
    c: StoreConsumer
    sub: FakeSubscriber
    c, sub = make(sink, journal, sleep)
    with pytest.raises(RuntimeError):
        await c.flush([obs(0)])
    assert sub.commits == 0
    assert sleep.calls == []


async def test_19_erasure_is_skipped_with_warning(caplog: pytest.LogCaptureFixture) -> None:
    journal: list[str] = []
    sink: FakeSink = FakeSink(journal=journal)
    c: StoreConsumer
    sub: FakeSubscriber
    c, sub = make(sink, journal, FakeSleep())
    gone: Erasure = erasure()
    with caplog.at_level(logging.DEBUG):
        report: BatchReport = await c.flush([obs(0), gone, obs(1)])
    assert len(sink.calls[0]) == 2
    assert report == BatchReport(events=3, rows=2, attempts=1, committed=True)
    assert any(
        r.name == "gamedev_trends.store"
        and r.levelno == logging.WARNING
        and str(gone.event_id) in r.getMessage()
        for r in caplog.records
    )


async def test_20_only_erasures_commit_without_write() -> None:
    journal: list[str] = []
    sink: FakeSink = FakeSink(journal=journal)
    c: StoreConsumer
    sub: FakeSubscriber
    c, sub = make(sink, journal, FakeSleep())
    report: BatchReport = await c.flush([erasure()])
    assert journal == ["commit"]
    assert report == BatchReport(events=1, rows=0, attempts=0, committed=True)


async def test_21_commit_failure_is_logged_not_raised(caplog: pytest.LogCaptureFixture) -> None:
    journal: list[str] = []
    sink: FakeSink = FakeSink(journal=journal)
    c: StoreConsumer
    sub: FakeSubscriber
    c, sub = make(sink, journal, FakeSleep())
    sub.commit_error = RuntimeError("kafka gone")
    with caplog.at_level(logging.DEBUG):
        report: BatchReport = await c.flush([obs(0)])
    assert report.committed is False
    assert report.rows == 1
    assert any(r.levelno == logging.ERROR for r in caplog.records)


async def test_22_ingested_at_computed_once_per_flush() -> None:
    journal: list[str] = []
    sink: FakeSink = FakeSink(failures=2, journal=journal)
    calls: list[int] = []
    c: StoreConsumer
    sub: FakeSubscriber
    c, sub = make(sink, journal, FakeSleep(), now_calls=calls)
    await c.flush([obs(0), obs(1)])
    assert len(calls) == 1
    assert len(sink.calls) == 3
    assert all(row["ingested_at"] == "2026-11-12 10:00:07.000000" for call in sink.calls for row in call)


async def test_run_flushes_each_batch_and_stops_at_max_batches() -> None:
    journal: list[str] = []
    sink: FakeSink = FakeSink(journal=journal)
    sub: FakeSubscriber = FakeSubscriber(
        [Step([obs(0)]), Step([]), Step([obs(1)]), Step([])], journal=journal
    )
    c: StoreConsumer = StoreConsumer(sub, sink, max_seconds=1.0, sleep=FakeSleep(), monotonic=sub.clock)
    await c.run(max_batches=2)
    assert journal == ["write", "commit", "write", "commit"]
    assert [len(call) for call in sink.calls] == [1, 1]
