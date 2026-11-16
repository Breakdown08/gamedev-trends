"""Spec checks 20-27: StoreConsumer.flush with an excerpt sink."""

import asyncio
import logging

import pytest
from store_support import (
    NOW,
    T10,
    FakeEraser,
    FakeExcerptSink,
    FakeSink,
    FakeSleep,
    FakeSubscriber,
    wait_until,
)

from gamedev_trends.contract import Erasure, Event, Excerpt, Observation
from gamedev_trends.store import BatchReport, EntityKey, StoreConsumer, to_excerpt_row, to_row

PA: str = "at://did:plc:a/app.bsky.feed.post/pa"
PB: str = "at://did:plc:b/app.bsky.feed.post/pb"
KA: EntityKey = ("bluesky", "post", PA)
KB: EntityKey = ("bluesky", "post", PB)


def ex(uri: str) -> Excerpt:
    return Excerpt.create(
        source="bluesky", entity_type="post", entity_id=uri, text=f"text of {uri[-2:]}", posted_at=T10
    )


def ob(uri: str) -> Observation:
    return Observation.create(
        source="bluesky",
        entity_type="post",
        entity_id=uri,
        metric="likes",
        value=1,
        observed_at=T10,
        window_start=None,
    )


def er(uri: str) -> Erasure:
    return Erasure.create(source="bluesky", entity_type="post", entity_id=uri, requested_at=T10)


class Rig:
    def __init__(
        self,
        *,
        eraser: bool = True,
        excerpts: bool = True,
        erased_keys: set[EntityKey] | None = None,
        write_failures: int = 0,
        erase_failures: int = 0,
        write_error: Exception | None = None,
    ) -> None:
        self.journal: list[str] = []
        self.sink: FakeSink = FakeSink(journal=self.journal)
        self.sleep: FakeSleep = FakeSleep()
        self.sub: FakeSubscriber = FakeSubscriber([], journal=self.journal)
        self.eraser: FakeEraser = FakeEraser(self.journal, erased_keys=erased_keys)
        self.excerpts: FakeExcerptSink = FakeExcerptSink(
            self.journal,
            write_failures=write_failures,
            erase_failures=erase_failures,
            write_error=write_error,
        )
        self.consumer: StoreConsumer = StoreConsumer(
            self.sub,
            self.sink,
            sleep=self.sleep,
            now=lambda: NOW,
            eraser=self.eraser if eraser else None,
            excerpts=self.excerpts if excerpts else None,
        )


async def test_20_excerpt_row() -> None:
    e: Excerpt = ex(PA)
    row: dict[str, str | float | None] = to_excerpt_row(e, NOW)
    assert list(row) == ["event_id", "source", "entity_type", "entity_id", "text", "posted_at", "ingested_at"]
    assert row == {
        "event_id": str(e.event_id),
        "source": "bluesky",
        "entity_type": "post",
        "entity_id": PA,
        "text": e.text,
        "posted_at": "2026-11-12 10:00:00.000000",
        "ingested_at": "2026-11-12 10:00:07.000000",
    }


async def test_21_everything_in_one_batch() -> None:
    rig: Rig = Rig(erased_keys={KB})
    batch: list[Event] = [ob(PA), ex(PA), er(PB), ex(PB)]
    report: BatchReport = await rig.consumer.flush(batch)
    assert rig.journal == ["erase", "erase_excerpts", "erased", "write", "write_excerpts", "commit"]
    assert rig.excerpts.erase_calls == [[KB]]
    assert rig.eraser.erased_calls == [[KA, KB]]
    assert rig.excerpts.write_calls == [[to_excerpt_row(ex(PA), NOW)]]
    assert rig.sink.calls == [[to_row(ob(PA), NOW)]]
    assert report == BatchReport(
        events=4, rows=1, attempts=1, committed=True, erasures=1, dropped=1, excerpts=1
    )


async def test_22_only_excerpts() -> None:
    rig: Rig = Rig()
    report: BatchReport = await rig.consumer.flush([ex(PA)])
    assert rig.journal == ["erased", "write_excerpts", "commit"]
    assert report.rows == 0
    assert report.attempts == 0
    assert report.excerpts == 1


async def test_23_all_excerpts_erased() -> None:
    rig: Rig = Rig(erased_keys={KA})
    report: BatchReport = await rig.consumer.flush([ex(PA)])
    assert rig.journal == ["erased", "commit"]
    assert rig.excerpts.write_calls == []
    assert report.dropped == 1
    assert report.excerpts == 0


async def test_24_only_erasures_with_an_excerpt_sink() -> None:
    rig: Rig = Rig()
    await rig.consumer.flush([er(PA)])
    assert rig.journal == ["erase", "erase_excerpts", "commit"]
    assert rig.excerpts.erase_calls == [[KA]]


async def test_25_write_excerpts_is_retried() -> None:
    rig: Rig = Rig(write_failures=2)
    report: BatchReport = await rig.consumer.flush([ex(PA)])
    assert rig.journal == ["erased", "write_excerpts", "write_excerpts", "write_excerpts", "commit"]
    assert rig.sleep.calls == [1.0, 2.0]
    assert report.excerpts == 1
    assert all(call == rig.excerpts.write_calls[0] for call in rig.excerpts.write_calls)


async def test_25_erase_excerpts_is_retried() -> None:
    rig: Rig = Rig(erase_failures=2)
    await rig.consumer.flush([er(PA)])
    assert rig.journal == ["erase", "erase_excerpts", "erase_excerpts", "erase_excerpts", "commit"]
    assert rig.sleep.calls == [1.0, 2.0]


async def test_25_endless_failure_never_commits() -> None:
    rig: Rig = Rig(write_failures=-1)
    task: asyncio.Task[BatchReport] = asyncio.create_task(rig.consumer.flush([ex(PA)]))
    await wait_until(lambda: len(rig.excerpts.write_calls) >= 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert rig.sub.commits == 0
    assert "commit" not in rig.journal


async def test_25_other_exception_propagates_without_commit() -> None:
    rig: Rig = Rig(write_error=RuntimeError("boom"))
    with pytest.raises(RuntimeError):
        await rig.consumer.flush([ex(PA)])
    assert rig.sub.commits == 0
    assert rig.sleep.calls == []


async def test_26_without_an_excerpt_sink_excerpts_are_skipped(caplog: pytest.LogCaptureFixture) -> None:
    rig: Rig = Rig(excerpts=False)
    skipped: Excerpt = ex(PA)
    with caplog.at_level(logging.DEBUG):
        report: BatchReport = await rig.consumer.flush([skipped, ob(PA)])
    assert rig.journal == ["erased", "write", "commit"]
    assert rig.eraser.erased_calls == [[KA]]
    assert rig.excerpts.write_calls == []
    assert report.excerpts == 0
    assert any(
        r.levelno == logging.WARNING
        and "skipping non-observation event" in r.getMessage()
        and str(skipped.event_id) in r.getMessage()
        for r in caplog.records
    )


async def test_26_without_an_excerpt_sink_erasures_do_not_touch_excerpts() -> None:
    rig: Rig = Rig(excerpts=False)
    await rig.consumer.flush([er(PA)])
    assert rig.journal == ["erase", "commit"]
    assert rig.excerpts.erase_calls == []


async def test_27_without_an_eraser_excerpts_are_written_unchecked() -> None:
    rig: Rig = Rig(eraser=False)
    report: BatchReport = await rig.consumer.flush([ex(PA)])
    assert rig.journal == ["write_excerpts", "commit"]
    assert report.excerpts == 1
