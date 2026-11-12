"""Spec checks 33-36: slot_start and run_forever with fake clock and sleep."""

import asyncio
import logging
from datetime import UTC, datetime, timedelta, timezone

import pytest
from steam_support import T10, FakeClock, FakeSleep

from gamedev_trends.steam import CycleReport, run_forever, slot_start

H11: datetime = T10 + timedelta(hours=1)
H12: datetime = T10 + timedelta(hours=2)


def test_33_slot_start() -> None:
    assert slot_start(datetime(2026, 11, 12, 10, 59, 59, 999999, tzinfo=UTC), 3600) == T10
    assert slot_start(T10, 3600) == T10
    local: datetime = slot_start(datetime(2026, 11, 12, 17, 20, tzinfo=timezone(timedelta(hours=7))), 3600)
    assert local == T10
    assert local.utcoffset() == timedelta(0)
    assert slot_start(datetime(2026, 11, 12, 10, 44, tzinfo=UTC), 900) == datetime(
        2026, 11, 12, 10, 30, tzinfo=UTC
    )


def test_33_slot_start_naive_is_value_error() -> None:
    with pytest.raises(ValueError):
        slot_start(datetime(2026, 11, 12, 10, 20), 3600)


class Runner:
    def __init__(self, clock: FakeClock, shifts: dict[datetime, float] | None = None) -> None:
        self.clock: FakeClock = clock
        self.slots: list[datetime] = []
        self.shifts: dict[datetime, float] = shifts or {}
        self.fail_first: bool = False

    async def __call__(self, slot: datetime) -> CycleReport:
        self.slots.append(slot)
        if self.fail_first and len(self.slots) == 1:
            raise RuntimeError("boom")
        self.clock.advance(self.shifts.get(slot, 0.0))
        return CycleReport(slot, True, 0, (), (), 0, True)


async def drive(runner: Runner, clock: FakeClock, sleep: FakeSleep, max_slots: int) -> None:
    await run_forever(runner, interval_seconds=3600, now=clock.now, sleep=sleep, max_slots=max_slots)


async def test_34_runs_current_slot_then_sleeps_to_boundaries() -> None:
    clock: FakeClock = FakeClock(datetime(2026, 11, 12, 10, 20, tzinfo=UTC))
    sleep: FakeSleep = FakeSleep(clock)
    runner: Runner = Runner(clock)
    await drive(runner, clock, sleep, 3)
    assert runner.slots == [T10, H11, H12]
    assert sleep.calls == [2400.0, 3600.0]


async def test_35_long_run_skips_missed_slots(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    clock: FakeClock = FakeClock(datetime(2026, 11, 12, 10, 20, tzinfo=UTC))
    sleep: FakeSleep = FakeSleep(clock)
    runner: Runner = Runner(clock, {T10: 150 * 60})
    await drive(runner, clock, sleep, 2)
    assert runner.slots == [T10, H12]
    assert sleep.calls == []
    assert any(r.levelno == logging.WARNING for r in caplog.records)


async def test_35_run_ending_exactly_on_boundary_runs_next_at_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    clock: FakeClock = FakeClock(datetime(2026, 11, 12, 10, 20, tzinfo=UTC))
    sleep: FakeSleep = FakeSleep(clock)
    runner: Runner = Runner(clock, {T10: 40 * 60})
    await drive(runner, clock, sleep, 2)
    assert runner.slots == [T10, H11]
    assert sleep.calls == []
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


async def test_36_exception_in_run_is_logged_and_loop_continues(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    clock: FakeClock = FakeClock(datetime(2026, 11, 12, 10, 20, tzinfo=UTC))
    sleep: FakeSleep = FakeSleep(clock)
    runner: Runner = Runner(clock)
    runner.fail_first = True
    await drive(runner, clock, sleep, 2)
    assert runner.slots == [T10, H11]
    assert any(r.levelno == logging.ERROR and r.exc_info for r in caplog.records)


async def test_cancellation_propagates() -> None:
    clock: FakeClock = FakeClock(T10)

    async def cancelled(slot: datetime) -> CycleReport:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await run_forever(
            cancelled, interval_seconds=3600, now=clock.now, sleep=FakeSleep(clock), max_slots=3
        )
