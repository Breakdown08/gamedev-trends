"""Spec checks 29-37: run_session, run_forever and websocket_connect with fake connections."""

import asyncio
import logging
import re
from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

import pytest
from bluesky_support import (
    T0,
    A,
    FakeSleep,
    RecordingCollector,
    Rig,
    ScriptedConnect,
    Ticker,
    identity_frame,
    make_rig,
    post_frame,
    yield_loop,
)

from gamedev_trends.bluesky import BlueskyCollector, InMemoryCursorStore, run_forever, run_session
from gamedev_trends.jetstream import (
    JETSTREAM_COLLECTIONS,
    ConnectionLost,
    FrameSource,
    parse_event,
    subscribe_url,
    websocket_connect,
)

URL: str = "ws://test/subscribe"
GUARD_SECONDS: float = 10.0


def frames(n: int) -> list[str | bytes]:
    return [identity_frame(A, T0 + i + 1) for i in range(n)]


def messages(caplog: pytest.LogCaptureFixture, level: int) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno == level]


async def test_29_five_frames_then_a_break(caplog: pytest.LogCaptureFixture) -> None:
    collector: RecordingCollector = RecordingCollector()
    raw: list[str | bytes] = frames(5)
    with caplog.at_level(logging.DEBUG):
        handled: int = await asyncio.wait_for(
            run_session(ScriptedConnect(raw), URL, collector), GUARD_SECONDS
        )
    assert handled == 5
    assert collector.received == [parse_event(f) for f in raw]
    assert len(messages(caplog, logging.WARNING)) >= 1


async def test_29_the_url_is_passed_to_connect() -> None:
    connect: ScriptedConnect = ScriptedConnect(frames(1))
    await asyncio.wait_for(run_session(connect, URL, RecordingCollector()), GUARD_SECONDS)
    assert connect.urls == [URL]


async def test_30_a_broken_frame_reaches_handle_as_none_and_is_counted() -> None:
    collector: RecordingCollector = RecordingCollector()
    raw: list[str | bytes] = [identity_frame(A, T0 + 1), b"not json", identity_frame(A, T0 + 3)]
    handled: int = await asyncio.wait_for(run_session(ScriptedConnect(raw), URL, collector), GUARD_SECONDS)
    assert handled == 3
    assert collector.received[1] is None
    assert collector.received[0] is not None
    assert collector.received[2] is not None


class EndlessSource:
    def __init__(self) -> None:
        self.recv_calls: int = 0

    async def recv(self) -> str | bytes:
        self.recv_calls += 1
        return identity_frame(A, T0 + self.recv_calls)


async def test_31_back_pressure_stops_reading_when_the_queue_is_full() -> None:
    source: EndlessSource = EndlessSource()

    @asynccontextmanager
    async def endless(url: str) -> AsyncGenerator[FrameSource]:
        yield source

    gate: asyncio.Event = asyncio.Event()
    collector: RecordingCollector = RecordingCollector(block=gate)
    task: asyncio.Task[int] = asyncio.create_task(
        run_session(endless, URL, collector, queue_size=10, batch_max=1)
    )
    await yield_loop(1000)
    assert source.recv_calls == 12
    await yield_loop(1000)
    assert source.recv_calls == 12
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


class SilentSource:
    async def recv(self) -> str | bytes:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


async def test_32_idle_connection_ends_the_session(caplog: pytest.LogCaptureFixture) -> None:
    @asynccontextmanager
    async def silent(url: str) -> AsyncGenerator[FrameSource]:
        yield SilentSource()

    with caplog.at_level(logging.DEBUG):
        handled: int = await asyncio.wait_for(
            run_session(silent, URL, RecordingCollector(), idle_timeout=0.05), GUARD_SECONDS
        )
    assert handled == 0
    assert any("idle" in m.lower() for m in messages(caplog, logging.WARNING))


async def test_33_connect_failure_ends_the_session(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.DEBUG):
        handled: int = await asyncio.wait_for(
            run_session(ScriptedConnect(OSError("refused")), URL, RecordingCollector()), GUARD_SECONDS
        )
    assert handled == 0
    assert len(messages(caplog, logging.WARNING)) >= 1


async def test_33_handle_failure_ends_the_session_and_counts_only_handled_frames(
    caplog: pytest.LogCaptureFixture,
) -> None:
    collector: RecordingCollector = RecordingCollector(fail_on_call=2)
    with caplog.at_level(logging.DEBUG):
        handled: int = await asyncio.wait_for(
            run_session(ScriptedConnect(frames(4)), URL, collector, batch_max=2), GUARD_SECONDS
        )
    assert handled == 2
    assert [len(c) for c in collector.calls] == [2, 2]
    assert len(messages(caplog, logging.ERROR)) >= 1
    assert any(r.exc_info is not None for r in caplog.records if r.levelno == logging.ERROR)


async def test_33_batches_are_cut_at_batch_max() -> None:
    collector: RecordingCollector = RecordingCollector()
    await asyncio.wait_for(
        run_session(ScriptedConnect(frames(5)), URL, collector, batch_max=2), GUARD_SECONDS
    )
    assert [len(c) for c in collector.calls] == [2, 2, 1]


STATS_LINE: re.Pattern[str] = re.compile(
    r"bluesky stats: rate=(\d+\.\d)/s events=(\d+) posts=0 likes=0 reposts=0 tagged_posts=0 "
    r"engagements=0 erasures=0 late=0 malformed=0 queue=\d+/10000"
)


async def test_34_stats_lines(caplog: pytest.LogCaptureFixture) -> None:
    rig: Rig = make_rig()
    collector: BlueskyCollector = rig.collector
    ticker: Ticker = Ticker(step=10.0)
    with caplog.at_level(logging.INFO, logger="gamedev_trends.bluesky"):
        handled: int = await asyncio.wait_for(
            run_session(
                ScriptedConnect(frames(5)),
                URL,
                collector,
                batch_max=1,
                stats_seconds=10.0,
                monotonic=ticker,
            ),
            GUARD_SECONDS,
        )
    assert handled == 5
    lines: list[str] = [m for m in messages(caplog, logging.INFO) if m.startswith("bluesky stats:")]
    assert lines
    total: int = 0
    line: str
    for line in lines:
        match: re.Match[str] | None = STATS_LINE.fullmatch(line)
        assert match is not None, line
        events: int = int(match.group(2))
        assert float(match.group(1)) == pytest.approx(events / 10, abs=0.051)
        total += events
    assert total == 5


async def test_34_no_stats_line_before_the_interval_has_passed(caplog: pytest.LogCaptureFixture) -> None:
    rig: Rig = make_rig()
    ticker: Ticker = Ticker(step=1.0)
    with caplog.at_level(logging.INFO, logger="gamedev_trends.bluesky"):
        await asyncio.wait_for(
            run_session(
                ScriptedConnect(frames(3)),
                URL,
                rig.collector,
                batch_max=1,
                stats_seconds=60.0,
                monotonic=ticker,
            ),
            GUARD_SECONDS,
        )
    assert [m for m in messages(caplog, logging.INFO) if m.startswith("bluesky stats:")] == []


async def test_34_monotonic_is_read_once_per_handle_and_once_at_the_start() -> None:
    rig: Rig = make_rig()
    ticker: Ticker = Ticker(step=1.0)
    await asyncio.wait_for(
        run_session(ScriptedConnect(frames(3)), URL, rig.collector, batch_max=1, monotonic=ticker),
        GUARD_SECONDS,
    )
    assert ticker.calls == 4


FIRST_CURSOR: int = 1794744060000000
EP_A: str = "wss://a.example/subscribe"
EP_B: str = "wss://b.example/subscribe"


def url_for(endpoint: str, cursor: int | None) -> str:
    return subscribe_url(endpoint, JETSTREAM_COLLECTIONS, cursor)


async def test_35_run_forever_rotates_endpoints_and_backs_off(caplog: pytest.LogCaptureFixture) -> None:
    connect: ScriptedConnect = ScriptedConnect(
        OSError("down"), OSError("down"), [identity_frame(A, FIRST_CURSOR + 1)], OSError("down")
    )
    collector: RecordingCollector = RecordingCollector()
    cursor: InMemoryCursorStore = InMemoryCursorStore(FIRST_CURSOR)
    sleep: FakeSleep = FakeSleep()
    with caplog.at_level(logging.DEBUG):
        await asyncio.wait_for(
            run_forever(
                connect,
                collector,
                cursor,
                endpoints=[EP_A, EP_B],
                sleep=sleep,
                max_sessions=4,
            ),
            GUARD_SECONDS,
        )
    assert (
        connect.urls
        == [url_for(EP_A, FIRST_CURSOR), url_for(EP_B, FIRST_CURSOR)] + [url_for(EP_A, FIRST_CURSOR)] * 2
    )
    assert connect.urls[0].endswith(f"&cursor={FIRST_CURSOR}")
    assert sleep.calls == [1.0, 2.0, 1.0]
    assert collector.starts == [FIRST_CURSOR] * 4
    sessions: list[str] = [
        m for m in messages(caplog, logging.INFO) if m.startswith("bluesky session: endpoint=")
    ]
    assert len(sessions) == 4
    assert [EP_A in s for s in sessions] == [True, False, True, True]
    assert [EP_B in s for s in sessions] == [False, True, False, False]
    assert all(f"cursor={FIRST_CURSOR}" in s for s in sessions)


async def test_35_the_delay_is_capped() -> None:
    connect: ScriptedConnect = ScriptedConnect(OSError("down"))
    sleep: FakeSleep = FakeSleep()
    await asyncio.wait_for(
        run_forever(
            connect,
            RecordingCollector(),
            InMemoryCursorStore(FIRST_CURSOR),
            endpoints=[EP_A],
            backoff_base=1.0,
            backoff_max=4.0,
            sleep=sleep,
            max_sessions=6,
        ),
        GUARD_SECONDS,
    )
    assert sleep.calls == [1.0, 2.0, 4.0, 4.0, 4.0]


async def test_35_the_first_session_without_a_cursor_has_no_cursor_in_the_url(
    caplog: pytest.LogCaptureFixture,
) -> None:
    connect: ScriptedConnect = ScriptedConnect(OSError("down"))
    collector: RecordingCollector = RecordingCollector()
    with caplog.at_level(logging.DEBUG):
        await asyncio.wait_for(
            run_forever(
                connect,
                collector,
                InMemoryCursorStore(),
                endpoints=[EP_A],
                sleep=FakeSleep(),
                max_sessions=1,
            ),
            GUARD_SECONDS,
        )
    assert connect.urls == [url_for(EP_A, None)]
    assert collector.starts == [None]
    assert any("cursor=none" in m for m in messages(caplog, logging.INFO))


class BrokenCursor:
    def __init__(self, fail_times: int, value: int | None) -> None:
        self.fail_times: int = fail_times
        self.value: int | None = value
        self.loads: int = 0

    async def load(self) -> int | None:
        self.loads += 1
        if self.loads <= self.fail_times:
            raise RuntimeError("redis down")
        return self.value

    async def save(self, time_us: int) -> None:
        self.value = time_us


async def test_36_cursor_load_failure_means_no_connection(caplog: pytest.LogCaptureFixture) -> None:
    connect: ScriptedConnect = ScriptedConnect([identity_frame(A, FIRST_CURSOR + 1)])
    collector: RecordingCollector = RecordingCollector()
    sleep: FakeSleep = FakeSleep()
    with caplog.at_level(logging.DEBUG):
        await asyncio.wait_for(
            run_forever(
                connect,
                collector,
                BrokenCursor(1, FIRST_CURSOR),
                endpoints=[EP_A, EP_B],
                sleep=sleep,
                max_sessions=2,
            ),
            GUARD_SECONDS,
        )
    assert sleep.calls == [1.0]
    assert connect.urls == [url_for(EP_B, FIRST_CURSOR)]
    assert collector.starts == [FIRST_CURSOR]
    assert len(messages(caplog, logging.ERROR)) >= 1


async def test_run_forever_does_not_swallow_cancellation() -> None:
    connect: ScriptedConnect = ScriptedConnect(OSError("down"))

    async def never_sleep(seconds: float) -> None:
        await asyncio.Event().wait()

    task: asyncio.Task[None] = asyncio.create_task(
        run_forever(
            connect,
            RecordingCollector(),
            InMemoryCursorStore(),
            endpoints=[EP_A],
            sleep=never_sleep,
        )
    )
    await yield_loop(100)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, GUARD_SECONDS)


async def test_run_session_with_a_real_collector_publishes_closed_windows() -> None:
    rig: Rig = make_rig()
    raw: list[str | bytes] = [
        post_frame(A, "p1", T0 + 1_000_000, facet_tags=["gamedev"]),
        identity_frame(A, T0 + 61_000_000),
    ]
    handled: int = await asyncio.wait_for(
        run_session(ScriptedConnect(raw), URL, rig.collector), GUARD_SECONDS
    )
    assert handled == 2
    assert rig.cursor.value == T0 + 60_000_000
    assert rig.collector.stats.tagged_posts == 1


async def test_37_websocket_connect_against_a_local_server() -> None:
    from websockets.asyncio.server import Server, ServerConnection, serve

    seen: dict[str, Any] = {}

    async def handler(connection: ServerConnection) -> None:
        request: Any = connection.request
        assert request is not None
        seen["path"] = request.path
        seen["user_agent"] = request.headers.get("User-Agent")
        text: str
        for text in ("one", "two", "three"):
            await connection.send(text)

    server: Server
    source: FrameSource
    async with serve(handler, "127.0.0.1", 0) as server:
        port: int = server.sockets[0].getsockname()[1]
        manager: AbstractAsyncContextManager[FrameSource] = websocket_connect(
            f"ws://127.0.0.1:{port}/subscribe?wantedCollections=app.bsky.feed.post"
        )
        async with manager as source:
            got: list[str | bytes] = [await asyncio.wait_for(source.recv(), GUARD_SECONDS) for _ in range(3)]
            with pytest.raises(ConnectionLost):
                await asyncio.wait_for(source.recv(), GUARD_SECONDS)
    assert got == ["one", "two", "three"]
    assert "wantedCollections=app.bsky.feed.post" in seen["path"]
    assert seen["user_agent"] == "gamedev-trends/0.1"


async def test_37_connect_errors_come_out_as_they_are() -> None:
    manager: AbstractAsyncContextManager[FrameSource] = websocket_connect("ws://127.0.0.1:1/subscribe")
    with pytest.raises(OSError):
        async with manager:
            pass
