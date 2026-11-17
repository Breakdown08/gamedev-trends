"""Spec checks 1-18 of radar-bluesky-reconnect: BLUESKY_RECONNECT=live reads the live stream after a drop."""

import asyncio
import logging
import re
from collections.abc import Callable
from typing import cast

import pytest
from bluesky_resume_support import FeedConnect, build_feed, cursor_of
from bluesky_support import (
    T0,
    A,
    FakeSleep,
    RecordingCollector,
    Rig,
    ScriptedConnect,
    identity_frame,
    make_rig,
    observations,
    stream_obs,
    t,
)
from dashboards_support import ROOT, compose_block

from gamedev_trends import bluesky
from gamedev_trends.bluesky import (
    BlueskyConfig,
    InMemoryCursorStore,
    Reconnect,
    main,
    run_forever,
)
from gamedev_trends.jetstream import JETSTREAM_COLLECTIONS, parse_event, subscribe_url

EP: str = "ws://test/subscribe"
GUARD_SECONDS: float = 10.0
LOGGER: str = "gamedev_trends.bluesky"
T0_SECONDS: float = T0 / 1e6
STALE: int = 1794744060000000
ENV_NAME: str = "BLUESKY_RECONNECT"
SESSIONS: list[int] = [4, 5, 5, 5, 5, 5, 5]


def messages(caplog: pytest.LogCaptureFixture, prefix: str) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.getMessage().startswith(prefix)]


def gap_texts(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in messages(caplog, "bluesky gap")]


def live_gap_texts(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [s for s in gap_texts(caplog) if "reconnect=live" in s]


def session_lines(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in messages(caplog, "bluesky session: endpoint=")]


def url_for(cursor: int | None) -> str:
    return subscribe_url(EP, JETSTREAM_COLLECTIONS, cursor)


def frames_of(*seconds: float) -> list[str | bytes]:
    return [identity_frame(A, t(s)) for s in seconds]


def clock_by_session(connect: ScriptedConnect, seconds: list[float]) -> Callable[[], float]:
    def clock() -> float:
        return T0_SECONDS + seconds[min(len(connect.urls), len(seconds) - 1)]

    return clock


def gap_line(from_s: float, to_s: float, seconds: str) -> str:
    return (
        f"bluesky gap: from={t(from_s)} to={t(to_s)} seconds={seconds} reconnect=live; "
        "reading the live stream"
    )


class CountingCursor(InMemoryCursorStore):
    def __init__(self, value: int | None) -> None:
        super().__init__(value)
        self.loads: int = 0

    async def load(self) -> int | None:
        self.loads += 1
        return self.value


async def drive(
    connect: ScriptedConnect,
    rig: Rig,
    clock_seconds: list[float],
    *,
    reconnect: Reconnect | None,
    sessions: int,
    max_replay_seconds: int | None = None,
) -> None:
    if reconnect is None:
        await asyncio.wait_for(
            run_forever(
                connect,
                rig.collector,
                rig.cursor,
                endpoints=[EP],
                sleep=FakeSleep(),
                max_replay_seconds=max_replay_seconds,
                clock=clock_by_session(connect, clock_seconds),
                max_sessions=sessions,
            ),
            GUARD_SECONDS,
        )
        return
    await asyncio.wait_for(
        run_forever(
            connect,
            rig.collector,
            rig.cursor,
            endpoints=[EP],
            sleep=FakeSleep(),
            max_replay_seconds=max_replay_seconds,
            reconnect=reconnect,
            clock=clock_by_session(connect, clock_seconds),
            max_sessions=sessions,
        ),
        GUARD_SECONDS,
    )


# --- run_forever (checks 1-10) ---


async def test_01_the_spec_example(caplog: pytest.LogCaptureFixture) -> None:
    rig: Rig = make_rig()
    connect: ScriptedConnect = ScriptedConnect(frames_of(1, 2), frames_of(5))
    with caplog.at_level(logging.DEBUG):
        await drive(connect, rig, [2.5, 4.5], reconnect="live", sessions=2, max_replay_seconds=300)
    assert connect.urls == [url_for(None), url_for(None)]
    records: list[logging.LogRecord] = messages(caplog, "bluesky gap")
    assert len(records) == 1
    assert records[0].levelno == logging.INFO
    assert records[0].name == LOGGER
    assert records[0].getMessage() == gap_line(2, 4.5, "2.5")
    assert session_lines(caplog)[1].endswith("cursor=none mode=live lag=none")
    assert session_lines(caplog)[1] == f"bluesky session: endpoint={EP} cursor=none mode=live lag=none"
    assert rig.collector.window_start_us == t(0)


async def test_02_the_window_is_kept(caplog: pytest.LogCaptureFixture) -> None:
    rig: Rig = make_rig()
    connect: ScriptedConnect = ScriptedConnect(frames_of(1), frames_of(30), frames_of(61))
    with caplog.at_level(logging.DEBUG):
        await drive(connect, rig, [1, 2, 31], reconnect="live", sessions=3)
    assert observations(rig.broker) == stream_obs(0, events=2, posts=0, likes=0, reposts=0)
    assert connect.urls == [url_for(None)] * 3
    assert rig.cursor.value == t(60)


async def test_03_every_reconnect_gives_a_line_and_live_mode(caplog: pytest.LogCaptureFixture) -> None:
    rig: Rig = make_rig()
    connect: ScriptedConnect = ScriptedConnect(frames_of(1), frames_of(10), frames_of(20))
    with caplog.at_level(logging.DEBUG):
        await drive(connect, rig, [0.5, 2, 12], reconnect="live", sessions=3)
    assert live_gap_texts(caplog) == [gap_line(1, 2, "1.0"), gap_line(10, 12, "2.0")]
    lines: list[str] = session_lines(caplog)
    assert len(lines) == 3
    assert "mode=live" in lines[1] and "mode=live" in lines[2]
    assert connect.urls == [url_for(None)] * 3


async def test_04_the_lines_do_not_overlap(caplog: pytest.LogCaptureFixture) -> None:
    rig: Rig = make_rig()
    connect: ScriptedConnect = ScriptedConnect(frames_of(1), OSError("down"), frames_of(10))
    with caplog.at_level(logging.DEBUG):
        await drive(connect, rig, [2, 3, 5], reconnect="live", sessions=3)
    assert live_gap_texts(caplog) == [gap_line(1, 3, "2.0"), gap_line(3, 5, "2.0")]


async def test_05_the_limit_beats_the_mode(caplog: pytest.LogCaptureFixture) -> None:
    rig: Rig = make_rig()
    connect: ScriptedConnect = ScriptedConnect(frames_of(1), frames_of(1200))
    with caplog.at_level(logging.DEBUG):
        await drive(connect, rig, [2, 1000], reconnect="live", sessions=2, max_replay_seconds=300)
    assert gap_texts(caplog) == [
        f"bluesky gap: from={t(1)} to={t(1000)} seconds=999 max_replay=300; reading the live stream"
    ]
    assert live_gap_texts(caplog) == []
    assert messages(caplog, "bluesky gap")[0].levelno == logging.WARNING


async def test_06_start_is_unchanged_under_live(caplog: pytest.LogCaptureFixture) -> None:
    connect: ScriptedConnect = ScriptedConnect(frames_of(1))
    collector: RecordingCollector = RecordingCollector()
    with caplog.at_level(logging.DEBUG):
        await asyncio.wait_for(
            run_forever(
                connect,
                collector,
                InMemoryCursorStore(t(0)),
                endpoints=[EP],
                sleep=FakeSleep(),
                max_replay_seconds=300,
                reconnect="live",
                clock=lambda: T0_SECONDS + 15.5,
                max_sessions=1,
            ),
            GUARD_SECONDS,
        )
    assert connect.urls == [url_for(t(0))]
    assert session_lines(caplog) == [f"bluesky session: endpoint={EP} cursor={t(0)} mode=start lag=15.5"]
    assert gap_texts(caplog) == []


async def test_06_a_stale_cursor_at_start_is_an_old_style_gap(caplog: pytest.LogCaptureFixture) -> None:
    connect: ScriptedConnect = ScriptedConnect(frames_of(1))
    collector: RecordingCollector = RecordingCollector()
    with caplog.at_level(logging.DEBUG):
        await asyncio.wait_for(
            run_forever(
                connect,
                collector,
                InMemoryCursorStore(STALE),
                endpoints=[EP],
                sleep=FakeSleep(),
                max_replay_seconds=300,
                reconnect="live",
                clock=lambda: 1794744060.0 + 400,
                max_sessions=1,
            ),
            GUARD_SECONDS,
        )
    assert connect.urls == [url_for(None)]
    assert gap_texts(caplog) == [
        "bluesky gap: from=1794744060000000 to=1794744460000000 seconds=400 max_replay=300; "
        "reading the live stream"
    ]
    assert "mode=live" in session_lines(caplog)[0]


@pytest.mark.parametrize("explicit", [False, True])
async def test_07_replay_is_the_default(explicit: bool, caplog: pytest.LogCaptureFixture) -> None:
    rig: Rig = make_rig()
    connect: ScriptedConnect = ScriptedConnect(frames_of(1, 2), frames_of(5))
    with caplog.at_level(logging.DEBUG):
        await drive(
            connect,
            rig,
            [2.5, 4.5],
            reconnect="replay" if explicit else None,
            sessions=2,
            max_replay_seconds=300,
        )
    assert connect.urls == [url_for(None), url_for(t(2))]
    assert gap_texts(caplog) == []
    assert session_lines(caplog)[1].endswith("mode=resume lag=2.5")


async def feed_in_pieces(reconnect: Reconnect) -> tuple[Rig, FeedConnect]:
    rig: Rig = make_rig()
    connect: FeedConnect = FeedConnect(build_feed(), SESSIONS)
    await asyncio.wait_for(
        run_forever(
            connect,
            rig.collector,
            rig.cursor,
            endpoints=[EP],
            sleep=FakeSleep(),
            reconnect=reconnect,
            max_sessions=len(SESSIONS),
        ),
        GUARD_SECONDS,
    )
    return rig, connect


async def test_08_replay_equals_the_feed_in_one_piece() -> None:
    whole: Rig = make_rig()
    await whole.collector.handle([parse_event(f) for f in build_feed()])
    pieces: Rig
    connect: FeedConnect
    pieces, connect = await feed_in_pieces("replay")
    assert observations(pieces.broker) == observations(whole.broker)
    assert pieces.cursor.value == whole.cursor.value
    cursors: list[int | None] = [cursor_of(u) for u in connect.urls]
    assert cursors[0] is None
    assert all(c is not None for c in cursors[1:])


async def test_09_live_never_puts_a_cursor_into_the_url() -> None:
    result: tuple[Rig, FeedConnect] = await feed_in_pieces("live")
    connect: FeedConnect = result[1]
    assert len(connect.urls) == len(SESSIONS)
    assert [cursor_of(u) for u in connect.urls] == [None] * len(SESSIONS)


async def test_10_a_bad_mode_is_rejected_before_anything_happens() -> None:
    connect: ScriptedConnect = ScriptedConnect(frames_of(1))
    cursor: CountingCursor = CountingCursor(None)
    rig: Rig = make_rig()
    with pytest.raises(ValueError, match="'fast'"):
        await asyncio.wait_for(
            run_forever(
                connect,
                rig.collector,
                cursor,
                endpoints=[EP],
                sleep=FakeSleep(),
                reconnect=cast(Reconnect, "fast"),
                max_sessions=1,
            ),
            GUARD_SECONDS,
        )
    assert connect.urls == []
    assert cursor.loads == 0


# --- configuration (checks 11-15) ---


def test_11_the_default_mode() -> None:
    assert BlueskyConfig().reconnect == "replay"
    assert BlueskyConfig.from_env({}).reconnect == "replay"
    assert BlueskyConfig.from_env({ENV_NAME: ""}).reconnect == "replay"
    assert BlueskyConfig.from_env({}) == BlueskyConfig()


@pytest.mark.parametrize(("raw", "mode"), [("live", "live"), (" live ", "live"), ("replay", "replay")])
def test_12_the_variable_is_read(raw: str, mode: str) -> None:
    assert BlueskyConfig.from_env({ENV_NAME: raw}).reconnect == mode


@pytest.mark.parametrize("raw", ["LIVE", "resume", "fast"])
def test_13_a_bad_mode_is_rejected(raw: str) -> None:
    message: str = f"{ENV_NAME}: expected replay or live, got {raw!r}"
    with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
        BlueskyConfig.from_env({ENV_NAME: raw})


def test_14_main_returns_2_on_a_bad_mode(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    async def not_reached(config: BlueskyConfig, duration: float | None) -> int:
        return 99

    monkeypatch.setattr(bluesky, "_run", not_reached)
    monkeypatch.setenv(ENV_NAME, "fast")
    assert main([]) == 2
    assert ENV_NAME in capsys.readouterr().err


async def test_15_serve_passes_the_mode(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    import gamedev_trends.kafka_bus as kafka_bus

    seen: dict[str, object] = {}

    class FakePublisher:
        def __init__(self, bootstrap_servers: str) -> None:
            return None

        async def close(self) -> None:
            return None

    async def fake_run_forever(*args: object, **kwargs: object) -> None:
        seen.update(kwargs)

    monkeypatch.setattr(kafka_bus, "KafkaPublisher", FakePublisher)
    monkeypatch.setattr(bluesky, "run_forever", fake_run_forever)
    with caplog.at_level(logging.INFO):
        await bluesky._serve(BlueskyConfig(reconnect="live"), None)
    assert seen["reconnect"] == "live"
    found: list[logging.LogRecord] = messages(caplog, "bluesky reconnect:")
    assert [r.getMessage() for r in found] == ["bluesky reconnect: live"]
    assert found[0].levelno == logging.INFO
    assert found[0].name == LOGGER


# --- project (checks 17, 18) ---


def test_17_compose_sets_live_with_a_comment() -> None:
    block: str = compose_block("bluesky-collector")
    lines: list[str] = block.splitlines()
    index: int = next(i for i, line in enumerate(lines) if line.strip() == "BLUESKY_RECONNECT: live")
    assert lines[index - 1].strip().startswith("#")
    assert all(len(line) <= 110 for line in lines)


def test_18_the_readme_describes_the_mode() -> None:
    text: str = (ROOT / "README.md").read_text()
    assert "BLUESKY_RECONNECT" in text
    assert "reconnect=live" in text
    assert any(line.startswith("| `BLUESKY_RECONNECT` | `replay` |") for line in text.splitlines())
