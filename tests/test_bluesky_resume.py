"""Spec checks 1-28 of radar-bluesky-resume: in-process resume, the boundary rule, the replay limit."""

import asyncio
import logging
import re
from collections.abc import Callable
from pathlib import Path

import pytest
from bluesky_resume_support import FeedConnect, build_feed, cursor_of, time_of
from bluesky_support import (
    P1,
    P2,
    T0,
    A,
    FakeSleep,
    FlakyPublisher,
    RecordingCollector,
    Rig,
    ScriptedConnect,
    erasures,
    example_events,
    excerpts,
    identity_frame,
    make_rig,
    observations,
    other,
    post_obs,
    stream_obs,
    t,
)
from test_bluesky_collector import EXAMPLE_WINDOW_1

from gamedev_trends import bluesky
from gamedev_trends.bluesky import (
    BlueskyCollector,
    BlueskyConfig,
    InMemoryCursorStore,
    InMemoryPostIndex,
    Stats,
    main,
    run_forever,
)
from gamedev_trends.bus import InMemoryBroker, InMemoryPublisher, PublishError
from gamedev_trends.jetstream import (
    JETSTREAM_COLLECTIONS,
    Engagement,
    JetstreamEvent,
    PostCreated,
    parse_event,
    subscribe_url,
)

EP: str = "ws://test/subscribe"
GUARD_SECONDS: float = 10.0
LOGGER: str = "gamedev_trends.bluesky"
T0_SECONDS: float = T0 / 1e6
STALE: int = 1794744060000000
ENV_NAME: str = "BLUESKY_MAX_REPLAY_SECONDS"
README: Path = Path(__file__).resolve().parents[1] / "README.md"


def like(seconds: float, subject: str = P1) -> Engagement:
    return Engagement(kind="like", subject_uri=subject, time_us=t(seconds))


def messages(caplog: pytest.LogCaptureFixture, prefix: str, level: int | None = None) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.getMessage().startswith(prefix) and (level is None or r.levelno == level)
    ]


def gap_lines(caplog: pytest.LogCaptureFixture) -> list[str]:
    return messages(caplog, "bluesky gap", logging.WARNING)


def session_lines(caplog: pytest.LogCaptureFixture) -> list[str]:
    return messages(caplog, "bluesky session: endpoint=", logging.INFO)


def url_for(cursor: int | None) -> str:
    return subscribe_url(EP, JETSTREAM_COLLECTIONS, cursor)


class CountingCursor(InMemoryCursorStore):
    def __init__(self, value: int | None) -> None:
        super().__init__(value)
        self.loads: int = 0

    async def load(self) -> int | None:
        self.loads += 1
        return self.value


class BrokenCursor(InMemoryCursorStore):
    def __init__(self, fail_times: int, value: int | None) -> None:
        super().__init__(value)
        self.fail_times: int = fail_times
        self.loads: int = 0

    async def load(self) -> int | None:
        self.loads += 1
        if self.loads <= self.fail_times:
            raise RuntimeError("redis down")
        return self.value


def frames_of(*seconds: float) -> list[str | bytes]:
    return [identity_frame(A, t(s)) for s in seconds]


# --- collector (checks 1-13) ---


async def test_01_position_follows_accepted_events() -> None:
    rig: Rig = make_rig()
    assert rig.collector.position_us is None
    await rig.collector.handle([other(1), other(2)])
    assert rig.collector.position_us == t(2)


async def test_01_malformed_and_late_events_do_not_move_the_position() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle([other(70)])
    assert rig.collector.position_us == t(70)
    await rig.collector.handle([None, other(30)])
    assert rig.collector.stats.late == 1
    assert rig.collector.stats.malformed == 1
    assert rig.collector.position_us == t(70)


async def test_01_a_lower_time_inside_the_open_window_does_not_lower_the_position() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle([other(5), other(3)])
    assert rig.collector.stats.late == 0
    assert rig.collector.position_us == t(5)


def test_02_a_collector_without_init_has_no_position() -> None:
    assert RecordingCollector().position_us is None


def test_03_resume_without_events_is_an_error() -> None:
    rig: Rig = make_rig()
    live: bool
    info: pytest.ExceptionInfo[RuntimeError]
    for live in (False, True):
        with pytest.raises(RuntimeError) as info:
            rig.collector.resume(live=live)
        assert info.type is RuntimeError  # NotImplementedError, the stub's error, is a subclass


async def test_04_a_short_session_keeps_the_window_and_the_progress() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle([other(1), other(2)])
    before: Stats = Stats(events=2, windows=0)
    assert rig.collector.resume() == t(2)
    assert rig.collector.window_start_us == t(0)
    assert rig.cursor.value is None
    assert rig.collector.stats == before


async def test_05_a_replay_before_the_position_is_not_counted() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle([other(1), other(2)])
    rig.collector.resume()
    await rig.collector.handle([other(1)])
    assert rig.collector.stats.late == 1
    await rig.collector.handle([other(61)])
    assert observations(rig.broker) == stream_obs(0, events=2, posts=0, likes=0, reposts=0)


async def test_06_the_worked_example_of_the_spec() -> None:
    rig: Rig = make_rig()
    e1: Engagement = like(3, P1)
    e2: Engagement = like(3, P2)
    e3: Engagement = like(3, P1)
    assert e1 == e3
    await rig.collector.handle([other(1), other(2), e1, e2])
    assert rig.collector.position_us == t(3)
    assert rig.collector.resume() == t(3)
    await rig.collector.handle([e2, e1, e3, other(4)])
    assert rig.collector.stats.late == 2
    assert rig.collector.stats.events == 8
    assert rig.collector.stats.likes == 3
    await rig.collector.handle([other(61)])
    assert observations(rig.broker) == stream_obs(0, events=6, posts=0, likes=3, reposts=0)


async def test_07_equal_events_on_the_boundary_are_a_multiset() -> None:
    rig: Rig = make_rig()
    e: Engagement = like(3)
    await rig.collector.handle([e, e])
    assert rig.collector.resume() == t(3)
    await rig.collector.handle([e, e, e])
    assert rig.collector.stats.late == 2
    assert rig.collector.stats.likes == 3
    await rig.collector.handle([other(61)])
    assert observations(rig.broker) == stream_obs(0, events=3, posts=0, likes=3, reposts=0)


async def test_08_a_new_event_on_the_boundary_microsecond_is_applied() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle([other(1), like(3)])
    rig.collector.resume()
    await rig.collector.handle([PostCreated(did=A, rkey="p1", time_us=t(3), tags=("gamedev",))])
    assert rig.collector.stats.late == 0
    await rig.collector.handle([other(61)])
    assert post_obs(P1, "tag_gamedev", 1) in observations(rig.broker)
    assert observations(rig.broker)[:4] == stream_obs(0, events=3, posts=1, likes=1, reposts=0)


async def test_09_two_resumes_in_a_row_skip_the_boundary_once() -> None:
    rig: Rig = make_rig()
    e: Engagement = like(3)
    await rig.collector.handle([other(1), e])
    assert rig.collector.resume() == t(3)
    assert rig.collector.resume() == t(3)
    await rig.collector.handle([e, e])
    assert rig.collector.stats.late == 1
    assert rig.collector.stats.likes == 2


async def test_09_every_resume_skips_the_boundary_again() -> None:
    rig: Rig = make_rig()
    e: Engagement = like(3)
    await rig.collector.handle([other(1), e])
    rig.collector.resume()
    await rig.collector.handle([e])
    assert rig.collector.stats.late == 1
    assert rig.collector.resume() == t(3)
    await rig.collector.handle([e])
    assert rig.collector.stats.late == 2
    assert rig.collector.stats.likes == 1


async def test_10_the_boundary_moves_with_the_position() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle([other(1), other(3)])
    rig.collector.resume()
    await rig.collector.handle([other(3), other(5)])
    assert rig.collector.stats.late == 1
    assert rig.collector.position_us == t(5)
    assert rig.collector.resume() == t(5)
    await rig.collector.handle([other(3)])
    assert rig.collector.stats.late == 2
    await rig.collector.handle([other(5), other(5)])
    assert rig.collector.stats.late == 3
    await rig.collector.handle([other(61)])
    assert observations(rig.broker) == stream_obs(0, events=4, posts=0, likes=0, reposts=0)


async def test_11_a_live_resume_keeps_the_window_and_skips_nothing() -> None:
    rig: Rig = make_rig()
    e: Engagement = like(3)
    await rig.collector.handle([other(1), e])
    assert rig.collector.resume(live=True) is None
    assert rig.collector.window_start_us == t(0)
    assert rig.collector.position_us == t(3)
    await rig.collector.handle([e, other(2)])
    assert rig.collector.stats.late == 0
    assert rig.collector.stats.likes == 2


async def test_12_start_drops_the_position_the_window_and_the_skips() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle([other(1), other(2)])
    rig.collector.start(t(0))
    assert rig.collector.position_us is None
    assert rig.collector.window_start_us is None


async def test_12_start_clears_the_boundary_skips() -> None:
    rig: Rig = make_rig()
    await rig.collector.handle([other(1), other(2)])
    rig.collector.resume()
    rig.collector.start(t(2))
    await rig.collector.handle([other(2)])
    assert rig.collector.stats.late == 0
    assert rig.collector.position_us == t(2)


async def test_13_a_publish_failure_then_resume() -> None:
    broker: InMemoryBroker = InMemoryBroker()
    cursor: InMemoryCursorStore = InMemoryCursorStore()
    collector: BlueskyCollector = BlueskyCollector(
        InMemoryPostIndex(), FlakyPublisher(InMemoryPublisher(broker), 1), cursor
    )
    collector.start(None)
    events: list[JetstreamEvent | None] = example_events()
    with pytest.raises(PublishError):
        await collector.handle(events[:7])
    assert collector.position_us == t(6)
    assert collector.resume() == t(6)
    await collector.handle(events[5:7])
    assert observations(broker) == EXAMPLE_WINDOW_1
    assert cursor.value == t(60)
    assert collector.stats.windows == 1


# --- run_forever (checks 14-23) ---


async def test_14_the_cursor_is_loaded_once_per_process() -> None:
    cursor: CountingCursor = CountingCursor(None)
    await asyncio.wait_for(
        run_forever(
            ScriptedConnect(frames_of(1)),
            RecordingCollector(),
            cursor,
            endpoints=[EP],
            sleep=FakeSleep(),
            max_sessions=3,
        ),
        GUARD_SECONDS,
    )
    assert cursor.loads == 1


async def test_15_a_failing_load_is_repeated_until_it_works_and_then_not_again(
    caplog: pytest.LogCaptureFixture,
) -> None:
    cursor: BrokenCursor = BrokenCursor(2, t(0))
    connect: ScriptedConnect = ScriptedConnect(frames_of(1))
    collector: RecordingCollector = RecordingCollector()
    with caplog.at_level(logging.DEBUG):
        await asyncio.wait_for(
            run_forever(connect, collector, cursor, endpoints=[EP], sleep=FakeSleep(), max_sessions=4),
            GUARD_SECONDS,
        )
    assert cursor.loads == 3
    assert len(connect.urls) == 2
    assert collector.starts == [t(0), t(0)]
    assert len([r for r in caplog.records if r.levelno == logging.ERROR]) >= 2


async def test_16_the_second_session_continues_from_the_position(caplog: pytest.LogCaptureFixture) -> None:
    rig: Rig = make_rig()
    feed: list[str] = [identity_frame(A, t(i)) for i in range(1, 6)]
    connect: FeedConnect = FeedConnect(feed, [3, 2])
    with caplog.at_level(logging.DEBUG):
        await asyncio.wait_for(
            run_forever(
                connect,
                rig.collector,
                InMemoryCursorStore(None),
                endpoints=[EP],
                sleep=FakeSleep(),
                max_sessions=2,
            ),
            GUARD_SECONDS,
        )
    assert connect.urls == [url_for(None), url_for(t(3))]
    lines: list[str] = session_lines(caplog)
    assert len(lines) == 2
    assert f"cursor={t(3)} mode=resume" in lines[1]
    assert "cursor=none mode=live lag=none" in lines[0]
    assert rig.collector.position_us == t(4)
    assert rig.collector.stats.late == 1


async def test_17_modes_and_lag_in_the_session_line(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.DEBUG):
        await asyncio.wait_for(
            run_forever(
                ScriptedConnect(frames_of(4)),
                RecordingCollector(),
                InMemoryCursorStore(t(3)),
                endpoints=[EP],
                sleep=FakeSleep(),
                clock=lambda: T0_SECONDS + 15.5,
                max_sessions=1,
            ),
            GUARD_SECONDS,
        )
    assert session_lines(caplog) == [f"bluesky session: endpoint={EP} cursor={t(3)} mode=start lag=12.5"]


async def test_17_without_a_cursor_the_mode_is_live(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.DEBUG):
        await asyncio.wait_for(
            run_forever(
                ScriptedConnect(frames_of(4)),
                RecordingCollector(),
                InMemoryCursorStore(None),
                endpoints=[EP],
                sleep=FakeSleep(),
                max_sessions=1,
            ),
            GUARD_SECONDS,
        )
    assert session_lines(caplog) == [f"bluesky session: endpoint={EP} cursor=none mode=live lag=none"]


async def test_18_a_stale_cursor_at_start_is_a_gap(caplog: pytest.LogCaptureFixture) -> None:
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
                clock=lambda: 1794744060.0 + 400,
                max_sessions=2,
            ),
            GUARD_SECONDS,
        )
    assert connect.urls == [url_for(None), url_for(None)]
    assert collector.starts == [None, None]
    assert gap_lines(caplog) == [
        "bluesky gap: from=1794744060000000 to=1794744460000000 seconds=400 max_replay=300; "
        "reading the live stream"
    ]
    record: logging.LogRecord = next(r for r in caplog.records if r.getMessage().startswith("bluesky gap"))
    assert record.name == LOGGER
    assert all("mode=live" in s and "cursor=none" in s for s in session_lines(caplog))


async def test_19_a_cursor_exactly_on_the_limit_is_replayed(caplog: pytest.LogCaptureFixture) -> None:
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
                clock=lambda: 1794744060.0 + 300,
                max_sessions=1,
            ),
            GUARD_SECONDS,
        )
    assert connect.urls == [url_for(STALE)]
    assert collector.starts == [STALE]
    assert gap_lines(caplog) == []
    assert "mode=start" in session_lines(caplog)[0]


def clock_by_session(connect: ScriptedConnect, seconds: list[float]) -> Callable[[], float]:
    """Unix time of the current iteration: the value for the n-th session (counted by connects so far)."""

    def clock() -> float:
        return T0_SECONDS + seconds[min(len(connect.urls), len(seconds) - 1)]

    return clock


async def test_22_a_stale_position_inside_the_process_goes_live_and_keeps_the_window(
    caplog: pytest.LogCaptureFixture,
) -> None:
    rig: Rig = make_rig()
    connect: ScriptedConnect = ScriptedConnect(frames_of(1), frames_of(1200))
    with caplog.at_level(logging.DEBUG):
        await asyncio.wait_for(
            run_forever(
                connect,
                rig.collector,
                rig.cursor,
                endpoints=[EP],
                sleep=FakeSleep(),
                max_replay_seconds=300,
                clock=clock_by_session(connect, [2, 1000]),
                max_sessions=2,
            ),
            GUARD_SECONDS,
        )
    assert connect.urls == [url_for(None), url_for(None)]
    assert gap_lines(caplog) == [
        f"bluesky gap: from={t(1)} to={t(1000)} seconds=999 max_replay=300; reading the live stream"
    ]
    assert session_lines(caplog)[1].endswith("cursor=none mode=live lag=none")
    assert observations(rig.broker) == stream_obs(0, events=1, posts=0, likes=0, reposts=0)
    assert rig.cursor.value == t(1200)


async def test_22_a_fresh_position_inside_the_process_is_resumed(caplog: pytest.LogCaptureFixture) -> None:
    rig: Rig = make_rig()
    connect: ScriptedConnect = ScriptedConnect(frames_of(1))
    with caplog.at_level(logging.DEBUG):
        await asyncio.wait_for(
            run_forever(
                connect,
                rig.collector,
                rig.cursor,
                endpoints=[EP],
                sleep=FakeSleep(),
                max_replay_seconds=300,
                clock=clock_by_session(connect, [2, 300]),
                max_sessions=2,
            ),
            GUARD_SECONDS,
        )
    assert connect.urls == [url_for(None), url_for(t(1))]
    assert gap_lines(caplog) == []


async def test_23_without_a_limit_even_an_old_cursor_is_replayed(caplog: pytest.LogCaptureFixture) -> None:
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
                clock=lambda: T0_SECONDS + 7 * 86400,
                max_sessions=2,
            ),
            GUARD_SECONDS,
        )
    assert connect.urls == [url_for(t(0))] * 2
    assert gap_lines(caplog) == []
    assert session_lines(caplog)[0] == (
        f"bluesky session: endpoint={EP} cursor={t(0)} mode=start lag=604800.0"
    )


# --- equivalence (checks 20, 21) ---

# frames per session; every one spans less than a window of the feed
SESSIONS: list[int] = [4, 5, 5, 5, 5, 5, 5]


async def feed_in_pieces(sessions: list[int]) -> tuple[Rig, FeedConnect]:
    rig: Rig = make_rig()
    connect: FeedConnect = FeedConnect(build_feed(), sessions)
    await asyncio.wait_for(
        run_forever(
            connect,
            rig.collector,
            rig.cursor,
            endpoints=[EP],
            sleep=FakeSleep(),
            max_sessions=len(sessions),
        ),
        GUARD_SECONDS,
    )
    return rig, connect


async def test_20_the_feed_cut_into_short_sessions_equals_the_feed_in_one_piece() -> None:
    feed: list[str] = build_feed()
    whole: Rig = make_rig()
    await whole.collector.handle([parse_event(f) for f in feed])
    assert whole.collector.stats.windows >= 3
    assert whole.cursor.value == t(180)

    pieces: Rig
    connect: FeedConnect
    pieces, connect = await feed_in_pieces(SESSIONS)

    # the scenario is what the spec asks for
    assert connect.ends[-1] == len(feed)
    served: list[str]
    for served in connect.served:
        assert time_of(served[-1]) - time_of(served[0]) < 60 * 10**6
    inside_group: int = sum(1 for end in connect.ends[:-1] if time_of(feed[end - 1]) == time_of(feed[end]))
    assert inside_group >= 2
    groups: set[int] = {time_of(f) for f in feed if sum(time_of(g) == time_of(f) for g in feed) >= 2}
    assert len(groups) >= 3

    assert observations(pieces.broker) == observations(whole.broker)
    assert erasures(pieces.broker) == erasures(whole.broker)
    assert excerpts(pieces.broker) == excerpts(whole.broker)
    assert pieces.cursor.value == whole.cursor.value == t(180)
    field: str
    for field in ("posts", "likes", "reposts", "tagged_posts", "engagements", "windows"):
        assert getattr(pieces.collector.stats, field) == getattr(whole.collector.stats, field), field
    assert pieces.collector.stats.windows >= 3
    assert pieces.collector.stats.late > 0


async def test_21_the_url_cursor_grows_from_session_to_session() -> None:
    result: tuple[Rig, FeedConnect] = await feed_in_pieces(SESSIONS)
    connect: FeedConnect = result[1]
    cursors: list[int | None] = [cursor_of(u) for u in connect.urls]
    assert cursors[0] is None
    values: list[int] = [c for c in cursors[1:] if c is not None]
    assert len(values) == len(SESSIONS) - 1
    assert values == sorted(set(values))


# --- configuration (checks 24-27) ---


def test_24_the_default_limit() -> None:
    assert BlueskyConfig().max_replay_seconds == 300
    assert BlueskyConfig.from_env({}).max_replay_seconds == 300
    assert BlueskyConfig.from_env({}) == BlueskyConfig()


def test_25_the_variable_is_read() -> None:
    assert BlueskyConfig.from_env({ENV_NAME: "60"}).max_replay_seconds == 60
    assert BlueskyConfig.from_env({ENV_NAME: ""}).max_replay_seconds == 300
    assert BlueskyConfig.from_env({ENV_NAME: "1"}).max_replay_seconds == 1


@pytest.mark.parametrize(("max_replay", "window", "limit"), [(300, 60, 300), (300, 600, 1200), (60, 60, 120)])
def test_25a_the_effective_limit(max_replay: int, window: int, limit: int) -> None:
    assert BlueskyConfig(max_replay_seconds=max_replay, window_seconds=window).replay_limit_seconds == limit
    config: BlueskyConfig = BlueskyConfig.from_env(
        {ENV_NAME: str(max_replay), "BLUESKY_WINDOW_SECONDS": str(window)}
    )
    assert config.replay_limit_seconds == limit


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ("0", f"{ENV_NAME}: must be >= 1, got 0"),
        ("-5", f"{ENV_NAME}: must be >= 1, got -5"),
        ("1.5", f"{ENV_NAME}: expected an integer, got '1.5'"),
        ("abc", f"{ENV_NAME}: expected an integer, got 'abc'"),
    ],
)
def test_26_a_bad_limit_is_rejected(raw: str, message: str) -> None:
    with pytest.raises(ValueError, match=re.escape(message)):
        BlueskyConfig.from_env({ENV_NAME: raw})


def test_26_main_returns_2_on_a_bad_limit(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    async def not_reached(config: BlueskyConfig, duration: float | None) -> int:
        return 99

    monkeypatch.setattr(bluesky, "_run", not_reached)
    monkeypatch.setenv(ENV_NAME, "0")
    assert main([]) == 2
    assert ENV_NAME in capsys.readouterr().err


async def test_27_serve_passes_the_effective_limit(
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
    config: BlueskyConfig = BlueskyConfig(max_replay_seconds=300, window_seconds=600)
    with caplog.at_level(logging.INFO):
        await bluesky._serve(config, None)
    assert seen["max_replay_seconds"] == 1200
    assert "clock" not in seen
    assert messages(caplog, "bluesky replay limit:") == ["bluesky replay limit: 1200s"]


# --- README (check 28) ---


def test_28_the_readme_describes_the_limit_and_the_new_cursor() -> None:
    text: str = README.read_text()
    assert ENV_NAME in text
    assert "bluesky gap" in text
    assert "After a reconnect or restart the Jetstream is read again from the cursor" not in text
