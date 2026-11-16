"""Spec checks 38-40: BlueskyConfig.from_env and main's exit code on bad configuration."""

import pytest

from gamedev_trends.bluesky import DEFAULT_TAGS, BlueskyConfig, main
from gamedev_trends.jetstream import DEFAULT_ENDPOINTS

FULL_ENV: dict[str, str] = {
    "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
    "REDIS_URL": "redis://redis:6379/3",
    "BLUESKY_ENDPOINTS": "wss://a.example/subscribe,ws://127.0.0.1:1/subscribe",
    "BLUESKY_TAGS": "godot,gamedev",
    "BLUESKY_WINDOW_SECONDS": "30",
    "BLUESKY_TRACK_HOURS": "48",
    "BLUESKY_QUEUE_SIZE": "500",
    "BLUESKY_IDLE_TIMEOUT_SECONDS": "15",
    "BLUESKY_STATS_SECONDS": "5",
    "BLUESKY_BACKOFF_BASE_SECONDS": "2",
    "BLUESKY_BACKOFF_MAX_SECONDS": "60",
}


def test_38_defaults() -> None:
    assert BlueskyConfig.from_env({}) == BlueskyConfig()
    config: BlueskyConfig = BlueskyConfig()
    assert config.endpoints == DEFAULT_ENDPOINTS
    assert config.tags == DEFAULT_TAGS
    assert (config.window_seconds, config.track_hours, config.queue_size) == (60, 24, 10_000)


def test_38_every_variable_is_read() -> None:
    assert BlueskyConfig.from_env(FULL_ENV) == BlueskyConfig(
        kafka_bootstrap_servers="kafka:29092",
        redis_url="redis://redis:6379/3",
        endpoints=("wss://a.example/subscribe", "ws://127.0.0.1:1/subscribe"),
        tags=("godot", "gamedev"),
        window_seconds=30,
        track_hours=48,
        queue_size=500,
        idle_timeout_seconds=15.0,
        stats_seconds=5.0,
        backoff_base_seconds=2.0,
        backoff_max_seconds=60.0,
    )


def test_38_empty_values_mean_defaults_and_unknown_ones_are_ignored() -> None:
    env: dict[str, str] = {name: "" for name in FULL_ENV} | {"UNRELATED": "x"}
    assert BlueskyConfig.from_env(env) == BlueskyConfig()


def test_38_tags_are_normalised_and_deduplicated_keeping_order() -> None:
    config: BlueskyConfig = BlueskyConfig.from_env({"BLUESKY_TAGS": "#GameDev, pixelart,gamedev"})
    assert config.tags == ("gamedev", "pixelart")


def test_38_single_endpoint() -> None:
    config: BlueskyConfig = BlueskyConfig.from_env({"BLUESKY_ENDPOINTS": "ws://127.0.0.1:1/subscribe"})
    assert config.endpoints == ("ws://127.0.0.1:1/subscribe",)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("BLUESKY_ENDPOINTS", "https://x"),
        ("BLUESKY_ENDPOINTS", "wss://x/subscribe?a=1"),
        ("BLUESKY_ENDPOINTS", "wss://a/s,http://b/s"),
        ("BLUESKY_TAGS", "game dev"),
        ("BLUESKY_TAGS", "1up"),
        ("BLUESKY_TAGS", "a" * 29),
        ("BLUESKY_TAGS", "gamedev,1up"),
        ("BLUESKY_WINDOW_SECONDS", "9"),
        ("BLUESKY_WINDOW_SECONDS", "7"),
        ("BLUESKY_WINDOW_SECONDS", "70"),
        ("BLUESKY_WINDOW_SECONDS", "7200"),
        ("BLUESKY_WINDOW_SECONDS", "abc"),
        ("BLUESKY_TRACK_HOURS", "0"),
        ("BLUESKY_TRACK_HOURS", "169"),
        ("BLUESKY_TRACK_HOURS", "abc"),
        ("BLUESKY_QUEUE_SIZE", "0"),
        ("BLUESKY_QUEUE_SIZE", "1000001"),
        ("BLUESKY_QUEUE_SIZE", "abc"),
        ("BLUESKY_IDLE_TIMEOUT_SECONDS", "0"),
        ("BLUESKY_IDLE_TIMEOUT_SECONDS", "-1"),
        ("BLUESKY_IDLE_TIMEOUT_SECONDS", "abc"),
        ("BLUESKY_STATS_SECONDS", "0"),
        ("BLUESKY_STATS_SECONDS", "abc"),
        ("BLUESKY_BACKOFF_BASE_SECONDS", "0"),
        ("BLUESKY_BACKOFF_BASE_SECONDS", "abc"),
        ("BLUESKY_BACKOFF_MAX_SECONDS", "0.5"),
    ],
)
def test_39_invalid_value_names_the_variable(name: str, value: str) -> None:
    with pytest.raises(ValueError, match=name):
        BlueskyConfig.from_env({name: value})


def test_39_backoff_max_below_a_custom_base() -> None:
    env: dict[str, str] = {"BLUESKY_BACKOFF_BASE_SECONDS": "5", "BLUESKY_BACKOFF_MAX_SECONDS": "2"}
    with pytest.raises(ValueError, match="BLUESKY_BACKOFF_MAX_SECONDS"):
        BlueskyConfig.from_env(env)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("BLUESKY_WINDOW_SECONDS", "10"),
        ("BLUESKY_WINDOW_SECONDS", "3600"),
        ("BLUESKY_TRACK_HOURS", "1"),
        ("BLUESKY_TRACK_HOURS", "168"),
        ("BLUESKY_QUEUE_SIZE", "1"),
        ("BLUESKY_QUEUE_SIZE", "1000000"),
        ("BLUESKY_BACKOFF_MAX_SECONDS", "1"),
    ],
)
def test_39_boundary_values_are_accepted(name: str, value: str) -> None:
    BlueskyConfig.from_env({name: value})


def test_40_main_reports_a_bad_configuration(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("BLUESKY_WINDOW_SECONDS", "7")
    assert main([]) == 2
    err: str = capsys.readouterr().err
    assert "configuration error:" in err
    assert "BLUESKY_WINDOW_SECONDS" in err


@pytest.mark.parametrize("value", ["-1", "0", "abc"])
def test_40_main_rejects_a_bad_duration(value: str) -> None:
    info: pytest.ExceptionInfo[SystemExit]
    with pytest.raises(SystemExit) as info:
        main(["--duration", value])
    assert info.value.code == 2
