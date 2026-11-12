"""Spec checks 37-39: SteamConfig.from_env and main's exit code on bad configuration."""

import socket
import subprocess
import sys
from pathlib import Path
from typing import NoReturn

import pytest

from gamedev_trends.steam import SteamConfig, main

ROOT: Path = Path(__file__).resolve().parents[1]


def test_37_defaults() -> None:
    assert SteamConfig.from_env({}) == SteamConfig()
    config: SteamConfig = SteamConfig()
    assert (config.poll_interval_seconds, config.top_n, config.rate_per_second, config.burst) == (
        3600,
        100,
        1.0,
        10.0,
    )


def test_37_overrides() -> None:
    config: SteamConfig = SteamConfig.from_env(
        {"STEAM_TOP_N": "5", "STEAM_API_BASE": "http://x/", "STEAM_RATE_PER_SECOND": "0.5"}
    )
    assert config.top_n == 5
    assert config.api_base == "http://x"
    assert config.rate_per_second == 0.5
    assert config.max_attempts == 4


def test_37_empty_values_mean_default_and_unknown_are_ignored() -> None:
    env: dict[str, str] = {"STEAM_TOP_N": "", "REDIS_URL": "", "UNRELATED": "x"}
    assert SteamConfig.from_env(env) == SteamConfig()


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("STEAM_TOP_N", "0"),
        ("STEAM_TOP_N", "101"),
        ("STEAM_TOP_N", "abc"),
        ("STEAM_POLL_INTERVAL_SECONDS", "59"),
        ("STEAM_POLL_INTERVAL_SECONDS", "7000"),
        ("STEAM_RATE_PER_SECOND", "0"),
        ("STEAM_BURST", "0.5"),
        ("STEAM_MAX_ATTEMPTS", "0"),
        ("STEAM_BACKOFF_MAX_SECONDS", "0.5"),
    ],
)
def test_38_invalid_value_names_the_variable(name: str, value: str) -> None:
    with pytest.raises(ValueError, match=name):
        SteamConfig.from_env({name: value})


def test_39_main_bad_config_exits_2_without_network(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def no_network(*args: object, **kwargs: object) -> NoReturn:
        raise AssertionError("network access")

    monkeypatch.setenv("STEAM_TOP_N", "abc")
    monkeypatch.setattr(socket.socket, "connect", no_network)
    assert main(["--once"]) == 2
    assert "STEAM_TOP_N" in capsys.readouterr().err


def test_39_module_entry_point_exits_2_on_bad_config() -> None:
    import os

    done: subprocess.CompletedProcess[str] = subprocess.run(
        [sys.executable, "-m", "gamedev_trends.steam", "--once"],
        cwd=ROOT,
        env={**os.environ, "STEAM_TOP_N": "abc"},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert done.returncode == 2
    assert "STEAM_TOP_N" in done.stderr
