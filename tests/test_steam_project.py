"""Spec checks 2-3 and the static parts of 44-45: dependencies, marker, Dockerfile, compose, README."""

import re
import subprocess
import tomllib
from pathlib import Path
from typing import Any

ROOT: Path = Path(__file__).resolve().parents[1]
FORBIDDEN_DEPS: set[str] = {"respx", "tenacity", "fakeredis", "pydantic-settings"}


def _names(requirements: list[str]) -> set[str]:
    return {re.split(r"[<>=!~\[ ;]", r, maxsplit=1)[0].lower() for r in requirements}


def test_02_dependencies() -> None:
    data: dict[str, Any] = tomllib.loads((ROOT / "pyproject.toml").read_text())
    runtime: set[str] = _names(data["project"]["dependencies"])
    assert {"httpx", "redis"} <= runtime
    everything: set[str] = set(runtime)
    group: list[Any]
    for group in data.get("dependency-groups", {}).values():
        everything |= _names([g for g in group if isinstance(g, str)])
    assert not FORBIDDEN_DEPS & everything


def test_03_marker_text_and_redis_integration_tests_collected() -> None:
    data: dict[str, Any] = tomllib.loads((ROOT / "pyproject.toml").read_text())
    assert (
        "integration: needs the docker compose stack (Kafka on localhost:9092, Redis on localhost:6379)"
        in data["tool"]["pytest"]["ini_options"]["markers"]
    )
    only: subprocess.CompletedProcess[str] = subprocess.run(
        ["uv", "run", "pytest", "--collect-only", "-q", "-m", "integration"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    name: str
    for name in ("test_40_", "test_41_", "test_42_"):
        assert f"test_ratelimit_redis.py::{name}" in only.stdout
    default: subprocess.CompletedProcess[str] = subprocess.run(
        ["uv", "run", "pytest", "--collect-only", "-q"], cwd=ROOT, capture_output=True, text=True
    )
    assert "test_ratelimit_redis.py::test_40_" not in default.stdout


def test_44_dockerfile() -> None:
    text: str = (ROOT / "Dockerfile").read_text()
    assert "ghcr.io/astral-sh/uv:0.12.11" in text
    assert "python:3.14.7-slim-trixie" in text
    assert "--locked" in text
    assert "--no-dev" in text
    assert re.search(r"^USER\s+(nobody|\d+)", text, re.MULTILINE)
    assert "gamedev_trends.steam" in text


def test_44_dockerignore() -> None:
    lines: set[str] = set((ROOT / ".dockerignore").read_text().split())
    assert {
        ".git",
        ".venv",
        ".env",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        "__pycache__",
        "tests",
    } <= lines


def test_44_compose_has_steam_collector_without_image_or_ports() -> None:
    text: str = (ROOT / "compose.yaml").read_text()
    match: re.Match[str] | None = re.search(r"^  steam-collector:\n((?:    .*\n|\n)+)", text, re.MULTILINE)
    assert match is not None
    block: str = match.group(1)
    assert "build: ." in block
    assert "KAFKA_BOOTSTRAP_SERVERS: kafka:29092" in block
    assert "REDIS_URL: redis://redis:6379/0" in block
    assert 'restart: "no"' in block
    assert "service_completed_successfully" in block
    assert "service_healthy" in block
    assert "image:" not in block
    assert "ports:" not in block


def test_readme_documents_steam_collector() -> None:
    text: str = (ROOT / "README.md").read_text()
    assert re.search(r"^#+\s+Steam collector", text, re.MULTILINE)
    assert "uv run python -m gamedev_trends.steam --once" in text
    assert "docker compose up -d --wait" in text
    assert "STEAM_RATE_PER_SECOND" in text
