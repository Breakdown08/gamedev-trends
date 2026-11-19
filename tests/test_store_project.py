"""Spec checks 2-5: pyproject untouched, store service in compose, README section."""

import re
import subprocess
from pathlib import Path

ROOT: Path = Path(__file__).resolve().parents[1]
BASE_COMMIT: str = "030389a"


def test_02_pyproject_unchanged_since_steam_task() -> None:
    """Since 030389a two dependency lines were added (the WebSocket client, then the MongoDB driver)
    and the integration marker text was widened to the whole stack (2026-11-19)."""
    diff: subprocess.CompletedProcess[str] = subprocess.run(
        ["git", "diff", BASE_COMMIT, "--", "pyproject.toml"], cwd=ROOT, capture_output=True, text=True
    )
    assert diff.returncode == 0, diff.stderr
    lines: list[str] = diff.stdout.splitlines()
    removed: list[str] = [x for x in lines if x.startswith("-") and not x.startswith("---")]
    added: list[str] = [x for x in lines if x.startswith("+") and not x.startswith("+++")]
    assert removed == [
        '-markers = ["integration: needs the docker compose stack '
        '(Kafka on localhost:9092, Redis on localhost:6379)"]'
    ]
    assert added == [
        '+    "websockets>=17.4,<18",',
        '+    "pymongo>=4.20,<5",',
        '+markers = ["integration: needs the docker compose stack '
        "(Kafka on localhost:9092, Redis on localhost:6379, "
        'ClickHouse on localhost:8123, MongoDB on localhost:27017, Grafana on localhost:3000)"]',
    ]


def test_03_store_integration_tests_collected_only_on_request() -> None:
    only: subprocess.CompletedProcess[str] = subprocess.run(
        ["uv", "run", "pytest", "--collect-only", "-q", "-m", "integration"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert "tests/test_store_integration.py::" in only.stdout
    default: subprocess.CompletedProcess[str] = subprocess.run(
        ["uv", "run", "pytest", "--collect-only", "-q"], cwd=ROOT, capture_output=True, text=True
    )
    assert "test_store_integration.py::" not in default.stdout
    assert "test_store_config.py::" in default.stdout


def test_04_compose_has_store_without_image_or_ports() -> None:
    text: str = (ROOT / "compose.yaml").read_text()
    match: re.Match[str] | None = re.search(r"^  store:\n((?:    .*\n|\n)+)", text, re.MULTILINE)
    assert match is not None
    block: str = match.group(1)
    assert "build: ." in block
    assert "gamedev_trends.store" in block
    assert "KAFKA_BOOTSTRAP_SERVERS: kafka:29092" in block
    assert "CLICKHOUSE_URL: http://clickhouse:8123" in block
    assert "CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD" in block
    assert "service_completed_successfully" in block
    assert "service_healthy" in block
    assert "restart: unless-stopped" in block
    assert "image:" not in block
    assert "ports:" not in block


def test_05_readme_documents_storage() -> None:
    text: str = (ROOT / "README.md").read_text()
    assert re.search(r"^#+\s+Storage", text, re.MULTILINE)
    needle: str
    for needle in (
        "FINAL",
        "observations_hourly",
        "CLICKHOUSE_DATABASE",
        "uv run python -m gamedev_trends.store",
    ):
        assert needle in text
    assert "There is no storage" not in text
