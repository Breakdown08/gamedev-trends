"""Spec checks 2-5 and static parts of the project layout."""

import hashlib
import re
import subprocess
from pathlib import Path

ROOT: Path = Path(__file__).resolve().parents[1]
ANNOTATIONS_SHA256: str = "e7b25e2338acc6522ad4e5af1174a6582df1fa91b0eb4b3883368c3a28d3714d"
CI_STEPS: list[str] = [
    "uv sync --locked",
    "uv run ruff check",
    "uv run ruff format --check",
    "uv run ty check",
    "uv run mypy",
    "uv run pytest",
]
FORBIDDEN: list[str] = ["kad" + "ath", "vau" + "lt"]
SKIP_DIRS: set[str] = {".git", ".venv", ".mypy_cache", ".pytest_cache", ".ruff_cache", "__pycache__"}


def test_02_annotations_test_matches_handbinder_sample() -> None:
    data: bytes = (ROOT / "tests" / "test_annotations.py").read_bytes()
    assert hashlib.sha256(data).hexdigest() == ANNOTATIONS_SHA256


def test_03_ci_steps_in_order() -> None:
    text: str = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
    runs: list[str] = [m.strip() for m in re.findall(r"^\s*-?\s*run:\s*(.+)$", text, re.MULTILINE)]
    assert runs == CI_STEPS
    assert "ubuntu-24.04" in text
    assert "actions/checkout@v4" in text
    assert "astral-sh/setup-uv@v6" in text
    assert "docker" not in text.lower()


def test_04_integration_marker_collected_but_deselected_by_default() -> None:
    only: subprocess.CompletedProcess[str] = subprocess.run(
        ["uv", "run", "pytest", "--collect-only", "-q", "-m", "integration"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert "test_kafka_integration.py::" in only.stdout
    default: subprocess.CompletedProcess[str] = subprocess.run(
        ["uv", "run", "pytest", "--collect-only", "-q"], cwd=ROOT, capture_output=True, text=True
    )
    assert "deselected" in default.stdout
    assert "test_kafka_integration.py::" not in default.stdout


def test_05_no_internal_names_in_repo() -> None:
    hits: list[str] = []
    path: Path
    for path in ROOT.rglob("*"):
        rel: tuple[str, ...] = path.relative_to(ROOT).parts
        if not path.is_file() or SKIP_DIRS & set(rel) or path.name == "uv.lock":
            continue
        text: str = path.read_text(errors="ignore").lower()
        hits += [f"{'/'.join(rel)}: {w}" for w in FORBIDDEN if w in text]
    assert hits == []


def test_gitignore_and_env_example() -> None:
    lines: set[str] = {x.strip() for x in (ROOT / ".gitignore").read_text().splitlines()}
    wanted: set[str] = {".venv/", "__pycache__/", ".env", ".mypy_cache/", ".pytest_cache/", ".ruff_cache/"}
    assert wanted <= lines
    assert (ROOT / ".env.example").read_text().strip() == "CLICKHOUSE_PASSWORD=change-me"


def test_readme_content() -> None:
    text: str = (ROOT / "README.md").read_text()
    needle: str
    for needle in [
        "uv sync",
        "cp .env.example .env",
        "docker compose up -d --wait",
        "uv run pytest",
        "-m integration",
    ]:
        assert needle in text


def test_compose_static() -> None:
    text: str = (ROOT / "compose.yaml").read_text()
    images: set[str] = set(re.findall(r"^\s*image:\s*(\S+)", text, re.MULTILINE))
    assert images == {"apache/kafka:4.3.2", "clickhouse/clickhouse-server:26.8.36.3", "redis:8.8.4-alpine"}
    assert "zookeeper" not in text.lower()
    assert "KAFKA_AUTO_CREATE_TOPICS_ENABLE" in text
    ports: list[str] = re.findall(r"^\s*-\s*[\"']?(\d+(?::\d+)?)[\"']?\s*$", text, re.MULTILINE)
    assert ports == []  # every published port must carry the 127.0.0.1 prefix
    assert text.count("127.0.0.1:") >= 4
