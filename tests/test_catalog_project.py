"""Spec checks 28-32: compose, pyproject, README, collection of integration tests, untouched modules."""

import re
import subprocess
import tomllib
from pathlib import Path
from typing import Any

ROOT: Path = Path(__file__).resolve().parents[1]
BASE_COMMIT: str = "f7e9d8c"
OLD_SERVICES: list[str] = [
    "kafka",
    "kafka-init",
    "clickhouse",
    "redis",
    "steam-collector",
    "bluesky-collector",
    "store",
    "grafana",
]


def git(*args: str) -> str:
    done: subprocess.CompletedProcess[str] = subprocess.run(
        ["git", *args], cwd=ROOT, capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
    return done.stdout


def service_block(text: str, name: str) -> str:
    match: re.Match[str] | None = re.search(rf"^  {re.escape(name)}:\n((?:    .*\n|\n)+)", text, re.MULTILINE)
    assert match is not None, f"no {name} block"
    return match.group(1)


def test_28_mongo_block() -> None:
    text: str = (ROOT / "compose.yaml").read_text()
    block: str = service_block(text, "mongo")
    needle: str
    for needle in (
        "image: mongo:8.0.38",
        '"127.0.0.1:27017:27017"',
        "mongo-data:/data/db",
        "healthcheck:",
        "restart: unless-stopped",
    ):
        assert needle in block, needle
    assert "build:" not in block
    assert "profiles" not in block
    assert re.search(r"^volumes:\n(?:  .*\n)*?  mongo-data:", text, re.MULTILINE)


def test_28_other_services_unchanged() -> None:
    old: str = git("show", f"{BASE_COMMIT}:compose.yaml")
    new: str = (ROOT / "compose.yaml").read_text()
    name: str
    for name in OLD_SERVICES:
        assert service_block(new, name) == service_block(old, name), name
    assert "mongo" in new


def test_29_pymongo_dependency() -> None:
    data: dict[str, Any] = tomllib.loads((ROOT / "pyproject.toml").read_text())
    assert "pymongo>=4.20,<5" in data["project"]["dependencies"]
    lock: dict[str, Any] = tomllib.loads((ROOT / "uv.lock").read_text())
    assert "pymongo" in {p["name"] for p in lock["package"]}


def readme_section() -> str:
    text: str = (ROOT / "README.md").read_text()
    start: int = text.find("## Game catalog (MongoDB)")
    assert start >= 0, "README has no '## Game catalog (MongoDB)'"
    assert start > text.index("## Read-only SQL interface")
    rest: str = text[start + 1 :]
    nxt: re.Match[str] | None = re.search(r"^## ", rest, re.MULTILINE)
    return text[start : start + 1 + (nxt.start() if nxt else len(rest))]


def test_30_readme_section() -> None:
    section: str = readme_section()
    needle: str
    for needle in (
        "uv run python -m gamedev_trends.catalog refresh",
        "IStoreBrowseService/GetItems",
        "appid_unique",
        "tags_name",
        "IXSCAN",
        "$jsonSchema",
        "MONGODB_URL",
    ):
        assert needle in section, needle
    assert "undocumented" in section or "not documented" in section


def test_30_readme_development_mentions_mongo_port() -> None:
    text: str = (ROOT / "README.md").read_text()
    match: re.Match[str] | None = re.search(r"^#+\s+Development\b.*\n", text, re.MULTILINE)
    assert match is not None
    rest: str = text[match.end() :]
    nxt: re.Match[str] | None = re.search(r"^#{1,2} ", rest, re.MULTILINE)
    development: str = rest[: nxt.start()] if nxt else rest
    assert "localhost:27017" in development


def collect(*extra: str) -> str:
    done: subprocess.CompletedProcess[str] = subprocess.run(
        ["uv", "run", "pytest", "--collect-only", "-q", *extra], cwd=ROOT, capture_output=True, text=True
    )
    return done.stdout


def test_31_integration_tests_collected_only_on_request() -> None:
    assert "tests/test_catalog_integration.py::" in collect("-m", "integration")
    assert "test_catalog_integration.py::" not in collect()


# The only allowed change: the shared basicConfig moved to gamedev_trends.logs.setup_logging.
LOGGING_REFACTOR: frozenset[str] = frozenset(
    {
        "-    logging.basicConfig(",
        "-        level=logging.INFO,",
        "-        stream=sys.stderr,",
        '-        format="%(asctime)s %(levelname)s %(name)s: %(message)s",',
        "-    )",
        "+    setup_logging()",
        "+from gamedev_trends.logs import setup_logging",
    }
)


def test_32_existing_modules_untouched() -> None:
    files: list[str] = [
        f"src/gamedev_trends/{n}.py" for n in ("steam", "store", "clickhouse", "bluesky", "contract")
    ]
    changed: list[str] = [
        line
        for line in git("diff", "-U0", BASE_COMMIT, "--", *files).splitlines()
        if line[:1] in "+-" and not line.startswith(("+++", "---"))
    ]
    assert [line for line in changed if line not in LOGGING_REFACTOR] == []
