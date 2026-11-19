"""Entry points run as `python -m` must execute the imported copy of their module.

`python -m gamedev_trends.store` loads store.py as `__main__`; clickhouse.py imports
`gamedev_trends.store`, a second copy with its own SinkError. The `__main__` copy's retries did not
catch the SinkError clickhouse.py raised, and the first ClickHouse outage crashed the store
(found live 2026-11-19, the container survived only by `restart: unless-stopped`). Each test runs
the module the way compose and the README do, through runpy as `__main__`, in a fresh interpreter.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT: Path = Path(__file__).resolve().parent.parent
ENTRY_MODULES: list[str] = [
    "gamedev_trends.store",
    "gamedev_trends.steam",
    "gamedev_trends.bluesky",
    "gamedev_trends.catalog",
]

DELEGATES: str = """
import importlib, runpy, sys
imported = importlib.import_module({module!r})
imported.main = lambda: 7
sys.argv = [{module!r}]
runpy.run_module({module!r}, run_name="__main__", alter_sys=True)
"""

STORE_OUTAGE: str = """
import asyncio, runpy, sys
import httpx
import gamedev_trends.store as store
from gamedev_trends.clickhouse import ClickHouseErasureSink
from store_support import FakeSink, FakeSleep, FakeSubscriber, make_client, obs

def outage_then_recovery() -> int:
    requests = []
    def clickhouse(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            raise httpx.ConnectError("[Errno -2] Name or service not known", request=request)
        return httpx.Response(200, text="")
    consumer = store.StoreConsumer(
        FakeSubscriber([]),
        FakeSink(),
        eraser=ClickHouseErasureSink(make_client(clickhouse), "radar"),
        sleep=FakeSleep(),
    )
    report = asyncio.run(consumer.flush([obs(1)]))
    print(f"committed={report.committed} requests={len(requests)}")
    return 0 if report.committed and len(requests) == 2 else 1

store.main = outage_then_recovery
sys.argv = ["gamedev_trends.store"]
runpy.run_module("gamedev_trends.store", run_name="__main__", alter_sys=True)
"""


def run_python(code: str) -> subprocess.CompletedProcess[str]:
    env: dict[str, str] = {**os.environ, "PYTHONPATH": str(ROOT / "tests")}
    return subprocess.run(
        [sys.executable, "-c", code], cwd=ROOT, env=env, capture_output=True, text=True, timeout=60
    )


@pytest.mark.parametrize("module", ENTRY_MODULES)
def test_dash_m_runs_main_of_the_imported_module(module: str) -> None:
    done: subprocess.CompletedProcess[str] = run_python(DELEGATES.format(module=module))
    assert done.returncode == 7, done.stderr


def test_store_dash_m_retries_a_clickhouse_outage() -> None:
    done: subprocess.CompletedProcess[str] = run_python(STORE_OUTAGE)
    assert done.returncode == 0, done.stdout + done.stderr
    assert "committed=True requests=2" in done.stdout
