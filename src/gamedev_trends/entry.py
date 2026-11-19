"""Run an entry point from the one imported copy of its module."""

import importlib
import sys
from collections.abc import Callable
from importlib.machinery import ModuleSpec
from types import ModuleType
from typing import NoReturn


def run_main(spec: ModuleSpec | None) -> NoReturn:
    """Exit with main() of the module named by spec, imported the regular way.

    `python -m gamedev_trends.store` executes store.py as `__main__`, while clickhouse.py imports
    `gamedev_trends.store`: two copies of one module with two SinkError classes. The copy running as
    `__main__` then failed to catch the SinkError raised by clickhouse.py, so its retries never ran and
    any ClickHouse outage crashed the process. Delegating to the imported copy leaves one of each class.
    """
    if spec is None:
        raise RuntimeError("run_main needs the module's __spec__; run it with python -m")
    module: ModuleType = importlib.import_module(spec.name)
    main: Callable[[], int | None] = module.main
    sys.exit(main())
