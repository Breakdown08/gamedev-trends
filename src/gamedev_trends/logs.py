"""Shared logging setup for the radar entry points."""

import logging
import sys

FORMAT: str = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def setup_logging() -> None:
    """INFO to stderr; httpx only from WARNING, its INFO line per request is noise."""
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format=FORMAT)
    logging.getLogger("httpx").setLevel(logging.WARNING)
