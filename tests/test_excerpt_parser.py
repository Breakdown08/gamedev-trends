"""Spec checks 6-11: make_excerpt and the excerpt of a parsed post."""

import json
import random
from datetime import UTC, datetime
from typing import Any

import pytest
from bluesky_support import T0, A, commit_frame, tag_facet

from gamedev_trends.bus import encode
from gamedev_trends.contract import EXCERPT_MAX_LENGTH, Excerpt
from gamedev_trends.jetstream import (
    ELLIPSIS,
    JetstreamEvent,
    OtherEvent,
    PostCreated,
    make_excerpt,
    parse_event,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Hello\n\n  #gamedev\tworld ", "Hello #gamedev world"),
        ("", ""),
        ("  \n ", ""),
        ("x" * 140, "x" * 140),
        ("x" * 141, "x" * 139 + "…"),
        ("a" * 138 + " " + "bbbb", "a" * 138 + "…"),
        ("\U0001f3ae" * 141, "\U0001f3ae" * 139 + "…"),
        ("a\ud800b", "a?b"),
        ("Привет, мир", "Привет, мир"),
        ("a b c", "a b c"),
    ],
)
def test_06_make_excerpt_table(raw: str, expected: str) -> None:
    assert make_excerpt(raw) == expected


def test_06_ellipsis_is_one_character() -> None:
    assert ELLIPSIS == "…"
    assert len(ELLIPSIS) == 1
    assert len(make_excerpt("x" * 141)) == 140


def test_06_limit_argument() -> None:
    assert make_excerpt("abcdefghij", 5) == "abcd" + "…"
    assert make_excerpt("abcde", 5) == "abcde"


def test_07_random_strings() -> None:
    rng: random.Random = random.Random(20261116)
    alphabet: list[str] = list("abcxyzЖщ") + [" ", " ", "\n", "\t", "\U0001f3ae", " "]
    index: int
    for index in range(500):
        s: str = "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 400)))
        out: str = make_excerpt(s)
        assert len(out) <= 140, index
        collapsed: str = " ".join(s.split())
        if len(collapsed) <= 140:
            assert out == collapsed
        else:
            assert out.endswith("…")


def post(record: dict[str, Any]) -> str:
    return commit_frame(A, T0, "create", "app.bsky.feed.post", "p1", record)


def test_08_text_with_newline_and_facet_tag() -> None:
    event: JetstreamEvent | None = parse_event(
        post({"text": "Hello\n#gamedev", "facets": [tag_facet("gamedev")]})
    )
    assert event == PostCreated(did=A, rkey="p1", time_us=T0, tags=("gamedev",), excerpt="Hello #gamedev")


@pytest.mark.parametrize("record", [{"text": 5, "facets": []}, {"facets": [tag_facet("gamedev")]}])
def test_08_no_string_text_gives_empty_excerpt(record: dict[str, Any]) -> None:
    event: JetstreamEvent | None = parse_event(post(record))
    assert isinstance(event, PostCreated)
    assert event.excerpt == ""


def test_09_full_text_is_not_kept() -> None:
    event: JetstreamEvent | None = parse_event(post({"text": "start " + "z" * 300}))
    assert isinstance(event, PostCreated)
    assert len(event.excerpt) == EXCERPT_MAX_LENGTH
    assert event.excerpt.endswith("…")
    assert event.excerpt.startswith("start ")
    assert "z" * 140 not in repr(event)


def test_10_lone_surrogate_from_json_escape() -> None:
    raw: str = post({"text": "a\ud800b"})
    assert "\\ud800" in raw
    event: JetstreamEvent | None = parse_event(raw)
    assert isinstance(event, PostCreated)
    assert event.excerpt == "a?b"
    excerpt: Excerpt = Excerpt.create(
        source="bluesky",
        entity_type="post",
        entity_id=event.uri,
        text=event.excerpt,
        posted_at=datetime(2026, 11, 15, tzinfo=UTC),
    )
    assert json.loads(encode(excerpt))["text"] == "a?b"


def test_11_update_with_text_is_other() -> None:
    raw: str = commit_frame(A, T0 + 6, "update", "app.bsky.feed.post", "p1", {"text": "edit"})
    assert parse_event(raw) == OtherEvent(did=A, time_us=T0 + 6)
