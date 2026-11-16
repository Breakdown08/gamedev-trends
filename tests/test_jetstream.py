"""Spec checks 1-11: Jetstream URLs, tag normalisation, DID extraction, frame parsing."""

import json
from datetime import UTC, datetime
from typing import Any

import pytest
from bluesky_support import (
    T0,
    TAG_FACET,
    A,
    account_frame,
    commit_frame,
    frame,
    identity_frame,
    like_frame,
    post_frame,
    tag_facet,
)

from gamedev_trends.jetstream import (
    JETSTREAM_COLLECTIONS,
    AccountStatus,
    Engagement,
    JetstreamEvent,
    OtherEvent,
    PostCreated,
    PostDeleted,
    did_of,
    normalize_tag,
    parse_event,
    post_uri,
    subscribe_url,
    us_to_datetime,
)

ENDPOINT: str = "wss://jetstream1.us-east.bsky.network/subscribe"
BASE_URL: str = (
    ENDPOINT
    + "?wantedCollections=app.bsky.feed.post&wantedCollections=app.bsky.feed.like"
    + "&wantedCollections=app.bsky.feed.repost"
)


def test_01_subscribe_url_with_cursor() -> None:
    assert subscribe_url(ENDPOINT, JETSTREAM_COLLECTIONS, 1794744060000000) == (
        BASE_URL + "&cursor=1794744060000000"
    )


def test_01_subscribe_url_without_cursor() -> None:
    assert subscribe_url(ENDPOINT, JETSTREAM_COLLECTIONS, None) == BASE_URL


def test_01_subscribe_url_keeps_collection_order() -> None:
    url: str = subscribe_url("ws://h/s", ("b.x", "a.y"), 5)
    assert url == "ws://h/s?wantedCollections=b.x&wantedCollections=a.y&cursor=5"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(" #GameDev ", "gamedev"), ("##x", "x"), ("#", ""), ("PixelArt", "pixelart")],
)
def test_02_normalize_tag(raw: str, expected: str) -> None:
    assert normalize_tag(raw) == expected


def test_03_did_of() -> None:
    assert did_of("at://did:plc:abc/app.bsky.feed.post/3m1") == "did:plc:abc"
    assert did_of("at://alice.bsky.social/app.bsky.feed.post/3m1") is None
    assert did_of("https://x") is None


def test_03_post_uri() -> None:
    assert post_uri("did:plc:a", "p1") == "at://did:plc:a/app.bsky.feed.post/p1"


def test_04_us_to_datetime_keeps_the_microsecond() -> None:
    assert us_to_datetime(1794744000000001) == datetime(2026, 11, 15, 12, 0, 0, 1, tzinfo=UTC)
    assert us_to_datetime(T0) == datetime(2026, 11, 15, 12, 0, tzinfo=UTC)


def test_05_post_with_facet_and_record_tags() -> None:
    raw: str = commit_frame(
        "did:plc:a",
        1794744001000000,
        "create",
        "app.bsky.feed.post",
        "p1",
        {"text": "x", "facets": [tag_facet("GameDev")], "tags": ["#art", "gamedev"]},
    )
    event: JetstreamEvent | None = parse_event(raw)
    assert event == PostCreated(
        did="did:plc:a", rkey="p1", time_us=1794744001000000, tags=("gamedev", "art"), excerpt="x"
    )
    assert isinstance(event, PostCreated)
    assert event.uri == "at://did:plc:a/app.bsky.feed.post/p1"


def test_05_tags_are_normalised_deduplicated_and_empty_ones_dropped() -> None:
    raw: str = post_frame(facet_tags=["B", "#a", "b", "#"], tags=["A", "c", " "])
    event: JetstreamEvent | None = parse_event(raw)
    assert isinstance(event, PostCreated)
    assert event.tags == ("b", "a", "c")


@pytest.mark.parametrize(
    ("record", "excerpt"),
    [
        (None, ""),
        ({"text": "x", "facets": "x"}, "x"),
        ({"text": "x", "tags": None}, "x"),
        ({"facets": [1, {"features": "x"}]}, ""),
    ],
)
def test_06_malformed_record_parts_give_no_tags(record: dict[str, Any] | None, excerpt: str) -> None:
    raw: str = commit_frame(A, T0, "create", "app.bsky.feed.post", "p1", record)
    assert parse_event(raw) == PostCreated(did=A, rkey="p1", time_us=T0, tags=(), excerpt=excerpt)


def test_06_other_facet_type_gives_no_tag() -> None:
    link: dict[str, Any] = {"features": [{"$type": "app.bsky.richtext.facet#link", "tag": "gamedev"}]}
    raw: str = commit_frame(A, T0, "create", "app.bsky.feed.post", "p1", {"facets": [link]})
    assert parse_event(raw) == PostCreated(did=A, rkey="p1", time_us=T0, tags=())


def test_06_non_string_tag_values_are_skipped() -> None:
    record: dict[str, Any] = {
        "facets": [{"features": [{"$type": TAG_FACET, "tag": 5}, {"$type": TAG_FACET, "tag": "ok"}]}],
        "tags": [1, "fine"],
    }
    raw: str = commit_frame(A, T0, "create", "app.bsky.feed.post", "p1", record)
    assert parse_event(raw) == PostCreated(did=A, rkey="p1", time_us=T0, tags=("ok", "fine"))


def test_07_post_delete_and_update() -> None:
    deleted: JetstreamEvent | None = parse_event(
        commit_frame(A, T0 + 5, "delete", "app.bsky.feed.post", "p1")
    )
    assert deleted == PostDeleted(did=A, rkey="p1", time_us=T0 + 5)
    assert isinstance(deleted, PostDeleted)
    assert deleted.uri == "at://did:plc:a/app.bsky.feed.post/p1"
    updated: JetstreamEvent | None = parse_event(
        commit_frame(A, T0 + 6, "update", "app.bsky.feed.post", "p1", {"text": "edit"})
    )
    assert updated == OtherEvent(did=A, time_us=T0 + 6)


def test_08_like_and_repost() -> None:
    subject: str = "at://did:plc:b/app.bsky.feed.post/p2"
    assert parse_event(like_frame(A, T0 + 1, subject)) == Engagement(
        kind="like", subject_uri=subject, time_us=T0 + 1
    )
    assert parse_event(like_frame(A, T0 + 2, subject, "app.bsky.feed.repost")) == Engagement(
        kind="repost", subject_uri=subject, time_us=T0 + 2
    )


def test_08_like_without_subject_uri_and_deletes_are_other() -> None:
    bare: str = commit_frame(A, T0 + 3, "create", "app.bsky.feed.like", "l1", {"subject": {}})
    assert parse_event(bare) == OtherEvent(did=A, time_us=T0 + 3)
    no_record: str = commit_frame(A, T0 + 3, "create", "app.bsky.feed.like", "l1")
    assert parse_event(no_record) == OtherEvent(did=A, time_us=T0 + 3)
    deleted_like: str = commit_frame(A, T0 + 4, "delete", "app.bsky.feed.like", "l1")
    assert parse_event(deleted_like) == OtherEvent(did=A, time_us=T0 + 4)
    deleted_repost: str = commit_frame(A, T0 + 5, "delete", "app.bsky.feed.repost", "r1")
    assert parse_event(deleted_repost) == OtherEvent(did=A, time_us=T0 + 5)


def test_08_foreign_collection_is_other() -> None:
    raw: str = commit_frame(A, T0, "create", "app.bsky.graph.follow", "f1", {"subject": "did:plc:b"})
    assert parse_event(raw) == OtherEvent(did=A, time_us=T0)


def test_09_account_deleted() -> None:
    assert parse_event(account_frame(A, T0, False, "deleted")) == AccountStatus(
        did=A, time_us=T0, active=False, status="deleted"
    )


def test_09_account_active_without_status() -> None:
    assert parse_event(account_frame(A, T0, True)) == AccountStatus(
        did=A, time_us=T0, active=True, status=None
    )


def test_09_account_with_non_bool_active_is_broken() -> None:
    assert parse_event(account_frame(A, T0, "no")) is None
    assert parse_event(frame({"did": A, "time_us": T0, "kind": "account", "account": {}})) is None


def test_10_identity_and_unknown_kind_are_other() -> None:
    assert parse_event(identity_frame(A, T0)) == OtherEvent(did=A, time_us=T0)
    assert parse_event(frame({"did": A, "time_us": T0, "kind": "brand-new"})) == OtherEvent(did=A, time_us=T0)


@pytest.mark.parametrize(
    "raw",
    [
        b"not json",
        "not json",
        "[]",
        "5",
        frame({"did": A, "kind": "identity"}),
        frame({"did": A, "time_us": True, "kind": "identity"}),
        frame({"did": A, "time_us": 0, "kind": "identity"}),
        frame({"did": A, "time_us": -5, "kind": "identity"}),
        frame({"did": A, "time_us": 1.5, "kind": "identity"}),
        frame({"did": "x", "time_us": T0, "kind": "identity"}),
        frame({"time_us": T0, "kind": "identity"}),
        frame(
            {
                "did": A,
                "time_us": T0,
                "kind": "commit",
                "commit": {"operation": "create", "collection": "app.bsky.feed.post"},
            }
        ),
        frame({"did": A, "time_us": T0, "kind": "commit", "commit": "x"}),
    ],
)
def test_11_broken_frames_give_none(raw: str | bytes) -> None:
    assert parse_event(raw) is None


def test_11_str_and_bytes_are_both_accepted() -> None:
    raw: str = identity_frame(A, T0)
    assert parse_event(raw) == parse_event(raw.encode("utf-8")) == OtherEvent(did=A, time_us=T0)
    assert json.loads(raw)["time_us"] == T0
