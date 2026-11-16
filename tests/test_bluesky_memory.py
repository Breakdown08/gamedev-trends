"""Spec checks 12-13: the in-memory post index and cursor store."""

from gamedev_trends.bluesky import InMemoryCursorStore, InMemoryPostIndex

PA1: str = "at://did:plc:a/app.bsky.feed.post/p1"
PA2: str = "at://did:plc:a/app.bsky.feed.post/p2"
PB1: str = "at://did:plc:b/app.bsky.feed.post/p1"


async def test_12_add_and_posted_at() -> None:
    index: InMemoryPostIndex = InMemoryPostIndex()
    await index.add(PA1, "did:plc:a", 1794744001234567)
    assert await index.posted_at(PA1) == 1794744001234567
    assert await index.posted_at(PA2) is None


async def test_12_add_is_idempotent() -> None:
    index: InMemoryPostIndex = InMemoryPostIndex()
    await index.add(PA1, "did:plc:a", 5)
    await index.add(PA1, "did:plc:a", 5)
    assert await index.posted_at(PA1) == 5
    assert await index.remove_author("did:plc:a") == [PA1]


async def test_12_remove() -> None:
    index: InMemoryPostIndex = InMemoryPostIndex()
    await index.add(PA1, "did:plc:a", 5)
    assert await index.remove(PA1) is True
    assert await index.remove(PA1) is False
    assert await index.posted_at(PA1) is None


async def test_12_remove_author_returns_uris_ascending_and_keeps_others() -> None:
    index: InMemoryPostIndex = InMemoryPostIndex()
    await index.add(PA2, "did:plc:a", 20)
    await index.add(PB1, "did:plc:b", 15)
    await index.add(PA1, "did:plc:a", 10)
    assert await index.remove_author("did:plc:a") == [PA1, PA2]
    assert await index.posted_at(PA1) is None
    assert await index.posted_at(PA2) is None
    assert await index.posted_at(PB1) == 15
    assert await index.remove_author("did:plc:a") == []


async def test_13_cursor_store() -> None:
    store: InMemoryCursorStore = InMemoryCursorStore()
    assert await store.load() is None
    assert store.value is None
    await store.save(5)
    assert await store.load() == 5
    assert store.value == 5


async def test_13_cursor_store_initial_value() -> None:
    store: InMemoryCursorStore = InMemoryCursorStore(7)
    assert await store.load() == 7
