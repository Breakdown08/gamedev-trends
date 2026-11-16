"""Jetstream protocol without the network: URLs, frame parsing, the WebSocket connection."""

import json
from collections.abc import AsyncGenerator, Callable, Sequence
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from gamedev_trends.contract import EXCERPT_MAX_LENGTH

JETSTREAM_COLLECTIONS: tuple[str, ...] = (
    "app.bsky.feed.post",
    "app.bsky.feed.like",
    "app.bsky.feed.repost",
)
DEFAULT_ENDPOINTS: tuple[str, ...] = (
    "wss://jetstream1.us-east.bsky.network/subscribe",
    "wss://jetstream2.us-west.bsky.network/subscribe",
)
TAG_FACET: str = "app.bsky.richtext.facet#tag"
USER_AGENT: str = "gamedev-trends/0.1"
ELLIPSIS: str = "\u2026"


def make_excerpt(text: str, limit: int = EXCERPT_MAX_LENGTH) -> str:
    cleaned: str = " ".join(text.encode("utf-8", "replace").decode("utf-8").split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 1].rstrip() + ELLIPSIS


def subscribe_url(endpoint: str, collections: Sequence[str], cursor: int | None) -> str:
    parts: list[str] = [f"wantedCollections={c}" for c in collections]
    if cursor is not None:
        parts.append(f"cursor={cursor}")
    return endpoint + "?" + "&".join(parts)


def normalize_tag(raw: str) -> str:
    return raw.strip().lstrip("#").casefold()


def post_uri(did: str, rkey: str) -> str:
    return f"at://{did}/app.bsky.feed.post/{rkey}"


def did_of(uri: str) -> str | None:
    prefix: str = "at://"
    if not uri.startswith(prefix):
        return None
    authority: str = uri[len(prefix) :].split("/", 1)[0]
    return authority if authority.startswith("did:") else None


def us_to_datetime(time_us: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=time_us)


@dataclass(frozen=True)
class PostCreated:
    did: str
    rkey: str
    time_us: int
    tags: tuple[str, ...]
    excerpt: str = ""

    @property
    def uri(self) -> str:
        return post_uri(self.did, self.rkey)


@dataclass(frozen=True)
class PostDeleted:
    did: str
    rkey: str
    time_us: int

    @property
    def uri(self) -> str:
        return post_uri(self.did, self.rkey)


@dataclass(frozen=True)
class Engagement:
    kind: Literal["like", "repost"]
    subject_uri: str
    time_us: int


@dataclass(frozen=True)
class AccountStatus:
    did: str
    time_us: int
    active: bool
    status: str | None


@dataclass(frozen=True)
class OtherEvent:
    did: str
    time_us: int


type JetstreamEvent = PostCreated | PostDeleted | Engagement | AccountStatus | OtherEvent


def parse_event(frame: str | bytes) -> JetstreamEvent | None:
    try:
        data: object = json.loads(frame)
    except ValueError:
        return None
    if not isinstance(data, dict):
        return None
    time_us: object = data.get("time_us")
    did: object = data.get("did")
    if not isinstance(time_us, int) or isinstance(time_us, bool) or time_us <= 0:
        return None
    if not isinstance(did, str) or not did.startswith("did:"):
        return None
    kind: object = data.get("kind")
    if kind == "commit":
        return _parse_commit(data.get("commit"), did, time_us)
    if kind == "account":
        account: object = data.get("account")
        if not isinstance(account, dict):
            return None
        active: object = account.get("active")
        if not isinstance(active, bool):
            return None
        status: object = account.get("status")
        return AccountStatus(
            did=did, time_us=time_us, active=active, status=status if isinstance(status, str) else None
        )
    return OtherEvent(did=did, time_us=time_us)


def _parse_commit(commit: object, did: str, time_us: int) -> JetstreamEvent | None:
    if not isinstance(commit, dict):
        return None
    operation: object = commit.get("operation")
    collection: object = commit.get("collection")
    rkey: object = commit.get("rkey")
    if not isinstance(operation, str) or not isinstance(collection, str) or not isinstance(rkey, str):
        return None
    record: object = commit.get("record")
    if collection == "app.bsky.feed.post" and operation == "create":
        text: object = record.get("text") if isinstance(record, dict) else None
        return PostCreated(
            did=did,
            rkey=rkey,
            time_us=time_us,
            tags=_tags(record),
            excerpt=make_excerpt(text) if isinstance(text, str) else "",
        )
    if collection == "app.bsky.feed.post" and operation == "delete":
        return PostDeleted(did=did, rkey=rkey, time_us=time_us)
    if operation == "create" and collection in ("app.bsky.feed.like", "app.bsky.feed.repost"):
        subject: object = record.get("subject") if isinstance(record, dict) else None
        uri: object = subject.get("uri") if isinstance(subject, dict) else None
        if isinstance(uri, str):
            return Engagement(
                kind="like" if collection == "app.bsky.feed.like" else "repost",
                subject_uri=uri,
                time_us=time_us,
            )
    return OtherEvent(did=did, time_us=time_us)


def _tags(record: object) -> tuple[str, ...]:
    raw: list[str] = []
    if isinstance(record, dict):
        facets: object = record.get("facets")
        if isinstance(facets, list):
            facet: object
            for facet in facets:
                features: object = facet.get("features") if isinstance(facet, dict) else None
                if not isinstance(features, list):
                    continue
                feature: object
                for feature in features:
                    if isinstance(feature, dict) and feature.get("$type") == TAG_FACET:
                        tag: object = feature.get("tag")
                        if isinstance(tag, str):
                            raw.append(tag)
        listed: object = record.get("tags")
        if isinstance(listed, list):
            raw.extend(x for x in listed if isinstance(x, str))
    seen: dict[str, None] = {}
    value: str
    for value in raw:
        normalized: str = normalize_tag(value)
        if normalized:
            seen.setdefault(normalized, None)
    return tuple(seen)


class ConnectionLost(Exception):
    """The connection was closed or broke."""


class FrameSource(Protocol):
    async def recv(self) -> str | bytes: ...


type Connect = Callable[[str], AbstractAsyncContextManager[FrameSource]]


def websocket_connect(url: str) -> AbstractAsyncContextManager[FrameSource]:
    return _connect(url)


class _WebSocketSource:
    def __init__(self, ws: ClientConnection) -> None:
        self._ws: ClientConnection = ws

    async def recv(self) -> str | bytes:
        try:
            frame: str | bytes = await self._ws.recv()
        except ConnectionClosed as exc:
            raise ConnectionLost(str(exc)) from exc
        return frame


@asynccontextmanager
async def _connect(url: str) -> AsyncGenerator[FrameSource]:
    stack: AsyncExitStack = AsyncExitStack()
    async with stack:
        ws: ClientConnection = await stack.enter_async_context(
            connect(
                url,
                open_timeout=10,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                max_size=4 * 2**20,
                user_agent_header=USER_AGENT,
            )
        )
        yield _WebSocketSource(ws)
