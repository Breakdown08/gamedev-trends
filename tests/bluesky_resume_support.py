"""Helpers for the resume tests: a feed emulator that honours the cursor in the URL, and a synthetic feed."""

import json
import re
from collections.abc import AsyncGenerator, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager

from bluesky_support import P1, A, B, ListSource, commit_frame, identity_frame, like_frame, post_frame, t

from gamedev_trends.jetstream import FrameSource

CURSOR_PATTERN: re.Pattern[str] = re.compile(r"[?&]cursor=(\d+)")
C: str = "did:plc:c"
D: str = "did:plc:d"
E: str = "did:plc:e"
F: str = "did:plc:f"
P4: str = f"at://{D}/app.bsky.feed.post/p4"
P6: str = f"at://{F}/app.bsky.feed.post/p6"
REPOST: str = "app.bsky.feed.repost"


def time_of(frame: str) -> int:
    value: int = json.loads(frame)["time_us"]
    return value


def cursor_of(url: str) -> int | None:
    match: re.Match[str] | None = CURSOR_PATTERN.search(url)
    return None if match is None else int(match.group(1))


class FeedConnect:
    """Connect over a fixed feed (frames with non-decreasing time_us).

    Each call takes the next session length k: it serves the first k frames of the feed with
    time_us >= the cursor of the URL (no cursor: from the start) and then ConnectionLost is raised.
    When the sessions are used up, entering the connection raises OSError.
    """

    def __init__(self, feed: Sequence[str], sessions: Sequence[int]) -> None:
        self.feed: list[str] = list(feed)
        self.sessions: list[int] = list(sessions)
        self.urls: list[str] = []
        self.served: list[list[str]] = []
        self.ends: list[int] = []  # index in the feed just after the last frame served

    def __call__(self, url: str) -> AbstractAsyncContextManager[FrameSource]:
        self.urls.append(url)
        if len(self.urls) > len(self.sessions):
            return self._broken()
        k: int = self.sessions[len(self.urls) - 1]
        cursor: int | None = cursor_of(url)
        start: int = 0
        if cursor is not None:
            while start < len(self.feed) and time_of(self.feed[start]) < cursor:
                start += 1
        end: int = min(start + k, len(self.feed))
        self.served.append(self.feed[start:end])
        self.ends.append(end)
        return self._open(self.feed[start:end])

    @asynccontextmanager
    async def _open(self, frames: list[str]) -> AsyncGenerator[FrameSource]:
        yield ListSource(frames)

    @asynccontextmanager
    async def _broken(self) -> AsyncGenerator[FrameSource]:
        raise OSError("feed is over")
        yield ListSource([])


def build_feed() -> list[str]:
    """About 200 seconds, window 60: tagged and plain posts, likes and reposts of tagged posts,
    a deletion, and groups of events with the same time_us (including equal likes)."""
    return [
        post_frame(A, "p1", t(1), facet_tags=["gamedev"]),
        post_frame(B, "p2", t(2)),
        like_frame("did:plc:x", t(3), P1),
        like_frame("did:plc:y", t(3), P1),  # equal to the previous event once parsed
        post_frame(C, "p3", t(3), facet_tags=["godot"]),
        identity_frame(A, t(10)),
        like_frame("did:plc:x", t(20), P1),
        like_frame("did:plc:x", t(20), P1, REPOST),
        post_frame(D, "p4", t(30), facet_tags=["indiedev"]),
        like_frame("did:plc:x", t(45), P4),
        like_frame("did:plc:y", t(45), P4),
        like_frame("did:plc:z", t(45), P4),
        identity_frame(B, t(61)),
        commit_frame(A, t(62), "delete", "app.bsky.feed.post", "p1"),
        like_frame("did:plc:x", t(70), P1),
        like_frame("did:plc:x", t(75), P4, REPOST),
        like_frame("did:plc:y", t(75), P4),
        identity_frame(A, t(100)),
        post_frame(E, "p5", t(121), facet_tags=["gamedev"]),
        identity_frame(B, t(121)),
        post_frame(F, "p6", t(130), facet_tags=["pixelart"]),
        like_frame("did:plc:x", t(150), P6),
        like_frame("did:plc:y", t(150), P6),
        like_frame("did:plc:x", t(170), P6, REPOST),
        identity_frame(A, t(181)),
        identity_frame(B, t(200)),
    ]
