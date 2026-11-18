"""Spec checks 17-18: StoreClient over httpx.MockTransport."""

import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from catalog_support import categories, getitems, taglist

from gamedev_trends.catalog import CatalogError, StoreClient, items_request

BASE: str = "https://steam.test"


class Calls:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.sleeps: list[float] = []

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)


def make(
    handler: Callable[[httpx.Request], httpx.Response], calls: Calls, *, base_url: str = BASE
) -> StoreClient:
    def recording(request: httpx.Request) -> httpx.Response:
        calls.requests.append(request)
        return handler(request)

    http: httpx.AsyncClient = httpx.AsyncClient(
        transport=httpx.MockTransport(recording), headers={"User-Agent": "gamedev-trends/0.1"}
    )
    return StoreClient(http, base_url=base_url, max_attempts=3, backoff_base=1.0, sleep=calls.sleep)


def by_path(request: httpx.Request) -> httpx.Response:
    path: str = request.url.path
    body: Any
    if path == "/IStoreBrowseService/GetItems/v1/":
        body = getitems()
    elif path == "/IStoreService/GetTagList/v1/":
        body = taglist()
    elif path == "/IStoreBrowseService/GetStoreCategories/v1/":
        body = categories()
    else:
        return httpx.Response(404)
    return httpx.Response(200, json=body)


async def test_17_items_request_shape() -> None:
    calls: Calls = Calls()
    client: StoreClient = make(by_path, calls, base_url=BASE + "/")
    result: object = await client.items([730])
    assert result == getitems()
    assert len(calls.requests) == 1
    req: httpx.Request = calls.requests[0]
    assert req.method == "GET"
    assert f"{req.url.scheme}://{req.url.host}{req.url.path}" == BASE + "/IStoreBrowseService/GetItems/v1/"
    assert req.url.params["input_json"] == items_request([730])
    assert req.headers["User-Agent"] == "gamedev-trends/0.1"


async def test_17_tag_names_request_shape() -> None:
    calls: Calls = Calls()
    client: StoreClient = make(by_path, calls)
    tags: dict[int, str] = await client.tag_names()
    assert tags[87918] == "Farming Sim"
    assert len(calls.requests) == 1
    req: httpx.Request = calls.requests[0]
    assert req.method == "GET"
    assert req.url.path == "/IStoreService/GetTagList/v1/"
    assert req.url.params["language"] == "english"
    assert req.headers["User-Agent"] == "gamedev-trends/0.1"


async def test_17_category_names_request_shape() -> None:
    calls: Calls = Calls()
    client: StoreClient = make(by_path, calls)
    cats: dict[int, str] = await client.category_names()
    assert cats[2] == "Single-player"
    assert len(calls.requests) == 1
    req: httpx.Request = calls.requests[0]
    assert req.method == "GET"
    assert req.url.path == "/IStoreBrowseService/GetStoreCategories/v1/"
    assert req.url.params["input_json"] == '{"language":"english"}'
    assert req.headers["User-Agent"] == "gamedev-trends/0.1"


async def test_18_retries_then_success() -> None:
    calls: Calls = Calls()
    answers: list[int] = [503, 429, 200]

    def handler(_: httpx.Request) -> httpx.Response:
        status: int = answers.pop(0)
        return httpx.Response(status, json={"response": {}}) if status == 200 else httpx.Response(status)

    client: StoreClient = make(handler, calls)
    assert await client.items([1]) == {"response": {}}
    assert len(calls.requests) == 3
    assert calls.sleeps == [1.0, 2.0]


async def test_18_exhausted_attempts() -> None:
    calls: Calls = Calls()
    client: StoreClient = make(lambda _: httpx.Response(500), calls)
    with pytest.raises(CatalogError):
        await client.items([1])
    assert len(calls.requests) == 3


async def test_18_client_error_is_not_retried() -> None:
    calls: Calls = Calls()
    client: StoreClient = make(lambda _: httpx.Response(403), calls)
    with pytest.raises(CatalogError):
        await client.items([1])
    assert len(calls.requests) == 1
    assert calls.sleeps == []


async def test_18_transport_error_is_retried() -> None:
    calls: Calls = Calls()
    answers: list[bool] = [True, False]

    def handler(request: httpx.Request) -> httpx.Response:
        if answers.pop(0):
            raise httpx.ConnectError("boom", request=request)
        return httpx.Response(200, json={"response": {"store_items": []}})

    client: StoreClient = make(handler, calls)
    assert json.dumps(await client.items([1])) == json.dumps({"response": {"store_items": []}})
    assert len(calls.requests) == 2
    assert calls.sleeps == [1.0]


async def test_18_body_not_json() -> None:
    calls: Calls = Calls()
    client: StoreClient = make(lambda _: httpx.Response(200, text="not json"), calls)
    with pytest.raises(CatalogError):
        await client.items([1])
