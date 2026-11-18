"""Spec checks 1-16: request string, dictionaries, parse_items on the recorded Steam answers."""

from datetime import UTC, datetime
from typing import Any

import pytest
from catalog_support import (
    BALATRO,
    FIXTURE_IDS,
    PARSED_IDS,
    STARDEW,
    T,
    categories,
    category_dict,
    fixture_copy,
    getitems,
    parsed_games,
    raw_item,
    tag_dict,
    taglist,
)

from gamedev_trends.catalog import (
    CatalogError,
    Category,
    Game,
    ParsedItems,
    Tag,
    items_request,
    parse_categories,
    parse_items,
    parse_tag_list,
)

DOCUMENT_KEYS: set[str] = {
    "appid",
    "name",
    "steam_type",
    "tags",
    "categories",
    "developers",
    "publishers",
    "is_free",
    "price_usd_cents",
    "discount_pct",
    "release_date",
    "coming_soon",
    "early_access",
    "updated_at",
}


def price_of(appid: int) -> tuple[int, int]:
    option: dict[str, Any] = raw_item(appid)["best_purchase_option"]
    return int(option["final_price_in_cents"]), int(option.get("discount_pct", 0))


def test_01_items_request_exact() -> None:
    assert items_request([413150, 730]) == (
        '{"ids":[{"appid":413150},{"appid":730}],'
        '"context":{"language":"english","country_code":"US"},'
        '"data_request":{"include_tag_count":20,"include_basic_info":true,"include_release":true}}'
    )


def test_02_parse_tag_list() -> None:
    tags: dict[int, str] = parse_tag_list(taglist())
    assert isinstance(tags, dict)
    assert len(tags) >= 400
    assert tags[87918] == "Farming Sim"
    assert tags[492] == "Indie"
    bad: object
    for bad in ({"response": {}}, [], {"response": {"tags": [{"tagid": "x", "name": "a"}]}}):
        with pytest.raises(CatalogError):
            parse_tag_list(bad)


def test_03_parse_categories() -> None:
    cats: dict[int, str] = parse_categories(categories())
    expected: dict[int, str] = {
        2: "Single-player",
        1: "Multi-player",
        22: "Steam Achievements",
        28: "Full controller support",
    }
    assert {k: cats[k] for k in expected} == expected
    with pytest.raises(CatalogError):
        parse_categories({"response": {}})


def test_04_parse_items_order_and_unavailable() -> None:
    result: ParsedItems = parse_items(getitems(), FIXTURE_IDS, tag_dict(), category_dict(), T)
    assert [g.appid for g in result.games] == PARSED_IDS
    assert result.unavailable == (1329410, 999999999)
    assert result.malformed == ()


def test_05_stardew_scalars() -> None:
    g: Game = parsed_games()[STARDEW]
    price: int
    discount: int
    price, discount = price_of(STARDEW)
    assert g.name == "Stardew Valley"
    assert g.steam_type == 0
    assert g.developers == ["ConcernedApe"]
    assert g.publishers == ["ConcernedApe"]
    assert g.release_date == datetime(2016, 2, 26, 17, 59, 38, tzinfo=UTC)
    assert g.is_free is False
    assert g.price_usd_cents == price
    assert g.discount_pct == discount
    assert g.coming_soon is False
    assert g.early_access is False
    assert g.updated_at == T


def test_06_stardew_tags_sorted_by_weight_then_tagid() -> None:
    raw: list[dict[str, int]] = raw_item(STARDEW)["tags"]
    tags_dict: dict[int, str] = tag_dict()
    # feed the tags in reverse and with a forced tie so the ordering rule is really exercised
    payload: Any = fixture_copy()
    shuffled: list[dict[str, int]] = list(reversed(raw))
    shuffled[0] = {"tagid": shuffled[0]["tagid"], "weight": shuffled[1]["weight"]}
    raw_item(STARDEW, payload)["tags"] = shuffled
    g: Game = parsed_games(payload)[STARDEW]
    assert len(g.tags) == 20
    a: Tag
    b: Tag
    for a, b in zip(g.tags, g.tags[1:], strict=False):
        assert a.weight > b.weight or (a.weight == b.weight and a.tagid < b.tagid)
    assert {t.tagid for t in g.tags} == {x["tagid"] for x in shuffled}
    assert all(t.name == tags_dict[t.tagid] for t in g.tags)
    original: Game = parsed_games()[STARDEW]
    assert {t.tagid for t in original.tags} == {x["tagid"] for x in raw}


def test_07_stardew_categories() -> None:
    g: Game = parsed_games()[STARDEW]
    assert g.categories[0] == Category(categoryid=2, name="Single-player")
    assert g.categories[1] == Category(categoryid=1, name="Multi-player")
    block: dict[str, list[int]] = raw_item(STARDEW)["categories"]
    expected: list[int] = []
    key: str
    for key in block:
        if not key.endswith("_categoryids"):
            continue
        cid: int
        for cid in block[key]:
            if cid not in expected:
                expected.append(cid)
    assert list(block) == [
        "supported_player_categoryids",
        "feature_categoryids",
        "controller_categoryids",
    ]
    assert [c.categoryid for c in g.categories] == expected


def test_08_counter_strike_free() -> None:
    g: Game = parsed_games()[730]
    assert g.is_free is True
    assert g.price_usd_cents is None
    assert g.discount_pct == 0


def test_09_deadlock_coming_soon() -> None:
    g: Game = parsed_games()[1422450]
    assert g.coming_soon is True
    assert g.release_date is None
    assert g.price_usd_cents is None


def test_10_tmodloader_not_a_game() -> None:
    g: Game = parsed_games()[1281930]
    assert g.steam_type == 2
    assert g.developers == ["TML Team"]
    assert g.publishers == ["Re-Logic", "TML Team"]


def test_11_euro_truck_price_and_discount() -> None:
    g: Game = parsed_games()[227300]
    price: int
    discount: int
    price, discount = price_of(227300)
    assert g.price_usd_cents == price
    assert g.discount_pct == discount


def test_12_names_missing_in_dictionaries_give_none() -> None:
    first_tag: int = parsed_games()[STARDEW].tags[0].tagid
    tags: dict[int, str] = {k: v for k, v in tag_dict().items() if k != first_tag}
    cats: dict[int, str] = {k: v for k, v in category_dict().items() if k != 2}
    g: Game = parsed_games(tags=tags, cats=cats)[STARDEW]
    assert g.tags[0].tagid == first_tag
    assert g.tags[0].name is None
    assert next(c for c in g.categories if c.categoryid == 2).name is None
    assert g.tags[1].name is not None


def test_13_malformed_items_are_reported_and_skipped() -> None:
    payload: Any = fixture_copy()
    del raw_item(STARDEW, payload)["name"]
    raw_item(BALATRO, payload)["type"] = "game"
    result: ParsedItems = parse_items(payload, FIXTURE_IDS, tag_dict(), category_dict(), T)
    assert result.malformed == (413150, 2379780)
    assert [g.appid for g in result.games] == [730, 227300, 1422450, 1281930]
    assert result.unavailable == (1329410, 999999999)


def test_14_bad_payloads() -> None:
    bad: object
    for bad in ([], {}, {"response": {"store_items": {}}}):
        with pytest.raises(CatalogError):
            parse_items(bad, [1], {}, {}, T)
    result: ParsedItems = parse_items({"response": {}}, [1, 2], {}, {}, T)
    assert result.games == ()
    assert result.unavailable == (1, 2)


def test_15_naive_now_and_duplicate_appids() -> None:
    with pytest.raises(ValueError):
        parse_items(getitems(), FIXTURE_IDS, tag_dict(), category_dict(), datetime(2026, 11, 18, 10, 0))
    result: ParsedItems = parse_items(getitems(), [730, 730, 999999999], tag_dict(), category_dict(), T)
    assert result.unavailable == (999999999,)
    assert [g.appid for g in result.games] == [730]


def test_16_model_dump_is_the_document() -> None:
    dump: dict[str, Any] = parsed_games()[STARDEW].model_dump()
    assert set(dump) == DOCUMENT_KEYS
    assert isinstance(dump["release_date"], datetime)
    assert isinstance(dump["updated_at"], datetime)
