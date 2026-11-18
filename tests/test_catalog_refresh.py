"""Spec checks 19-21: refresh with a fake StoreSource and CatalogSink."""

from datetime import UTC, datetime

import pytest
from catalog_support import FakeSource, RecordingSink, as_source

from gamedev_trends.catalog import CatalogError, RefreshReport, refresh

NOW: datetime = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)
LOT: list[int] = list(range(1000, 1120))


async def test_19_chunks_now_and_totals() -> None:
    source: FakeSource = FakeSource()
    sink: RecordingSink = RecordingSink()
    report: RefreshReport = await refresh(as_source(source), sink, LOT, now=NOW, chunk_size=50)
    assert [len(c) for c in source.item_calls] == [50, 50, 20]
    assert source.item_calls[0] == LOT[:50]
    assert source.item_calls[2] == LOT[100:]
    assert len(sink.calls) == 3
    assert all(call_now == NOW for _, call_now in sink.calls)
    assert report.requested == 120
    assert report.games == 120
    assert report.inserted == 120
    assert report.matched == 3
    assert report.failed == ()


async def test_19_report_sums_upsert_results() -> None:
    ids: list[int] = [413150, 730, 227300, 1422450, 1281930, 2379780, 1329410, 999999999]
    source: FakeSource = FakeSource()
    sink: RecordingSink = RecordingSink()
    report: RefreshReport = await refresh(as_source(source), sink, ids, now=NOW, chunk_size=4)
    assert [len(c) for c in source.item_calls] == [4, 4]
    # chunk 1: 413150, 730, 227300, 1422450 -> 4 games; chunk 2: 1281930, 2379780 + 2 unavailable -> 2 games
    assert [len(g) for g, _ in sink.calls] == [4, 2]
    assert report == RefreshReport(
        requested=8,
        games=6,
        inserted=6,
        matched=2,
        unavailable=(1329410, 999999999),
        malformed=(),
        failed=(),
    )


async def test_20_failed_chunk_is_reported_and_others_written() -> None:
    source: FakeSource = FakeSource(fail_chunks=[1])
    sink: RecordingSink = RecordingSink()
    report: RefreshReport = await refresh(as_source(source), sink, LOT, now=NOW, chunk_size=50)
    assert report.failed == tuple(LOT[50:100])
    assert len(source.item_calls) == 3
    assert len(sink.calls) == 2
    assert report.games == 70
    ids: list[int] = [413150, 730, 227300, 1422450, 1281930, 2379780]
    source2: FakeSource = FakeSource(fail_chunks=[1])
    sink2: RecordingSink = RecordingSink()
    report2: RefreshReport = await refresh(as_source(source2), sink2, ids, now=NOW, chunk_size=2)
    assert report2.failed == (227300, 1422450)
    assert [[g.appid for g in games] for games, _ in sink2.calls] == [[413150, 730], [1281930, 2379780]]
    assert report2.games == 4


async def test_21_dictionary_error_propagates_before_any_write() -> None:
    source: FakeSource = FakeSource(tags_error=True)
    sink: RecordingSink = RecordingSink()
    with pytest.raises(CatalogError):
        await refresh(as_source(source), sink, [730], now=NOW)
    assert sink.calls == []
    assert source.item_calls == []


async def test_21_duplicates_counted_once() -> None:
    source: FakeSource = FakeSource()
    sink: RecordingSink = RecordingSink()
    report: RefreshReport = await refresh(as_source(source), sink, [730, 730, 413150, 730], now=NOW)
    assert report.requested == 2
    assert source.item_calls == [[730, 413150]]


async def test_21_bad_parameters() -> None:
    sink: RecordingSink = RecordingSink()
    with pytest.raises(ValueError):
        await refresh(as_source(FakeSource()), sink, [730], now=NOW, chunk_size=0)
    with pytest.raises(ValueError):
        await refresh(as_source(FakeSource()), sink, [730], now=datetime(2026, 11, 18, 12, 0))


async def test_empty_appids_still_fetch_dictionaries() -> None:
    source: FakeSource = FakeSource()
    sink: RecordingSink = RecordingSink()
    report: RefreshReport = await refresh(as_source(source), sink, [], now=NOW)
    assert report == RefreshReport(0, 0, 0, 0, (), (), ())
    assert source.dictionary_calls == 2
    assert source.item_calls == []
    assert sink.calls == []
