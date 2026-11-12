"""Spec checks 24-32: SteamCollector.run_slot with in-memory bus, scripted HTTP, fake sleep."""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

import pytest
from steam_support import (
    DEFAULT_ONLINE,
    ROLLUP,
    T10,
    CountingPublisher,
    Rig,
    Router,
    chart_body,
    no_data_404,
    observations,
    resp,
    rig,
)

from gamedev_trends.bus import InMemoryBroker, InMemoryPublisher
from gamedev_trends.contract import Observation
from gamedev_trends.steam import Chart, ChartEntry, CycleReport, SteamClient, SteamCollector

LOGGER: str = "gamedev_trends.steam"


@dataclass
class Outcome:
    report: CycleReport
    broker: InMemoryBroker
    rig: Rig


async def run(
    router: Router, *, slot: datetime = T10, top_n: int = 100, broker: InMemoryBroker | None = None
) -> Outcome:
    the_broker: InMemoryBroker = broker if broker is not None else InMemoryBroker()
    r: Rig
    async with rig(router, max_attempts=4) as r:
        collector: SteamCollector = SteamCollector(r.client, InMemoryPublisher(the_broker), top_n=top_n)
        report: CycleReport = await collector.run_slot(slot)
    return Outcome(report, the_broker, r)


async def test_24_nine_observations_in_order() -> None:
    out: Outcome = await run(Router())
    report: CycleReport = out.report
    broker: InMemoryBroker = out.broker
    obs: list[Observation] = observations(broker)
    assert [(o.entity_id, o.metric) for o in obs] == [
        (a, m)
        for a in ("730", "570", "578080")
        for m in ("chart_rank", "chart_peak_players", "players_current")
    ]
    assert all(o.source == "steam" and o.entity_type == "game" and o.window_start is None for o in obs)
    assert all(o.observed_at == (T10 if o.metric == "players_current" else ROLLUP) for o in obs)
    assert [o.value for o in obs if o.metric == "chart_rank"] == [1, 2, 3]
    assert [o.value for o in obs if o.metric == "chart_peak_players"] == [1217867, 716944, 728370]
    assert [o.value for o in obs if o.metric == "players_current"] == [747936, 500000, 300000]
    assert report == CycleReport(
        slot=T10, chart_ok=True, games=3, no_data=(), failed=(), published=9, publish_ok=True
    )


async def test_25_players_event_id_matches_contract() -> None:
    out: Outcome = await run(Router())
    broker: InMemoryBroker = out.broker
    expected: Observation = Observation.create(
        source="steam",
        entity_type="game",
        entity_id="730",
        metric="players_current",
        value=0,
        observed_at=T10,
    )
    got: list[Observation] = [
        o for o in observations(broker) if o.entity_id == "730" and o.metric == "players_current"
    ]
    assert [o.event_id for o in got] == [expected.event_id]


async def test_26_rerun_same_slot_same_event_ids() -> None:
    out: Outcome = await run(Router())
    first: InMemoryBroker = out.broker
    out2: Outcome = await run(Router(online={a: v + 1 for a, v in DEFAULT_ONLINE.items()}))
    second: InMemoryBroker = out2.broker
    ids1: set[object] = {o.event_id for o in observations(first)}
    ids2: set[object] = {o.event_id for o in observations(second)}
    assert len(ids1) == 9
    assert ids1 == ids2


async def test_26_next_slot_keeps_chart_ids_and_renews_players_ids() -> None:
    out: Outcome = await run(Router(), slot=T10)
    a: InMemoryBroker = out.broker
    out2: Outcome = await run(Router(), slot=T10 + timedelta(hours=1))
    b: InMemoryBroker = out2.broker
    chart_a: set[object] = {o.event_id for o in observations(a) if o.metric != "players_current"}
    chart_b: set[object] = {o.event_id for o in observations(b) if o.metric != "players_current"}
    players_a: set[object] = {o.event_id for o in observations(a) if o.metric == "players_current"}
    players_b: set[object] = {o.event_id for o in observations(b) if o.metric == "players_current"}
    assert len(chart_a) == 6
    assert chart_a == chart_b
    assert len(players_a) == 3
    assert players_a.isdisjoint(players_b)


async def test_27_failure_isolation(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    router: Router = Router(online={730: 747936, 570: [resp(500)], 578080: [no_data_404()]})
    out: Outcome = await run(router)
    report: CycleReport = out.report
    broker: InMemoryBroker = out.broker
    obs: list[Observation] = observations(broker)
    assert len(obs) == 7
    assert sum(1 for o in obs if o.metric == "chart_rank") == 3
    assert sum(1 for o in obs if o.metric == "chart_peak_players") == 3
    assert [o.entity_id for o in obs if o.metric == "players_current"] == ["730"]
    assert report.failed == (570,)
    assert report.no_data == (578080,)
    assert report.publish_ok is True
    assert report.published == 7
    assert any(
        rec.levelno == logging.WARNING and rec.name == LOGGER and "570" in rec.getMessage()
        for rec in caplog.records
    )


class BrokenClient(SteamClient):
    def __init__(self, chart: Chart) -> None:
        self.chart: Chart = chart
        self.asked: list[int] = []

    async def most_played(self) -> Chart:
        return self.chart

    async def current_players(self, appid: int) -> int | None:
        self.asked.append(appid)
        if appid == 570:
            raise RuntimeError("unexpected")
        return 10


async def test_28_arbitrary_exception_goes_to_failed() -> None:
    chart: Chart = Chart(
        rollup_date=ROLLUP,
        entries=(ChartEntry(1, 730, 5), ChartEntry(2, 570, 6), ChartEntry(3, 578080, 7)),
    )
    client: BrokenClient = BrokenClient(chart)
    broker: InMemoryBroker = InMemoryBroker()
    report: CycleReport = await SteamCollector(client, InMemoryPublisher(broker)).run_slot(T10)
    assert report.failed == (570,)
    assert client.asked == [730, 570, 578080]
    assert [o.entity_id for o in observations(broker) if o.metric == "players_current"] == ["730", "578080"]


async def test_29_chart_failure(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    publisher: CountingPublisher = CountingPublisher()
    router: Router = Router(chart=resp(503))
    r: Rig
    async with rig(router, max_attempts=4) as r:
        report: CycleReport = await SteamCollector(r.client, publisher).run_slot(T10)
    assert report == CycleReport(
        slot=T10, chart_ok=False, games=0, no_data=(), failed=(), published=0, publish_ok=True
    )
    assert publisher.calls == []
    assert router.asked_appids() == []
    assert any(rec.levelno == logging.ERROR for rec in caplog.records)


async def test_30_top_n_limits_games() -> None:
    router: Router = Router()
    out: Outcome = await run(router, top_n=2)
    report: CycleReport = out.report
    assert router.asked_appids() == [730, 570]
    assert report.games == 2


async def test_30_duplicate_appid_keeps_first_entry() -> None:
    ranks: Sequence[tuple[int, int, int]] = [(1, 730, 111), (2, 570, 222), (5, 730, 999)]
    router: Router = Router(chart=resp(200, chart_body(ranks)))
    out: Outcome = await run(router)
    broker: InMemoryBroker = out.broker
    assert router.asked_appids() == [730, 570]
    mine: list[Observation] = [o for o in observations(broker) if o.entity_id == "730"]
    assert [(o.metric, o.value) for o in mine if o.metric != "players_current"] == [
        ("chart_rank", 1),
        ("chart_peak_players", 111),
    ]


async def test_30_empty_chart() -> None:
    publisher: CountingPublisher = CountingPublisher()
    router: Router = Router(chart=resp(200, chart_body([])))
    r: Rig
    async with rig(router) as r:
        report: CycleReport = await SteamCollector(r.client, publisher).run_slot(T10)
    assert report.chart_ok is True
    assert report.games == 0
    assert publisher.calls == []


async def test_31_publish_error_is_reported_not_raised(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    publisher: CountingPublisher = CountingPublisher(fail=True)
    r: Rig
    async with rig(Router()) as r:
        report: CycleReport = await SteamCollector(r.client, publisher).run_slot(T10)
    assert report.publish_ok is False
    assert report.published == 0
    assert report.chart_ok is True
    assert len(publisher.calls) == 1
    assert len(publisher.calls[0]) == 9
    assert any(rec.levelno == logging.ERROR for rec in caplog.records)


async def test_32_naive_slot_is_value_error() -> None:
    r: Rig
    async with rig(Router()) as r:
        with pytest.raises(ValueError):
            await SteamCollector(r.client, CountingPublisher()).run_slot(datetime(2026, 11, 12, 10, 0))


async def test_all_events_in_one_publish_call() -> None:
    publisher: CountingPublisher = CountingPublisher()
    r: Rig
    async with rig(Router()) as r:
        await SteamCollector(r.client, publisher).run_slot(T10)
    assert [len(c) for c in publisher.calls] == [9]
