from datetime import datetime, timedelta, timezone

import pytest

from kalshi_research.domain import BtcPriceTick, MarketSnapshot
from kalshi_research.features import (
    FEATURE_NAMES,
    extract_market_features,
    format_feature_report,
    inspect_feature_report,
)
from kalshi_research.storage.repository import ResearchRepository


def _seed_resolved_market(
    repository: ResearchRepository,
    ticker: str,
    start: datetime,
    *,
    result: str,
    first_bid: int,
    first_ask: int,
    last_ask: int | None = None,
    close_after_seconds: int | None = 600,
    snapshot_count: int = 3,
) -> None:
    close_time = start + timedelta(seconds=close_after_seconds) if close_after_seconds is not None else None
    last_ask = first_ask if last_ask is None else last_ask
    for index in range(snapshot_count):
        ask = first_ask if index == 0 else last_ask
        bid = first_bid if index == 0 else max(0, ask - (first_ask - first_bid))
        repository.save_market_snapshot(MarketSnapshot(
            ticker,
            start + timedelta(seconds=index * 30),
            bid,
            ask,
            close_time=close_time,
        ))
    repository.record_market_outcome(ticker, result, start + timedelta(hours=1))


def test_extract_market_features_computes_deterministic_values() -> None:
    start = datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc)
    close = start + timedelta(minutes=15)
    snapshots = [
        {
            "ticker": "M1",
            "observed_at": start,
            "yes_bid_cents": 40,
            "yes_ask_cents": 45,
            "close_time": close,
        },
        {
            "ticker": "M1",
            "observed_at": start + timedelta(minutes=5),
            "yes_bid_cents": 42,
            "yes_ask_cents": 50,
            "close_time": close,
        },
    ]
    btc_ticks = [
        (start - timedelta(seconds=1), 100_000.0),
        (start + timedelta(minutes=5), 100_250.0),
    ]

    features = extract_market_features(snapshots, "yes", btc_ticks)

    assert features.ticker == "M1"
    assert features.result == "yes"
    assert features.first_yes_ask_cents == 45
    assert features.first_yes_bid_cents == 40
    assert features.spread_cents == 5
    assert features.midpoint_cents == 42.5
    assert features.seconds_to_close == 900.0
    assert features.snapshot_count == 2
    assert features.ask_move_cents == 5
    assert features.btc_move_usd == 250.0


def test_extract_market_features_handles_missing_optional_fields() -> None:
    start = datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc)
    snapshots = [
        {
            "ticker": "M2",
            "observed_at": start,
            "yes_bid_cents": None,
            "yes_ask_cents": 30,
            "close_time": None,
        },
    ]
    features = extract_market_features(snapshots, "no", btc_ticks=[])
    assert features.spread_cents is None
    assert features.midpoint_cents is None
    assert features.seconds_to_close is None
    assert features.ask_move_cents == 0
    assert features.btc_move_usd is None
    assert features.snapshot_count == 1


def test_extract_market_features_rejects_empty_or_invalid_result() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        extract_market_features([], "yes")
    with pytest.raises(ValueError, match="result"):
        extract_market_features(
            [{"ticker": "X", "observed_at": datetime.now(timezone.utc),
              "yes_bid_cents": 1, "yes_ask_cents": 2, "close_time": None}],
            "maybe",
        )


def test_feature_report_splits_train_test_and_win_lose(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'research.db'}"
    repository = ResearchRepository(database_url)
    repository.initialize()
    now = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)

    # Chronological train window: two YES markets with tight spreads / small ask moves.
    _seed_resolved_market(repository, "T1", now, result="yes", first_bid=40, first_ask=42, last_ask=43)
    _seed_resolved_market(
        repository, "T2", now + timedelta(minutes=20), result="yes",
        first_bid=38, first_ask=40, last_ask=41,
    )
    # Chronological test window: one YES + one NO with wider spreads / larger ask moves.
    _seed_resolved_market(
        repository, "T3", now + timedelta(minutes=40), result="yes",
        first_bid=20, first_ask=30, last_ask=55,
    )
    _seed_resolved_market(
        repository, "T4", now + timedelta(minutes=60), result="no",
        first_bid=10, first_ask=25, last_ask=70,
    )

    repository.save_btc_tick(BtcPriceTick(60_000.0, now - timedelta(seconds=5), "coinbase"))
    repository.save_btc_tick(BtcPriceTick(60_100.0, now + timedelta(minutes=61), "coinbase"))

    report = inspect_feature_report(database_url, train_fraction=0.5)

    assert report.total_resolved == 4
    assert (report.train_markets, report.test_markets) == (2, 2)
    assert report.feature_names == FEATURE_NAMES
    assert report.train.yes_group.market_count == 2
    assert report.train.no_group.market_count == 0
    assert report.test.yes_group.market_count == 1
    assert report.test.no_group.market_count == 1

    train_ask = next(d for d in report.train.yes_group.distributions if d.feature_name == "first_yes_ask_cents")
    assert train_ask.n == 2
    assert train_ask.mean == 41.0

    test_spread_yes = next(d for d in report.test.yes_group.distributions if d.feature_name == "spread_cents")
    test_spread_no = next(d for d in report.test.no_group.distributions if d.feature_name == "spread_cents")
    assert test_spread_yes.mean == 10.0
    assert test_spread_no.mean == 15.0

    output = format_feature_report(report)
    assert "Feature Report" in output
    assert "TRAIN" in output and "TEST" in output
    assert "first_yes_ask_cents" in output
    assert "Δmean" in output
    assert "read-only" in output.lower()
    assert "no live trading" in output.lower()


def test_feature_report_is_read_only(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'research.db'}"
    repository = ResearchRepository(database_url)
    repository.initialize()
    now = datetime.now(timezone.utc)
    _seed_resolved_market(repository, "A", now, result="yes", first_bid=30, first_ask=35)
    _seed_resolved_market(
        repository, "B", now + timedelta(minutes=1), result="no", first_bid=50, first_ask=55,
    )

    before = inspect_feature_report(database_url)
    inspect_feature_report(database_url)
    after = inspect_feature_report(database_url)
    assert before == after


def test_feature_report_requires_two_resolved_markets(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'research.db'}"
    repository = ResearchRepository(database_url)
    repository.initialize()
    now = datetime.now(timezone.utc)
    _seed_resolved_market(repository, "ONLY", now, result="yes", first_bid=30, first_ask=35)

    with pytest.raises(ValueError, match="at least two resolved markets"):
        inspect_feature_report(database_url)


def test_feature_report_rejects_invalid_train_fraction(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'research.db'}"
    repository = ResearchRepository(database_url)
    repository.initialize()
    now = datetime.now(timezone.utc)
    _seed_resolved_market(repository, "A", now, result="yes", first_bid=30, first_ask=35)
    _seed_resolved_market(
        repository, "B", now + timedelta(minutes=1), result="no", first_bid=50, first_ask=55,
    )
    with pytest.raises(ValueError, match="train_fraction"):
        inspect_feature_report(database_url, train_fraction=1.0)
