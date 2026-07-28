from datetime import datetime, timedelta, timezone

from kalshi_research.domain import MarketSnapshot
from kalshi_research.replay import fee_cents_for_trade
from kalshi_research.reporting.diagnostics import (
    DataDiagnostics,
    MAX_MARKETS_WITH_GAPS_PCT,
    MAX_MISSING_BID_ASK_PCT,
    MIN_RESOLVED_MARKETS_FOR_FEATURES,
    MIN_TEST_MARKETS,
    PriceBand,
    format_diagnostics,
    inspect_diagnostics,
    readiness_verdict,
)
from kalshi_research.storage.repository import ResearchRepository


def test_diagnose_data_counts_markets_snapshots_and_price_bands(tmp_path) -> None:
    repository = ResearchRepository(f"sqlite:///{tmp_path / 'research.db'}")
    repository.initialize()
    now = datetime.now(timezone.utc)
    repository.save_market_snapshot(MarketSnapshot("A", now, 20, 25))
    repository.save_market_snapshot(MarketSnapshot("B", now + timedelta(minutes=1), 50, 55))
    # Unresolved market with missing bid/ask fields, to exercise the data-quality checks.
    repository.save_market_snapshot(MarketSnapshot("C", now + timedelta(minutes=2), None, None))
    repository.record_market_outcome("A", "yes", now)
    repository.record_market_outcome("B", "no", now)

    diagnostics = inspect_diagnostics(f"sqlite:///{tmp_path / 'research.db'}", train_fraction=0.5)

    assert (diagnostics.total_markets, diagnostics.resolved_markets, diagnostics.unresolved_markets) == (3, 2, 1)
    assert diagnostics.total_snapshots == 3
    assert (diagnostics.snapshots_per_market_mean, diagnostics.snapshots_per_market_max) == (1.0, 1)
    assert (diagnostics.snapshots_missing_bid, diagnostics.snapshots_missing_ask) == (1, 1)
    # Only resolved markets participate in the train/test coverage count.
    assert (diagnostics.train_markets, diagnostics.test_markets) == (1, 1)
    # Single-snapshot markets cannot have a within-market gap.
    assert diagnostics.markets_with_within_market_gaps == 0
    assert diagnostics.largest_within_market_gap_seconds is None

    band_25 = next(b for b in diagnostics.price_bands if b.low_cents <= 25 <= b.high_cents)
    band_55 = next(b for b in diagnostics.price_bands if b.low_cents <= 55 <= b.high_cents)
    assert band_25.entries == 1
    assert band_25.notional_cents == 25
    assert band_25.fees_cents == fee_cents_for_trade(25)
    assert band_55.entries == 1
    assert band_55.notional_cents == 55

    output = format_diagnostics(diagnostics)
    assert "Data Diagnostics" in output
    assert "NOT YET ready" in output
    assert "read-only" in output.lower()


def test_diagnose_data_flags_within_market_collection_gaps(tmp_path) -> None:
    repository = ResearchRepository(f"sqlite:///{tmp_path / 'research.db'}")
    repository.initialize()
    now = datetime.now(timezone.utc)
    repository.save_market_snapshot(MarketSnapshot("GAPPY", now, 30, 35))
    repository.save_market_snapshot(MarketSnapshot("GAPPY", now + timedelta(minutes=5), 30, 35))
    repository.record_market_outcome("GAPPY", "yes", now)

    diagnostics = inspect_diagnostics(f"sqlite:///{tmp_path / 'research.db'}")

    assert diagnostics.markets_with_within_market_gaps == 1
    assert diagnostics.largest_within_market_gap_seconds == 300.0


def test_diagnose_data_is_read_only_and_does_not_mutate_database(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'research.db'}"
    repository = ResearchRepository(database_url)
    repository.initialize()
    now = datetime.now(timezone.utc)
    repository.save_market_snapshot(MarketSnapshot("A", now, 20, 25))
    repository.record_market_outcome("A", "yes", now)

    before = inspect_diagnostics(database_url)
    inspect_diagnostics(database_url)
    after = inspect_diagnostics(database_url)
    assert before == after


def test_readiness_verdict_flags_thin_sample_and_data_quality_issues() -> None:
    thin = DataDiagnostics(
        total_markets=10, resolved_markets=10, unresolved_markets=0,
        first_snapshot="t0", last_snapshot="t1", train_fraction=0.5,
        train_markets=5, test_markets=5, total_snapshots=100,
        snapshots_per_market_mean=10.0, snapshots_per_market_median=10.0,
        snapshots_per_market_min=10, snapshots_per_market_max=10,
        markets_with_within_market_gaps=0, largest_within_market_gap_seconds=None,
        snapshots_missing_bid=0, snapshots_missing_ask=0,
        fee_rate=0.07, price_bands=(),
    )
    reasons = readiness_verdict(thin)
    assert any("resolved markets" in reason for reason in reasons)
    assert any("test window" in reason for reason in reasons)
    assert "NOT YET ready" in format_diagnostics(thin)


def test_readiness_verdict_passes_when_all_thresholds_are_met() -> None:
    ready = DataDiagnostics(
        total_markets=200, resolved_markets=MIN_RESOLVED_MARKETS_FOR_FEATURES,
        unresolved_markets=50, first_snapshot="t0", last_snapshot="t1", train_fraction=0.5,
        train_markets=100, test_markets=MIN_TEST_MARKETS,
        total_snapshots=10_000,
        snapshots_per_market_mean=50.0, snapshots_per_market_median=50.0,
        snapshots_per_market_min=10, snapshots_per_market_max=90,
        markets_with_within_market_gaps=0, largest_within_market_gap_seconds=None,
        snapshots_missing_bid=0, snapshots_missing_ask=0,
        fee_rate=0.07, price_bands=(PriceBand(21, 30, 5, 10, 125),),
    )
    assert readiness_verdict(ready) == []
    output = format_diagnostics(ready)
    assert "dataset looks ready for feature research" in output
    assert "NOT YET" not in output


def test_readiness_verdict_flags_missing_bid_ask_and_collection_gaps() -> None:
    noisy = DataDiagnostics(
        total_markets=200, resolved_markets=MIN_RESOLVED_MARKETS_FOR_FEATURES,
        unresolved_markets=0, first_snapshot="t0", last_snapshot="t1", train_fraction=0.5,
        train_markets=100, test_markets=MIN_TEST_MARKETS,
        total_snapshots=1_000,
        # 5% of bid/ask fields missing, well above MAX_MISSING_BID_ASK_PCT.
        snapshots_per_market_mean=5.0, snapshots_per_market_median=5.0,
        snapshots_per_market_min=1, snapshots_per_market_max=20,
        markets_with_within_market_gaps=40,  # 20% of markets, above MAX_MARKETS_WITH_GAPS_PCT
        largest_within_market_gap_seconds=900.0,
        snapshots_missing_bid=50, snapshots_missing_ask=50,
        fee_rate=0.07, price_bands=(),
    )
    reasons = readiness_verdict(noisy)
    assert any("bid/ask" in reason for reason in reasons)
    assert any("collection gap" in reason for reason in reasons)


def test_price_band_fee_drag_pct_handles_zero_notional() -> None:
    empty_band = PriceBand(1, 10, 0, 0, 0)
    assert empty_band.fee_drag_pct == 0.0


def test_max_missing_bid_ask_and_gap_constants_are_percentages() -> None:
    assert 0 < MAX_MISSING_BID_ASK_PCT < 100
    assert 0 < MAX_MARKETS_WITH_GAPS_PCT < 100
