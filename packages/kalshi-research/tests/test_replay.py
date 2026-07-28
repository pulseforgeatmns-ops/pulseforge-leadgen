import pytest

from datetime import datetime, timedelta, timezone

from kalshi_research.domain import MarketSnapshot
from kalshi_research.replay import (
    DEFAULT_FEE_RATE,
    fee_cents_for_trade,
    format_replay,
    format_replay_sweep,
    format_train_test_split,
    replay_buy_below,
    replay_threshold_sweep,
    replay_train_test_split,
)
from kalshi_research.storage.repository import ResearchRepository


def test_fee_cents_for_trade_uses_kalshi_style_formula() -> None:
    # fee = ceil(0.07 * 1 * 0.30 * 0.70 * 100) = ceil(1.47) = 2
    assert fee_cents_for_trade(30, fee_rate=0.07) == 2
    # A zero fee rate never charges a fee.
    assert fee_cents_for_trade(30, fee_rate=0) == 0
    # Fee scales with contract count.
    assert fee_cents_for_trade(30, contracts=10, fee_rate=0.07) == 15


def test_fee_cents_for_trade_rejects_invalid_input() -> None:
    with pytest.raises(ValueError):
        fee_cents_for_trade(150)
    with pytest.raises(ValueError):
        fee_cents_for_trade(30, fee_rate=-0.01)
    with pytest.raises(ValueError):
        fee_cents_for_trade(30, contracts=-1)


def test_replay_uses_first_eligible_snapshot_and_outcome(tmp_path) -> None:
    repository = ResearchRepository(f"sqlite:///{tmp_path / 'research.db'}")
    repository.initialize()
    now = datetime.now(timezone.utc)
    repository.save_market_snapshot(MarketSnapshot("YES", now, 30, 40))
    repository.save_market_snapshot(MarketSnapshot("YES", now, 30, 30))
    repository.save_market_snapshot(MarketSnapshot("NO", now, 30, 20))
    repository.record_market_outcome("YES", "yes", now)
    repository.record_market_outcome("NO", "no", now)

    summary = replay_buy_below(f"sqlite:///{tmp_path / 'research.db'}", 35)
    assert (summary.entries, summary.wins, summary.pnl_cents) == (2, 1, 50)
    # Default fee assumptions are applied automatically and are always <= pre-fee P/L.
    assert summary.fee_rate == DEFAULT_FEE_RATE
    assert summary.fees_cents == fee_cents_for_trade(30) + fee_cents_for_trade(20)
    assert summary.pnl_after_fees_cents == summary.pnl_cents - summary.fees_cents
    assert "fee-adjusted" in format_replay(summary).lower()


def test_replay_fee_rate_is_configurable_and_can_be_disabled(tmp_path) -> None:
    repository = ResearchRepository(f"sqlite:///{tmp_path / 'research.db'}")
    repository.initialize()
    now = datetime.now(timezone.utc)
    repository.save_market_snapshot(MarketSnapshot("YES", now, 30, 30))
    repository.record_market_outcome("YES", "yes", now)

    zero_fee = replay_buy_below(f"sqlite:///{tmp_path / 'research.db'}", 35, fee_rate=0)
    assert zero_fee.fees_cents == 0
    assert zero_fee.pnl_after_fees_cents == zero_fee.pnl_cents

    higher_fee = replay_buy_below(f"sqlite:///{tmp_path / 'research.db'}", 35, fee_rate=0.5)
    assert higher_fee.fees_cents > zero_fee.fees_cents
    assert higher_fee.pnl_after_fees_cents < higher_fee.pnl_cents


def test_replay_min_edge_filter_excludes_thin_edge_entries(tmp_path) -> None:
    repository = ResearchRepository(f"sqlite:///{tmp_path / 'research.db'}")
    repository.initialize()
    now = datetime.now(timezone.utc)
    # ask=48c: best case (100-48-fee) minus worst case (48+fee) = 4c minus 2x fee, a thin edge.
    repository.save_market_snapshot(MarketSnapshot("THIN", now, 45, 48))
    repository.record_market_outcome("THIN", "yes", now)

    unfiltered = replay_buy_below(f"sqlite:///{tmp_path / 'research.db'}", 48, min_edge_cents=0)
    assert unfiltered.entries == 1

    filtered = replay_buy_below(f"sqlite:///{tmp_path / 'research.db'}", 48, min_edge_cents=10)
    assert filtered.entries == 0
    assert filtered.pnl_cents == 0
    assert filtered.min_edge_cents == 10
    assert "edge filter" in format_replay(filtered).lower()


def test_replay_threshold_sweep_ranks_multiple_cutoffs(tmp_path) -> None:
    repository = ResearchRepository(f"sqlite:///{tmp_path / 'research.db'}")
    repository.initialize()
    now = datetime.now(timezone.utc)
    repository.save_market_snapshot(MarketSnapshot("YES", now, 20, 25))
    repository.save_market_snapshot(MarketSnapshot("NO", now, 30, 35))
    repository.record_market_outcome("YES", "yes", now)
    repository.record_market_outcome("NO", "no", now)

    rows = replay_threshold_sweep(f"sqlite:///{tmp_path / 'research.db'}", 20, 35)
    assert rows[0].threshold_cents == 20
    assert rows[-1].entries == 2
    assert rows[-1].pnl_cents == 40
    expected_fees = fee_cents_for_trade(25) + fee_cents_for_trade(35)
    assert rows[-1].fees_cents == expected_fees
    assert rows[-1].pnl_after_fees_cents == 40 - expected_fees
    assert "Threshold Sweep" in format_replay_sweep(rows, limit=3)


def test_replay_train_test_split_selects_on_train_then_scores_test(tmp_path) -> None:
    repository = ResearchRepository(f"sqlite:///{tmp_path / 'research.db'}")
    repository.initialize()
    now = datetime.now(timezone.utc)
    repository.save_market_snapshot(MarketSnapshot("TRAIN_WIN_LOW", now, 10, 15))
    repository.save_market_snapshot(MarketSnapshot("TRAIN_LOSE_HIGH", now + timedelta(minutes=1), 60, 65))
    repository.save_market_snapshot(MarketSnapshot("TEST_LOSE_LOW", now + timedelta(minutes=2), 10, 15))
    repository.save_market_snapshot(MarketSnapshot("TEST_WIN_HIGH", now + timedelta(minutes=3), 60, 65))
    repository.record_market_outcome("TRAIN_WIN_LOW", "yes", now)
    repository.record_market_outcome("TRAIN_LOSE_HIGH", "no", now)
    repository.record_market_outcome("TEST_LOSE_LOW", "no", now)
    repository.record_market_outcome("TEST_WIN_HIGH", "yes", now)

    summary = replay_train_test_split(f"sqlite:///{tmp_path / 'research.db'}", 10, 60, 0.5)
    assert summary.selected_threshold_cents == 15
    assert summary.train_result.pnl_cents == 85
    assert summary.test_result.pnl_cents == -15
    fee_15 = fee_cents_for_trade(15)
    assert summary.train_result.fees_cents == fee_15
    assert summary.train_result.pnl_after_fees_cents == 85 - fee_15
    assert summary.test_result.fees_cents == fee_15
    assert summary.test_result.pnl_after_fees_cents == -15 - fee_15
    output = format_train_test_split(summary)
    assert "Train/Test Replay" in output
    assert "research-only" in output.lower()


def test_replay_train_test_split_selection_uses_fee_adjusted_pnl(tmp_path) -> None:
    # A single train ticker whose raw win is small; a punitive fee rate should turn its
    # fee-adjusted result negative, and the split must select/report that fee-adjusted value
    # (not silently fall back to the pre-fee number) for both train and test.
    repository = ResearchRepository(f"sqlite:///{tmp_path / 'research.db'}")
    repository.initialize()
    now = datetime.now(timezone.utc)
    repository.save_market_snapshot(MarketSnapshot("TRAIN_A", now, 45, 50))
    repository.save_market_snapshot(MarketSnapshot("TEST_A", now + timedelta(minutes=1), 45, 50))
    repository.record_market_outcome("TRAIN_A", "yes", now)
    repository.record_market_outcome("TEST_A", "yes", now)

    summary = replay_train_test_split(
        f"sqlite:///{tmp_path / 'research.db'}", 50, 50, 0.5, fee_rate=3.0,
    )
    fee_50_at_rate_3 = fee_cents_for_trade(50, fee_rate=3.0)
    assert fee_50_at_rate_3 > 50  # the fee alone exceeds the raw $0.50 win
    assert summary.train_result.pnl_after_fees_cents == 50 - fee_50_at_rate_3
    assert summary.test_result.pnl_after_fees_cents == 50 - fee_50_at_rate_3
    assert summary.train_result.pnl_after_fees_cents < 0


def test_min_edge_and_fee_rate_are_carried_through_sweep_and_split(tmp_path) -> None:
    repository = ResearchRepository(f"sqlite:///{tmp_path / 'research.db'}")
    repository.initialize()
    now = datetime.now(timezone.utc)
    repository.save_market_snapshot(MarketSnapshot("A", now, 45, 48))
    repository.save_market_snapshot(MarketSnapshot("B", now + timedelta(minutes=1), 45, 48))
    repository.record_market_outcome("A", "yes", now)
    repository.record_market_outcome("B", "yes", now)

    rows = replay_threshold_sweep(
        f"sqlite:///{tmp_path / 'research.db'}", 48, 48, min_edge_cents=10,
    )
    assert rows[0].entries == 0

    summary = replay_train_test_split(
        f"sqlite:///{tmp_path / 'research.db'}", 48, 48, 0.5, min_edge_cents=10,
    )
    assert summary.min_edge_cents == 10
    assert summary.train_result.entries == 0
    assert summary.test_result.entries == 0
