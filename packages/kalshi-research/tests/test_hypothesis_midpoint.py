"""Tests for BuyWhenEntryMidpointAbove and H-005 evaluation."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from kalshi_research.domain import MarketSnapshot
from kalshi_research.hypotheses.evaluate import (
    EntryMarket,
    assert_no_forbidden_feature_refs,
    chronological_split,
    evaluate_hypothesis,
    evaluate_midpoint_above,
    format_hypothesis_evaluation,
)
from kalshi_research.hypotheses.registry import (
    FORBIDDEN_ENTRY_FEATURES,
    get_hypothesis,
    list_hypotheses,
)
from kalshi_research.replay import fee_cents_for_trade
from kalshi_research.strategies.midpoint import BuyWhenEntryMidpointAbove


def _dt(minutes: int = 0) -> datetime:
    return datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=minutes)


def _market(
    ticker: str,
    minutes: int,
    bid: int,
    ask: int,
    result: str,
) -> EntryMarket:
    return EntryMarket(
        ticker=ticker,
        first_observed_at=_dt(minutes),
        first_yes_ask_cents=ask,
        first_yes_bid_cents=bid,
        midpoint_cents=(bid + ask) / 2.0,
        result=result,
    )


def test_buy_when_entry_midpoint_above_enters_on_strict_midpoint() -> None:
    strategy = BuyWhenEntryMidpointAbove(min_midpoint_cents=50)
    snap = MarketSnapshot("M1", _dt(), 50, 55)
    intents = strategy.evaluate(snap, None)
    assert len(intents) == 1
    assert intents[0].limit_price_cents == 55
    assert intents[0].side.value == "yes"
    assert strategy.hypothesis_id == "H-005"


def test_buy_when_entry_midpoint_above_skips_equal_or_below_and_bad_quotes() -> None:
    strategy = BuyWhenEntryMidpointAbove(min_midpoint_cents=50)
    assert strategy.evaluate(MarketSnapshot("EQ", _dt(), 49, 51), None) == []  # mid=50
    assert strategy.evaluate(MarketSnapshot("LOW", _dt(), 40, 45), None) == []
    assert strategy.evaluate(MarketSnapshot("MISS", _dt(), None, 55), None) == []
    assert strategy.evaluate(MarketSnapshot("ZERO", _dt(), 0, 0), None) == []


def test_strategy_source_avoids_path_features() -> None:
    import kalshi_research.strategies.midpoint as mod

    text = Path(mod.__file__).read_text(encoding="utf-8")
    assert_no_forbidden_feature_refs(text)
    for name in FORBIDDEN_ENTRY_FEATURES:
        assert name not in text


def test_h001_is_retired_and_h005_is_registered() -> None:
    h001 = get_hypothesis("H-001")
    assert h001.status == "retired"
    h005 = get_hypothesis("H-005")
    assert h005.name == "BuyWhenEntryMidpointAbove"
    assert h005.train_threshold_min == 50
    assert h005.train_threshold_max == 65
    assert h005.entry_price_feature == "first_yes_ask_cents"
    assert h005.condition_feature == "midpoint_cents"
    ids = {h.hypothesis_id for h in list_hypotheses()}
    assert {"H-001", "H-005"} <= ids


def test_evaluate_midpoint_above_uses_ask_price_and_fees() -> None:
    markets = [
        _market("WIN", 0, 60, 62, "yes"),   # mid=61 > 50
        _market("LOSE", 1, 60, 64, "no"),   # mid=62 > 50
        _market("SKIP", 2, 40, 42, "yes"),  # mid=41 <= 50
    ]
    row = evaluate_midpoint_above(markets, threshold_cents=50, fee_rate=0.07)
    assert row.entries == 2
    assert row.wins == 1
    assert row.pnl_cents == (100 - 62) + (-64)
    assert row.fees_cents == fee_cents_for_trade(62) + fee_cents_for_trade(64)
    assert row.pnl_after_fees_cents == row.pnl_cents - row.fees_cents


def test_threshold_selection_uses_train_fee_adjusted_pnl_only() -> None:
    # Train prefers the higher threshold: the low-midpoint loser is excluded only above 55.
    markets = [
        _market("T_WIN_HIGH", 0, 60, 62, "yes"),     # mid 61
        _market("T_LOSE_MID", 1, 54, 56, "no"),      # mid 55
        _market("X_WIN_HIGH", 2, 60, 62, "yes"),     # test
        _market("X_LOSE_MID", 3, 54, 56, "no"),      # test
    ]
    report = evaluate_hypothesis(
        database_url="sqlite://",
        hypothesis_id="H-005",
        train_fraction=0.5,
        fee_rate=0.0,
        walk_forward_folds=2,
        sensitivity_radius=1,
        markets=markets,
    )
    assert report.selected_threshold_cents == 55
    assert report.train_result.entries == 1
    assert report.train_result.pnl_after_fees_cents == 38
    assert report.test_result.entries == 1
    assert report.test_result.pnl_after_fees_cents == 38


def test_probably_noise_when_test_is_negative() -> None:
    markets = [
        _market("T1", 0, 60, 62, "yes"),
        _market("T2", 1, 60, 64, "yes"),
        _market("X1", 2, 60, 62, "no"),
        _market("X2", 3, 60, 64, "no"),
    ]
    report = evaluate_hypothesis(
        "sqlite://",
        markets=markets,
        fee_rate=0.0,
        walk_forward_folds=2,
        sensitivity_radius=1,
    )
    assert report.test_result.pnl_after_fees_cents < 0
    assert report.verdict == "probably_noise"
    assert "probably_noise" in format_hypothesis_evaluation(report)


def test_promoted_when_test_folds_and_sensitivity_are_positive() -> None:
    # Eight chronological markets, all high midpoint, all YES → robustly positive.
    markets = [
        _market(f"M{i}", i, 60, 62, "yes")
        for i in range(8)
    ]
    report = evaluate_hypothesis(
        "sqlite://",
        markets=markets,
        fee_rate=0.0,
        walk_forward_folds=3,
        sensitivity_radius=2,
    )
    assert report.selected_threshold_cents >= 50
    assert report.test_result.pnl_after_fees_cents > 0
    assert report.sign_stable_walk_forward
    assert report.sign_stable_sensitivity
    assert report.verdict == "promoted"
    text = format_hypothesis_evaluation(report)
    assert "VERDICT: promoted" in text
    assert "trades=" in text
    assert "gross P/L=" in text
    assert "net P/L=" in text
    assert "max drawdown=" in text


def test_chronological_split_and_forbidden_features() -> None:
    markets = [_market(f"M{i}", i, 50, 60, "yes") for i in range(4)]
    train, test = chronological_split(markets, 0.5)
    assert [m.ticker for m in train] == ["M0", "M1"]
    assert [m.ticker for m in test] == ["M2", "M3"]
    with pytest.raises(AssertionError):
        assert_no_forbidden_feature_refs("uses ask_move_cents badly")


def test_evaluate_hypothesis_from_sqlite(tmp_path) -> None:
    from kalshi_research.storage.repository import ResearchRepository

    db = f"sqlite:///{tmp_path / 'research.db'}"
    repo = ResearchRepository(db)
    repo.initialize()
    now = _dt()
    # Train: high mid YES, mid-band NO → select higher threshold.
    repo.save_market_snapshot(MarketSnapshot("TRAIN_WIN", now, 60, 62))
    repo.save_market_snapshot(MarketSnapshot("TRAIN_LOSE", now + timedelta(minutes=1), 52, 54))
    repo.save_market_snapshot(MarketSnapshot("TEST_WIN", now + timedelta(minutes=2), 60, 62))
    repo.save_market_snapshot(MarketSnapshot("TEST_LOSE", now + timedelta(minutes=3), 52, 54))
    repo.record_market_outcome("TRAIN_WIN", "yes", now)
    repo.record_market_outcome("TRAIN_LOSE", "no", now)
    repo.record_market_outcome("TEST_WIN", "yes", now)
    repo.record_market_outcome("TEST_LOSE", "no", now)

    report = evaluate_hypothesis(db, fee_rate=0.0, walk_forward_folds=2, sensitivity_radius=1)
    assert report.hypothesis.hypothesis_id == "H-005"
    assert report.selected_threshold_cents >= 53
    assert report.train_markets == 2
    assert report.test_markets == 2
    # Path features must remain unused by the rule definition.
    assert report.hypothesis.entry_price_feature not in FORBIDDEN_ENTRY_FEATURES
    assert report.hypothesis.condition_feature not in FORBIDDEN_ENTRY_FEATURES
