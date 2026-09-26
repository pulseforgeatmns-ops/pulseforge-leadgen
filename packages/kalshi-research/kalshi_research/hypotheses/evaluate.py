"""Fee-aware evaluation for entry-time midpoint hypotheses (paper/replay only)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Sequence

from sqlalchemy import create_engine, text

from kalshi_research.hypotheses.registry import (
    FORBIDDEN_ENTRY_FEATURES,
    HypothesisCandidate,
    get_hypothesis,
)
from kalshi_research.replay import (
    DEFAULT_FEE_RATE,
    ReplaySweepRow,
    fee_cents_for_trade,
)


@dataclass(frozen=True, slots=True)
class EntryMarket:
    """One resolved market summarized at first observation (entry time)."""

    ticker: str
    first_observed_at: datetime
    first_yes_ask_cents: int
    first_yes_bid_cents: int
    midpoint_cents: float
    result: str

    def __post_init__(self) -> None:
        if self.result not in ("yes", "no"):
            raise ValueError("result must be 'yes' or 'no'")


@dataclass(frozen=True, slots=True)
class FoldResult:
    fold_index: int
    train_markets: int
    test_markets: int
    selected_threshold_cents: int
    train_result: ReplaySweepRow
    test_result: ReplaySweepRow


@dataclass(frozen=True, slots=True)
class SensitivityPoint:
    threshold_cents: int
    result: ReplaySweepRow
    delta_from_selected: int


@dataclass(frozen=True, slots=True)
class HypothesisEvaluationReport:
    hypothesis: HypothesisCandidate
    train_fraction: float
    train_markets: int
    test_markets: int
    threshold_min: int
    threshold_max: int
    selected_threshold_cents: int
    train_sweep: tuple[ReplaySweepRow, ...]
    train_result: ReplaySweepRow
    test_result: ReplaySweepRow
    walk_forward: tuple[FoldResult, ...]
    sensitivity: tuple[SensitivityPoint, ...]
    fee_rate: float
    verdict: str
    verdict_reasons: tuple[str, ...]

    @property
    def sign_stable_walk_forward(self) -> bool:
        return all(fold.test_result.pnl_after_fees_cents > 0 for fold in self.walk_forward)

    @property
    def sign_stable_sensitivity(self) -> bool:
        if not self.sensitivity:
            return False
        selected_sign = _sign(self.test_result.pnl_after_fees_cents)
        return all(_sign(point.result.pnl_after_fees_cents) == selected_sign for point in self.sensitivity)


def evaluate_hypothesis(
    database_url: str,
    hypothesis_id: str = "H-005",
    train_fraction: float = 0.5,
    fee_rate: float = DEFAULT_FEE_RATE,
    walk_forward_folds: int = 4,
    sensitivity_radius: int = 2,
    markets: Sequence[EntryMarket] | None = None,
) -> HypothesisEvaluationReport:
    """Select threshold on train fee-adjusted P/L, then score test / folds / sensitivity."""
    hypothesis = get_hypothesis(hypothesis_id)
    _assert_entry_time_safe(hypothesis)

    if hypothesis.condition_feature != "midpoint_cents" or hypothesis.condition_op != ">":
        raise ValueError(f"{hypothesis_id} is not an entry-midpoint-above rule")
    if hypothesis.train_threshold_min is None or hypothesis.train_threshold_max is None:
        raise ValueError(f"{hypothesis_id} is missing a train threshold sweep range")

    threshold_min = hypothesis.train_threshold_min
    threshold_max = hypothesis.train_threshold_max
    if threshold_min > threshold_max:
        raise ValueError("train threshold range is inverted")

    all_markets = list(markets) if markets is not None else load_entry_markets(database_url)
    if len(all_markets) < 2:
        raise ValueError("at least two resolved markets are required")

    train, test = chronological_split(all_markets, train_fraction)
    train_sweep = tuple(
        evaluate_midpoint_above(train, threshold, fee_rate)
        for threshold in range(threshold_min, threshold_max + 1)
    )
    selected = _select_best_row(train_sweep)
    train_result = selected
    test_result = evaluate_midpoint_above(test, selected.threshold_cents, fee_rate)

    folds = tuple(
        _walk_forward_fold(
            all_markets,
            fold_index=index,
            fold_count=walk_forward_folds,
            threshold_min=threshold_min,
            threshold_max=threshold_max,
            fee_rate=fee_rate,
        )
        for index in range(walk_forward_folds)
    )
    sensitivity = tuple(
        SensitivityPoint(
            threshold_cents=threshold,
            result=evaluate_midpoint_above(test, threshold, fee_rate),
            delta_from_selected=threshold - selected.threshold_cents,
        )
        for threshold in _sensitivity_thresholds(
            selected.threshold_cents, sensitivity_radius, threshold_min, threshold_max
        )
    )

    verdict, reasons = _decide_verdict(test_result, folds, sensitivity)
    return HypothesisEvaluationReport(
        hypothesis=hypothesis,
        train_fraction=train_fraction,
        train_markets=len(train),
        test_markets=len(test),
        threshold_min=threshold_min,
        threshold_max=threshold_max,
        selected_threshold_cents=selected.threshold_cents,
        train_sweep=train_sweep,
        train_result=train_result,
        test_result=test_result,
        walk_forward=folds,
        sensitivity=sensitivity,
        fee_rate=fee_rate,
        verdict=verdict,
        verdict_reasons=reasons,
    )


def load_entry_markets(database_url: str) -> list[EntryMarket]:
    """Load first-snapshot entry features for every resolved market (read-only)."""
    engine = create_engine(database_url)
    with engine.connect() as connection:
        rows = connection.execute(text("""
            with ranked as (
                select s.ticker, s.yes_ask_cents, s.yes_bid_cents, s.observed_at, o.result,
                       row_number() over (
                           partition by s.ticker order by s.observed_at, s.id
                       ) as snapshot_rank
                from market_snapshots s
                join market_outcomes o on o.ticker = s.ticker
                where o.result in ('yes', 'no')
            )
            select ticker, yes_ask_cents, yes_bid_cents, observed_at, result
            from ranked
            where snapshot_rank = 1
            order by observed_at, ticker
        """)).mappings().all()

    markets: list[EntryMarket] = []
    for row in rows:
        ask = row["yes_ask_cents"]
        bid = row["yes_bid_cents"]
        if ask is None or bid is None:
            continue
        if not 1 <= int(ask) <= 99:
            continue
        markets.append(
            EntryMarket(
                ticker=row["ticker"],
                first_observed_at=_as_datetime(row["observed_at"]),
                first_yes_ask_cents=int(ask),
                first_yes_bid_cents=int(bid),
                midpoint_cents=(float(bid) + float(ask)) / 2.0,
                result=row["result"],
            )
        )
    return markets


def chronological_split(
    markets: Sequence[EntryMarket],
    train_fraction: float,
) -> tuple[list[EntryMarket], list[EntryMarket]]:
    if not 0 < train_fraction < 1:
        raise ValueError("train_fraction must be between 0 and 1")
    if len(markets) < 2:
        raise ValueError("at least two markets are required for a train/test split")
    split_at = round(len(markets) * train_fraction)
    split_at = min(max(1, split_at), len(markets) - 1)
    return list(markets[:split_at]), list(markets[split_at:])


def evaluate_midpoint_above(
    markets: Sequence[EntryMarket],
    threshold_cents: int | float,
    fee_rate: float = DEFAULT_FEE_RATE,
) -> ReplaySweepRow:
    """Buy YES at first_yes_ask iff entry midpoint > threshold; one trade per market."""
    pnl = 0
    fees = 0
    pnl_after_fees = 0
    peak = 0
    max_drawdown = 0
    wins = 0
    entries = 0
    for market in markets:
        if market.midpoint_cents <= float(threshold_cents):
            continue
        price = market.first_yes_ask_cents
        fee = fee_cents_for_trade(price, fee_rate=fee_rate)
        trade_pnl = (100 - price) if market.result == "yes" else -price
        entries += 1
        wins += trade_pnl > 0
        pnl += trade_pnl
        fees += fee
        pnl_after_fees += trade_pnl - fee
        peak = max(peak, pnl)
        max_drawdown = max(max_drawdown, peak - pnl)
    return ReplaySweepRow(
        threshold_cents=int(threshold_cents),
        entries=entries,
        wins=wins,
        pnl_cents=pnl,
        max_drawdown_cents=max_drawdown,
        fees_cents=fees,
        pnl_after_fees_cents=pnl_after_fees,
    )


def format_hypothesis_evaluation(report: HypothesisEvaluationReport) -> str:
    hyp = report.hypothesis
    train = report.train_result
    test = report.test_result
    lines = [
        "Kalshi Research — Hypothesis Evaluation (paper/replay only)",
        f"Hypothesis: {hyp.hypothesis_id} / {hyp.name}",
        f"Rule: buy YES @ first_yes_ask when entry midpoint > threshold "
        f"(sweep {report.threshold_min}-{report.threshold_max}c on train fee-adjusted P/L)",
        f"Forbidden entry features: {', '.join(sorted(FORBIDDEN_ENTRY_FEATURES))}",
        f"Fee rate: {report.fee_rate:.2%}",
        f"Train markets: {report.train_markets}; test markets: {report.test_markets} "
        f"(chronological {report.train_fraction:.0%} split)",
        f"Selected threshold: {report.selected_threshold_cents}c",
        "",
        "=== TRAIN (selection window) ===",
        _format_metrics("Train", train),
        "",
        "=== TEST (untouched holdout) ===",
        _format_metrics("Test", test),
        "",
        "=== WALK-FORWARD ===",
    ]
    if not report.walk_forward:
        lines.append("No walk-forward folds produced.")
    for fold in report.walk_forward:
        lines.append(
            f"Fold {fold.fold_index + 1}: selected={fold.selected_threshold_cents}c; "
            f"train_n={fold.train_markets}; test_n={fold.test_markets}; "
            f"test_entries={fold.test_result.entries}; "
            f"test_net=${fold.test_result.pnl_after_fees_cents / 100:.2f}; "
            f"test_sign={_sign_label(fold.test_result.pnl_after_fees_cents)}"
        )
    lines.append(
        f"Walk-forward sign stability: "
        f"{'PASS' if report.sign_stable_walk_forward else 'FAIL'}"
    )
    lines.extend(("", "=== SENSITIVITY (test window around selected threshold) ==="))
    for point in report.sensitivity:
        marker = " <-- selected" if point.delta_from_selected == 0 else ""
        lines.append(
            f"threshold={point.threshold_cents}c (Δ{point.delta_from_selected:+d}): "
            f"entries={point.result.entries}; "
            f"net=${point.result.pnl_after_fees_cents / 100:.2f}; "
            f"sign={_sign_label(point.result.pnl_after_fees_cents)}{marker}"
        )
    lines.append(
        f"Sensitivity sign stability: "
        f"{'PASS' if report.sign_stable_sensitivity else 'FAIL'}"
    )
    lines.extend(
        (
            "",
            f"VERDICT: {report.verdict}",
            *[f"- {reason}" for reason in report.verdict_reasons],
            "Paper/replay research only — not evidence of a durable live edge.",
        )
    )
    return "\n".join(lines)


def _walk_forward_fold(
    markets: Sequence[EntryMarket],
    fold_index: int,
    fold_count: int,
    threshold_min: int,
    threshold_max: int,
    fee_rate: float,
) -> FoldResult:
    if fold_count < 2:
        raise ValueError("walk_forward_folds must be >= 2")
    n = len(markets)
    # Expanding-window walk-forward: each fold trains on all prior markets and tests
    # the next chronological block. Thresholds are re-selected inside each train window only.
    block = n // (fold_count + 1)
    if block < 1:
        block = 1
    train_end = min(n - 1, block * (fold_index + 1))
    test_end = min(n, train_end + block)
    if fold_index == fold_count - 1:
        test_end = n
    train = list(markets[:train_end])
    test = list(markets[train_end:test_end])
    if not train or not test:
        empty = ReplaySweepRow(threshold_min, 0, 0, 0, 0, 0, 0)
        return FoldResult(fold_index, len(train), len(test), threshold_min, empty, empty)

    sweep = [
        evaluate_midpoint_above(train, threshold, fee_rate)
        for threshold in range(threshold_min, threshold_max + 1)
    ]
    selected = _select_best_row(sweep)
    test_result = evaluate_midpoint_above(test, selected.threshold_cents, fee_rate)
    return FoldResult(
        fold_index=fold_index,
        train_markets=len(train),
        test_markets=len(test),
        selected_threshold_cents=selected.threshold_cents,
        train_result=selected,
        test_result=test_result,
    )


def _select_best_row(rows: Sequence[ReplaySweepRow]) -> ReplaySweepRow:
    return sorted(
        rows,
        key=lambda row: (row.pnl_after_fees_cents, -row.max_drawdown_cents, row.entries),
        reverse=True,
    )[0]


def _sensitivity_thresholds(
    selected: int,
    radius: int,
    lo: int,
    hi: int,
) -> list[int]:
    if radius < 0:
        raise ValueError("sensitivity_radius must be non-negative")
    values = [t for t in range(selected - radius, selected + radius + 1) if lo <= t <= hi]
    if selected not in values:
        values.append(selected)
    return sorted(set(values))


def _decide_verdict(
    test_result: ReplaySweepRow,
    folds: Sequence[FoldResult],
    sensitivity: Sequence[SensitivityPoint],
) -> tuple[str, tuple[str, ...]]:
    reasons: list[str] = []
    failures = 0

    if test_result.entries == 0:
        failures += 1
        reasons.append("Test produced zero entries at the selected threshold.")
    elif test_result.pnl_after_fees_cents <= 0:
        failures += 1
        reasons.append(
            f"Test fee-adjusted P/L is non-positive "
            f"(${test_result.pnl_after_fees_cents / 100:.2f})."
        )
    else:
        reasons.append(
            f"Test fee-adjusted P/L is positive "
            f"(${test_result.pnl_after_fees_cents / 100:.2f})."
        )

    scored_folds = [fold for fold in folds if fold.test_markets > 0]
    if not scored_folds:
        failures += 1
        reasons.append("Walk-forward produced no scored folds.")
    else:
        positive = sum(1 for fold in scored_folds if fold.test_result.pnl_after_fees_cents > 0)
        if positive < len(scored_folds):
            failures += 1
            reasons.append(
                f"Walk-forward sign stability failed ({positive}/{len(scored_folds)} "
                "folds positive on fee-adjusted test P/L)."
            )
        else:
            reasons.append(
                f"Walk-forward sign stability passed ({positive}/{len(scored_folds)} folds positive)."
            )

    if not sensitivity:
        failures += 1
        reasons.append("Sensitivity grid is empty.")
    else:
        selected_sign = _sign(test_result.pnl_after_fees_cents)
        stable = all(_sign(point.result.pnl_after_fees_cents) == selected_sign for point in sensitivity)
        if not stable or selected_sign <= 0:
            failures += 1
            reasons.append(
                "Sensitivity around the selected threshold is not stably positive on test."
            )
        else:
            reasons.append(
                "Sensitivity around the selected threshold keeps a positive fee-adjusted sign on test."
            )

    if failures:
        reasons.append("Marking hypothesis as probably_noise; do not promote.")
        return "probably_noise", tuple(reasons)
    reasons.append("Survived test, walk-forward, and sensitivity checks — promote as research candidate only.")
    return "promoted", tuple(reasons)


def _assert_entry_time_safe(hypothesis: HypothesisCandidate) -> None:
    used = {hypothesis.entry_price_feature, hypothesis.condition_feature}
    leaked = used & FORBIDDEN_ENTRY_FEATURES
    if leaked:
        raise ValueError(f"entry rule must not use {sorted(leaked)}")


def _format_metrics(label: str, row: ReplaySweepRow) -> str:
    return (
        f"{label}: trades={row.entries}; wins={row.wins} ({row.win_rate:.1f}%); "
        f"gross P/L=${row.pnl_cents / 100:.2f}; fees=${row.fees_cents / 100:.2f}; "
        f"net P/L=${row.pnl_after_fees_cents / 100:.2f}; "
        f"max drawdown=${row.max_drawdown_cents / 100:.2f}"
    )


def _sign(value: int) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _sign_label(value: int) -> str:
    return {1: "+", -1: "-", 0: "0"}[_sign(value)]


def _as_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def assert_no_forbidden_feature_refs(source: str, labels: Iterable[str] | None = None) -> None:
    """Test helper: fail if source text references post-entry leakage features."""
    labels = tuple(labels or FORBIDDEN_ENTRY_FEATURES)
    for name in labels:
        if name in source:
            raise AssertionError(f"forbidden feature reference found: {name}")
