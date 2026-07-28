"""Deterministic replay of stored market snapshots. No external writes or live orders."""
import math
from dataclasses import dataclass

from sqlalchemy import create_engine, text

#: Kalshi's published general taker-fee multiplier: fee = ceil(rate * C * P * (1-P)),
#: with P expressed in dollars. Used as the conservative default fee assumption; some
#: series carry lower fees, so this is an upper-bound estimate, not an exact reproduction
#: of every market's fee schedule.
DEFAULT_FEE_RATE = 0.07


def fee_cents_for_trade(price_cents: int, contracts: int = 1, fee_rate: float = DEFAULT_FEE_RATE) -> int:
    """Conservative per-fill fee estimate in whole cents, rounded up.

    Mirrors Kalshi's general taker-fee formula: fee = ceil(rate * contracts * P * (1 - P)),
    where P is the fill price in dollars. Rounding up keeps the estimate conservative
    (never understates fees).
    """
    if not 0 <= price_cents <= 100:
        raise ValueError("price_cents must be between 0 and 100")
    if contracts < 0:
        raise ValueError("contracts must be non-negative")
    if fee_rate < 0:
        raise ValueError("fee_rate must be non-negative")
    price_fraction = price_cents / 100
    fee_dollars = fee_rate * contracts * price_fraction * (1 - price_fraction)
    # Subtract a tiny epsilon before ceiling to avoid floating-point noise (e.g. 2.0000000001)
    # rounding up to 3 when the exact value is 2.
    return max(0, math.ceil(fee_dollars * 100 - 1e-9))


def _passes_min_edge(price_cents: int, fee_cents: int, min_edge_cents: int) -> bool:
    """Deterministic, price-only edge filter.

    With no probability model beyond the entry price itself, the only objective "edge"
    this rule family can demand — without inventing a live signal — is that the
    fee-adjusted best case (contract resolves YES) outweighs the fee-adjusted worst case
    (contract resolves NO) by at least `min_edge_cents`. Both cases already include the
    fee paid on entry, so this only tightens (never loosens) the existing price threshold.
    """
    if min_edge_cents <= 0:
        return True
    best_case = (100 - price_cents) - fee_cents
    worst_case = price_cents + fee_cents
    return (best_case - worst_case) >= min_edge_cents


@dataclass(frozen=True)
class ReplaySummary:
    threshold_cents: int
    observed_markets: int
    resolved_markets: int
    entries: int
    wins: int
    pnl_cents: int
    max_drawdown_cents: int
    fee_rate: float = DEFAULT_FEE_RATE
    min_edge_cents: int = 0
    fees_cents: int = 0
    pnl_after_fees_cents: int = 0


@dataclass(frozen=True)
class ReplaySweepRow:
    threshold_cents: int
    entries: int
    wins: int
    pnl_cents: int
    max_drawdown_cents: int
    fees_cents: int = 0
    pnl_after_fees_cents: int = 0

    @property
    def win_rate(self) -> float:
        return (self.wins / self.entries * 100) if self.entries else 0.0


@dataclass(frozen=True)
class ReplayTrainTestSummary:
    train_fraction: float
    train_markets: int
    test_markets: int
    selected_threshold_cents: int
    train_result: ReplaySweepRow
    test_result: ReplaySweepRow
    fee_rate: float = DEFAULT_FEE_RATE
    min_edge_cents: int = 0


def replay_buy_below(
    database_url: str,
    threshold_cents: int = 35,
    fee_rate: float = DEFAULT_FEE_RATE,
    min_edge_cents: int = 0,
) -> ReplaySummary:
    engine = create_engine(database_url)
    with engine.connect() as connection:
        rows = connection.execute(text("""
            with ranked as (
                select s.ticker, s.yes_ask_cents, s.observed_at, o.result,
                       row_number() over(partition by s.ticker order by s.observed_at) as snapshot_rank
                from market_snapshots s
                left join market_outcomes o on o.ticker = s.ticker
                where s.yes_ask_cents between 1 and :threshold
            ), first_entries as (
                select ticker, yes_ask_cents, observed_at, result,
                       row_number() over(partition by ticker order by observed_at) as entry_rank
                from ranked
            )
            select ticker, yes_ask_cents, observed_at, result from first_entries
            where entry_rank = 1 order by observed_at
        """), {"threshold": threshold_cents}).mappings().all()
        observed = connection.execute(text("select count(distinct ticker) from market_snapshots")).scalar_one()
        resolved = connection.execute(text("select count(*) from market_outcomes")).scalar_one()
    pnl = 0
    fees = 0
    pnl_after_fees = 0
    peak = 0
    max_drawdown = 0
    wins = 0
    entries = 0
    for row in rows:
        if row["result"] not in ("yes", "no"):
            continue
        price = row["yes_ask_cents"]
        fee = fee_cents_for_trade(price, fee_rate=fee_rate)
        if not _passes_min_edge(price, fee, min_edge_cents):
            continue
        entries += 1
        trade_pnl = 100 - price if row["result"] == "yes" else -price
        trade_pnl_after_fees = trade_pnl - fee
        wins += trade_pnl > 0
        pnl += trade_pnl
        fees += fee
        pnl_after_fees += trade_pnl_after_fees
        peak = max(peak, pnl)
        max_drawdown = max(max_drawdown, peak - pnl)
    return ReplaySummary(
        threshold_cents, observed, resolved, entries, wins, pnl, max_drawdown,
        fee_rate=fee_rate, min_edge_cents=min_edge_cents,
        fees_cents=fees, pnl_after_fees_cents=pnl_after_fees,
    )


def replay_threshold_sweep(
    database_url: str,
    min_threshold: int = 1,
    max_threshold: int = 99,
    fee_rate: float = DEFAULT_FEE_RATE,
    min_edge_cents: int = 0,
) -> list[ReplaySweepRow]:
    lower = max(1, min_threshold)
    upper = min(99, max_threshold)
    if lower > upper:
        raise ValueError("min_threshold must be less than or equal to max_threshold")

    rows = [
        replay_buy_below(database_url, threshold, fee_rate=fee_rate, min_edge_cents=min_edge_cents)
        for threshold in range(lower, upper + 1)
    ]
    return [
        ReplaySweepRow(
            threshold_cents=row.threshold_cents,
            entries=row.entries,
            wins=row.wins,
            pnl_cents=row.pnl_cents,
            max_drawdown_cents=row.max_drawdown_cents,
            fees_cents=row.fees_cents,
            pnl_after_fees_cents=row.pnl_after_fees_cents,
        )
        for row in rows
    ]


def replay_train_test_split(
    database_url: str,
    min_threshold: int = 1,
    max_threshold: int = 99,
    train_fraction: float = 0.5,
    fee_rate: float = DEFAULT_FEE_RATE,
    min_edge_cents: int = 0,
) -> ReplayTrainTestSummary:
    lower = max(1, min_threshold)
    upper = min(99, max_threshold)
    if lower > upper:
        raise ValueError("min_threshold must be less than or equal to max_threshold")
    if not 0 < train_fraction < 1:
        raise ValueError("train_fraction must be between 0 and 1")

    market_rows, snapshot_rows = _load_resolved_snapshot_rows(database_url)
    if len(market_rows) < 2:
        raise ValueError("at least two resolved markets are required for a train/test split")

    split_at = round(len(market_rows) * train_fraction)
    split_at = min(max(1, split_at), len(market_rows) - 1)
    train_tickers = {row["ticker"] for row in market_rows[:split_at]}
    test_tickers = {row["ticker"] for row in market_rows[split_at:]}

    train_rows = [
        _evaluate_threshold_snapshot_rows(snapshot_rows, threshold, train_tickers, fee_rate, min_edge_cents)
        for threshold in range(lower, upper + 1)
    ]
    # Selection is done on fee-adjusted P/L: a threshold that only looks good before fees
    # is not a threshold worth carrying into the test window.
    selected = sorted(
        train_rows,
        key=lambda row: (row.pnl_after_fees_cents, -row.max_drawdown_cents, row.entries),
        reverse=True,
    )[0]
    test_result = _evaluate_threshold_snapshot_rows(
        snapshot_rows, selected.threshold_cents, test_tickers, fee_rate, min_edge_cents
    )
    return ReplayTrainTestSummary(
        train_fraction=train_fraction,
        train_markets=len(train_tickers),
        test_markets=len(test_tickers),
        selected_threshold_cents=selected.threshold_cents,
        train_result=selected,
        test_result=test_result,
        fee_rate=fee_rate,
        min_edge_cents=min_edge_cents,
    )


def format_replay(summary: ReplaySummary) -> str:
    rate = (summary.wins / summary.entries * 100) if summary.entries else 0
    lines = [
        "Kalshi Research — Replay",
        f"Rule: buy YES when ask <= {summary.threshold_cents}c; one entry per market",
    ]
    if summary.min_edge_cents > 0:
        lines.append(f"Minimum fee-adjusted edge filter: {summary.min_edge_cents}c")
    lines.extend((
        f"Market outcomes available: {summary.resolved_markets}/{summary.observed_markets}",
        f"Entries: {summary.entries}; wins: {summary.wins} ({rate:.1f}%)",
        f"P/L (fee excluded): ${summary.pnl_cents / 100:.2f}; max drawdown: ${summary.max_drawdown_cents / 100:.2f}",
        f"Fees (rate={summary.fee_rate:.2%}): ${summary.fees_cents / 100:.2f}",
        f"P/L (fee-adjusted): ${summary.pnl_after_fees_cents / 100:.2f}",
        "Fee-adjusted results are still in-sample research, not evidence of a durable edge.",
    ))
    return "\n".join(lines)


def format_replay_sweep(rows: list[ReplaySweepRow], limit: int = 10) -> str:
    ranked = sorted(rows, key=lambda row: (row.pnl_after_fees_cents, -row.max_drawdown_cents, row.entries), reverse=True)
    top = ranked[:limit]
    lines = [
        "Kalshi Research — Threshold Sweep",
        "Rule family: buy YES when ask <= threshold; one entry per market",
        "Top thresholds by in-sample fee-adjusted P/L:",
        "threshold  entries  wins  win_rate  pnl(pre-fee)  fees   pnl(after-fee)  max_drawdown",
    ]
    for row in top:
        lines.append(
            f"{row.threshold_cents:>8}c  {row.entries:>7}  {row.wins:>4}  "
            f"{row.win_rate:>7.1f}%  ${row.pnl_cents / 100:>9.2f}  "
            f"${row.fees_cents / 100:>5.2f}  ${row.pnl_after_fees_cents / 100:>11.2f}  "
            f"${row.max_drawdown_cents / 100:>5.2f}"
        )
    lines.append("This is a search over one captured sample. Fee-adjusted results are still research-only, not evidence of a durable edge.")
    return "\n".join(lines)


def format_train_test_split(summary: ReplayTrainTestSummary) -> str:
    train = summary.train_result
    test = summary.test_result
    lines = [
        "Kalshi Research — Train/Test Replay",
        f"Rule family: buy YES when ask <= threshold; selected on first {summary.train_fraction:.0%} of markets",
    ]
    if summary.min_edge_cents > 0:
        lines.append(f"Minimum fee-adjusted edge filter: {summary.min_edge_cents}c")
    lines.extend((
        f"Fee rate: {summary.fee_rate:.2%} (selection uses fee-adjusted P/L)",
        f"Train markets: {summary.train_markets}; test markets: {summary.test_markets}",
        f"Selected threshold: {summary.selected_threshold_cents}c",
        f"Train: entries={train.entries}; wins={train.wins} ({train.win_rate:.1f}%); "
        f"P/L(pre-fee)=${train.pnl_cents / 100:.2f}; fees=${train.fees_cents / 100:.2f}; "
        f"P/L(after-fee)=${train.pnl_after_fees_cents / 100:.2f}; max drawdown=${train.max_drawdown_cents / 100:.2f}",
        f"Test:  entries={test.entries}; wins={test.wins} ({test.win_rate:.1f}%); "
        f"P/L(pre-fee)=${test.pnl_cents / 100:.2f}; fees=${test.fees_cents / 100:.2f}; "
        f"P/L(after-fee)=${test.pnl_after_fees_cents / 100:.2f}; max drawdown=${test.max_drawdown_cents / 100:.2f}",
        "This is a first out-of-sample check. Fee-adjusted results are still research-only, not evidence of a durable edge.",
    ))
    return "\n".join(lines)


def _load_resolved_snapshot_rows(database_url: str) -> tuple[list[dict], list[dict]]:
    engine = create_engine(database_url)
    with engine.connect() as connection:
        snapshot_rows = [
            dict(row)
            for row in connection.execute(text("""
                select s.ticker, s.yes_ask_cents, s.observed_at, o.result
                from market_snapshots s
                join market_outcomes o on o.ticker = s.ticker
                where o.result in ('yes', 'no')
                order by s.observed_at, s.id
            """)).mappings().all()
        ]
        market_rows = [
            dict(row)
            for row in connection.execute(text("""
                select s.ticker, min(s.observed_at) as first_observed_at
                from market_snapshots s
                join market_outcomes o on o.ticker = s.ticker
                where o.result in ('yes', 'no')
                group by s.ticker
                order by first_observed_at, s.ticker
            """)).mappings().all()
        ]
    return market_rows, snapshot_rows


def _evaluate_threshold_snapshot_rows(
    snapshot_rows: list[dict],
    threshold_cents: int,
    allowed_tickers: set[str],
    fee_rate: float = DEFAULT_FEE_RATE,
    min_edge_cents: int = 0,
) -> ReplaySweepRow:
    entries = []
    seen = set()
    for row in snapshot_rows:
        ticker = row["ticker"]
        if ticker in seen or ticker not in allowed_tickers:
            continue
        ask = row["yes_ask_cents"]
        if ask is None or not 1 <= ask <= threshold_cents:
            continue
        fee = fee_cents_for_trade(ask, fee_rate=fee_rate)
        if not _passes_min_edge(ask, fee, min_edge_cents):
            continue
        seen.add(ticker)
        entries.append((row, fee))

    pnl = 0
    fees = 0
    pnl_after_fees = 0
    peak = 0
    max_drawdown = 0
    wins = 0
    for row, fee in entries:
        trade_pnl = 100 - row["yes_ask_cents"] if row["result"] == "yes" else -row["yes_ask_cents"]
        wins += trade_pnl > 0
        pnl += trade_pnl
        fees += fee
        pnl_after_fees += trade_pnl - fee
        peak = max(peak, pnl)
        max_drawdown = max(max_drawdown, peak - pnl)
    return ReplaySweepRow(threshold_cents, len(entries), wins, pnl, max_drawdown, fees, pnl_after_fees)
