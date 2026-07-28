"""Read-only data-quality and feature-readiness diagnostics.

This module only reads already-captured, already-resolved research data. It makes no
network calls, submits no orders, and writes nothing back to the database.
"""
import statistics
from dataclasses import dataclass

from sqlalchemy import create_engine, text

from kalshi_research.replay import DEFAULT_FEE_RATE, fee_cents_for_trade
from kalshi_research.storage.repository import ResearchRepository

#: Bucket width, in cents, used for the yes-ask price distribution and fee-drag bands.
BAND_WIDTH_CENTS = 10

#: Snapshot gap, in seconds, above which we flag a within-market collection gap.
GAP_THRESHOLD_SECONDS = 60.0

#: Below this many resolved markets, a train/test split is considered too thin to trust.
MIN_RESOLVED_MARKETS_FOR_FEATURES = 150

#: Below this many test-window markets, an out-of-sample check is considered too thin.
MIN_TEST_MARKETS = 50

#: Above this percentage of snapshots missing a bid or ask, price-based features are suspect.
MAX_MISSING_BID_ASK_PCT = 2.0

#: Above this percentage of markets with an in-window collection gap, density is suspect.
MAX_MARKETS_WITH_GAPS_PCT = 10.0


@dataclass(frozen=True)
class PriceBand:
    """Fee drag for markets whose first observed yes-ask fell in [low_cents, high_cents]."""

    low_cents: int
    high_cents: int
    entries: int
    fees_cents: int
    notional_cents: int

    @property
    def fee_drag_pct(self) -> float:
        return (self.fees_cents / self.notional_cents * 100) if self.notional_cents else 0.0


@dataclass(frozen=True)
class DataDiagnostics:
    total_markets: int
    resolved_markets: int
    unresolved_markets: int
    first_snapshot: str | None
    last_snapshot: str | None
    train_fraction: float
    train_markets: int
    test_markets: int
    total_snapshots: int
    snapshots_per_market_mean: float
    snapshots_per_market_median: float
    snapshots_per_market_min: int
    snapshots_per_market_max: int
    markets_with_within_market_gaps: int
    largest_within_market_gap_seconds: float | None
    snapshots_missing_bid: int
    snapshots_missing_ask: int
    fee_rate: float
    price_bands: tuple[PriceBand, ...]


def inspect_diagnostics(
    database_url: str,
    fee_rate: float = DEFAULT_FEE_RATE,
    train_fraction: float = 0.5,
) -> DataDiagnostics:
    ResearchRepository(database_url).initialize()
    engine = create_engine(database_url)
    with engine.connect() as connection:
        coverage = connection.execute(text("""
            select count(*) as snapshots, min(observed_at) as first_snapshot,
                   max(observed_at) as last_snapshot, count(distinct ticker) as markets
            from market_snapshots
        """)).mappings().one()
        resolved_markets = connection.execute(text("select count(*) from market_outcomes")).scalar_one()

        per_market_counts = [
            row["n"]
            for row in connection.execute(text(
                "select count(*) as n from market_snapshots group by ticker"
            )).mappings().all()
        ]

        # First-observed-at per resolved market, in chronological order, mirrors the
        # ordering used for replay's train/test split so this reports the same coverage
        # a `replay-split` run would actually see.
        resolved_market_rows = connection.execute(text("""
            select s.ticker, min(s.observed_at) as first_observed_at
            from market_snapshots s
            join market_outcomes o on o.ticker = s.ticker
            where o.result in ('yes', 'no')
            group by s.ticker
            order by first_observed_at, s.ticker
        """)).mappings().all()

        gaps = connection.execute(text("""
            with ordered as (
                select ticker, observed_at,
                       lag(observed_at) over(partition by ticker order by observed_at) as previous_observed_at
                from market_snapshots
            ), gapped as (
                select ticker, (julianday(observed_at) - julianday(previous_observed_at)) * 86400 as gap_seconds
                from ordered
                where previous_observed_at is not null
            )
            select count(distinct ticker) filter (where gap_seconds > :threshold) as markets_with_gaps,
                   max(gap_seconds) as largest_gap
            from gapped
        """), {"threshold": GAP_THRESHOLD_SECONDS}).mappings().one()

        missing = connection.execute(text("""
            select count(*) filter (where yes_bid_cents is null) as missing_bid,
                   count(*) filter (where yes_ask_cents is null) as missing_ask
            from market_snapshots
        """)).mappings().one()

        first_ask_by_market = connection.execute(text("""
            with ranked as (
                select s.ticker, s.yes_ask_cents,
                       row_number() over(partition by s.ticker order by s.observed_at) as snapshot_rank
                from market_snapshots s
                join market_outcomes o on o.ticker = s.ticker
                where o.result in ('yes', 'no')
            )
            select yes_ask_cents from ranked where snapshot_rank = 1 and yes_ask_cents is not null
        """)).mappings().all()

    total_markets = coverage["markets"]
    total_snapshots = coverage["snapshots"]

    split_at = round(len(resolved_market_rows) * train_fraction)
    split_at = min(max(1, split_at), max(1, len(resolved_market_rows) - 1)) if resolved_market_rows else 0
    train_markets = split_at
    test_markets = max(0, len(resolved_market_rows) - split_at)

    price_bands = tuple(
        _price_band(low, min(low + BAND_WIDTH_CENTS - 1, 100), first_ask_by_market, fee_rate)
        for low in range(1, 101, BAND_WIDTH_CENTS)
    )

    return DataDiagnostics(
        total_markets=total_markets,
        resolved_markets=resolved_markets,
        unresolved_markets=max(0, total_markets - resolved_markets),
        first_snapshot=str(coverage["first_snapshot"]) if coverage["first_snapshot"] else None,
        last_snapshot=str(coverage["last_snapshot"]) if coverage["last_snapshot"] else None,
        train_fraction=train_fraction,
        train_markets=train_markets,
        test_markets=test_markets,
        total_snapshots=total_snapshots,
        snapshots_per_market_mean=round(statistics.mean(per_market_counts), 1) if per_market_counts else 0.0,
        snapshots_per_market_median=round(statistics.median(per_market_counts), 1) if per_market_counts else 0.0,
        snapshots_per_market_min=min(per_market_counts) if per_market_counts else 0,
        snapshots_per_market_max=max(per_market_counts) if per_market_counts else 0,
        markets_with_within_market_gaps=gaps["markets_with_gaps"] or 0,
        largest_within_market_gap_seconds=round(gaps["largest_gap"], 1) if gaps["largest_gap"] else None,
        snapshots_missing_bid=missing["missing_bid"] or 0,
        snapshots_missing_ask=missing["missing_ask"] or 0,
        fee_rate=fee_rate,
        price_bands=price_bands,
    )


def _price_band(low_cents: int, high_cents: int, first_ask_rows: list, fee_rate: float) -> PriceBand:
    asks_in_band = [row["yes_ask_cents"] for row in first_ask_rows if low_cents <= row["yes_ask_cents"] <= high_cents]
    fees_cents = sum(fee_cents_for_trade(ask, fee_rate=fee_rate) for ask in asks_in_band)
    notional_cents = sum(asks_in_band)
    return PriceBand(low_cents, high_cents, len(asks_in_band), fees_cents, notional_cents)


def readiness_verdict(diagnostics: DataDiagnostics) -> list[str]:
    """Returns a list of blocking reasons. An empty list means the dataset looks ready."""
    reasons = []
    if diagnostics.resolved_markets < MIN_RESOLVED_MARKETS_FOR_FEATURES:
        reasons.append(
            f"Only {diagnostics.resolved_markets} resolved markets "
            f"(recommend at least {MIN_RESOLVED_MARKETS_FOR_FEATURES} before trusting a feature-search result)."
        )
    if diagnostics.test_markets < MIN_TEST_MARKETS:
        reasons.append(
            f"Only {diagnostics.test_markets} markets in the test window at a "
            f"{diagnostics.train_fraction:.0%} train split (recommend at least {MIN_TEST_MARKETS})."
        )
    missing_pct = (
        (diagnostics.snapshots_missing_bid + diagnostics.snapshots_missing_ask) / (2 * diagnostics.total_snapshots) * 100
        if diagnostics.total_snapshots else 0.0
    )
    if missing_pct > MAX_MISSING_BID_ASK_PCT:
        reasons.append(f"{missing_pct:.1f}% of bid/ask fields are missing (recommend under {MAX_MISSING_BID_ASK_PCT:.1f}%).")
    gap_pct = (diagnostics.markets_with_within_market_gaps / diagnostics.total_markets * 100) if diagnostics.total_markets else 0.0
    if gap_pct > MAX_MARKETS_WITH_GAPS_PCT:
        reasons.append(
            f"{gap_pct:.1f}% of markets have a >{GAP_THRESHOLD_SECONDS:.0f}s collection gap "
            f"(recommend under {MAX_MARKETS_WITH_GAPS_PCT:.1f}%)."
        )
    return reasons


def format_diagnostics(diagnostics: DataDiagnostics) -> str:
    lines = [
        "Kalshi Research — Data Diagnostics (read-only, local)",
        f"Markets observed: {diagnostics.total_markets} "
        f"({diagnostics.resolved_markets} resolved, {diagnostics.unresolved_markets} unresolved)",
        f"Coverage: {diagnostics.first_snapshot} to {diagnostics.last_snapshot}",
        f"Snapshots: {diagnostics.total_snapshots:,} total; "
        f"per-market mean={diagnostics.snapshots_per_market_mean}, "
        f"median={diagnostics.snapshots_per_market_median}, "
        f"min={diagnostics.snapshots_per_market_min}, max={diagnostics.snapshots_per_market_max}",
        f"Train/test coverage at {diagnostics.train_fraction:.0%} split: "
        f"{diagnostics.train_markets} train markets, {diagnostics.test_markets} test markets",
        f"Within-market collection gaps (>{GAP_THRESHOLD_SECONDS:.0f}s): "
        f"{diagnostics.markets_with_within_market_gaps} markets affected "
        f"(largest gap: {diagnostics.largest_within_market_gap_seconds or 0:,.1f}s)",
        f"Missing bid/ask fields: {diagnostics.snapshots_missing_bid} missing bid, "
        f"{diagnostics.snapshots_missing_ask} missing ask (of {diagnostics.total_snapshots} snapshots)",
        f"Fee-drag by first-seen yes-ask price band (fee rate={diagnostics.fee_rate:.2%}):",
        "  band        entries  fees    notional  fee_drag",
    ]
    for band in diagnostics.price_bands:
        if band.entries == 0:
            continue
        lines.append(
            f"  {band.low_cents:>3}-{band.high_cents:<3}c   {band.entries:>7}  "
            f"${band.fees_cents / 100:>5.2f}  ${band.notional_cents / 100:>7.2f}  {band.fee_drag_pct:>6.1f}%"
        )
    reasons = readiness_verdict(diagnostics)
    lines.append("")
    if reasons:
        lines.append("Verdict: NOT YET ready for feature research — recommend another 48-72h capture pass.")
        lines.extend(f"  - {reason}" for reason in reasons)
    else:
        lines.append("Verdict: dataset looks ready for feature research (all baseline checks passed).")
    lines.append("This is a data-quality check only, not a claim about strategy performance.")
    return "\n".join(lines)
