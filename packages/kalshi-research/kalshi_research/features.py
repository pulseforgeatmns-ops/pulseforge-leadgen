"""Deterministic feature extraction from stored snapshots (read-only, local-only).

Computes per-market features from already-captured, already-resolved research data.
Makes no network calls, submits no orders, and writes nothing back to the database.
Does not train a model — distributions only, to inform which features merit rule tests.
"""
from __future__ import annotations

import statistics
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from sqlalchemy import create_engine, text

from kalshi_research.storage.repository import ResearchRepository

#: Feature names emitted by extract_market_features / the feature-report CLI.
FEATURE_NAMES: tuple[str, ...] = (
    "first_yes_ask_cents",
    "first_yes_bid_cents",
    "spread_cents",
    "midpoint_cents",
    "seconds_to_close",
    "snapshot_count",
    "ask_move_cents",
    "btc_move_usd",
)


@dataclass(frozen=True, slots=True)
class MarketFeatures:
    """One resolved market's deterministic snapshot-derived features."""

    ticker: str
    result: str
    first_observed_at: datetime
    first_yes_ask_cents: int | None
    first_yes_bid_cents: int | None
    spread_cents: int | None
    midpoint_cents: float | None
    seconds_to_close: float | None
    snapshot_count: int
    ask_move_cents: int | None
    btc_move_usd: float | None

    def value(self, feature_name: str) -> float | None:
        raw = getattr(self, feature_name)
        if raw is None:
            return None
        return float(raw)


@dataclass(frozen=True, slots=True)
class FeatureDistribution:
    """Summary stats for one numeric feature over a group of markets."""

    feature_name: str
    n: int
    n_missing: int
    mean: float | None
    median: float | None
    stdev: float | None
    min_value: float | None
    max_value: float | None


@dataclass(frozen=True, slots=True)
class OutcomeFeatureGroup:
    """Feature distributions for markets that resolved to one outcome."""

    result: str
    market_count: int
    distributions: tuple[FeatureDistribution, ...]


@dataclass(frozen=True, slots=True)
class SplitFeatureReport:
    """Win/lose feature distributions for one chronological split window."""

    split_name: str
    market_count: int
    yes_group: OutcomeFeatureGroup
    no_group: OutcomeFeatureGroup


@dataclass(frozen=True, slots=True)
class FeatureReport:
    """Train/test feature research summary over resolved markets."""

    train_fraction: float
    total_resolved: int
    train_markets: int
    test_markets: int
    feature_names: tuple[str, ...]
    train: SplitFeatureReport
    test: SplitFeatureReport


def extract_market_features(
    snapshots: list[dict],
    result: str,
    btc_ticks: list[tuple[datetime, float]] | None = None,
) -> MarketFeatures:
    """Compute deterministic features for one resolved market from ordered snapshots.

    ``snapshots`` must be non-empty rows for a single ticker, ordered by observed_at.
    Each row needs at least: ticker, observed_at, yes_bid_cents, yes_ask_cents;
    close_time is optional. ``btc_ticks`` are (observed_at, price_usd) ascending.
    """
    if not snapshots:
        raise ValueError("snapshots must be non-empty")
    if result not in ("yes", "no"):
        raise ValueError("result must be 'yes' or 'no'")

    first = snapshots[0]
    last = snapshots[-1]
    ticker = first["ticker"]
    first_at = _as_datetime(first["observed_at"])
    last_at = _as_datetime(last["observed_at"])

    first_ask = first.get("yes_ask_cents")
    first_bid = first.get("yes_bid_cents")
    last_ask = last.get("yes_ask_cents")

    spread = None
    midpoint = None
    if first_bid is not None and first_ask is not None:
        spread = int(first_ask) - int(first_bid)
        midpoint = (float(first_bid) + float(first_ask)) / 2.0

    close_time = _first_close_time(snapshots)
    seconds_to_close = None
    if close_time is not None:
        seconds_to_close = (close_time - first_at).total_seconds()

    ask_move = None
    if first_ask is not None and last_ask is not None:
        ask_move = int(last_ask) - int(first_ask)

    btc_move = None
    if btc_ticks:
        start_px, end_px = _btc_prices_at_bounds(btc_ticks, first_at, last_at)
        if start_px is not None and end_px is not None:
            btc_move = end_px - start_px

    return MarketFeatures(
        ticker=ticker,
        result=result,
        first_observed_at=first_at,
        first_yes_ask_cents=int(first_ask) if first_ask is not None else None,
        first_yes_bid_cents=int(first_bid) if first_bid is not None else None,
        spread_cents=spread,
        midpoint_cents=midpoint,
        seconds_to_close=seconds_to_close,
        snapshot_count=len(snapshots),
        ask_move_cents=ask_move,
        btc_move_usd=btc_move,
    )


def inspect_feature_report(database_url: str, train_fraction: float = 0.5) -> FeatureReport:
    """Load resolved markets, extract features, and summarize train/test win/lose splits."""
    if not 0 < train_fraction < 1:
        raise ValueError("train_fraction must be between 0 and 1")

    ResearchRepository(database_url).initialize()
    market_order, snapshots_by_ticker, results, btc_ticks = _load_feature_inputs(database_url)
    if len(market_order) < 2:
        raise ValueError("at least two resolved markets are required for a train/test feature report")

    features = [
        extract_market_features(snapshots_by_ticker[ticker], results[ticker], btc_ticks)
        for ticker in market_order
    ]

    split_at = round(len(features) * train_fraction)
    split_at = min(max(1, split_at), len(features) - 1)
    train_features = features[:split_at]
    test_features = features[split_at:]

    return FeatureReport(
        train_fraction=train_fraction,
        total_resolved=len(features),
        train_markets=len(train_features),
        test_markets=len(test_features),
        feature_names=FEATURE_NAMES,
        train=_split_report("train", train_features),
        test=_split_report("test", test_features),
    )


def format_feature_report(report: FeatureReport) -> str:
    """Human-readable win/lose feature distributions for train and test windows."""
    lines = [
        "Kalshi Research — Feature Report (read-only, local)",
        f"Resolved markets: {report.total_resolved} "
        f"(train={report.train_markets}, test={report.test_markets} "
        f"at {report.train_fraction:.0%} chronological split)",
        "Features are deterministic and derived only from stored snapshots / BTC ticks.",
        "Winning = resolved YES; losing = resolved NO (YES-buyer perspective).",
        "",
    ]
    for split in (report.train, report.test):
        lines.extend(_format_split(split))
        lines.append("")
    lines.append(
        "Use mean/median gaps between YES and NO groups to shortlist rule candidates; "
        "this report does not train a model or claim an edge."
    )
    lines.append("Paper/replay research only — no live trading path.")
    return "\n".join(lines)


def _format_split(split: SplitFeatureReport) -> list[str]:
    lines = [
        f"=== {split.split_name.upper()} ({split.market_count} markets: "
        f"{split.yes_group.market_count} YES / {split.no_group.market_count} NO) ===",
        f"{'feature':<22} {'group':<6} {'n':>4} {'miss':>4} "
        f"{'mean':>10} {'median':>10} {'stdev':>10} {'min':>10} {'max':>10}",
    ]
    yes_by_name = {d.feature_name: d for d in split.yes_group.distributions}
    no_by_name = {d.feature_name: d for d in split.no_group.distributions}
    for name in FEATURE_NAMES:
        for label, dist in (("YES", yes_by_name[name]), ("NO", no_by_name[name])):
            lines.append(
                f"{name:<22} {label:<6} {dist.n:>4} {dist.n_missing:>4} "
                f"{_fmt_num(dist.mean):>10} {_fmt_num(dist.median):>10} "
                f"{_fmt_num(dist.stdev):>10} {_fmt_num(dist.min_value):>10} "
                f"{_fmt_num(dist.max_value):>10}"
            )
        yes_mean = yes_by_name[name].mean
        no_mean = no_by_name[name].mean
        if yes_mean is not None and no_mean is not None:
            lines.append(
                f"{'':<22} {'Δmean':<6} {'':>4} {'':>4} "
                f"{_fmt_num(yes_mean - no_mean):>10}"
            )
    return lines


def _fmt_num(value: float | None) -> str:
    if value is None:
        return "—"
    if abs(value) >= 1000:
        return f"{value:,.1f}"
    if abs(value) >= 10:
        return f"{value:.2f}"
    return f"{value:.4f}"


def _split_report(split_name: str, features: list[MarketFeatures]) -> SplitFeatureReport:
    yes_rows = [row for row in features if row.result == "yes"]
    no_rows = [row for row in features if row.result == "no"]
    return SplitFeatureReport(
        split_name=split_name,
        market_count=len(features),
        yes_group=OutcomeFeatureGroup("yes", len(yes_rows), _distributions(yes_rows)),
        no_group=OutcomeFeatureGroup("no", len(no_rows), _distributions(no_rows)),
    )


def _distributions(features: list[MarketFeatures]) -> tuple[FeatureDistribution, ...]:
    return tuple(_distribution(name, features) for name in FEATURE_NAMES)


def _distribution(feature_name: str, features: list[MarketFeatures]) -> FeatureDistribution:
    values = [row.value(feature_name) for row in features]
    present = [v for v in values if v is not None]
    n_missing = len(values) - len(present)
    if not present:
        return FeatureDistribution(feature_name, 0, n_missing, None, None, None, None, None)
    return FeatureDistribution(
        feature_name=feature_name,
        n=len(present),
        n_missing=n_missing,
        mean=statistics.mean(present),
        median=statistics.median(present),
        stdev=statistics.stdev(present) if len(present) >= 2 else 0.0,
        min_value=min(present),
        max_value=max(present),
    )


def _load_feature_inputs(
    database_url: str,
) -> tuple[list[str], dict[str, list[dict]], dict[str, str], list[tuple[datetime, float]]]:
    engine = create_engine(database_url)
    with engine.connect() as connection:
        market_rows = connection.execute(text("""
            select s.ticker, min(s.observed_at) as first_observed_at, o.result
            from market_snapshots s
            join market_outcomes o on o.ticker = s.ticker
            where o.result in ('yes', 'no')
            group by s.ticker, o.result
            order by first_observed_at, s.ticker
        """)).mappings().all()

        snapshot_rows = connection.execute(text("""
            select s.ticker, s.observed_at, s.yes_bid_cents, s.yes_ask_cents, s.close_time
            from market_snapshots s
            join market_outcomes o on o.ticker = s.ticker
            where o.result in ('yes', 'no')
            order by s.ticker, s.observed_at, s.id
        """)).mappings().all()

        tick_rows = connection.execute(text("""
            select observed_at, price_usd_micros
            from btc_price_ticks
            order by observed_at, id
        """)).mappings().all()

    market_order = [row["ticker"] for row in market_rows]
    results = {row["ticker"]: row["result"] for row in market_rows}
    snapshots_by_ticker: dict[str, list[dict]] = {ticker: [] for ticker in market_order}
    for row in snapshot_rows:
        snapshots_by_ticker[row["ticker"]].append(dict(row))

    btc_ticks = [
        (_as_datetime(row["observed_at"]), row["price_usd_micros"] / 1_000_000.0)
        for row in tick_rows
    ]
    return market_order, snapshots_by_ticker, results, btc_ticks


def _btc_prices_at_bounds(
    ticks: list[tuple[datetime, float]],
    start: datetime,
    end: datetime,
) -> tuple[float | None, float | None]:
    """Last tick at or before each bound (deterministic as-of lookup)."""
    start_price: float | None = None
    end_price: float | None = None
    for observed_at, price in ticks:
        if observed_at <= start:
            start_price = price
        if observed_at <= end:
            end_price = price
        if observed_at > end:
            break
    return start_price, end_price


def _first_close_time(snapshots: Iterable[dict]) -> datetime | None:
    for row in snapshots:
        close_time = row.get("close_time")
        if close_time is not None:
            return _as_datetime(close_time)
    return None


def _as_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value
    # SQLite may return ISO strings depending on driver/settings.
    text_value = str(value).replace("Z", "+00:00")
    return datetime.fromisoformat(text_value)
