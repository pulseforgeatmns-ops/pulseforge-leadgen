from dataclasses import dataclass

from sqlalchemy import create_engine, text

from kalshi_research.storage.repository import ResearchRepository


@dataclass(frozen=True)
class DataHealth:
    snapshots: int
    btc_ticks: int
    markets: int
    markets_with_50_plus_snapshots: int
    first_snapshot: str | None
    last_snapshot: str | None
    gaps_over_60_seconds: int
    largest_gap_seconds: float | None
    simulated_fills: int
    rejected_intents: int
    resolved_fills: int
    paper_pnl_cents: int
    unique_market_fills: int
    unique_market_wins: int
    unique_market_pnl_cents: int


def inspect_data(database_url: str) -> DataHealth:
    ResearchRepository(database_url).initialize()
    engine = create_engine(database_url)
    with engine.connect() as connection:
        coverage = connection.execute(text("""
            select count(*) as snapshots, min(observed_at) as first_snapshot,
                   max(observed_at) as last_snapshot, count(distinct ticker) as markets
            from market_snapshots
        """)).mappings().one()
        ticks = connection.execute(text("select count(*) as n from btc_price_ticks")).mappings().one()["n"]
        quality = connection.execute(text("""
            select sum(case when n >= 50 then 1 else 0 end) as sampled_markets
            from (select ticker, count(*) as n from market_snapshots group by ticker)
        """)).mappings().one()["sampled_markets"] or 0
        gaps = connection.execute(text("""
            with ordered as (
                select observed_at, lag(observed_at) over(order by observed_at) as previous_observed_at
                from market_snapshots
            )
            select count(*) filter (where (julianday(observed_at) - julianday(previous_observed_at)) * 86400 > 60) as n,
                   max((julianday(observed_at) - julianday(previous_observed_at)) * 86400) as largest
            from ordered
        """)).mappings().one()
        orders = connection.execute(text("""
            select count(*) filter (where status = 'filled') as fills,
                   count(*) filter (where status = 'rejected') as rejected,
                   count(*) filter (where status = 'filled' and settlement_result is not null) as resolved,
                   coalesce(sum(pnl_cents) filter (where status = 'filled' and settlement_result is not null), 0) as pnl
            from paper_trades
        """)).mappings().one()
        unique = connection.execute(text("""
            with first_fills as (
                select pnl_cents, row_number() over(partition by ticker, side order by created_at) as row_number
                from paper_trades
                where status = 'filled' and settlement_result is not null
            )
            select count(*) as fills,
                   coalesce(sum(case when pnl_cents > 0 then 1 else 0 end), 0) as wins,
                   coalesce(sum(pnl_cents), 0) as pnl
            from first_fills where row_number = 1
        """)).mappings().one()
    return DataHealth(
        snapshots=coverage["snapshots"], btc_ticks=ticks, markets=coverage["markets"],
        markets_with_50_plus_snapshots=quality, first_snapshot=str(coverage["first_snapshot"]) if coverage["first_snapshot"] else None,
        last_snapshot=str(coverage["last_snapshot"]) if coverage["last_snapshot"] else None,
        gaps_over_60_seconds=gaps["n"], largest_gap_seconds=round(gaps["largest"], 1) if gaps["largest"] else None,
        simulated_fills=orders["fills"], rejected_intents=orders["rejected"],
        resolved_fills=orders["resolved"], paper_pnl_cents=orders["pnl"],
        unique_market_fills=unique["fills"], unique_market_wins=unique["wins"], unique_market_pnl_cents=unique["pnl"],
    )


def format_data_health(health: DataHealth) -> str:
    lines = [
        "Kalshi Research — Data Health",
        f"Snapshots: {health.snapshots:,} across {health.markets} markets",
        f"BTC ticks: {health.btc_ticks:,}",
        f"Coverage: {health.first_snapshot} to {health.last_snapshot}",
        f"Markets with 50+ snapshots: {health.markets_with_50_plus_snapshots}",
        f"Collection gaps >60s: {health.gaps_over_60_seconds} (largest: {health.largest_gap_seconds or 0:,.1f}s)",
        f"Paper results: {health.simulated_fills} fills; {health.rejected_intents} rejected intents",
        f"Settled paper fills: {health.resolved_fills}; P/L: ${health.paper_pnl_cents / 100:.2f} (fees excluded)",
        f"Clean first-fill view: {health.unique_market_fills} unique market entries; {health.unique_market_wins} wins; P/L: ${health.unique_market_pnl_cents / 100:.2f} (fees excluded)",
        "Note: paper fills are placeholder-strategy observations, not evidence of profitability.",
    ]
    return "\n".join(lines)
