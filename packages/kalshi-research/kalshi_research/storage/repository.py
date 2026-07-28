"""Durable, idempotent persistence for paper-research observations."""
from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.orm import sessionmaker

from kalshi_research.domain import BtcPriceTick, MarketSnapshot, SimulatedOrder
from kalshi_research.storage.models import Base, BtcPriceRecord, MarketOutcomeRecord, MarketSnapshotRecord, TradeRecord


class ResearchRepository:
    def __init__(self, database_url: str) -> None:
        self.engine = create_engine(database_url)
        self._sessions = sessionmaker(self.engine, expire_on_commit=False)

    def initialize(self) -> None:
        Base.metadata.create_all(self.engine)
        # Minimal forward migration for early local databases created before metadata fields existed.
        # New deployments receive these columns via create_all above.
        existing = {column["name"] for column in inspect(self.engine).get_columns("market_snapshots")}
        additions = {"title": "TEXT", "event_ticker": "VARCHAR(128)"}
        with self.engine.begin() as connection:
            for name, column_type in additions.items():
                if name not in existing:
                    connection.execute(text(f"ALTER TABLE market_snapshots ADD COLUMN {name} {column_type}"))
        trade_columns = {column["name"] for column in inspect(self.engine).get_columns("paper_trades")}
        trade_additions = {"settlement_result": "VARCHAR(16)", "settled_at": "DATETIME", "pnl_cents": "INTEGER"}
        with self.engine.begin() as connection:
            for name, column_type in trade_additions.items():
                if name not in trade_columns:
                    connection.execute(text(f"ALTER TABLE paper_trades ADD COLUMN {name} {column_type}"))

    def save_market_snapshot(self, snapshot: MarketSnapshot) -> None:
        with self._sessions.begin() as session:
            session.add(MarketSnapshotRecord(
                ticker=snapshot.ticker, observed_at=snapshot.observed_at,
                yes_bid_cents=snapshot.yes_bid_cents, yes_ask_cents=snapshot.yes_ask_cents,
                last_price_cents=snapshot.last_price_cents, close_time=snapshot.close_time,
                status=snapshot.status, result=snapshot.result, title=snapshot.title,
                event_ticker=snapshot.event_ticker,
            ))

    def save_btc_tick(self, tick: BtcPriceTick) -> None:
        with self._sessions.begin() as session:
            session.add(BtcPriceRecord(observed_at=tick.observed_at,
                                       price_usd_micros=round(tick.price_usd * 1_000_000), source=tick.source))

    def save_order(self, order: SimulatedOrder) -> None:
        with self._sessions.begin() as session:
            existing = session.scalar(select(TradeRecord.id).where(TradeRecord.signal_id == str(order.intent.signal_id)))
            if existing:
                return
            session.add(TradeRecord(
                signal_id=str(order.intent.signal_id), ticker=order.intent.ticker, side=order.intent.side,
                count=order.intent.count, limit_price_cents=order.intent.limit_price_cents,
                fill_price_cents=order.fill_price_cents, status=order.status,
                strategy_name=order.intent.strategy_name, rationale=order.intent.rationale,
                created_at=order.created_at,
            ))

    def unsettled_filled_trades(self) -> list[TradeRecord]:
        with self._sessions() as session:
            return list(session.scalars(select(TradeRecord).where(
                TradeRecord.status == "filled", TradeRecord.settlement_result.is_(None)
            ).order_by(TradeRecord.created_at)))

    def record_settlement(self, trade_id: int, result: str, settled_at, pnl_cents: int) -> None:
        with self._sessions.begin() as session:
            trade = session.get(TradeRecord, trade_id)
            if trade is None:
                raise ValueError(f"unknown trade id: {trade_id}")
            trade.settlement_result, trade.settled_at, trade.pnl_cents = result, settled_at, pnl_cents

    def captured_tickers_without_outcomes(self) -> list[str]:
        with self._sessions() as session:
            return list(session.scalars(text("""
                select distinct s.ticker from market_snapshots s
                left join market_outcomes o on o.ticker = s.ticker
                where o.ticker is null order by s.ticker
            """)))

    def record_market_outcome(self, ticker: str, result: str, resolved_at) -> None:
        with self._sessions.begin() as session:
            outcome = session.get(MarketOutcomeRecord, ticker)
            if outcome is None:
                session.add(MarketOutcomeRecord(ticker=ticker, result=result, resolved_at=resolved_at))
            else:
                outcome.result, outcome.resolved_at = result, resolved_at
