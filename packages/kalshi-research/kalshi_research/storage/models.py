from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class MarketSnapshotRecord(Base):
    __tablename__ = "market_snapshots"
    id: Mapped[int] = mapped_column(primary_key=True)
    ticker: Mapped[str] = mapped_column(String(128), index=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    yes_bid_cents: Mapped[int | None] = mapped_column(Integer)
    yes_ask_cents: Mapped[int | None] = mapped_column(Integer)
    last_price_cents: Mapped[int | None] = mapped_column(Integer)
    close_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    status: Mapped[str | None] = mapped_column(String(32), index=True)
    result: Mapped[str | None] = mapped_column(String(16))
    title: Mapped[str | None] = mapped_column(Text)
    event_ticker: Mapped[str | None] = mapped_column(String(128), index=True)


class BtcPriceRecord(Base):
    __tablename__ = "btc_price_ticks"
    id: Mapped[int] = mapped_column(primary_key=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    price_usd_micros: Mapped[int] = mapped_column(Integer)
    source: Mapped[str] = mapped_column(String(64), index=True)


class MarketOutcomeRecord(Base):
    __tablename__ = "market_outcomes"
    ticker: Mapped[str] = mapped_column(String(128), primary_key=True)
    result: Mapped[str] = mapped_column(String(16), index=True)
    resolved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class TradeRecord(Base):
    __tablename__ = "paper_trades"
    id: Mapped[int] = mapped_column(primary_key=True)
    signal_id: Mapped[str] = mapped_column(String(36), unique=True, index=True)
    ticker: Mapped[str] = mapped_column(String(128), index=True)
    side: Mapped[str] = mapped_column(String(3))
    count: Mapped[int] = mapped_column(Integer)
    limit_price_cents: Mapped[int] = mapped_column(Integer)
    fill_price_cents: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(16), index=True)
    strategy_name: Mapped[str] = mapped_column(String(128), index=True)
    rationale: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    settlement_result: Mapped[str | None] = mapped_column(String(16), index=True)
    settled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    pnl_cents: Mapped[int | None] = mapped_column(Integer)
