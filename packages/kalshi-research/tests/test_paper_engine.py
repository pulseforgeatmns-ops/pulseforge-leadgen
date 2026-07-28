from datetime import datetime, timezone

from kalshi_research.domain import MarketSnapshot, OrderIntent, OrderStatus, Side
from kalshi_research.engine.paper import PaperExecutionEngine
from kalshi_research.engine.portfolio import VirtualPortfolio
from kalshi_research.engine.risk import RiskLimits, RiskManager


def test_marketable_yes_order_fills_and_debits_cash() -> None:
    portfolio = VirtualPortfolio(1_000)
    engine = PaperExecutionEngine(portfolio, RiskManager(RiskLimits(500, 2, 200, 2)))
    snapshot = MarketSnapshot("TEST", datetime.now(timezone.utc), 30, 32)
    order = engine.submit(OrderIntent("TEST", Side.YES, 2, 32, "test", "test"), snapshot)
    assert order.status == OrderStatus.FILLED
    assert order.fill_price_cents == 32
    assert portfolio.cash_cents == 936


def test_limit_that_is_not_marketable_stays_open() -> None:
    portfolio = VirtualPortfolio(1_000)
    engine = PaperExecutionEngine(portfolio, RiskManager(RiskLimits(500, 1, 200, 2)))
    snapshot = MarketSnapshot("TEST", datetime.now(timezone.utc), 30, 32)
    order = engine.submit(OrderIntent("TEST", Side.YES, 1, 31, "test", "test"), snapshot)
    assert order.status == OrderStatus.OPEN
    assert portfolio.cash_cents == 1_000


def test_second_entry_for_same_market_is_rejected() -> None:
    portfolio = VirtualPortfolio(1_000)
    engine = PaperExecutionEngine(portfolio, RiskManager(RiskLimits(500, 1, 200, 2)))
    snapshot = MarketSnapshot("TEST", datetime.now(timezone.utc), 30, 32)
    intent = OrderIntent("TEST", Side.YES, 1, 32, "test", "test")
    assert engine.submit(intent, snapshot).status == OrderStatus.FILLED
    assert engine.submit(intent, snapshot).status == OrderStatus.REJECTED


def test_engine_exposes_existing_market_position() -> None:
    portfolio = VirtualPortfolio(1_000)
    engine = PaperExecutionEngine(portfolio, RiskManager(RiskLimits(500, 1, 200, 2)))
    snapshot = MarketSnapshot("TEST", datetime.now(timezone.utc), 30, 32)
    intent = OrderIntent("TEST", Side.YES, 1, 32, "test", "test")
    assert not engine.has_position(intent)
    engine.submit(intent, snapshot)
    assert engine.has_position(intent)
