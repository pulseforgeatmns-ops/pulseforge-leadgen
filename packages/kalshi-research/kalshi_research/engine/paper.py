from kalshi_research.domain import MarketSnapshot, OrderIntent, OrderStatus, SimulatedOrder, Side, utc_now
from kalshi_research.engine.portfolio import VirtualPortfolio
from kalshi_research.engine.risk import RiskManager


class PaperExecutionEngine:
    """Simple, explicit fill model. It never performs external writes."""

    def __init__(self, portfolio: VirtualPortfolio, risk: RiskManager) -> None:
        self.portfolio = portfolio
        self.risk = risk
        self.orders: list[SimulatedOrder] = []

    def has_position(self, intent: OrderIntent) -> bool:
        """A baseline paper strategy opens at most one directional position per market."""
        return self.portfolio.positions[(intent.ticker, intent.side)] > 0

    def submit(self, intent: OrderIntent, market: MarketSnapshot) -> SimulatedOrder:
        reason = self.risk.check(intent, self.portfolio, sum(o.status == OrderStatus.OPEN for o in self.orders))
        if reason:
            order = SimulatedOrder(intent, OrderStatus.REJECTED, utc_now(), rejection_reason=reason)
        else:
            best_offer = market.yes_ask_cents if intent.side == Side.YES else (100 - market.yes_bid_cents if market.yes_bid_cents is not None else None)
            if best_offer is not None and intent.limit_price_cents >= best_offer:
                self.portfolio.buy(intent.ticker, intent.side, intent.count, best_offer)
                order = SimulatedOrder(intent, OrderStatus.FILLED, utc_now(), utc_now(), best_offer)
            else:
                order = SimulatedOrder(intent, OrderStatus.OPEN, utc_now())
        self.orders.append(order)
        return order
