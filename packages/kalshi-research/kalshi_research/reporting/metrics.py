from dataclasses import dataclass

from kalshi_research.domain import OrderStatus, SimulatedOrder
from kalshi_research.engine.portfolio import VirtualPortfolio


@dataclass(frozen=True)
class PaperSummary:
    cash_cents: int
    orders: int
    filled: int
    open: int
    rejected: int


def summarize(portfolio: VirtualPortfolio, orders: list[SimulatedOrder]) -> PaperSummary:
    return PaperSummary(portfolio.cash_cents, len(orders),
                        sum(x.status == OrderStatus.FILLED for x in orders),
                        sum(x.status == OrderStatus.OPEN for x in orders),
                        sum(x.status == OrderStatus.REJECTED for x in orders))
