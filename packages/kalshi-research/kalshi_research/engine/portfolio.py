from collections import defaultdict
from dataclasses import dataclass, field

from kalshi_research.domain import Side


@dataclass
class VirtualPortfolio:
    cash_cents: int
    positions: dict[tuple[str, Side], int] = field(default_factory=lambda: defaultdict(int))
    realized_pnl_cents: int = 0

    def buy(self, ticker: str, side: Side, count: int, price_cents: int) -> None:
        cost = count * price_cents
        if cost > self.cash_cents:
            raise ValueError("insufficient paper cash")
        self.cash_cents -= cost
        self.positions[(ticker, side)] += count

    def position_cost_cents(self, ticker: str, side: Side, price_cents: int) -> int:
        return self.positions[(ticker, side)] * price_cents
