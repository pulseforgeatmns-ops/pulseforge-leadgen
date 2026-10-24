from dataclasses import dataclass

from kalshi_research.domain import OrderIntent
from kalshi_research.engine.portfolio import VirtualPortfolio


@dataclass(frozen=True)
class RiskLimits:
    max_position_cents: int
    max_contracts_per_market: int
    max_daily_loss_cents: int
    max_open_orders: int


class RiskManager:
    def __init__(self, limits: RiskLimits) -> None:
        self.limits = limits

    def check(self, intent: OrderIntent, portfolio: VirtualPortfolio, open_orders: int) -> str | None:
        if open_orders >= self.limits.max_open_orders:
            return "maximum open orders reached"
        if portfolio.realized_pnl_cents <= -self.limits.max_daily_loss_cents:
            return "daily loss limit reached"
        existing_contracts = portfolio.positions[(intent.ticker, intent.side)]
        if existing_contracts + intent.count > self.limits.max_contracts_per_market:
            return "maximum paper position for this market reached"
        if (existing_contracts + intent.count) * intent.limit_price_cents > self.limits.max_position_cents:
            return "market position exceeds exposure limit"
        if intent.count * intent.limit_price_cents > portfolio.cash_cents:
            return "insufficient paper cash"
        return None
