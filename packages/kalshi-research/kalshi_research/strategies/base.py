from typing import Protocol

from kalshi_research.domain import BtcPriceTick, MarketSnapshot, OrderIntent


class Strategy(Protocol):
    """Pure decision interface: no network, clock, portfolio, or execution access."""

    name: str

    def evaluate(self, market: MarketSnapshot, btc: BtcPriceTick | None) -> list[OrderIntent]: ...
