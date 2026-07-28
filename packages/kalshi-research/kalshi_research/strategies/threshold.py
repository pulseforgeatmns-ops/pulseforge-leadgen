from dataclasses import dataclass

from kalshi_research.domain import BtcPriceTick, MarketSnapshot, OrderIntent, Side


@dataclass(frozen=True)
class BuyBelowThreshold:
    """Illustrative deterministic research strategy, not a claim of profitability."""

    max_yes_ask_cents: int = 35
    contracts: int = 1
    name: str = "buy_below_threshold"

    def evaluate(self, market: MarketSnapshot, btc: BtcPriceTick | None) -> list[OrderIntent]:
        if market.yes_ask_cents is None or not 1 <= market.yes_ask_cents <= self.max_yes_ask_cents:
            return []
        return [OrderIntent(market.ticker, Side.YES, self.contracts, market.yes_ask_cents,
                            self.name, f"yes ask <= {self.max_yes_ask_cents}c")]
