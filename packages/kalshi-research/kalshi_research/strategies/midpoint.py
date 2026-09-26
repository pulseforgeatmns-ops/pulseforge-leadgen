"""Entry-time midpoint strategies. Paper/replay research only — not live trading."""
from dataclasses import dataclass

from kalshi_research.domain import BtcPriceTick, MarketSnapshot, OrderIntent, Side


@dataclass(frozen=True)
class BuyWhenEntryMidpointAbove:
    """Buy YES at first_yes_ask when entry midpoint is strictly above a threshold.

    Uses only entry-time book quotes (bid/ask → midpoint). Does not consult
    path-dependent ask or underlying-spot move features.
    Research hypothesis H-005; not a claim of profitability.
    """

    min_midpoint_cents: float = 50.0
    contracts: int = 1
    name: str = "buy_when_entry_midpoint_above"
    hypothesis_id: str = "H-005"

    def evaluate(self, market: MarketSnapshot, btc: BtcPriceTick | None) -> list[OrderIntent]:
        del btc  # Entry rule is book-only; underlying path features are intentionally unused.
        if market.yes_bid_cents is None or market.yes_ask_cents is None:
            return []
        ask = market.yes_ask_cents
        if not 1 <= ask <= 99:
            return []
        midpoint = (float(market.yes_bid_cents) + float(ask)) / 2.0
        if midpoint <= self.min_midpoint_cents:
            return []
        return [
            OrderIntent(
                market.ticker,
                Side.YES,
                self.contracts,
                ask,
                self.name,
                f"entry midpoint {midpoint:.1f}c > {self.min_midpoint_cents:g}c; buy YES @{ask}c",
            )
        ]
