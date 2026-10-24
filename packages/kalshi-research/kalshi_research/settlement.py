"""Read-only settlement resolution for simulated filled trades."""
import logging
import time
from dataclasses import dataclass

import httpx

from kalshi_research.data.kalshi import MarketDataSource
from kalshi_research.domain import Side, utc_now
from kalshi_research.storage.repository import ResearchRepository

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SettlementSummary:
    checked: int
    resolved: int
    still_open: int


class SettlementResolver:
    def __init__(self, markets: MarketDataSource, repository: ResearchRepository, request_interval_seconds: float = 0.2) -> None:
        self.markets, self.repository = markets, repository
        self.request_interval_seconds = request_interval_seconds

    def resolve(self) -> SettlementSummary:
        self.repository.initialize()
        trades = self.repository.unsettled_filled_trades()
        resolved = 0
        open_trades = 0
        market_results: dict[str, str | None] = {}
        for trade in trades:
            if trade.ticker not in market_results:
                try:
                    market_results[trade.ticker] = self.markets.get_market(trade.ticker).result
                except httpx.HTTPError as error:
                    logger.warning("settlement_lookup_failed ticker=%s error=%s", trade.ticker, error)
                    market_results[trade.ticker] = None
                time.sleep(self.request_interval_seconds)
            result = market_results[trade.ticker]
            if result not in ("yes", "no"):
                open_trades += 1
            else:
                is_win = trade.side == result
                pnl = trade.count * ((100 - trade.fill_price_cents) if is_win else -trade.fill_price_cents)
                self.repository.record_settlement(trade.id, result, utc_now(), pnl)
                resolved += 1
        return SettlementSummary(checked=len(trades), resolved=resolved, still_open=open_trades)
