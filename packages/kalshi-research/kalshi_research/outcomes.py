import time

import httpx

from kalshi_research.data.kalshi import MarketDataSource
from kalshi_research.domain import utc_now
from kalshi_research.storage.repository import ResearchRepository


def resolve_captured_outcomes(markets: MarketDataSource, repository: ResearchRepository,
                              request_interval_seconds: float = 0.2) -> tuple[int, int]:
    repository.initialize()
    resolved = 0
    pending = 0
    for ticker in repository.captured_tickers_without_outcomes():
        try:
            result = markets.get_market(ticker).result
        except httpx.HTTPError:
            pending += 1
            time.sleep(request_interval_seconds)
            continue
        if result in ("yes", "no"):
            repository.record_market_outcome(ticker, result, utc_now())
            resolved += 1
        else:
            pending += 1
        time.sleep(request_interval_seconds)
    return resolved, pending
