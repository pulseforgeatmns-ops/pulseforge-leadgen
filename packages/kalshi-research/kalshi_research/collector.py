"""Paper-only collection service. It reads public market data and writes local research data."""
import logging
import time

import httpx

from kalshi_research.data.btc import BtcPriceSource
from kalshi_research.data.kalshi import MarketDataSource
from kalshi_research.domain import MarketSnapshot
from kalshi_research.engine.research import ResearchRunner
from kalshi_research.storage.repository import ResearchRepository

logger = logging.getLogger(__name__)


class PaperCollector:
    def __init__(self, markets: MarketDataSource, runner: ResearchRunner,
                 repository: ResearchRepository, btc_source: BtcPriceSource | None,
                 market_keyword: str, market_limit: int, series_tickers: tuple[str, ...] = ()) -> None:
        self.markets, self.runner, self.repository = markets, runner, repository
        self.btc_source = btc_source
        self.market_keyword, self.market_limit = market_keyword.upper(), market_limit
        self.series_tickers = series_tickers

    def collect_once(self) -> int:
        """Persist public observations and paper decisions; performs no external writes."""
        self.repository.initialize()
        btc = self.btc_source.latest() if self.btc_source else None
        if btc:
            self.repository.save_btc_tick(btc)
        if self.series_tickers:
            matching_series = list(self.series_tickers)
        else:
            series = self.markets.list_series()
            matching_series = [item["ticker"] for item in series if self._series_matches(item)]
        snapshots = [snapshot for series_ticker in matching_series
                     for snapshot in self.markets.list_markets(limit=self.market_limit, series_ticker=series_ticker)]
        # Markets came from series selected by BTC/Bitcoin metadata, so do not rely
        # on every individual contract repeating that wording in its ticker/title.
        selected = snapshots
        for snapshot in selected:
            self.repository.save_market_snapshot(snapshot)
            orders = self.runner.process_with_btc(snapshot, btc)
            for order in orders:
                self.repository.save_order(order)
        logger.info("collector_complete series=%s observed=%s selected=%s", len(matching_series), len(snapshots), len(selected))
        return len(selected)

    def run_forever(self, interval_seconds: float) -> None:
        """Collect continuously until interrupted; all actions remain local paper research."""
        logger.info("collector_started interval_seconds=%s", interval_seconds)
        retry_delay = interval_seconds
        try:
            while True:
                try:
                    self.collect_once()
                    retry_delay = interval_seconds
                    time.sleep(interval_seconds)
                except httpx.HTTPError as error:
                    logger.warning("collector_network_error retry_in_seconds=%s error=%s", retry_delay, error)
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 60)
        except KeyboardInterrupt:
            logger.info("collector_stopped")

    def _series_matches(self, series: dict[str, object]) -> bool:
        terms = (str(series.get("ticker", "")), str(series.get("title", "")),
                 " ".join(str(x) for x in (series.get("tags") or [])))
        searchable = " ".join(terms).upper()
        aliases = {"BTC": ("BTC", "BITCOIN")}
        return any(term in searchable for term in aliases.get(self.market_keyword, (self.market_keyword,)))

    def _matches(self, snapshot: MarketSnapshot) -> bool:
        searchable = " ".join(filter(None, (snapshot.ticker, snapshot.title, snapshot.event_ticker))).upper()
        aliases = {"BTC": ("BTC", "BITCOIN")}
        return any(term in searchable for term in aliases.get(self.market_keyword, (self.market_keyword,)))
