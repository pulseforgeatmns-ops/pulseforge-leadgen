from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from kalshi_research.collector import PaperCollector
from kalshi_research.domain import BtcPriceTick, MarketSnapshot
from kalshi_research.engine.paper import PaperExecutionEngine
from kalshi_research.engine.portfolio import VirtualPortfolio
from kalshi_research.engine.research import ResearchRunner
from kalshi_research.engine.risk import RiskLimits, RiskManager
from kalshi_research.storage.models import BtcPriceRecord, MarketSnapshotRecord, TradeRecord
from kalshi_research.storage.repository import ResearchRepository
from kalshi_research.strategies.threshold import BuyBelowThreshold


class FakeMarkets:
    def list_series(self):
        return [{"ticker": "KXBTC15M", "title": "Bitcoin 15 minute market", "tags": ["crypto"]}]

    def list_markets(self, *, status: str = "open", limit: int = 200, series_ticker: str | None = None):
        return [
            MarketSnapshot("KXBTC15M-TEST", datetime.now(timezone.utc), 32, 34, 33),
            MarketSnapshot("KXOTHER-TEST", datetime.now(timezone.utc), 32, 34, 33, title="Bitcoin price market"),
        ]


class FakeBtc:
    def latest(self):
        return BtcPriceTick(100_000, datetime.now(timezone.utc), "fake")


def test_collector_persists_only_matching_market_and_paper_order(tmp_path: Path) -> None:
    repository = ResearchRepository(f"sqlite:///{tmp_path / 'research.db'}")
    portfolio = VirtualPortfolio(1_000)
    runner = ResearchRunner(BuyBelowThreshold(), PaperExecutionEngine(
        portfolio, RiskManager(RiskLimits(500, 1, 200, 2))))
    collector = PaperCollector(FakeMarkets(), runner, repository, FakeBtc(), "BTC", 10, ("KXBTC15M",))

    assert collector.collect_once() == 2
    with Session(repository.engine) as session:
        assert session.scalar(select(func.count()).select_from(MarketSnapshotRecord)) == 2
        assert session.scalar(select(func.count()).select_from(BtcPriceRecord)) == 1
        assert session.scalar(select(func.count()).select_from(TradeRecord)) == 2


def test_series_matcher_accepts_null_tags(tmp_path: Path) -> None:
    repository = ResearchRepository(f"sqlite:///{tmp_path / 'research.db'}")
    runner = ResearchRunner(BuyBelowThreshold(), PaperExecutionEngine(
        VirtualPortfolio(1_000), RiskManager(RiskLimits(500, 1, 200, 2))))
    collector = PaperCollector(FakeMarkets(), runner, repository, None, "BTC", 10)
    assert collector._series_matches({"ticker": "KXBTC15M", "title": "Bitcoin", "tags": None})
