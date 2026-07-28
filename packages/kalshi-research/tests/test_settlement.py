from datetime import datetime, timezone

from kalshi_research.domain import MarketSnapshot, OrderIntent, OrderStatus, Side, SimulatedOrder
from kalshi_research.settlement import SettlementResolver
from kalshi_research.storage.repository import ResearchRepository


class SettledMarket:
    def get_market(self, ticker: str) -> MarketSnapshot:
        return MarketSnapshot(ticker, datetime.now(timezone.utc), 50, 51, result="yes")


def test_resolver_records_yes_payout(tmp_path) -> None:
    repository = ResearchRepository(f"sqlite:///{tmp_path / 'research.db'}")
    repository.initialize()
    intent = OrderIntent("TEST", Side.YES, 1, 40, "test", "test")
    now = datetime.now(timezone.utc)
    repository.save_order(SimulatedOrder(intent, OrderStatus.FILLED, now, now, 40))

    summary = SettlementResolver(SettledMarket(), repository, 0).resolve()
    trade = repository.unsettled_filled_trades()
    assert (summary.checked, summary.resolved, summary.still_open) == (1, 1, 0)
    assert trade == []
