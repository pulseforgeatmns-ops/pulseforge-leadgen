from datetime import datetime, timezone
from pathlib import Path

from kalshi_research.domain import BtcPriceTick, MarketSnapshot, OrderIntent, OrderStatus, Side, SimulatedOrder
from kalshi_research.reporting.health import inspect_data
from kalshi_research.storage.repository import ResearchRepository


def test_health_report_counts_persisted_research_data(tmp_path: Path) -> None:
    repository = ResearchRepository(f"sqlite:///{tmp_path / 'research.db'}")
    repository.initialize()
    now = datetime.now(timezone.utc)
    repository.save_market_snapshot(MarketSnapshot("TEST", now, 30, 32))
    repository.save_btc_tick(BtcPriceTick(100_000, now, "test"))
    intent = OrderIntent("TEST", Side.YES, 1, 32, "test", "test")
    repository.save_order(SimulatedOrder(intent, OrderStatus.FILLED, now, now, 32))
    repository.record_settlement(repository.unsettled_filled_trades()[0].id, "yes", now, 68)

    health = inspect_data(f"sqlite:///{tmp_path / 'research.db'}")
    assert (health.snapshots, health.btc_ticks, health.markets, health.simulated_fills, health.unique_market_fills) == (1, 1, 1, 1, 1)
