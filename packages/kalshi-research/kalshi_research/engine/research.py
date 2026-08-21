import logging

from kalshi_research.data.btc import BtcPriceSource
from kalshi_research.domain import BtcPriceTick, MarketSnapshot, SimulatedOrder
from kalshi_research.engine.paper import PaperExecutionEngine
from kalshi_research.strategies.base import Strategy

logger = logging.getLogger(__name__)


class ResearchRunner:
    def __init__(self, strategy: Strategy, execution: PaperExecutionEngine, btc_source: BtcPriceSource | None = None) -> None:
        self.strategy, self.execution, self.btc_source = strategy, execution, btc_source

    def process(self, market: MarketSnapshot) -> list[SimulatedOrder]:
        btc = self.btc_source.latest() if self.btc_source else None
        return self.process_with_btc(market, btc)

    def process_with_btc(self, market: MarketSnapshot, btc: BtcPriceTick | None) -> list[SimulatedOrder]:
        intents = self.strategy.evaluate(market, btc)
        orders = [self.execution.submit(intent, market) for intent in intents if not self.execution.has_position(intent)]
        for order in orders:
            logger.info("paper_order status=%s ticker=%s strategy=%s price=%s reason=%s",
                        order.status, order.intent.ticker, order.intent.strategy_name,
                        order.fill_price_cents, order.rejection_reason)
        return orders
