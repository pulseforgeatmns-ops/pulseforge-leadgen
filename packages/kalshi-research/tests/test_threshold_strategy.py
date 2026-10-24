from datetime import datetime, timezone

from kalshi_research.domain import MarketSnapshot
from kalshi_research.strategies.threshold import BuyBelowThreshold


def test_threshold_ignores_zero_cent_ask() -> None:
    snapshot = MarketSnapshot("TEST", datetime.now(timezone.utc), 0, 0)
    assert BuyBelowThreshold().evaluate(snapshot, None) == []
