from typing import Protocol

import httpx

from kalshi_research.domain import BtcPriceTick, utc_now


class BtcPriceSource(Protocol):
    def latest(self) -> BtcPriceTick: ...


class CoinbaseSpotPrice:
    """Public Coinbase spot-price adapter, replaceable with an exchange websocket feed."""

    def __init__(self, timeout_seconds: float = 10) -> None:
        self._client = httpx.Client(base_url="https://api.coinbase.com", timeout=timeout_seconds)

    def latest(self) -> BtcPriceTick:
        response = self._client.get("/v2/prices/BTC-USD/spot")
        response.raise_for_status()
        data = response.json()["data"]
        return BtcPriceTick(price_usd=float(data["amount"]), observed_at=utc_now(), source="coinbase_spot")

    def close(self) -> None:
        self._client.close()
