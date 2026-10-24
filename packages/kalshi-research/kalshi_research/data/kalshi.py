"""Read-only Kalshi market-data adapter; it contains no order endpoints."""
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Protocol

import httpx

from kalshi_research.domain import MarketSnapshot, utc_now


class MarketDataSource(Protocol):
    def get_market(self, ticker: str) -> MarketSnapshot: ...

    def list_markets(self, *, status: str = "open", limit: int = 200, series_ticker: str | None = None) -> list[MarketSnapshot]: ...

    def list_series(self) -> list[dict[str, Any]]: ...


def _cents(value: Any) -> int | None:
    if value is None:
        return None
    return int((Decimal(str(value)) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _timestamp(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def _snapshot(market: dict[str, Any]) -> MarketSnapshot:
    """Accept current dollar fields and the legacy cent fields used by old responses."""
    return MarketSnapshot(
        ticker=market["ticker"], observed_at=utc_now(),
        yes_bid_cents=_cents(market.get("yes_bid_dollars")) if "yes_bid_dollars" in market else market.get("yes_bid"),
        yes_ask_cents=_cents(market.get("yes_ask_dollars")) if "yes_ask_dollars" in market else market.get("yes_ask"),
        last_price_cents=_cents(market.get("last_price_dollars")) if "last_price_dollars" in market else market.get("last_price"),
        volume=int(Decimal(str(market["volume_fp"]))) if market.get("volume_fp") is not None else market.get("volume"),
        close_time=_timestamp(market.get("close_time")), status=market.get("status"), result=market.get("result"),
        title=market.get("title") or market.get("subtitle") or market.get("yes_sub_title"),
        event_ticker=market.get("event_ticker"),
    )


class KalshiRestMarketData:
    def __init__(self, base_url: str, timeout_seconds: float = 10) -> None:
        self._client = httpx.Client(base_url=base_url, timeout=timeout_seconds)

    def get_market(self, ticker: str) -> MarketSnapshot:
        response = self._client.get(f"/markets/{ticker}")
        response.raise_for_status()
        return _snapshot(response.json()["market"])

    def list_markets(self, *, status: str = "open", limit: int = 200,
                     series_ticker: str | None = None) -> list[MarketSnapshot]:
        """Page through open markets up to ``limit``; API pages max out at 1,000."""
        snapshots: list[MarketSnapshot] = []
        cursor: str | None = None
        while len(snapshots) < limit:
            params: dict[str, str | int] = {"status": status, "limit": min(1_000, limit - len(snapshots))}
            if series_ticker:
                params["series_ticker"] = series_ticker
            if cursor:
                params["cursor"] = cursor
            response = self._client.get("/markets", params=params)
            response.raise_for_status()
            payload = response.json()
            snapshots.extend(_snapshot(market) for market in payload.get("markets", []))
            cursor = payload.get("cursor")
            if not cursor or not payload.get("markets"):
                break
        return snapshots[:limit]

    def list_series(self) -> list[dict[str, Any]]:
        response = self._client.get("/series")
        response.raise_for_status()
        return response.json().get("series", [])

    def close(self) -> None:
        self._client.close()
