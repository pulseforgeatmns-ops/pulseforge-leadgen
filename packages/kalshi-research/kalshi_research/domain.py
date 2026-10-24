from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from uuid import UUID, uuid4


class Side(StrEnum):
    YES = "yes"
    NO = "no"


class OrderStatus(StrEnum):
    FILLED = "filled"
    OPEN = "open"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    ticker: str
    observed_at: datetime
    yes_bid_cents: int | None
    yes_ask_cents: int | None
    last_price_cents: int | None = None
    volume: int | None = None
    close_time: datetime | None = None
    status: str | None = None
    result: str | None = None
    title: str | None = None
    event_ticker: str | None = None

    def __post_init__(self) -> None:
        for price in (self.yes_bid_cents, self.yes_ask_cents, self.last_price_cents):
            if price is not None and not 0 <= price <= 100:
                raise ValueError("contract prices must be between 0 and 100 cents")


@dataclass(frozen=True, slots=True)
class BtcPriceTick:
    price_usd: float
    observed_at: datetime
    source: str


@dataclass(frozen=True, slots=True)
class OrderIntent:
    ticker: str
    side: Side
    count: int
    limit_price_cents: int
    strategy_name: str
    rationale: str
    signal_id: UUID = field(default_factory=uuid4)

    def __post_init__(self) -> None:
        if self.count <= 0:
            raise ValueError("count must be positive")
        if not 1 <= self.limit_price_cents <= 99:
            raise ValueError("limit price must be 1..99 cents")


@dataclass(frozen=True, slots=True)
class SimulatedOrder:
    intent: OrderIntent
    status: OrderStatus
    created_at: datetime
    filled_at: datetime | None = None
    fill_price_cents: int | None = None
    rejection_reason: str | None = None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
