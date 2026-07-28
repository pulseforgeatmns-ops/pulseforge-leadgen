from enum import StrEnum

from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class ExecutionMode(StrEnum):
    PAPER = "paper"
    REPLAY = "replay"


class Settings(BaseSettings):
    """Application settings. Live execution is intentionally not an option."""

    model_config = SettingsConfigDict(env_file=".env", env_prefix="KALSHI_RESEARCH_", extra="ignore")

    mode: ExecutionMode = ExecutionMode.PAPER
    kalshi_api_base_url: HttpUrl = "https://external-api.kalshi.com/trade-api/v2"
    kalshi_timeout_seconds: float = Field(default=10, gt=0, le=60)
    kalshi_api_key_id: str | None = None
    kalshi_private_key_path: str | None = None
    kalshi_btc_market_keyword: str = "BTC"
    kalshi_btc_series_tickers: str = "KXBTC15M"
    collector_market_limit: int = Field(default=5_000, ge=1, le=20_000)
    collector_interval_seconds: float = Field(default=10, ge=2, le=300)
    database_url: str = "sqlite:///kalshi_research.db"
    initial_cash_cents: int = Field(default=100_000, ge=0)
    max_position_cents: int = Field(default=10_000, gt=0)
    max_contracts_per_market: int = Field(default=1, gt=0)
    max_daily_loss_cents: int = Field(default=5_000, gt=0)
    max_open_orders: int = Field(default=20, gt=0)
    log_level: str = "INFO"
