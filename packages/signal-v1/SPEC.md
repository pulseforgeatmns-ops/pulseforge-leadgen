# SIGNAL-V1

Implementation of the attention-driven Solana market intelligence engine (paper-only).

See repository issue/spec for full doctrine. V1 explicitly excludes real-money execution paths.

## Ground truth (SIGNAL-V1-002)

- Historical market observations persist in `signal_market_observations` (Postgres in production).
- Default provider: **GeckoTerminal** public OHLCV (`aggregate=1` → 1-minute candles when trades exist; gaps are not interpolated).
- Operator commands: `npm run signal:ingest-history`, `npm run signal:replay`.
- PASS/FAIL/UNRESOLVED uses achievable entry (15s–5m delays) from observed prices only.
