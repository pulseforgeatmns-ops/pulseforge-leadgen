# SPEC-015 — Market Intelligence Domain (MID)

| Field | Value |
|---|---|
| **Status** | Draft |
| **Target Version** | TBD (post SPEC-014) |
| **Priority** | High |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-26 |
| **Depends on** | SPEC-001 / 001A–C, SPEC-002, SPEC-003, SPEC-014, **SPEC-015A** |

## Objective

Prove that Pulseforge's Evidence Core is domain-agnostic by introducing the first non-CRM domain: financial market observation and reasoning.

Success is **understanding**, not trading. By completion, the platform answers regime, analog, hypothesis-confidence, evidence, unusualness, and recent-change questions about markets — using the existing Knowledge → Memory → Reasoning stack **without modification** to those engines — and without placing or simulating a single trade.

## Vision References

- `docs/vision/Intelligence_Architecture.md`
- [ADR-004](../adr/ADR-004_Knowledge_Graph.md) — Knowledge Graph as durable memory
- [ADR-002](../adr/ADR-002_Explainable_AI.md) — explainable reasoning
- [SPEC-001](SPEC-001_Persistent_Knowledge_Store.md) · [SPEC-001A](SPEC-001A_Knowledge_Layer_Foundation.md) · [SPEC-001B](SPEC-001B_Graph_Synchronization_Engine.md) · [SPEC-001C](SPEC-001C_Knowledge_Query_Engine.md)
- [SPEC-002](SPEC-002_Max_Reasoning_Engine.md)
- [SPEC-003](SPEC-003_Temporal_Intelligence_Memory.md)
- [SPEC-014](SPEC-014_Knowledge_Dual_Write.md)
- Command Deck pattern: [SPEC-006](SPEC-006_Command_Deck.md) · [SPEC-007](SPEC-007_Command_Deck_Composition_Engine.md) · [SPEC-008](SPEC-008_Command_Deck_UI.md)

## Problem

The intelligence library (Knowledge → Reasoning → Memory → Briefing → Policy → Command Deck) was built and validated against CRM entities (companies, prospects, interactions). Domain-agnosticism is asserted but unproven.

Without a second domain:

- Evidence Core assumptions may remain CRM-shaped (node factories, mappers, strategy inputs)
- There is no external proof that adapters alone can translate a new world into Entity / Event / Relationship / Evidence / Hypothesis / Outcome
- Future domains (ops, finance, support) lack a reference implementation

## Scope

- `packages/market-domain/` — market event schema, provider adapters, graph entity extensions, dual-write fan-in, research workspace composition
- Market Event Bus → Normalization → existing Evidence Core ingest path (SPEC-014 dual-write / KnowledgeEvent contract)
- Knowledge Graph extensions for market entity types and relationships
- Temporal Memory integration (generic; only entity types differ)
- Hypothesis objects as evidence-backed graph/claim structures consumed by unchanged Reasoning Engine
- Research Workspace UI/API mirroring Command Deck (thinking screen, not trading screen)
- Replay-compatible append-only market event storage
- Feature flag(s) so CRM dual-write and market ingest can run independently

## Out of Scope

- Live trading / order execution
- Broker integrations
- Position sizing / portfolio management
- Risk engine
- Automated execution
- Strategy optimization / backtest-as-trading
- Paper-trade simulation that mutates “positions” (observation of outcomes only is allowed when derived from market data, not from engine-placed orders)
- Hardcoded trading rules or regime classifiers that bypass evidence accumulation
- Changes to Reasoning Engine / Memory Engine internals to special-case markets

## Dependencies

- Persistent graph (`knowledge_*`) + Query Engine (SPEC-001 / 001C)
- `KnowledgeService` + node/edge model (SPEC-001A) — extended, not forked
- Dual-write / outbox / operational event contract (SPEC-014)
- Max Reasoning Engine (SPEC-002) — consume market evidence via existing evaluate path (requires SPEC-015A domain-neutral runtime + market strategy pack)
- Temporal Memory (SPEC-003) — snapshots/diffs/analogs over market entities
- **SPEC-015A Reasoning Runtime Decoupling** — injectable strategy packs; runtime has no CRM/market vocabulary
- Postgres pool (`db.js`); Railway env for provider API keys (Phase 1 may start with recorded fixtures)

## Design Principles

### Domain separation

Evidence Core must never know whether it is reasoning about companies, prospects, BTC, ETH, Fed announcements, Kalshi contracts, or news events.

Everything becomes:

```text
Entity · Event · Relationship · Evidence · Hypothesis · Outcome
```

Domain adapters perform translation. The reasoning engine remains unchanged.

### Observe before acting

Execution is prohibited. The Market Domain exists only to observe, normalize, learn, and reason.

### Every belief requires evidence

No hardcoded trading rules. Confidence scores emerge solely from accumulated observations.

## Architecture

```text
Market Sources
  Coinbase · Binance · Kalshi · Benzinga
  Economic Calendar · Fear & Greed
  Funding Rates · Open Interest · Liquidations
        │
        ▼
Market Event Bus          (packages/market-domain)
        │
        ▼
Normalization Layer       (provider adapters → Evidence Events)
        │
        ▼
Evidence Core ingest      (SPEC-014 KnowledgeEvent / dual-write / outbox)
        │
        ▼
Knowledge Graph           (SPEC-001*)
        │
        ▼
Temporal Memory           (SPEC-003)
        │
        ▼
Hypothesis + Reasoning    (SPEC-002 — unchanged)
        │
        ▼
Research Workspace        (Command Deck pattern; market tenant/surface)
```

Principle: adapters translate; core reasons; UI explains — never executes.

## Data Model

### Market event schema (canonical)

All provider payloads normalize to Evidence Events compatible with the SPEC-014 contract, e.g.:

```text
MarketEvidenceEvent {
  id, tenantId, timestamp, source, type,
  entityId, entityType, payload, evidence
}
```

Example event types (Phase 1):

| type | Notes |
|---|---|
| `price_tick` | asset, price, venue |
| `volume_update` | asset, volume, window |
| `news_event` | headline, symbols, sentiment optional |
| `economic_release` | series, actual/forecast/prior |
| `liquidation` | asset, side, notional |
| `funding_rate` | asset, rate |
| `open_interest` | asset/contract, oi |
| `order_book_snapshot` | asset, bids/asks summary |

### Graph entity extensions

New node types (additive to SPEC-001A factories / `NODE_TYPES`):

| Entity | Role |
|---|---|
| Asset | BTC, ETH, SPY, … |
| Exchange | Coinbase, Binance, … |
| Contract | Kalshi / listed contracts |
| EconomicEvent | calendar releases |
| MarketRegime | trending, choppy, breakout, news-driven, risk-on/off (claimed, evidence-backed) |
| Indicator | Fear & Greed, funding, OI, … |
| Hypothesis | momentum continuation, mean reversion, … |
| Observation | durable observation nodes linking events to claims |
| Outcome | observed move / confidence update (not trade fills) |

### Relationships (examples)

```text
Asset  —EXPERIENCED→  VolatilitySpike
VolatilitySpike  —SUPPORTED→  MomentumHypothesis
MomentumHypothesis  —VERIFIED_BY→  ObservedOutcome
```

Edge types are additive; no CRM edge semantics change.

### Temporal memory shape (illustrative)

Memory remains generic. Example operator-facing aggregate:

```text
Volatility Spike
  Observed: 621
  Most similar: 43
  Average continuation: +0.82%
  Confidence: 78%
```

### Hypothesis object

```text
Hypothesis {
  description,
  supportingObservations[],
  contradictingObservations[],
  confidence,
  sampleSize,
  lastUpdated
}
```

Confidence updates automatically from evidence; nothing is manually edited as ground truth.

## Implementation Plan

1. **Scaffold** `packages/market-domain/` — schema, types, package exports, feature flag `MARKET_DOMAIN`
2. **Market Event Bus + replay store** — append-only market events (migration); idempotent keys by source+provider id
3. **Adapters (fixture-first)** — Coinbase, Kalshi, Benzinga, Macro Calendar; normalize to Evidence Events; live HTTP behind flag
4. **Graph extensions** — node/edge factories + sync mappers into `GraphSyncEngine` / Knowledge dual-write
5. **Memory + reasoning wiring** — feed market tenant (or dedicated client) through existing Max runtime; no engine forks
6. **Research Workspace** — `GET /api/v1/market-research` (or equivalent) + read-only UI mirroring Command Deck cards (regime, analogs, hypotheses, evidence shifts, recent observations)
7. **Validation harness** — ingest fixtures → graph → evaluate → explain; optional live smoke with recorded keys
8. **Docs** — CURRENT_STATE, CHANGELOG, vision note that Evidence Core is multi-domain

Ordered PR slices preferred over a single mega-PR.

## Migration Strategy

- Additive SQL only (market event / replay tables; any new graph type constraints)
- Feature flag `MARKET_DOMAIN` default off until adapters + workspace smoke-pass
- No changes to CRM dual-write semantics; market ingest is a parallel producer into the same Evidence Core
- Rollback drops market-domain tables and disables flag; CRM knowledge remains intact

## Testing

```bash
npm run test:knowledge
npm run test:dual-write
npm run test:max
# proposed
npm run test:market-domain
node scripts/marketDomainE2EValidation.js --fixtures
```

Coverage expectations:

- Adapter golden fixtures → canonical events
- Idempotent re-ingest
- Graph nodes/edges created without CRM regressions
- Reasoning `explain()` cites market evidence + analogs
- Workspace API returns regime / hypotheses / shifts with no execution fields

## Acceptance Criteria

The implementation is complete when the platform can:

- [ ] Ingest market data (live and/or replay fixtures) via domain adapters
- [ ] Persist events into the existing Knowledge architecture (SPEC-001 / SPEC-014 path)
- [ ] Reason using the **unchanged** Evidence Core / Reasoning Engine / Temporal Memory
- [ ] Answer: current regime; nearest historical matches; hypotheses gaining/losing confidence; supporting evidence; unusualness; what changed in the last hour
- [ ] Explain conclusions with evidence and historical analogs
- [ ] Continuously update hypothesis confidence from new observations
- [ ] Research Workspace shows thinking cards only — **no** buy/sell, broker, or execution controls
- [ ] Place or simulate **zero** trades

## Future Work

- Additional venues and on-chain / macro feeds
- Dedicated market tenant provisioning UX
- Strategy / paper-trade specs **only after** MID demonstrates durable reasoning value
- Cross-domain analog search (CRM ↔ market) — explicitly deferred
- ADR if graph tenancy model needs a formal “domain” axis beyond `client_id`

## Explicit Non-Goals (reminder)

Not included in SPEC-015:

❌ Live trading · Broker integrations · Position sizing · Portfolio management · Risk engine · Automated execution · Strategy optimization
