# SPEC-044 — Trade Capture Engine

| Field | Value |
|---|---|
| **Status** | Done |
| **Target Version** | v1.0.1 |
| **Priority** | High |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-28 |
| **Completed** | 2026-07-28 |
| **Depends on** | SPEC-017, SPEC-018, SPEC-019, SPEC-020 |
| **Blocks** | Operator trade journal UI; market evidence ingestion at trading speed |
| **Note** | Draft was labeled SPEC-021; that number is [Learning & Belief Evolution](SPEC-021_Learning_and_Belief_Evolution_Engine.md). This ships as SPEC-044. |

## Objective

Create the fastest possible mechanism for capturing market observations with almost zero friction.

The goal is not journaling. The goal is ensuring 100% of trades become Evidence.

Success: the operator can capture a complete trade in under 15 seconds; OCR / extraction never blocks save; every screenshot becomes an immutable Observation.

## Vision References

- [ADR-009 Evidence Platform Architecture](../adr/ADR-009_Evidence_Platform_Architecture.md)
- [SPEC-017 Domain Ontology Framework](SPEC-017_Domain_Ontology_Framework_and_Market_Ontology.md)
- [SPEC-018 Deterministic Replay](SPEC-018_Deterministic_Replay_and_Temporal_Reasoning_Engine.md)
- [SPEC-019 Evidence Laboratory](SPEC-019_Evidence_Laboratory.md)
- [SPEC-020 Evidence Query Language](SPEC-020_Evidence_Query_Language.md)

## Problem

Market operators cannot afford a journaling workflow mid-session. Without a sub-15-second capture path, trades never enter the Evidence Graph — so Replay, Laboratory, Learning, and EQL have nothing to work with.

## Scope

- Package `packages/trade-capture/`
  - `CaptureEngine.js` — façade (paste → answer → save)
  - `CaptureSession.js` — session lifecycle
  - `ScreenshotProcessor.js` — immutable image + `chart_snapshot` observation
  - `TradeBuilder.js` — Trade + Evidence → Claim → Outcome graph
  - `ObservationExtractor.js` — pluggable OCR / Chart / Pattern / Indicator / CV
  - `types.js` / `index.js`
- Market ontology: `chart_snapshot` observation type
- Laboratory: `lab.findTrades`, `lab.compareWinningTrades`, `lab.compareLosingTrades`
- EQL: `Trades`, `Screenshots` targets; `COMPARE WinningTrades WITH LosingTrades`
- Replay helper: `operatorView(tradeId)` — what the operator saw before entry
- Unit tests + spec index / CHANGELOG / CURRENT_STATE

## Out of Scope

- Operator UI (paste surface, chip buttons)
- Real OCR / computer-vision backends (stubs + pluggable registry only)
- Durable Postgres persistence of screenshots / trades
- Paper trading or live order routing
- Mutating historical screenshots after capture

## Dependencies

- ✅ SPEC-017 Domain Ontology Framework + Market Ontology
- ✅ SPEC-018 Deterministic Replay
- ✅ SPEC-019 Evidence Laboratory
- ✅ SPEC-020 Evidence Query Language

## Architecture

```text
Paste / Drop screenshot
        │
        ▼
 ScreenshotProcessor ──► immutable chart_snapshot Observation
        │
        ▼
  CaptureSession (4 chips: Outcome · Direction · Hypothesis · Confidence)
        │
        ▼
   TradeBuilder ──► Trade · Evidence · Claim · Outcome
        │
        ▼
 CaptureEngine.save()  ── returns immediately
        │
        └──► ObservationExtractor queue (OCR / indicators / price / metadata / patterns)
```

### Principles

| Rule | Meaning |
|---|---|
| 1 | Screenshot first |
| 2 | Ask almost nothing |
| 3 | Everything else is inferred |
| 4 | Never interrupt trading |
| 5 | Missing data can be filled later |
| 6 | Images are first-class Evidence |
| 7 | OCR / extraction never blocks capture |
| 8 | Screenshots are immutable Observations |

### Capture flow

1. Paste screenshot (or drag/drop) → immediately create `CaptureSession`
2. Ask only four questions (no typing): Outcome · Direction · Hypothesis · Confidence
3. Save — background extraction continues independently

### Observation graph

```text
Screenshot → Trade → Evidence → Claim → Outcome
```

### Automatic extraction (non-blocking)

Attempts to infer: timestamp, exchange, timeframe, symbol, current price, indicators visible, ATR, VWAP, volume, chart image hash, screenshot dimensions.

Failures never block capture. Unknown values remain `null`.

### Extractor registry

OCR · Chart · Pattern · Indicator · Computer Vision — each contributes Evidence independently.

## Data Model

### Trade

| Field | Notes |
|---|---|
| `id` | Capture id |
| `entryTime` | Inferred or capture time |
| `direction` | Long / Short |
| `hypothesis` | Velocity / Breakout / Pullback / Mean Reversion / Other |
| `confidence` | 1–5 |
| `result` | Win / Loss |
| `screenshotId` | FK to immutable screenshot |

### Observation

| Field | Notes |
|---|---|
| `type` | `chart_snapshot` |
| `payload.imageHash` | SHA-256 of original bytes |
| `payload.screenshotDimensions` | width / height when sniffable |

## Implementation Plan

1. Scaffold `@pulseforge/trade-capture`
2. Wire market ontology `chart_snapshot`
3. Extend EQL targets + bare `COMPARE WinningTrades WITH LosingTrades`
4. Laboratory find / compare helpers + ingest
5. Tests + docs

## Migration Strategy

No durable schema in v1. In-memory CaptureEngine only. Future persistence can store screenshot bytes by `imageHash` without rewriting originals.

## Testing

```bash
npm run test:trade-capture
npm run test:eql
npm run test:laboratory
npm run test:market-ontology
```

## Acceptance Criteria

- [x] Operator workflow: paste → Win/Loss · Long/Short · Hypothesis · Confidence → Save
- [x] Total operator path designed for &lt;15 seconds (no OCR await on save)
- [x] No OCR or extraction step blocks capture
- [x] All screenshots become immutable Evidence (`chart_snapshot`)
- [x] `lab.findTrades({ hypothesis: "Velocity" })`
- [x] `lab.compareWinningTrades` / `lab.compareLosingTrades`
- [x] EQL: `FIND Trades` · `SHOW Screenshots FOR Trade("…")` · `COMPARE WinningTrades WITH LosingTrades`
- [x] Replay helper recreates operator view from screenshot + extracted observations
- [x] Spec documented; CHANGELOG + CURRENT_STATE updated

## Future Work

- Operator capture UI (keyboard paste + chip pad)
- Real OCR / CV extractors reprocessing the same immutable image
- Durable screenshot object store keyed by content hash
- Pattern indexing across the trade corpus
