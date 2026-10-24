# SPEC-046 — Trade Intelligence Engine

| Field | Value |
|---|---|
| **Status** | Done |
| **Target Version** | v1.0.1 |
| **Priority** | High |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-28 |
| **Completed** | 2026-07-28 |
| **Depends on** | SPEC-044, SPEC-021, SPEC-018, SPEC-019, SPEC-020 |
| **Blocks** | Operator trade review UI; market intelligence dashboards |
| **Note** | Draft was labeled SPEC-045; that number is [Command Deck UX Polish](SPEC-045_Command_Deck_UX_Polish.md). This ships as SPEC-046. |

## Objective

Convert captured trades into actionable intelligence.

The Trade Intelligence Engine does not execute trades. It analyzes historical evidence and continuously answers: What is working? What is failing? What is changing? What should I investigate next?

Success: daily/weekly reviews, pattern discovery, calibration analysis, and explainable recommendations — all derived from evidence, reproducible through Replay, queryable via EQL and the Laboratory.

## Vision References

- [ADR-009 Evidence Platform Architecture](../adr/ADR-009_Evidence_Platform_Architecture.md)
- [SPEC-044 Trade Capture Engine](SPEC-044_Trade_Capture_Engine.md)
- [SPEC-021 Learning & Belief Evolution](SPEC-021_Learning_and_Belief_Evolution_Engine.md)
- [SPEC-018 Deterministic Replay](SPEC-018_Deterministic_Replay_and_Temporal_Reasoning_Engine.md)
- [SPEC-019 Evidence Laboratory](SPEC-019_Evidence_Laboratory.md)
- [SPEC-020 Evidence Query Language](SPEC-020_Evidence_Query_Language.md)

## Problem

SPEC-044 makes capture frictionless, but captured trades are inert without analysis. Operators need session reviews, pattern discovery, calibration feedback, and evidence-backed recommendations — without manual journaling or black-box optimization.

## Scope

- Package `packages/trade-intelligence/`
  - `TradeIntelligenceEngine.js` — façade
  - `TradeAnalyzer.js` — aggregates and hypothesis performance
  - `PatternDiscovery.js` — recurring pattern findings
  - `CalibrationAnalyzer.js` — confidence vs outcome
  - `ReviewGenerator.js` — daily / weekly reviews
  - `RecommendationEngine.js` — explainable recommendations
  - `types.js` / `index.js`
- Replay: `reviewTrade`, `compareWeek`, `generateDailyReview`
- Laboratory: `discoverTradePatterns`, `compareTradeStrategies`, `compareTimeWindows`, `compareConfidenceBands`
- EQL: `SHOW DailyReview FOR Today`, `SHOW WeeklyReview FOR LastWeek`, `SHOW BestHypotheses`, `SHOW TradeCalibration`, `SHOW SimilarTrades FOR Trade("…")`, `SHOW Recommendations`
- Immutable Finding objects
- Tests + spec index / CHANGELOG / CURRENT_STATE

## Out of Scope

- Trade execution or order routing
- ML / neural pattern recognition (v1 uses hypothesis + metadata similarity)
- Chart embedding similarity (future; API unchanged)
- Manual editing of findings
- Durable Postgres persistence

## Dependencies

- ✅ SPEC-044 Trade Capture Engine
- ✅ SPEC-021 Learning & Belief Evolution
- ✅ SPEC-018 Deterministic Replay
- ✅ SPEC-019 Evidence Laboratory
- ✅ SPEC-020 Evidence Query Language

## Architecture

```text
Trade → Evidence → Claims → Outcomes → Learning
                    │
                    ▼
          Trade Intelligence Engine
                    │
      ┌─────────────┼─────────────┐
      ▼             ▼             ▼
 Daily/Weekly   Patterns    Recommendations
   Reviews      Findings      (explainable)
```

### Principles

| Rule | Meaning |
|---|---|
| 1 | Evidence first |
| 2 | Never optimize for a single strategy |
| 3 | Recommendations must be explainable |
| 4 | Everything reproducible through Replay |
| 5 | Intelligence derived from observations — never manually edited |
| 6 | Does not execute trades |

### Finding (immutable)

| Field | Notes |
|---|---|
| `id` | Unique finding id |
| `type` | pattern · calibration · hypothesis · recommendation · review · similarity |
| `title` | Operator-facing headline |
| `summary` | Short explanation |
| `supportingEvidence` | Trade / evidence refs |
| `contradictingEvidence` | Counter-examples |
| `confidence` | 0–1 or label |
| `createdAt` | ISO timestamp |
| `runtimeVersion` | `trade-intelligence@1.0.0` |
| `replayRefs` | Trade ids for Replay reproduction |

## Implementation Plan

1. Scaffold `@pulseforge/trade-intelligence`
2. Analyzer + pattern + calibration + review + recommendation modules
3. TradeIntelligenceEngine façade + Replay helpers
4. EQL targets + Laboratory helpers
5. Tests + docs

## Testing

```bash
npm run test:trade-intelligence
npm run test:trade-capture
npm run test:eql
npm run test:laboratory
```

## Acceptance Criteria

- [x] Daily review generated from historical evidence
- [x] Weekly review generated without manual input
- [x] Pattern discovery identifies statistically meaningful recurring behaviors
- [x] Calibration analysis compares confidence against outcomes
- [x] Recommendations are evidence-backed and explainable
- [x] Every recommendation reproducible through Replay refs
- [x] Findings queryable through EQL and comparable in the Laboratory
- [x] Spec documented; CHANGELOG + CURRENT_STATE updated

## Future Work

- Chart embedding similarity (no API change)
- Visual pattern recognition reprocessing
- Durable finding store keyed by content hash
- Operator review UI surfaces
