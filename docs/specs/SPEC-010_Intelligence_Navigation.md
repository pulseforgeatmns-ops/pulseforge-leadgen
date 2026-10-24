# SPEC-010 — Intelligence Navigation

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v1.0.0 |
| **Priority** | Critical |
| **Owner** | TBD |
| **Created** | 2026-07-26 |
| **Completed** | 2026-07-26 |
| **Version** | v1.0.0 |

## Objective

Make every piece of intelligence explorable without losing context. The operator moves through a continuous investigation graph — Command Deck → recommendation / company → evidence → related intelligence → decision → back to Command Deck — never hitting a dead end.

## Vision References

- `docs/vision/Product_Constitution.md` (§11 Cognitive load)
- `docs/vision/Product_Experience.md`
- `docs/vision/Intelligence_Architecture.md`
- [SPEC-006](SPEC-006_Command_Deck.md) — Company Intelligence + Recommendation Detail
- [SPEC-007](SPEC-007_Command_Deck_Composition_Engine.md) — composition pattern
- [SPEC-008](SPEC-008_Command_Deck_UI.md) — render-only UI
- [SPEC-009](SPEC-009_Max_Intelligence_Workspace.md) — Ask Max as investigation
- [ADR-001](../adr/ADR-001_Conversation_First.md)
- [ADR-002](../adr/ADR-002_Explainable_AI.md)

## Problem

The intelligence stack is explainable, but the UI still exposes it as isolated cards and a Max modal. `open_company` / `review_recommendation` only reopen Ask Max. Related entities in evidence panels are display-only. There is no intelligence trail, no Company or Recommendation destination, and every drill-down feels like starting over.

## Scope

- Intelligence trail (investigation breadcrumbs, not URL crumbs)
- Related Intelligence section on every node
- Progressive exploration (one depth per click)
- Persistent MaxContext synced to trail focus
- Company Intelligence view model + `GET /api/v1/companies/:id/intelligence`
- Recommendation Detail view model + `GET /api/v1/recommendations/:id`
- Investigation stack on `/command-deck` (same application feel; history deep links)
- Navigable evidence / related entities from Max responses
- Closes SPEC-006 remaining: Company Intelligence + Recommendation Detail

## Out of Scope

- Market / Timeline top-level destination pages
- Durable cross-process trail store
- Autonomous decide / execute
- Live CRM dual-write (fail closed when stack empty)
- Replacing `/dashboard` as default landing
- Expanding EvidenceAssembler into live knowledge queries (ADR-005 / SPEC-009 boundary)

## Dependencies

- ✅ SPEC-001C Knowledge Query Engine (`related`, `timeline`, `explain`)
- ✅ SPEC-002–005 Reasoning / Memory / Briefing / Policy
- ✅ SPEC-007 Command Deck Composer pattern
- ✅ SPEC-008 / SPEC-009 Command Deck UI + Max Workspace

## Architecture

```text
Command Deck
      │
      ├── review_recommendation → RecommendationDetailComposer
      ├── open_company          → CompanyIntelligenceComposer
      │
      ▼
Investigation stack (client trail + history)
      │
      ├── Evidence (progressive)
      ├── Related Intelligence
      └── Ask Max (MaxContext = trail focus)
```

Composers may assemble and summarize. They must not score, rank, or invent. UI remains render-only.

### Related Intelligence (per node)

| Node | Answers “What else?” |
|---|---|
| Company | Similar companies, shared signals, competing opportunities, recent changes |
| Recommendation | Supporting / contradicting evidence, alternative recommendations |
| Evidence | Other recommendations using this evidence, source interactions |

### Intelligence trail

Client-owned stack of `{ kind, id, label }`. Example:

```text
Today's Brief > Highest Leverage Action > Marlowe Properties > Staffing Expansion > Evidence
```

## Data Model

No new intelligence tables. View models are frozen presentation objects:

- `RecommendationDetailModel`
- `CompanyIntelligenceModel`
- Shared `RelatedIntelligence` + navigable `NavRef` `{ type, id, label }`

## Implementation Plan

1. Spec + heartbeat docs
2. Composers + RelatedIntelligence builder + HTTP routes
3. Command Deck investigation UI (trail, views, deep links)
4. Max evidence/related click → graph push; MaxContext sync
5. Tests + smoke path

## Migration Strategy

- Ship on `/command-deck` only; `/dashboard` unchanged
- No schema migration
- Rollback: hide investigation panel; restore Ask-Max-only action handlers

## Testing

- Unit: composers from seeded tenant; fail-closed empty / missing ids
- Unit: RelatedIntelligence only uses present graph ids
- Unit: trail push/pop helpers
- Manual: Deck → HLA → Recommendation → Evidence → Related company → Ask Max → back

## Acceptance Criteria

- [x] Operator can move Deck → Recommendation → Evidence → Related → Company → Decision → Deck without a dead end
- [x] Intelligence trail explains how they got there
- [x] Every node answers “What else should I look at?”
- [x] Max context updates with trail focus
- [x] UI remains render-only; composers do not re-score
- [x] `open_company` / `review_recommendation` open real intelligence views

## Future Work

- Market / Timeline destination pages
- Shareable durable investigation URLs across processes
- Deeper live graph once dual-write is production
