# SPEC-020 — Evidence Query Language (EQL)

| Field | Value |
|---|---|
| **Status** | Done |
| **Target Version** | v1.0.1 |
| **Priority** | High |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Completed** | 2026-07-27 |
| **Depends on** | SPEC-017, SPEC-018, SPEC-019 |
| **Blocks** | Research debugging UI; operator query surfaces |

## Objective

Provide a domain-neutral query language for exploring the Evidence Graph, replay history, and reasoning outputs.

Rather than calling many APIs, consumers ask questions declaratively.

Success: the same EQL statement executes for CRM and Market subjects with no runtime domain branching.

## Vision References

- [ADR-009 Evidence Platform Architecture](../adr/ADR-009_Evidence_Platform_Architecture.md)
- [SPEC-017 Domain Ontology Framework](SPEC-017_Domain_Ontology_Framework_and_Market_Ontology.md)
- [SPEC-018 Deterministic Replay](SPEC-018_Deterministic_Replay_and_Temporal_Reasoning_Engine.md)
- [SPEC-019 Evidence Laboratory](SPEC-019_Evidence_Laboratory.md)

## Problem

Consumers of the Evidence Platform currently stitch together ReplayEngine, EvidenceQuery helpers, and laboratory APIs. That creates:

- Many imperative call sites for one research question
- Risk of domain-specific branching (`if market … else crm …`)
- No shared grammar for FIND / SHOW / REPLAY / COMPARE / EXPLAIN

## Scope

- Package `packages/eql/`
  - `Parser.js` — tokenize + AST
  - `QueryPlanner.js` — AST → execution plan
  - `Executor.js` — plan + EvidenceCatalog → result
  - `types.js` — statement kinds, targets, rules
  - `index.js` — `createEqlEngine`, `createEvidenceCatalog`, `catalogFromResult`
- Laboratory integration: `lab.query(\`…\`)` (SPEC-019 façade)
- Unit tests covering domain-neutral acceptance criteria
- Spec index + CHANGELOG / CURRENT_STATE updates

## Out of Scope

- Mutation statements (`UPDATE` / `DELETE` / `INSERT`)
- SQL generation / Postgres pushdown
- UI query builder
- Persisting saved queries
- Domain-specific grammar extensions

## Dependencies

- ✅ SPEC-017 Domain Ontology Framework (core node categories)
- ✅ SPEC-018 Deterministic Replay
- ✅ SPEC-019 Evidence Laboratory

## Architecture

```text
EQL source
   │
   ▼
 Parser  →  AST
   │
   ▼
QueryPlanner  →  Plan
   │
   ▼
 Executor  ──►  EvidenceCatalog (domain-neutral)
   │
   ▼
 Result { rows, explanation, plan }
```

### Guiding rules

| Rule | Meaning |
|---|---|
| 1 | EQL is domain-neutral |
| 2 | No mutation statements |
| 3 | No CRM / Market runtime branching |
| 4 | Queries are declarative |
| 5 | Every query may end with `EXPLAIN` |

### Initial statements

Support:

- `FIND`
- `SHOW`
- `REPLAY`
- `COMPARE`
- `EXPLAIN`

### Query targets

Subjects · Observations · Evidence · Claims · Outcomes · Recommendations · Replay Sessions

### Examples

```js
lab.query(`
FIND Claims
WHERE subject = "BTC"
AND confidence > 0.75
ORDER BY confidence DESC
`)

lab.query(`
SHOW Evidence
SUPPORTING Claim("momentum_continuation")
`)

lab.query(`
REPLAY
FROM "2026-07-26T09:30:00Z"
TO "2026-07-26T10:00:00Z"
`)

lab.query(`
FIND Claim("momentum_continuation")
EXPLAIN
`)
```

`EXPLAIN` returns supporting evidence, contradicting evidence, confidence history, and reasoning trace when available.

### Subject identity

`WHERE subject = "…"` resolves through domain-neutral field aliases (`subject`, `subjectId`, `companyId`, …). CRM and Market records use the same predicate path.

## Data Model

No new durable tables. EQL executes against an in-process `EvidenceCatalog` (seeded, projected from replay/lab results, or injected).

## Implementation Plan

1. Land `packages/eql/` with Parser, QueryPlanner, Executor, types, index
2. Wire `lab.query` on EvidenceLab
3. Unit tests for CRM vs Market subject queries on one engine
4. Index SPEC-020; update CHANGELOG + CURRENT_STATE

## Migration Strategy

None. Pure additive package. Existing EvidenceQuery helpers remain available on `lab.query.*` method attachments / `lab.queryHelpers`.

## Testing

```bash
npm run test:eql
npm run test:laboratory
```

Coverage:

- Parser accepts FIND / SHOW / REPLAY / COMPARE / EXPLAIN
- Mutation keywords rejected
- Same FIND plan/execution path for `subject="Company123"` and `subject="BTC"`
- SHOW SUPPORTING / REPLAY / EXPLAIN / COMPARE
- `catalogFromResult` projects replay outputs

## Acceptance Criteria

- [x] `packages/eql/` delivers Parser, Executor, QueryPlanner, types, index
- [x] Support FIND, SHOW, REPLAY, COMPARE, EXPLAIN (no mutations)
- [x] Query targets: Subjects, Observations, Evidence, Claims, Outcomes, Recommendations, Replay Sessions
- [x] Trailing `EXPLAIN` returns supporting / contradicting / confidence history / reasoning trace surface
- [x] Same query executes regardless of domain (CRM `Company123` / Market `BTC`) with no runtime branching
- [x] Spec documented at `docs/specs/SPEC-020_Evidence_Query_Language.md`

## Future Work

- Postgres-backed catalog adapter (read-only)
- Query planner cost model / indexes
- Laboratory UI query console
- Saved notebooks of EQL statements
