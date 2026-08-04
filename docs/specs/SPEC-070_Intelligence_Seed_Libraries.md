# SPEC-070 — Intelligence Seed Libraries

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Max Platform |
| **Created** | 2026-08-03 |
| **Depends** | [SPEC-065](SPEC-065_Market_Intelligence_Foundation.md), [ADR-045](../adr/ADR-045_Evidence_Before_Reasoning.md) |

## Objective

Create a curated seed-library system so Max (and future services) can retrieve useful reference knowledge—sales methodology, industry playbooks, offer positioning, operating preferences, and market background—without confusing that guidance with observed Market Intelligence evidence or Relationship/Knowledge facts.

## Vision References

- [ADR-045](../adr/ADR-045_Evidence_Before_Reasoning.md) — Evidence Before Reasoning
- [SPEC-065](SPEC-065_Market_Intelligence_Foundation.md) — Market Intelligence observations (separate layer)
- [SPEC-001](SPEC-001_Persistent_Knowledge_Store.md) — Knowledge / relationship facts (separate layer)
- [SPEC-066](SPEC-066_Max_Market_Intelligence_Integration.md) — future Max consumer (out of scope here)

## Problem

Max needs bootstrap guidance (how we sell, how industries work, operator preferences) before enough observational evidence exists. Without a dedicated seed store, that material either missing or gets mixed into market observations / knowledge claims—polluting provenance and trust.

## Scope

- Postgres table `intelligence_seed_libraries` with provenance, trust_level, enabled soft-retire, and integer version
- Categories: `sales_methodology`, `industry_playbook`, `offer_positioning`, `operating_preferences`, `market_reference`
- Read-only query service: list, search by category/tag/scope, fetch detail
- CLI + curated local JSON seed entries (permissioned summaries only)
- GET-only admin/internal API under `/api/v1/intelligence/seed-libraries`
- Tests and explicit documentation that seeds are reference/guidance, not evidence

## Out of Scope

- Max autonomous behavior or prompt wiring
- Recommendations written to DB
- Prospect/company facts derived from seed libraries
- Dual-write into Market Intelligence or Knowledge Graph
- Copyrighted long-form content ingestion unless summarized/permissioned
- Composer copy generation

## Dependencies

- Existing Express auth (`requireAuth` / `requireRole`)
- Shared `pg` pool (`db.js`)
- No dependency on `market_*` or `knowledge_*` tables

## Architecture

```text
data/seed-libraries/*.json
        ↓
scripts/seedIntelligenceLibraries.js   (CLI upsert only)
        ↓
intelligence_seed_libraries            (Postgres, separate from market/knowledge)
        ↓
services/intelligenceSeedLibraryQuery.js
        ↓
routes/intelligenceSeedLibraries.js    (GET-only, admin/manager)
```

### Boundary (non-negotiable)

| Layer | What it holds | Is evidence? |
|---|---|---|
| **Seed libraries** | Methodology, preferences, industry background, offer framing | **No** — reference/guidance |
| **Market Intelligence** | Structured observations from imported competitor emails | **Yes** — observed |
| **Knowledge / Relationship Intelligence** | Prospect/company facts and claims | **Yes** — factual graph |

Every API/query result stamps `kind: 'seed_library_reference'` and `isEvidence: false`.

## Data Model

### `intelligence_seed_libraries`

| Column | Type | Notes |
|---|---|---|
| `library_id` | TEXT PK | Stable id, e.g. `sales_methodology.discovery_checklist` |
| `title` | TEXT NOT NULL | |
| `category` | TEXT NOT NULL | Enum CHECK (5 categories) |
| `source_type` | TEXT NOT NULL | `curated_operator`, `public_method_summary`, `internal_preference`, `market_background` |
| `trust_level` | TEXT NOT NULL | `high`, `medium`, `low`, `provisional` |
| `scope` | JSONB NOT NULL | Audience / vertical / client scoping |
| `summary` | TEXT NOT NULL | Short operator-facing blurb |
| `content_text` | TEXT | Optional inline summary body |
| `content_ref` | TEXT | Optional path under `data/seed-libraries/` |
| `tags` | TEXT[] | |
| `provenance` | JSONB NOT NULL | Requires `curated_by`, `curated_at`, `notes` |
| `enabled` | BOOLEAN NOT NULL DEFAULT TRUE | Soft-retire without deleting provenance |
| `version` | INTEGER NOT NULL DEFAULT 1 | Monotonic revision for future Max/Cal attribution |
| `created_at` / `updated_at` | TIMESTAMPTZ | |

Constraints: at least one of `content_text` / `content_ref`; `version >= 1`. No FKs to CRM, market, or knowledge tables.

## Implementation Plan

1. This spec + README registry
2. Additive migration + rollback
3. Query service (list / search / detail)
4. Curated JSON + seed CLI + npm script
5. GET-only routes mounted from `server.js`
6. Unit + route smoke tests

## Migration Strategy

- Forward: additive `CREATE TABLE IF NOT EXISTS`
- Rollback: `DROP TABLE IF EXISTS intelligence_seed_libraries`
- Compatibility: no changes to CRM, market intel, knowledge, or Max runtime

## Testing

```bash
node --test test/intelligenceSeedLibraryQuery.test.js \
  test/intelligenceSeedLibrariesRoutes.test.js \
  test/seedIntelligenceLibraries.test.js
```

## Acceptance Criteria

- [x] Max or future services can retrieve seed guidance by category/tag via query service + GET APIs
- [x] Every seed item has provenance and trust_level
- [x] Soft-retire via `enabled`; guidance revisions via integer `version`
- [x] Seed guidance kept separate from Market Intelligence observations and Relationship Intelligence facts
- [x] Docs state seeds are reference/guidance, not observed evidence
- [x] No Max wiring, recommendations, or dual-writes in this release

## Future Work

- Max / Cal consumers that attribute `library_id` + `version` in reasoning traces
- Operator UI to browse / pause seeds
- Scoped seeds per client or vertical beyond JSON scope filters
