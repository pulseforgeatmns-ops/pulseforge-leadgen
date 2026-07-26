# SPEC-000 — Repository Foundation & Source of Truth

| Field | Value |
|---|---|
| **Status** | Done |
| **Target Version** | v0.7.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-26 |

## Objective

Transform the repository into a self-documenting engineering project where humans and AI contributors can immediately understand what Pulseforge is, what is being built, why decisions were made, and what should be built next. The repository becomes the authoritative source of truth.

## Vision References

- `docs/vision/Mission.md`
- `docs/vision/Product_Thesis.md`
- `docs/vision/Product_Constitution.md`
- `docs/vision/Product_Roadmap.md`

## Problem

Product intent and architectural rationale lived in chat history, scattered runbooks, and large operational files (`CLAUDE.md`, `AGENTS.md`). New contributors (human or AI) could not reliably answer: current version, next task, why the architecture looks this way, or where to change code safely—without tribal knowledge.

## Scope

- Root docs: `README.md`, `CONTRIBUTING.md`, `PROJECT_CONTEXT.md`, `CURRENT_STATE.md`, `DECISIONS.md`, `CHANGELOG.md`
- `docs/00_START_HERE.md`
- Vision suite under `docs/vision/`
- Architecture suite under `docs/architecture/`
- Specs suite with template + SPEC-000/001/002
- ADR suite with template + ADR-001–004
- Release plans `v0.7.0`–`v1.0`
- Align documented version to v0.7.0

## Out of Scope

- Runtime behavior changes
- Schema migrations
- Enabling Max non-shadow flags
- Production deploy of Inquiry Foundation
- Rewriting all legacy flat `docs/*.md` runbooks into the new hierarchy

## Dependencies

- None (foundation)

## Architecture

Documentation hierarchy only. No service topology changes. Product philosophy (`vision/`) is separated from engineering design (`architecture/`) and implementation contracts (`specs/`).

## Data Model

N/A

## Implementation Plan

1. Create directory structure
2. Write root heartbeat and contributor docs
3. Write vision, architecture, specs, ADRs, releases
4. Point README → START_HERE → CURRENT_STATE
5. Record completion in CHANGELOG + CURRENT_STATE

## Migration Strategy

- Additive documentation only
- Legacy `CLAUDE.md` / `AGENTS.md` / flat runbooks remain valid; new hierarchy takes precedence for planning and product doctrine

## Testing

Manual verification:

- [x] All deliverable paths exist
- [x] Cross-links resolve for primary navigation
- [x] A reader can answer the Definition of Done questions from docs alone

## Acceptance Criteria

- [x] Repository is self-documenting
- [x] Documentation hierarchy is complete per deliverables list
- [x] Templates exist for future specs and ADRs
- [x] Release planning is documented through v1.0
- [x] AI contributor onboarding (`PROJECT_CONTEXT.md`) is complete
- [x] Repository is ready for SPEC-001
- [x] CURRENT_STATE reflects v0.7.0 foundation work

## Future Work

- SPEC-001 Business Knowledge Graph
- Fold high-value operational notes from `CLAUDE.md` into architecture docs over time
- Optionally add `docs/runbooks/` index for legacy flat files
