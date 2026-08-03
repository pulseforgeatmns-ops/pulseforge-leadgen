# Contributing to Pulseforge

This repository is the authoritative source of truth.

## Before you write code

1. Read [PROJECT_CONTEXT.md](PROJECT_CONTEXT.md) (required for AI contributors).
2. Read [CURRENT_STATE.md](CURRENT_STATE.md) — confirm version, current spec, blockers.
3. Open the active spec under `docs/specs/`.
4. Skim relevant ADRs under `docs/adr/` and architecture docs under `docs/architecture/`.

## Spec workflow

Every future implementation ships behind a numbered spec (`SPEC-NNN`). Specs use [docs/specs/TEMPLATE.md](docs/specs/TEMPLATE.md):

- Objective
- Vision References
- Problem
- Scope / Out of Scope
- Dependencies
- Architecture
- Data Model
- Implementation Plan
- Migration Strategy
- Testing
- Acceptance Criteria
- Future Work

**Rules**

- Do not start implementation without an approved or in-progress spec linked from `CURRENT_STATE.md`.
- Specs reference vision docs; they do not redefine product philosophy.
- Large architectural choices require an ADR before or with the PR.

## Pull request checklist

Every PR must:

| Requirement | When |
|---|---|
| Update [CURRENT_STATE.md](CURRENT_STATE.md) | Project state, sprint, blockers, or priority changes |
| Update [CHANGELOG.md](CHANGELOG.md) | Always for user/operator-visible or engineering-milestone work |
| Create an ADR | Architecture, data model, agent authority, or safety boundary changes |
| Link the relevant spec | In PR description and CHANGELOG entry |
| Preserve backwards compatibility | Unless explicitly approved in the spec/ADR |

Also:

- Prefer small, reviewable PRs scoped to one spec slice.
- Do not commit secrets (`.env`, token JSON, session cookies).
- Do not call `pool.end()` in agents — the DB pool is process-shared.
- New HTTP routes belong under `routes/`, not in `server.js`.
- Follow DNC and client-scoping rules (`checkDNC`, explicit `client_id`, fail closed).

## Review process

1. **Spec alignment** — Does the diff match Scope and Acceptance Criteria?
2. **Safety** — Shadow/default-off for new outbound or state-mutation paths?
3. **Docs** — CURRENT_STATE, CHANGELOG, ADR/spec links updated?
4. **Tests** — New behavior covered; migrations validated where schema changes?
5. **Compatibility** — Existing clients, cron jobs, and dashboards still work?

Reviewers should reject PRs that change architecture without an ADR or that leave CURRENT_STATE stale.

## Naming conventions

| Kind | Convention | Example |
|---|---|---|
| Specs | `SPEC-NNN_Short_Title.md` | `SPEC-001_Business_Knowledge_Graph.md` |
| ADRs | `ADR-NNN_Short_Title.md` | `ADR-001_Conversation_First.md` |
| Releases | `vX.Y.Z.md` | `v0.7.0.md` |
| Agents | `{name}Agent.js` | `maxAgent.js` |
| Migrations | `YYYY-MM-DD-description.sql` | `2026-07-25-new-inquiry-foundation.sql` |

## Where to put things

| Change type | Location |
|---|---|
| Product philosophy | `docs/vision/` |
| System design | `docs/architecture/` |
| Implementation plan | `docs/specs/` |
| Decision record | `docs/adr/` |
| Release plan | `docs/releases/` |
| Operator runbooks | `docs/` (flat) until migrated |
| Agent operational rules | `AGENT_RULES.md`, agent modules |

## Definition of done (contributor)

A change is done when Acceptance Criteria in the linked spec are met, docs are current, and a fresh engineer can explain the change from the repository alone.
