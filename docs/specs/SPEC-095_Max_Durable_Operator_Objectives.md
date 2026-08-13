# SPEC-095 — Max Durable Operator Objectives & Pre-Routing Context Resolution

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Pulseforge |
| **Created** | 2026-08-13 |

## Objective

Max must remember what the operator is trying to accomplish before deciding what the operator’s next words mean. SPEC-095 adds durable operator/client strategic objectives that survive fresh Max sessions, retrieve before intent routing, resolve clear references deterministically, and supply recovered context to SPEC-094 Paige delegation — without Missions, provider memory, or a new campaign-management subsystem.

## Vision References

- [SPEC-009 Max Intelligence Workspace](SPEC-009_Max_Intelligence_Workspace.md)
- [SPEC-013 Outcome Intelligence](SPEC-013_Outcome_Intelligence.md)
- [SPEC-022 Mission Engine and Agent Orchestration](SPEC-022_Mission_Engine_and_Agent_Orchestration.md)
- [SPEC-094 Max to Paige Campaign Content Delegation](SPEC-094_Max_to_Paige_Campaign_Content_Delegation.md)
- ADR-045 Evidence Before Reasoning

## Problem

Max can reason correctly about an operator objective when it is present in the current request. He cannot reliably recover that objective in a fresh conversation.

SPEC-094 exposed this in production: with the Public Max Launch objective in-message, Max delegated content strategy to Paige correctly. In a fresh session, “Where are we with the Max launch campaign?” cold-interpreted as `campaign_creation` → Mission Engine → Commercial Cleaning - Manchester.

The failure is not Paige, not the Mission Engine, and not API conversation state. Max interprets language before reconstructing what the operator is already trying to accomplish.

## Scope

- Durable `operator_objectives` persistence (operator + client scope)
- Explicit objective establishment / update / lifecycle (fail closed)
- Pre-routing retrieval + deterministic reference resolution
- ContextEnvelope passthrough for `activeObjectives` / `resolvedObjective`
- Status/planning about resolved objectives stays in Workspace (no Mission create)
- SPEC-094 receives recovered objective context for Paige delegation
- Max synthesis places Paige recommendations inside objective context
- Public Max Launch production seed (tenant `1`, operator scope)
- Automated coverage for persistence, resolution, ambiguity, mission regression, Paige integration, tenant isolation

## Out of Scope

- General conversational memory / automatic memory extraction
- Autonomous objective discovery, prioritization, phase advancement, or execution
- Project/task/campaign-management boards
- Vector search, embeddings, semantic memory
- Persistent provider/LLM sessions
- New agent framework or orchestration engine
- Paige learning-engine changes (SPEC-092/093 remain authoritative for content evidence)

## Dependencies

- `packages/max/workspace/WorkspaceEngine.js`
- `packages/max/workspace/ContextEnvelope.js`
- `packages/max/workspace/ExecutionDomain.js`
- `services/maxPaigeCampaignDelegation.js` (SPEC-094)
- `services/contentLearning.js` (SPEC-093)
- Existing Mission Engine (must remain intact for genuine campaign creation)

## Architecture

```text
                    PULSEFORGE DURABLE STATE
                            │
                    Active Objectives
                            │
                            ▼
Operator → Max Request → Pre-Routing Context
                            │
                    Reference Resolution
                            │
                    Intent Understanding
                            │
              ┌─────────────┼──────────────┐
              ▼             ▼              ▼
          Workspace       Mission        Paige
                                         │
                                   SPEC-092/093
                                         │
                                         ▼
                                        Max
```

Runtime order in `WorkspaceEngine.ask()`:

1. Session
2. Retrieve active objectives (`getActiveObjectives`)
3. Resolve references (`resolveObjectiveReference`) — fail closed on ambiguity
4. Establish / update / status handlers when applicable
5. Paige gate (SPEC-094) with recovered objective on context
6. `selectExecutionDomain` (mission suppressed only when resolved objective + status/content flag)
7. Mission Engine for genuine new campaign requests

**Operator Objective ≠ Mission.** Objectives are interpretive context only. Creating one never plans or executes a Mission.

## Data Model

Table: `operator_objectives`

| Column | Notes |
|---|---|
| `id` | UUID |
| `tenant_id` | TEXT NOT NULL |
| `scope` | `operator` \| `client` |
| `client_id` | NULL for operator; required for client |
| `title` | TEXT |
| `objective_text` | TEXT |
| `status` | `active` \| `paused` \| `completed` \| `cancelled` |
| `time_horizon` | nullable |
| `current_phase` | nullable |
| `context` | JSONB |
| `aliases` | TEXT[] for deterministic resolution |
| `created_at` / `updated_at` | timestamptz |

Migration: `migrations/2026-08-13-operator-objectives.sql` (seeds Public Max Launch).

## Implementation Plan

1. Migration + rollback + Public Max Launch seed
2. `services/operatorObjectives.js` — store, CRUD, resolve, detect
3. `packages/max/workspace/OperatorObjectiveContext.js` — pre-routing adapter
4. Wire into `WorkspaceEngine.ask()` before Paige / domain selection
5. Extend ContextEnvelope + SPEC-094 context pickers
6. Tests + docs

## Migration Strategy

Additive. Idempotent Public Max Launch seed for tenant `1`. Rollback drops the table. No Mission schema changes. Compatible with existing SessionStore (objectives do not live there).

## Testing

- `test/operatorObjectives.test.js` — service persistence, resolution, lifecycle, determinism, tenant isolation
- `packages/max/workspace/tests/operatorObjectives.test.js` — fresh-session recovery, status vs Mission, Paige delegation with recovered context, ambiguity, weak-language rejection

## Acceptance Criteria

- [x] Explicit operator strategic objectives persist outside SessionStore
- [x] Operator vs client scope supported; tenant isolation enforced
- [x] Active objectives retrieved before intent routing
- [x] Clear references resolve deterministically; ambiguous fail closed
- [x] ContextEnvelope carries objective context (carrier only)
- [x] Fresh Max conversations recover active objectives
- [x] Existing-objective status questions do not create Missions
- [x] Explicit Mission creation still works
- [x] Max launch campaign no longer collides with Anchor commercial campaign when objective is persisted
- [x] SPEC-094 receives recovered objective context; Paige uses SPEC-092/093 evidence
- [x] Max presents Paige recommendation inside objective context
- [x] No autonomous execution / phase advancement / phrase-specific routing patches
- [x] No provider/LLM session dependency
- [x] Tests pass; production migration + Public Max Launch seed included

## Future Work

Deferred per thin-slice: general memory, autonomous objective systems, campaign boards, vector search, phase engines, multi-agent orchestration.
