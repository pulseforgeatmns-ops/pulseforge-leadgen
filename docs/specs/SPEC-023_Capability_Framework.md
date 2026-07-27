# SPEC-023 — Capability Framework

| Field | Value |
|---|---|
| **Status** | Approved |
| **Target Version** | v1.2.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Version** | v1.2.0 |
| **Depends on** | ADR-010, ADR-011, SPEC-014, SPEC-015A |
| **Consumed by** | SPEC-022 |
| **Blocks** | MissionExecutor capability adapters; Mission Workspace progress events |

## Objective

Define a standardized contract for every executable capability within Pulseforge so Max can orchestrate them uniformly.

The Mission Engine never knows about Scout. It never knows about Knowledge. It never knows about Reasoning. It only knows **capabilities**.

Success: `MissionPlanner` discovers work from a registry; `MissionExecutor` runs only through that registry; every capability implements one contract; progress is live in Mission Workspace; results persist and replay; capability tests run without spinning up a full mission.

## Vision References

- `docs/vision/Mission.md`
- `docs/vision/Product_Constitution.md`
- [ADR-010](../adr/ADR-010_Mission_Engine.md) — Mission Engine orchestration
- [ADR-011](../adr/ADR-011_Capability_Framework.md) — capabilities as the stable API
- [ADR-003](../adr/ADR-003_Human_Approval.md) — review before outreach
- [SPEC-022](SPEC-022_Mission_Engine_and_Agent_Orchestration.md) — Mission Engine & Mission-First UX
- [SPEC-014](SPEC-014_Knowledge_Dual_Write.md) · [SPEC-015A](SPEC-015A_Reasoning_Runtime_Decoupling.md)

## Problem

SPEC-022 introduces missions and a Capability Registry in principle, but does not lock the executable contract. Without a uniform capability API:

- MissionExecutor risks agent-specific branching (`if scout…`, `if emmett…`)
- Planners hardcode routing instead of discovering matches
- Progress, failure, and replay semantics diverge per adapter
- Replacing Scout (or any agent) forces changes above the capability layer

## Principle

**Every executable action in Pulseforge is a capability.**

Capabilities are:

| Property | Meaning |
|---|---|
| Discoverable | Registry lists and matches by objective / outcomes |
| Composable | Outputs of one step feed inputs of the next |
| Durable | Invocations and results persist with the mission |
| Replayable | Same plan + inputs can re-run without inventing side channels |
| Observable | Queued → Running → Progress → Completed / Failed / Retrying / Cancelled |

**ADR-011 rule:** Capabilities are the stable API of Pulseforge. Agents are implementation details.

If Scout is replaced entirely, nothing above the capability layer changes. Missions still request Prospect Discovery; the registry routes to a different implementation.

## Scope

- `Capability` interface (contract every executable implements)
- Capability categories taxonomy
- `CapabilityRegistry` — register, discover, version, validate schemas
- `CapabilityContext` / `CapabilityResult` / `CapabilityEstimate` envelopes
- Capability progress events for Mission Workspace
- Failure metadata: retryable, timeout, rollback, idempotent
- Capability discovery flow (objective → outcomes → registry → graph)
- Initial built-in capabilities (v1 ship set)
- Capability unit tests independent of full missions
- Package skeleton under mission / capabilities (alongside SPEC-022 mission-engine)

## Out of Scope

- Implementing every category listed below (only the five built-ins ship in v1)
- Agent module rewrites that do not go through a capability adapter
- LLM-only open-ended capability invention (registry matching is deterministic for v1)
- Autonomous outreach send (still ADR-003 / Review Mode)
- Cross-tenant capability sharing
- Marketplace / third-party capability packaging (future)

## Dependencies

- ADR-010 Mission Engine decision (missions invoke capabilities only)
- ADR-011 Capability Framework decision (this ADR)
- SPEC-014 Knowledge dual-write (Knowledge Update backing)
- SPEC-015A Reasoning Runtime (Reasoning / Opportunity Ranking backing)
- Existing producers: Scout (`leadgen.js`), enrichment, campaigns foundation
- Consumed by SPEC-022 MissionPlanner / MissionExecutor / Mission Workspace

## Architecture

```text
Mission
  ↓
Planner
  ↓
Execution Plan
  ↓
Capability Registry
  ↓
Capability Runner
  ↓
Capability Results
  ↓
Mission Review
```

### Design rule

- `MissionPlanner` requests capabilities from the registry. **Never imports concrete implementations.**
- `MissionExecutor` executes **only** through the registry / runner. **No agent-specific branching.**
- Product copy and mission APIs use capability ids/names, not agent module names.

### Capability contract

Every capability implements:

```ts
interface Capability {
  id: string
  name: string
  description: string
  category: CapabilityCategory
  inputSchema: JsonSchema
  outputSchema: JsonSchema
  canRun(context: CapabilityContext): boolean
  estimate(context: CapabilityContext): CapabilityEstimate
  execute(context: CapabilityContext): Promise<CapabilityResult>
  rollback?(context: CapabilityContext): Promise<void>
}
```

No special cases. Optional `rollback` only when the capability can undo its durable side effects.

### Categories

| Category | Capabilities (catalog; not all ship in v1) |
|---|---|
| **Discovery** | Prospect Discovery, Competitor Discovery, Acquisition Discovery |
| **Enrichment** | Company Enrichment, Contact Enrichment, Technology Detection |
| **Intelligence** | Knowledge Update, Reasoning, Brief Generation, Outcome Recording |
| **Campaign** | Campaign Builder, Priority Ranking, Mail Merge Preparation |
| **Monitoring** | Competitor Watch, Overflow Monitor, Hiring Watch |
| **Reporting** | Weekly Brief, Mission Summary, Outcome Analysis |

### Registry

`CapabilityRegistry` responsibilities:

- register capabilities
- discover capabilities (match required outcomes / objective tags)
- version capabilities
- validate input/output schemas against context and results

`MissionPlanner` never imports concrete implementations — only registry queries.

### Capability context

Every capability receives:

```ts
interface CapabilityContext {
  missionId: string
  tenantId: string
  clientId: number | string
  objective: string | object
  constraints: object
  inputs: object
  knowledge: object
}
```

Capabilities never query global / ambient state directly. Tenant and client scope come from context. Knowledge snapshots are passed in (or via injected context stores), not ad-hoc global pools inside capability code.

### Capability estimate

```ts
interface CapabilityEstimate {
  durationMs?: number
  confidence?: number
  costHint?: string
  notes?: string[]
}
```

### Capability result

```ts
interface CapabilityResult {
  status: 'completed' | 'failed' | 'cancelled' | 'partial'
  outputs: object
  evidence: object[]
  artifacts: object[]
  duration: number
  warnings: string[]
  errors: object[]
  nextRecommendations: object[]
}
```

Everything is structured. Durable results become Knowledge (via Knowledge Update capability / dual-write path) — not unstructured log dumps.

### Composition

Capabilities may depend on prior outputs. Example graph:

```text
Discover Prospects
  ↓
Company Enrichment
  ↓
Knowledge Update
  ↓
Reasoning
  ↓
Campaign Builder
```

`MissionPlanner` assembles the graph. `MissionExecutor` executes it via the registry runner.

### Progress reporting

Capabilities emit events:

| Event | Use |
|---|---|
| Queued | Step accepted into plan / queue |
| Running | Execution started |
| Progress | Partial completion (counts, %). Feeds mission cards |
| Completed | Step finished successfully |
| Failed | Step failed (see failure handling) |
| Retrying | Executor-initiated retry |
| Cancelled | Operator or system cancel |

Mission cards and Mission Workspace update live from these events (SPEC-022 Mission-First UX).

### Failure handling

Each capability declares (metadata alongside the contract):

| Flag | Meaning |
|---|---|
| `retryable` | Executor may retry |
| `timeout` | Soft/hard timeout for the step |
| `rollback` | `rollback?` supported |
| `idempotent` | Safe to re-execute without duplicate side effects |

`MissionExecutor` decides recovery (pause mission, retry, rollback, fail step). Capabilities do not own mission lifecycle.

### Capability discovery

```text
Objective
  ↓
Required outcomes
  ↓
Registry
  ↓
Matching capabilities
  ↓
Execution graph
```

No hardcoded routing in Max or the planner for which agent module to call.

## Initial built-in capabilities

Ship with:

| Capability | Category | Example backing (implementation detail) |
|---|---|---|
| Prospect Discovery | Discovery | Scout (`leadgen.js`) adapter |
| Company Enrichment | Enrichment | Enrichment / Prospeo / Hunter adapters |
| Knowledge Update | Intelligence | Dual-write / GraphSync (SPEC-014) |
| Opportunity Ranking | Campaign / Intelligence | Reasoning Runtime ranking (SPEC-015A) |
| Campaign Builder | Campaign | Campaign service / draft campaign record |

Everything else in the category catalog comes later as additional registry entries — no Mission Engine redesign.

## Data Model

### Capability registration (in-process + durable catalog)

```text
CapabilityDescriptor {
  id, version, name, description, category,
  inputSchema, outputSchema,
  retryable, timeoutMs, supportsRollback, idempotent,
  outcomeTags[]          // for planner discovery matching
}
```

### Capability invocation (durable, mission-scoped)

```text
CapabilityInvocation {
  id, missionId, capabilityId, capabilityVersion,
  status,                // queued | running | completed | failed | retrying | cancelled
  contextSnapshot,       // CapabilityContext (redact secrets)
  estimate,
  result,                // CapabilityResult when terminal
  startedAt, completedAt, durationMs,
  attempt, maxAttempts
}
```

### Capability progress event

```text
CapabilityProgressEvent {
  id, missionId, invocationId, at,
  kind,                  // queued | running | progress | completed | failed | retrying | cancelled
  payload                // percent, counts, message, error refs
}
```

Reuse / extend SPEC-022 `MissionAuditEvent` where practical; do not invent a parallel audit store if one event stream can carry both mission and capability kinds.

## Implementation Plan

1. **ADR-011 + types** — `Capability`, `CapabilityContext`, `CapabilityResult`, `CapabilityEstimate`, categories, progress event kinds
2. **CapabilityRegistry** — register / get / list / discover / validate schemas / version pin
3. **CapabilityRunner** — execute via registry only; emit progress events; apply retry/timeout/idempotency policy from metadata
4. **Built-in adapters (v1)** — Prospect Discovery, Company Enrichment, Knowledge Update, Opportunity Ranking, Campaign Builder
5. **Wire SPEC-022** — MissionPlanner discovery against registry; MissionExecutor uses runner only
6. **Persistence** — invocations + progress events (or unified mission audit)
7. **Mission Workspace** — subscribe to progress events for live cards
8. **Tests** — each capability unit-tested with fixture context; registry discovery tests; executor-without-agent-branching tests

## Migration Strategy

- Additive only: capability packages + tables/columns for invocations/events
- Feature flag inherits `MISSION_ENGINE` (SPEC-022); capabilities are not separately operator-facing
- Legacy `POST /api/run/:agent` remains; agents are not deleted — they become optional backings behind adapters
- Rollback: disable mission flag; capability rows remain for forensic read

## Testing

- Unit: each built-in capability `canRun` / `estimate` / `execute` with fixture `CapabilityContext` (no full mission)
- Unit: registry register, version, schema validation, discover-by-outcome
- Unit: runner emits progress events; respects retryable / timeout / idempotent
- Integration: composition graph Discover → Enrich → Knowledge → Rank → Campaign Builder
- Negative: MissionExecutor module has no imports of `leadgen.js` / agent filenames (lint or architecture test)
- UI smoke: Mission Workspace progress mirrors capability events

## Acceptance Criteria

- [ ] Registry operational (register, discover, version, validate)
- [ ] All shipped capabilities implement the contract
- [ ] MissionExecutor executes only through the registry / runner
- [ ] No agent-specific branching in planner or executor
- [ ] Progress visible in Mission Workspace (capability events)
- [ ] Results persisted (invocation + result envelopes)
- [ ] Replay supported (re-run plan from durable context / invocations)
- [ ] Capability tests independent of missions
- [ ] Built-ins ship: Prospect Discovery, Company Enrichment, Knowledge Update, Opportunity Ranking, Campaign Builder

## Future Work

- Remaining category catalog entries (Competitor Discovery, Monitoring watches, Reporting, etc.)
- Contact Enrichment / Technology Detection as separate capabilities
- Third-party / tenant-scoped capability packs
- Stricter architecture tests (dependency-cruiser) forbidding agent imports above the adapter layer
- Capability-level Evidence Laboratory experiments (SPEC-019)
