# SPEC-039 — Active Mission Resolver

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.3.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Version** | v1.0.0 |
| **Depends on** | SPEC-022 (Mission Engine), SPEC-023 (Capability Framework), SPEC-032 (Mission Memory), SPEC-037 (Operator Inbox), ADR-001, ADR-010, ADR-019, ADR-025 |
| **Consumed by** | Max Workspace Ask, Max Chat, Mission API (when not explicit new), Command Deck Mission Workspace |

## Objective

Resolve whether an incoming operator message should **resume an existing Mission** or **create a new Mission**.

The Active Mission Resolver is the **first routing layer** for all operator interactions. IntentRouter runs only when the resolver decides a new Mission is required ([ADR-025](../adr/ADR-025_Active_Missions_Take_Precedence.md)).

Success looks like: “Investigate why Campaign Review failed…” → attaches to the bound active Mission, surfaces audit / failure evidence — **no new Mission**. “Build Campaign 002” / “New Mission” → IntentRouter → Mission Engine create path.

## Vision References

- `docs/vision/Mission.md`
- `docs/vision/Product_Constitution.md`
- `docs/vision/Product_Experience.md`
- [ADR-025](../adr/ADR-025_Active_Missions_Take_Precedence.md) — active Missions take precedence
- [ADR-019](../adr/ADR-019_Missions_Are_Conversations.md) — Missions are conversations
- [ADR-001](../adr/ADR-001_Conversation_First.md) — conversation as the control surface
- [ADR-010](../adr/ADR-010_Mission_Engine.md) — Mission Engine
- [SPEC-032](SPEC-032_Mission_Memory.md) — Mission Memory (messages, revisions, decisions)
- [SPEC-022](SPEC-022_Mission_Engine_and_Agent_Orchestration.md) — Mission Planner / Executor thin slice
- [SPEC-037](SPEC-037_Operator_Inbox.md) — Operator Inbox coordination
- [SPEC-009](SPEC-009_Max_Intelligence_Workspace.md) — Max Ask / workspace presentation

## Problem

Today Max Ask routes:

```text
Operator Prompt → IntentRouter → MissionEngine.createFromObjective (always new)
```

There is no ActiveMissionStore, no session→Mission binding, and no resume/attach path. IntentRouter is a stateless regex classifier. Follow-ups that mention stage names (e.g. “Campaign Review”) create sibling Missions. Diagnostics never read the prior Mission’s audit trail. SPEC-032 Mission Memory cannot function without a resolver that runs **before** IntentRouter.

## Guiding Principle

**Required routing order:**

```text
Operator Prompt
       ↓
 Active Mission Resolver
       ↓
 Resume Active Mission
    OR
 Create New Mission
       ↓
 IntentRouter (new missions only)
       ↓
 Mission Engine
```

## Scope

### Responsibilities

- Resolve active Mission for the operator session
- Resume active Mission
- Attach follow-up messages
- Detect Mission modifications
- Detect Mission diagnostics
- Detect explicit new Mission requests
- Bind operator session to Mission
- Emit Mission events and resolution audit

### Active Mission Rules

A Mission is **active** when:

- Status is not terminal
- Status is not archived
- Status is not cancelled

**Terminal states:** Completed · Failed · Archived · Cancelled

Only **one** active Mission may exist per operator session.

Non-terminal statuses that keep a Mission active include (align with SPEC-022): `requested`, `planning`, `executing`, `waiting`, `review_required`, `reviewed` (until explicitly closed / archived). Exact set is owned by Mission lifecycle types; resolver must treat any non-terminal, non-archived, non-cancelled Mission as eligible for binding.

### Session Binding

Maintain:

| Field | Purpose |
|---|---|
| Session ID | Max workspace / chat session |
| Operator ID | Acting user |
| Tenant ID | Tenancy scope |
| Active Mission ID | Bound Mission |

Removing the active Mission (terminal state, explicit clear, or “New Mission” that supersedes) clears the binding.

### Message Classification

Incoming messages classify as:

| Class | Examples |
|---|---|
| **Resume** | Continue · Run again · Show progress · Open review · What failed? · Show evidence |
| **Modify** | Use Manchester · Increase target count · Remove restaurants · Change discovery profile |
| **Diagnose** | Why did this fail? · Explain the ranking · Show audit log · Investigate |
| **New Mission** | Build Campaign 002 · Generate Proposal · Research ABC Company · **New Mission** · **Start Over** · **Create Another Campaign** |

### Resolution Order

1. **Explicit New Mission** → clear/supersede binding → IntentRouter → create
2. **Active Mission exists** → classify Resume / Modify / Diagnose → attach (never IntentRouter)
3. **Otherwise** → IntentRouter → create new Mission → bind session

### Mission Attachment

When resuming / modifying / diagnosing:

- Append operator message (Mission Memory)
- Create Mission Event
- Update Mission Memory
- Preserve current revision (modifications append a new revision per SPEC-032; diagnostics do not mutate revision)
- **No new Mission created**

### Diagnostics

Diagnostics execute against the **active Mission**.

- Never routed through IntentRouter
- Examples: Why did this fail? · Show audit · Explain ranking · Show evidence
- Surface plan steps, audit events, errors, evidence, deliverables from the bound Mission

### Modifications

- Update Mission state (constraints, profile, playbook pins, etc.)
- Affected capabilities become **stale**
- Only stale capabilities rerun (SPEC-032 revision + executor)
- Emit `MissionModified`

### Audit

Record for every resolution:

- Session · Mission · Message · Classification · Timestamp · Resolution path

### API

```ts
resolveActiveMission(sessionId): Promise<ActiveMissionResolution>
attachMessage(missionId, message): Promise<void>
resumeMission(missionId): Promise<Mission>
clearActiveMission(sessionId): Promise<void>
startNewMission(input): Promise<Mission>  // IntentRouter + createFromObjective + bind
```

### Mission Events

Generate:

- `MissionResumed`
- `MissionModified`
- `MissionDiagnosed`
- `MissionCompleted`
- `MissionClosed`

## Out of Scope

- Full semantic NLU for objective-change beyond v1 heuristics + clarification (deep LLM classification deferred; ambiguity → ask)
- Cross-session “steal” of another operator’s active Mission without explicit open
- Auto-chaining all campaign lifecycle capabilities into one Mission type in this spec (may be a later Mission Planner change; resolver must still attach follow-ups regardless)
- Replacing IntentRouter keyword tables for cold-start classification
- Operator Inbox item processing (SPEC-037) — inbox remains coordination; resolver owns conversational Mission continuity
- Implementing full Mission Memory schema (owned by SPEC-032); this spec consumes Memory attach/update APIs

## Dependencies

- SPEC-022 Mission Engine (store, executor, review, statuses)
- SPEC-032 Mission Memory (messages, revisions, decisions — Proposed)
- SPEC-037 Operator Inbox (session/operator context; do not conflate inbox open with Mission create)
- ADR-025 Active Missions Take Precedence
- ADR-019 Missions Are Conversations
- Max Workspace Ask / Max Chat entry points (`WorkspaceEngine.ask`, `routes/maxChat.js`)
- Feature flag recommended: fall back to SPEC-022 create-on-intent when resolver disabled

## Architecture

```text
Operator message
       ↓
 resolveActiveMission(sessionId)
       ↓
 Explicit New Mission? ──yes──→ clearActiveMission → IntentRouter → startNewMission → bind
       │ no
       ↓
 Active Mission bound?
       │ no ──→ IntentRouter → startNewMission → bind
       │ yes
       ↓
 Classify: Resume | Modify | Diagnose | (ambiguous → clarify)
       ↓
 attachMessage + Mission Event
       ↓
 Resume → resumeMission / surface progress
 Modify → MissionMemory revision + stale-capability rerun
 Diagnose → audit / evidence / explain against missionId (no IntentRouter)
```

### Integration points

| Component | Change |
|---|---|
| `WorkspaceEngine.ask` | Call resolver **before** `routeIntent` |
| `routes/maxChat.js` | Same |
| `MissionEngine.createFromObjective` | Used only via `startNewMission` / explicit API |
| IntentRouter | Unchanged for cold-start; **not** invoked on Resume/Modify/Diagnose |
| MissionStore | Add or compose ActiveMission binding (session → missionId); list/filter non-terminal |
| Mission Memory (SPEC-032) | `attachMessage`, revision on Modify |

## Data Model

### Session binding (v1)

| Store | Fields |
|---|---|
| `mission_session_bindings` (or session metadata) | `session_id`, `operator_id`, `tenant_id`, `client_id`, `active_mission_id`, `updated_at` |
| Unique | One active binding per `session_id` |

Clear `active_mission_id` when Mission becomes terminal or on `clearActiveMission` / explicit New Mission.

### Resolution audit

| Store | Fields |
|---|---|
| `mission_resolution_events` (or Mission audit kind) | `id`, `session_id`, `mission_id`, `message`, `classification`, `resolution_path`, `at`, `operator_id` |

Tenancy: scoped by existing mission `client_id` / tenant rules from SPEC-022.

## Implementation Plan

1. **ADR + indexes** — ADR-025 Accepted; this spec Proposed; README / DECISIONS / CURRENT_STATE linked.
2. **Session binding store** — persist session → active Mission; clear on terminal.
3. **ActiveMissionResolver** — `resolveActiveMission`, classification heuristics (Resume / Modify / Diagnose / New), clarification on ambiguity.
4. **Wire ask paths** — `WorkspaceEngine.ask` and `maxChat` call resolver before IntentRouter.
5. **Attach + events** — `attachMessage`, MissionResumed / Modified / Diagnosed; integrate SPEC-032 Memory when available (thin slice may append to mission audit + session until Memory tables land).
6. **Modify path** — mark stale capabilities; rerun via MissionExecutor against current revision.
7. **Diagnose path** — read audit / plan / deliverables / evidence; compose Max response; never `createFromObjective`.
8. **Tests** — precedence before IntentRouter; no duplicate Mission on follow-up; diagnostics attach; New Mission escape hatch; session binding clear on terminal.
9. **Flag** — `ACTIVE_MISSION_RESOLVER=1` (or equivalent); off = SPEC-022 behavior.

## Migration Strategy

- Additive binding + resolution audit tables/columns.
- Existing open Missions are not auto-bound; next ask with no binding uses IntentRouter (create or, if heuristics find a single non-terminal Mission for tenant/operator, optional soft-bind — **v1: do not auto-bind historical Missions**; bind only on create or explicit open).
- Rollback: feature-flag resolver off; IntentRouter-first path restored; binding rows remain inert.
- Compatibility: Mission API `POST /api/v1/missions` remains explicit create; Ask path gains resolver.

## Testing

- Unit: classification (Resume / Modify / Diagnose / New); explicit phrases; ambiguity → clarify
- Unit: active rules (terminal vs non-terminal); one binding per session
- Integration: follow-up never creates duplicate Mission; diagnostics do not call IntentRouter
- Integration: “New Mission” / “Build Campaign 002” creates and rebinds
- Integration: Modify marks stale steps and reruns only those
- Regression: IntentRouter cold-start still creates `campaign_creation` for “Build Campaign 001…”
- Flag-off: create-on-intent unchanged

## Acceptance Criteria

- [x] Active Mission checked **before** IntentRouter on all Ask paths
- [x] One active Mission per operator session
- [x] Follow-up prompts never create duplicate Missions
- [x] Diagnostics attach to the current Mission (never IntentRouter)
- [x] Modifications update Mission state and rerun only affected capabilities
- [x] Session binding maintained and cleared on terminal / explicit clear / New Mission
- [x] Full resolution audit history preserved
- [x] Explicit New Mission phrases and new-objective heuristics still create via IntentRouter

## Future Work

- Soft-bind single non-terminal Mission when session has no binding (operator returns after refresh)
- Cross-device session merge for the same operator
- Deeper objective-change detection (LLM-assisted) with mandatory clarification
- Durable Postgres binding store (v1 uses in-memory bindings on MissionEngine)
- Unified campaign lifecycle Mission type (build → review → mail → execute) so resolver resumes stages without sibling types — complementary to this spec, not a substitute
- Full Mission Memory tables (SPEC-032) — v1 attaches via mission audit `message` / `resolution` kinds