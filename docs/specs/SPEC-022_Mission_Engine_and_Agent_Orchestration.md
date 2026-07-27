# SPEC-022 — Mission Engine & Agent Orchestration

| Field | Value |
|---|---|
| **Status** | Approved |
| **Target Version** | v1.1.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Version** | v1.1.1 |
| **Depends on** | SPEC-002, SPEC-005, SPEC-006–009, SPEC-014, SPEC-015A, ADR-003, ADR-010 |
| **Note** | Product draft was labeled “SPEC-015”; renumbered to SPEC-022 because SPEC-015 is Market Intelligence Domain. v1.1.1 adds Mission-First UX addendum. |

## Objective

Enable Max to execute complex business objectives by orchestrating platform agents and services on behalf of the operator. Operators describe intent; Max determines execution. A first-time user should be able to type “Build Campaign 001 for Anchor Cleaning” and, without knowing Scout, Knowledge, or Reasoning, receive a fully prepared campaign ready for review.

The Mission Engine is not only an orchestration service; it is the **primary way operators interact with Pulseforge**. Mission-First UX supersedes the standalone Operations experience.

## Vision References

- `docs/vision/Mission.md`
- `docs/vision/Product_Constitution.md` (§ human approval, cognitive load)
- `docs/vision/Product_Experience.md`
- [ADR-001](../adr/ADR-001_Conversation_First.md) — conversation as the control surface
- [ADR-002](../adr/ADR-002_Explainable_AI.md) — explainable plans and rankings
- [ADR-003](../adr/ADR-003_Human_Approval.md) — no automatic outreach
- [ADR-010](../adr/ADR-010_Mission_Engine.md) — capability-driven orchestration
- [SPEC-002](SPEC-002_Max_Reasoning_Engine.md) · [SPEC-005](SPEC-005_Policy_Decision_Engine.md)
- [SPEC-006](SPEC-006_Command_Deck.md) · [SPEC-007](SPEC-007_Command_Deck_Composition_Engine.md) · [SPEC-008](SPEC-008_Command_Deck_UI.md)
- [SPEC-009](SPEC-009_Max_Intelligence_Workspace.md) · [SPEC-014](SPEC-014_Knowledge_Dual_Write.md)
- [SPEC-015A](SPEC-015A_Reasoning_Runtime_Decoupling.md)

## Problem

Today the operator knows there is a Scout agent, so they ask “Run Scout.” That leaks internal architecture. Customers should never need to know which agent performs a task.

Max is still primarily a conversational / intelligence surface (SPEC-009). It can recommend and explain; it cannot durable-plan and execute multi-step business objectives across capabilities.

## Principle

**Operators describe intent. Max determines execution.**

| Operator says | Max decides |
|---|---|
| Build Campaign 001. | Mission → Scout Discovery → Company Enrichment → Knowledge Dual Write → Reasoning → Ranking → Campaign Builder → Operator Review |

## Scope

- Mission types (v1): Prospect Discovery, Campaign Creation, Overflow Partner Discovery, Acquisition Search, Market Research, Competitor Intelligence
- Durable mission lifecycle: Requested → Planned → Executing → Waiting → Completed → Reviewed → Archived
- `MissionPlanner` — parse objective, identify capabilities, create plan, estimate duration & confidence
- `MissionExecutor` — invoke capabilities, recover from failures, preserve progress
- Capability Registry — map intents to plug-in capabilities (never hardcode agent names in Max)
- Mission context envelope (client, geography, vertical, objective, constraints, budget, timeline)
- Real progress UI (capability-backed stages, not fake loaders)
- Review mode — Approve / Edit / Reject / Run Again; never auto-launch outreach
- Campaign Creation deliverables: ranked prospects, mail merge fields, campaign record with prospects attached
- Durable audit trail + replayability of mission requests, plans, invocations, evidence, outcomes
- **Mission-First UX** (addendum v1.1.1):
  - Remove standalone Operations nav destination; fold into Command Deck Operations section
  - Live Mission Queue (persistent mission cards) beneath Highest Leverage Action
  - Mission Workspace (expand from card): objective, plan, progress, evidence, results, actions, audit
  - Navigation philosophy: users navigate to work, not modules

## Out of Scope

- Autonomous outreach send (email / SMS / call / social) — blocked by ADR-003 / Review Mode
- Forced rewrite of every legacy dashboard agent button in v1 (endpoints may remain; Operations *nav* is superseded)
- Deleting Company / Recommendation / Timeline detail routes where they already exist — prefer entry via missions, search, recommendations, or Max; do not invent new module hubs
- Market Intelligence Domain providers (SPEC-015) — consumed via capabilities when available
- Full natural-language planning via LLM as sole planner (deterministic capability matching is required for v1; LLM may assist parse only)
- Cross-tenant mission sharing
- Billing / budget enforcement beyond recording constraints on the mission envelope

## Dependencies

- Knowledge dual-write + outbox (SPEC-014)
- Reasoning Runtime + strategy packs (SPEC-015A)
- Policy / requireApproval (SPEC-005, ADR-003)
- Command Deck composer + UI (SPEC-006 / 007 / 008) — Operations section consumer
- Max Workspace presentation (SPEC-009) — Mission Workspace / Ask Max
- Existing producers: Scout (`leadgen.js`), enrichment, campaigns table foundation
- ADR-010 Mission Engine (capability registry + durable missions + Mission-First UX)

## Architecture

```text
Operator objective (NL or structured)
        ↓
  MissionPlanner
        ↓  execution plan + capability chain
  MissionExecutor
        ↓
  Capability Registry
   ├── Prospect Discovery  → Scout Capability
   ├── Enrichment          → Enrichment Capability
   ├── Knowledge Update    → Knowledge Capability
   ├── Reasoning / Ranking → Reasoning Capability
   ├── Campaign Builder    → Campaign Capability
   └── …future plug-ins
        ↓
  Durable Mission Store (lifecycle + audit)
        ↓
  Review Mode (operator gate)
        ↓
  Approved → downstream execution systems (out of auto-send)
```

### Planner (`MissionPlanner`)

Responsibilities:

- parse objective
- identify required capabilities
- create execution plan
- estimate duration
- estimate confidence

### Executor (`MissionExecutor`)

Responsibilities:

- invoke Scout (via Scout Capability — never by agent name in product copy)
- invoke enrichment
- update Knowledge
- invoke Reasoning
- invoke campaign generation
- recover from failures (pause, retry, no progress lost)

### Capability Registry

Max must never hardcode agent names. Capabilities are plug-ins:

| Capability | Example backing |
|---|---|
| Prospect Discovery | Scout |
| Knowledge Update | Dual-write / GraphSync |
| Reasoning | Reasoning Runtime |
| Campaign Builder | Campaign service |
| Market Research | SPEC-015 adapters (when ready) |

### Mission context

Every mission receives:

```json
{
  "client": "Anchor Cleaning",
  "location": "Manchester, NH",
  "objective": "Campaign 001",
  "targetCount": 50,
  "verticals": [
    "Property Management",
    "Law Firm",
    "CPA",
    "Medical"
  ]
}
```

Also: constraints, budget, timeline (optional fields on the same envelope).

### Mission types (v1)

| Type | Example objective |
|---|---|
| Prospect Discovery | Find the best commercial cleaning prospects in Manchester. |
| Campaign Creation | Build Campaign 001. |
| Overflow Partner Discovery | Find cleaning companies likely to have overflow work. |
| Acquisition Search | Find owners approaching retirement. |
| Market Research | Research the Manchester commercial cleaning market. |
| Competitor Intelligence | Monitor regional competitors. |

### Campaign Creation pipeline (canonical)

```text
Mission
  → Scout Discovery
  → Company Enrichment
  → Knowledge Dual Write
  → Reasoning
  → Ranking
  → Campaign Builder
  → Operator Review
```

### Progress UI

Real stages backed by capability completion — not fake loading indicators:

- Planning Mission
- Discovering Companies
- Enriching Prospects
- Ranking Opportunities
- Building Campaign
- Ready for Review

### Review Mode

Max never launches outreach automatically.

```text
Mission Complete
50 companies discovered
Operator Review Required
```

Actions: **Approve** · **Edit** · **Reject** · **Run Again**

### Failure recovery

If a capability fails (e.g. Scout):

- Mission pauses (`Waiting` or failed step marked recoverable)
- Retry supported
- No progress lost (completed steps remain durable)

### Audit trail

Every mission stores: request, execution plan, capabilities invoked, evidence generated, duration, operator actions, outcome. Replayable.

## Data Model

### Mission

```text
Mission {
  id, tenantId / clientId,
  type,                    // prospect_discovery | campaign_creation | …
  status,                  // requested | planned | executing | waiting | completed | reviewed | archived
  objectiveText,
  context,                 // JSON envelope
  plan,                    // ordered capability steps + estimates
  confidence, durationEstimateMs,
  progress,                // per-step status + timestamps
  deliverablesRef,         // campaign_id, ranked set ids, etc.
  review,                  // approve | edit | reject | run_again + actor + at
  createdAt, updatedAt, completedAt
}
```

### Mission audit event

```text
MissionAuditEvent {
  id, missionId, at,
  kind,                    // request | plan | step_start | step_ok | step_fail | retry | review | archive
  capabilityId?,
  payload, evidenceRefs?
}
```

### Campaign Creation deliverables

Ranked Prospects (per row):

- Priority score
- Decision maker
- Phone, Email, Website, Address
- Estimated contract value
- Reason selected
- Confidence

Mail Merge:

- Personalization sentence
- Opening hook
- Company notes

Campaign:

- Named campaign (e.g. Campaign 001) with all prospects attached

Reuse / extend existing `campaigns` foundation where present; do not invent a parallel campaign store.

## Implementation Plan

1. **ADR-010 + package skeleton** — `@pulseforge/mission-engine` (or `packages/mission`): types, lifecycle state machine, Capability Registry interface
2. **Durable store** — Postgres missions + audit tables; interruption-safe status
3. **MissionPlanner** — objective → mission type + capability plan for the six v1 types
4. **Capability adapters (v1)** — Scout, Enrichment, Knowledge dual-write, Reasoning/Ranking, Campaign Builder
5. **MissionExecutor** — step runner, pause/retry, progress events
6. **API + Max Workspace** — create mission from objective; stream/poll progress; review actions
7. **Campaign Creation path** — end-to-end for Anchor “Build Campaign 001”
8. **Tests + docs** — unit lifecycle, capability registry, failure/resume, acceptance harness

## Migration Strategy

- Additive tables only (`missions`, `mission_audit_events`; campaign attachment columns if needed)
- Feature flag `MISSION_ENGINE` (default off) until dual-write is healthy in the target client
- Legacy `POST /api/run/:agent` and cron remain; Mission Engine is an additional control plane
- Rollback: disable flag; leave mission rows intact for forensic read

## Testing

- Unit: planner plans for each mission type; executor transitions; registry resolution without agent-name leakage in operator-facing strings
- Integration: Scout capability → knowledge update → reasoning rank → campaign draft (harness / fixtures)
- Failure: kill mid-Scout → resume → no duplicate completed steps
- Policy: completed mission never triggers send without Approve
- Manual smoke: “Build Campaign 001 for Anchor Cleaning” → review UI with deliverables

## Acceptance Criteria

- [ ] Max accepts business objectives (NL or structured)
- [ ] Mission planner creates execution plans
- [ ] Mission executor invokes capabilities (not raw agent names in product API)
- [ ] Scout runs without operator naming Scout
- [ ] Knowledge updates automatically on discovery/enrichment
- [ ] Reasoning ranks results
- [ ] Campaigns generated automatically (draft / review-gated)
- [ ] Operator reviews before execution / outreach
- [ ] Mission survives interruption (durable state + retry)
- [ ] Complete audit trail recorded and replayable
- [ ] Success metric: first-time user path for “Build Campaign 001 for Anchor Cleaning” works without agent vocabulary

## Future Work

Because missions are capability-driven, new abilities require no redesign. Examples:

- “Find 25 restaurants opening this month.”
- “Research hospitals hiring facilities managers.”
- “Build a Q4 outreach campaign.”
- “Identify companies with acquisition signals.”

Deferred:

- LLM-native open-ended planning beyond the six v1 types
- Auto-approve narrow internal-only missions (would need ADR amendment)
- Multi-mission portfolios / scheduled recurring missions
- Customer-facing mission templates marketplace
- Merging Command Deck HLA into Mission create shortcuts
