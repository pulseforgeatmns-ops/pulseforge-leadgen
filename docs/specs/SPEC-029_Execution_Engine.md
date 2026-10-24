# SPEC-029 — Execution Engine

| Field | Value |
|---|---|
| **Status** | Proposed |
| **Target Version** | v1.3.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Version** | v0.1.0 |
| **Depends on** | SPEC-022, SPEC-023, SPEC-024, SPEC-030 (Company Intelligence packages; ADR-017 — ship intelligence before execution), SPEC-026, SPEC-027B, SPEC-028, SPEC-033, SPEC-034 (Campaign Review / ADR-021 — approved revision required), Campaign Builder (approved campaign artifact), ADR-003, ADR-010, ADR-011, ADR-015, ADR-016, ADR-017, ADR-021 |
| **Consumed by** | Mission Engine (outcome follow-ups), Knowledge dual-write, Learning Loop (SPEC-021), Command Deck Operator Dashboard |

## Objective

The **Execution Engine** transforms approved campaigns into completed work.

It is responsible for **doing, not deciding**.

All strategic decisions have already been made by:

- Discovery Profile ([SPEC-024](SPEC-024_Prospect_Discovery_Capability.md))
- Client Playbook ([SPEC-028](SPEC-028_Client_Playbook_Capability.md))
- Opportunity Ranking ([SPEC-026](SPEC-026_Opportunity_Ranking_Capability.md))
- Campaign Builder
- Proposal Generator when proposals are in scope ([SPEC-027B](SPEC-027B_Proposal_Generator_Capability.md))

Execution simply carries them out reliably, durably, and fail-closed ([ADR-016](../adr/ADR-016_Execution_Does_Not_Decide.md)).

## Vision References

- `docs/vision/Mission.md`
- `docs/vision/Product_Constitution.md` (§ human approval, DNC, cognitive load)
- `docs/vision/Intelligence_Architecture.md` (§ Execution)
- [ADR-016](../adr/ADR-016_Execution_Does_Not_Decide.md) — execution does not decide
- [ADR-015](../adr/ADR-015_Strategy_Lives_in_the_Playbook.md) — retry / channel / constraint rules from Playbook
- [ADR-003](../adr/ADR-003_Human_Approval.md) — never send unapproved outreach
- [ADR-010](../adr/ADR-010_Mission_Engine.md) — Mission Engine orchestrates capabilities
- [ADR-011](../adr/ADR-011_Capability_Framework.md) — Execution registers as a capability
- [ADR-002](../adr/ADR-002_Explainable_AI.md) — every touch is auditable
- [ADR-008](../adr/ADR-008_Outcome_Intelligence.md) — outcomes feed evaluation, not strategy invention
- [SPEC-022](SPEC-022_Mission_Engine_and_Agent_Orchestration.md)
- [SPEC-023](SPEC-023_Capability_Framework.md)
- [SPEC-028](SPEC-028_Client_Playbook_Capability.md)

## Problem

Today the Mission Engine can discover, rank, draft campaigns, and generate proposals — then stop at operator review. There is no durable system that:

1. Launches **approved** campaigns into scheduled touchpoints
2. Orchestrates multi-channel tasks (mail, email, phone, LinkedIn, manual)
3. Pauses safely when a human must act
4. Records evidence for every completed touch
5. Applies Client Playbook retry / schedule rules (instead of hardcoded agent timing)
6. Triggers the next Mission from a structured outcome

Without an Execution Engine, approved strategy dies in review mode, or legacy agents re-invent when/how to send — violating ADR-015 and ADR-016.

## Scope

- Campaign launch from **Approved** campaign artifacts only
- Execution Plan generation (touch queue from Playbook sequence × approved prospects)
- Task orchestration for: Direct Mail, Email, Phone, LinkedIn, Manual Tasks
- Durable state machine for campaigns and touch tasks
- Human-in-the-loop pause / resume
- Evidence capture per completed touch → Knowledge
- Structured outcome handling → optional follow-up Missions
- Retry / schedule enforcement from Client Playbook (no hardcoded retry logic)
- Fail-closed safety gates (approval, DNC, playbook constraints, contact data presence)
- Operator Dashboard live status surface (Command Deck Operations / Mission Workspace)
- Execution Engine as a Mission capability (`execution_engine`) invoked after campaign approval

## Out of Scope

- Creating strategy (channels, sequence, offers, ICP, ranking) — owned upstream ([ADR-016](../adr/ADR-016_Execution_Does_Not_Decide.md))
- Live Campaign Builder rewrite (consumes approved campaign outputs; live builder may land in parallel)
- Autonomously inventing contact data when Company Intelligence (SPEC-030) is missing contacts
- Replacing legacy `/api/run/:agent` endpoints in v1 (agents remain channel adapters behind capabilities)
- Full Learning Loop auto-mutation of Playbooks (advisory only; operator approval required)
- Cross-tenant execution sharing
- Billing / rate-limit productization beyond recording playbook constraints

## Dependencies

| Dependency | Role |
|---|---|
| SPEC-022 Mission Engine | Approval gate; follow-up mission spawn; durable mission lifecycle |
| SPEC-023 Capability Framework | `execution_engine` capability contract |
| SPEC-024–026 · SPEC-030 | Provenance of who/why targets were chosen; intelligence packages for contacts/context (pinned on campaign) |
| SPEC-027B | Proposal approval as a manual / review gate when required |
| SPEC-028 Client Playbook | Channels, sequence timing, constraints, retry rules |
| ADR-003 | No unapproved outreach |
| Knowledge dual-write (SPEC-014) | Evidence / touchpoints land in Knowledge |
| Channel adapters | Brevo, Twilio/Bland, mail packet ops, LinkedIn draft queue — behind capabilities, not named in product copy |

## Architecture

```text
Campaign Builder
      ↓
Operator Review / Approve
      ↓
Execution Plan          ← Playbook sequence · schedules · constraints
      ↓
Execution Engine        ← does; does not decide (ADR-016)
      ↓
Knowledge Update
      ↓
Learning Loop (observe) · Outcome → next Mission (when appropriate)
```

### Design principles

1. **Do, don’t decide** — strategy is pinned on the approved campaign + Playbook version.
2. **Fail-closed** — missing approval, missing contact, DNC, or playbook violation → block, never invent around it.
3. **Durable transitions** — every state change is persisted and auditable.
4. **Human-safe** — tasks that require operators enter `Waiting` and resume only on explicit completion.
5. **Playbook-governed timing** — retries, windows, max attempts come from the Playbook, not code constants.
6. **Evidence-first** — a completed touch without timestamp + result + evidence is incomplete.

### Responsibilities

#### Campaign Launch

- Start only **Approved** campaigns
- Create an Execution Plan (one plan per campaign run)
- Queue touchpoints from Playbook `outreachSequence` × approved prospects
- Respect schedules (day offsets, business hours, weekend skip when playbook says so)

#### Task Orchestration

Each touch becomes an executable task. Supported channels (v1):

| Channel | Typical backing |
|---|---|
| Direct Mail | Manual packet assembly + ship confirmation — consumes Ready-to-Print packages from [SPEC-033](SPEC-033_Mail_Package_Generator.md) |
| Email | Emmett / Brevo adapter (post-approval) |
| Phone | Operator call task or Cal/Bland when configured |
| LinkedIn | Draft → approval queue (Link / publish pipeline) |
| Manual Tasks | Operator checklist items (proposal send, walkthrough prep, etc.) |

#### State Machine

Campaign / execution run:

```text
Draft
  ↓
Approved
  ↓
Queued
  ↓
Executing
  ↓
Waiting          ← human or prospect gate
  ↓
Completed
  ↓
Archived
```

Touch tasks use the same vocabulary at task grain (`Queued` → `Executing` → `Waiting` → `Completed`, with recoverable `Failed` / `Skipped` leaves). Every transition is durable and auditable.

#### Human-in-the-Loop

Execution pauses (`Waiting`) whenever human action is required. Examples:

- Mail packet must be assembled
- Operator must place a phone call
- Proposal requires approval before send

The engine resumes automatically once the operator marks the task complete (or supplies the required artifact). No silent skip of required reviews.

#### Evidence Capture

Every completed touch records:

| Field | Required |
|---|---|
| Timestamp | Yes |
| Result | Yes |
| Evidence | Yes (refs / artifacts) |
| Operator notes | Optional |
| Artifacts | When produced (email id, letter PDF, call log, etc.) |

Everything flows into Knowledge via the Knowledge Update path.

#### Outcome Handling

Supported outcomes (campaign / prospect grain):

- No response
- Positive response
- Negative response
- Walkthrough scheduled
- Proposal requested
- Closed Won
- Closed Lost

Each outcome may trigger the next Mission when MissionPlanner rules match (e.g. Proposal requested → `proposal_generation`). Execution records the outcome; it does not invent the follow-up strategy.

#### Retry Rules

Execution respects the Client Playbook. Examples (illustrative — values live on the Playbook, not in engine code):

- Retry after 5 days
- Maximum 3 attempts
- Skip weekends
- Business hours only

**No hardcoded retry logic** in the Execution Engine.

#### Safety

Execution is fail-closed. Never:

- Send unapproved outreach
- Skip required reviews
- Execute outside client / Playbook constraints
- Invent contact data

DNC remains absolute ([Product Constitution](../vision/Product_Constitution.md)).

### Mission Integration

```text
Campaign Builder
      ↓
Execution Plan
      ↓
Execution Engine
      ↓
Knowledge Update
      ↓
Learning Loop
```

Mission Engine remains the orchestrator. After campaign **Approve**, planner may append / spawn an `execution_engine` step (or a dedicated `campaign_execution` mission type) with the pinned campaign id + playbook version. Execution never bypasses Mission audit.

### Operator Dashboard

Live status visible per touchpoint / campaign:

- Queued
- In Progress (`Executing`)
- Waiting on Operator
- Waiting on Prospect
- Completed

Every touchpoint is visible in Command Deck Operations / Mission Workspace (SPEC-022 Mission-First UX). No fake progress — status is durable state.

## Data Model

```text
packages/capabilities/execution/
  types.js
  ExecutionPlanBuilder.js
  ExecutionEngine.js
  TouchTaskRunner.js
  RetryPolicy.js              ← reads Playbook only
  OutcomeHandler.js
  ExecutionStore.js
  PostgresExecutionStore.js
  index.js

migrations/YYYY-MM-DD-execution-engine.sql
```

```ts
type ExecutionStatus =
  | 'draft'
  | 'approved'
  | 'queued'
  | 'executing'
  | 'waiting'
  | 'completed'
  | 'archived'
  | 'failed'
  | 'cancelled'

type TouchChannel =
  | 'direct_mail'
  | 'email'
  | 'phone'
  | 'linkedin'
  | 'manual'

type TouchTaskStatus =
  | 'queued'
  | 'executing'
  | 'waiting'
  | 'completed'
  | 'skipped'
  | 'failed'
  | 'cancelled'

type WaitingReason =
  | 'operator_action'
  | 'prospect_response'
  | 'approval_required'
  | 'schedule_gate'

type ExecutionOutcome =
  | 'no_response'
  | 'positive_response'
  | 'negative_response'
  | 'walkthrough_scheduled'
  | 'proposal_requested'
  | 'closed_won'
  | 'closed_lost'

interface ExecutionRun {
  id: string
  campaignId: string
  missionId: string
  clientId: number | string
  clientPlaybookId: string
  clientPlaybookVersion: string
  status: ExecutionStatus
  plan: ExecutionPlan
  outcome: ExecutionOutcome | null
  createdAt: string
  updatedAt: string
  startedAt: string | null
  completedAt: string | null
}

interface ExecutionPlan {
  runId: string
  tasks: TouchTask[]
  pinnedConstraints: object   // snapshot from playbook + campaign
  schedule: SchedulePolicy    // derived from playbook — never hardcoded defaults that invent strategy
}

interface TouchTask {
  id: string
  runId: string
  prospectId: string
  channel: TouchChannel
  action: string
  sequenceDay: number
  attempt: number
  status: TouchTaskStatus
  waitingReason: WaitingReason | null
  scheduledAt: string
  executedAt: string | null
  result: string | null
  evidence: object[]
  artifacts: object[]
  operatorNotes: string | null
}

interface SchedulePolicy {
  // All fields sourced from Client Playbook (SPEC-028 extensions as needed)
  retryAfterDays: number | null
  maxAttempts: number | null
  skipWeekends: boolean
  businessHoursOnly: boolean
  callWindow: string | null
}
```

### Playbook extension (additive)

SPEC-028 Playbooks already carry `outreachSequence` and `constraints`. Execution may require explicit schedule fields on the Playbook (or structured constraint types such as `retry_after_days`, `max_attempts`, `skip_weekends`, `business_hours`). Those fields remain Playbook-owned; the engine only interprets them.

### Persistence

- `execution_runs` — durable run + status + pinned playbook/campaign refs
- `execution_touch_tasks` — one row per touch attempt
- `execution_transitions` — append-only audit of state changes
- Idempotent `ENSURE_SQL` for local/dev; rollback SQL drops new tables only
- Historical runs remain pinned to Playbook version used (ADR-015)

## Implementation Plan

1. **Types + state machine** — transitions, fail-closed guards, unit tests
2. **ExecutionPlanBuilder** — approved campaign + Playbook → queued tasks (no sends)
3. **Stores** — in-memory + Postgres migration
4. **TouchTaskRunner** — channel adapters behind capability interfaces; Waiting for manual
5. **RetryPolicy** — interpret Playbook only; refuse hardcoded defaults that invent strategy
6. **Evidence + Knowledge Update** — dual-write path for completed touches
7. **OutcomeHandler** — map outcomes → MissionPlanner follow-up recommendations / spawn
8. **Capability registration** — `execution_engine` in registry; MissionPlanner post-approve hook
9. **Operator Dashboard** — live status on Command Deck Operations / Mission Workspace
10. **Safety tests** — unapproved block, DNC block, missing contact block, playbook window block

## Migration Strategy

- Additive tables only; no mutation of `missions` / `client_playbooks` beyond optional JSON schedule fields
- Campaigns already `review_required` remain non-executable until explicit Approve
- Rollback drops execution tables; mission/campaign history untouched
- Feature flag `EXECUTION_ENGINE` (default off until thin slice proven) — when off, Approve stops at review artifact (current behavior)

## Testing

- Unit: state transitions; illegal transitions rejected
- Plan builder: sequence × prospects; constraints applied; pin playbook version
- RetryPolicy: values from playbook; engine has no baked-in “retry in 5 days”
- Safety: unapproved campaign cannot queue; DNC prospect skipped/blocked with audit; missing email cannot send email task
- Human-in-loop: mail/phone tasks enter `Waiting`; resume only on operator complete
- Evidence: completed touch without evidence fails validation
- Outcomes: `proposal_requested` emits follow-up mission recommendation
- Integration: `npm run test:capabilities` · `npm run test:mission`
- Manual smoke: Approve Campaign 001 → queue visible → complete manual mail task → Knowledge touch recorded

## Acceptance Criteria

- [ ] Approved campaigns execute automatically (plan queued + runner advances eligible tasks)
- [ ] Manual tasks pause execution safely (`Waiting` + operator resume)
- [ ] Every touchpoint is recorded (timestamp, result, evidence; Knowledge path)
- [ ] Outcomes trigger follow-up Missions when appropriate
- [ ] Execution respects Client Playbooks (channels, sequence, constraints, retry/schedule)
- [ ] All activity is durable, reviewable, and recoverable
- [ ] Fail-closed: never unapproved send, skipped required review, out-of-constraint execute, or invented contact data
- [ ] ADR-016 accepted and linked
- [ ] Operator Dashboard shows Queued / In Progress / Waiting on Operator / Waiting on Prospect / Completed

## Future Work

- Richer channel adapters (GBP, SMS/Sam) as Playbook channels expand
- Bulk operator “packet day” UX for direct mail
- Shadow mode (record intended sends without side effects) for new clients
- Multi-timezone schedule interpretation
- Automatic Closed Won / Lost commission hooks (closer pipeline) from execution outcomes
- Playbook learning recommendations from execution evidence (advisory; ADR-003)
