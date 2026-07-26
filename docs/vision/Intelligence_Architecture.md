# Intelligence Architecture (Vision)

This document describes the **intended** intelligence stack. Implementation status lives in CURRENT_STATE and specs.

## Layers

```text
┌─────────────────────────────────────────┐
│  Experience: conversation + role UIs    │
├─────────────────────────────────────────┤
│  Reasoning: Max (+ specialist planners) │
├─────────────────────────────────────────┤
│  Memory: Business Knowledge Graph       │
├─────────────────────────────────────────┤
│  Evidence: events, touchpoints, audits  │
├─────────────────────────────────────────┤
│  Execution: agents, outbox, approvals   │
└─────────────────────────────────────────┘
```

## Evidence

Raw facts: email opens/clicks, calls, inquiry events, Scout inserts, approvals, calendar bookings. Prefer append-only logs with stable idempotency keys.

## Memory (SPEC-001)

The Knowledge Graph stores entities (client, company, prospect, inquiry, opportunity), relationships (employs, contacted, booked), and derived claims with provenance.

## Reasoning (SPEC-002)

Max consumes graph + evidence via `packages/max` to produce:

- Ranked structured recommendations (score + independent confidence)
- Supporting and contradicting evidence
- Explanation chains (ADR-002)
- Optional draft actions routed to approval (ADR-003) — not wired in v0.8.0

## Memory (SPEC-003)

Max remembers **transitions**, not facts:

- Append-only reasoning snapshots
- Deterministic diffs and change events
- Trends, history, and watch detection (no notifications yet)
- Temporal explanations: Why → Evidence → History → Change → Reason

## Briefing (SPEC-004)

Max assembles Knowledge + Reasoning + Memory into deterministic operator briefings:

- `brief({ tenantId, asOf, period })` — daily / weekly / monthly
- Structured sections only (summary, priorities, changes, watches, risks, recommendations, metrics)
- Never computes — never calls `evaluate()` during assembly
- Presentation Adapter extension point for CLI / dashboard / assistant surfaces

## Policy (SPEC-005)

Max evaluates every recommendation against explicit tenant policy before operator or automation:

- `decide({ tenantId, recommendation, context })` — allow / warn / requireApproval / block
- Modular Rule Registry (confidence, contradiction, tenant, risk, cooldown, contact, freshness)
- Immutable audit trail + explainability chain
- Never executes — evaluation only

## Command Deck Composer (SPEC-007)

Max presents the stack as one immutable view model for the operator surface:

- `compose({ tenantId, asOf, period })` — Morning Brief, Highest Leverage Action, Watch Alerts, Market Trends, Priority Queue
- Common `IntelligenceCard` contract + composer-owned empty states
- Explainability metadata on every card (`sources`, `reasoningId`, `policyId`, `briefingId`)
- May sort / merge / rank / summarize / group — never reason / score / infer / invent
- HTTP: `GET /api/v1/command-deck`

## Execution

Specialist agents and outbox adapters perform channel work. Shadow mode records intent without side effects until flags and approvals permit.

## Separation from dashboards

Dashboards read the same truth; they are not a second brain. Conversation and command surfaces query the same graph/services.

See also: [architecture/Memory_Architecture.md](../architecture/Memory_Architecture.md), [architecture/Knowledge_Graph_Architecture.md](../architecture/Knowledge_Graph_Architecture.md), [architecture/Agent_Architecture.md](../architecture/Agent_Architecture.md).
