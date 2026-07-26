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

Max consumes graph + evidence to produce:

- Situation summaries
- Ranked recommendations
- Explanations (ADR-002)
- Optional draft actions routed to approval (ADR-003)

## Execution

Specialist agents and outbox adapters perform channel work. Shadow mode records intent without side effects until flags and approvals permit.

## Separation from dashboards

Dashboards read the same truth; they are not a second brain. Conversation and command surfaces query the same graph/services.

See also: [architecture/Memory_Architecture.md](../architecture/Memory_Architecture.md), [architecture/Knowledge_Graph_Architecture.md](../architecture/Knowledge_Graph_Architecture.md), [architecture/Agent_Architecture.md](../architecture/Agent_Architecture.md).
