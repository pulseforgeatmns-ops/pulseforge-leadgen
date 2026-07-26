# ADR-001 — Conversation First

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-26 |
| **Spec** | SPEC-000 (recorded); product delivery in v1.0 trajectory |
| **Supersedes** | — |

## Context

Pulseforge already has role dashboards (admin, setter, closer, approvals, operator command center). Dashboards are necessary instruments, but a dashboard-only product trains users to hunt rows instead of asking “what matters and why?” The product thesis targets an operating partner experience.

## Decision

The **primary long-term interaction model** is conversational intelligence over shared business truth (KG + evidence). Dashboards remain first-class for focused operational work (queues, approvals, metrics) but must not become a second conflicting brain.

New major capabilities should prefer:

1. Queryable truth services usable by both UI and conversation
2. Explanations before actions
3. Command/attention surfaces over dense inventory UIs when designing net-new operator flows

## Consequences

### Positive

- Aligns engineering toward shared APIs for Max and humans
- Reduces duplicate “dashboard-only” data models

### Negative / tradeoffs

- Conversation UX is not fully built in v0.7–v0.9; dashboards remain the daily driver meanwhile
- Requires discipline to avoid chatbot theater without memory (see ADR-004)

### Follow-ups

- SPEC-001 / SPEC-002 enable the substrate
- v1.0 release plan for conversation surfaces
