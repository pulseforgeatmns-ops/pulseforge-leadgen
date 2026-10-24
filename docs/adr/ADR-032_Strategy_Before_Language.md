# ADR-032 — Strategy Before Language

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-28 |
| **Spec** | [SPEC-048](../specs/SPEC-048_Sales_Intelligence_Engine.md) |
| **Supersedes** | — |
| **Related** | [ADR-014](ADR-014_Personalized_by_Default.md), [ADR-015](ADR-015_Strategy_Lives_in_the_Playbook.md), [ADR-016](ADR-016_Execution_Does_Not_Decide.md), [ADR-017](ADR-017_Intelligence_Before_Execution.md), [ADR-031](ADR-031_Review_Must_Be_Evidence_First.md) |

## Context

Channel generators (especially Direct Mail) were inventing buyer framing, value props, and personalization inside prompt-adjacent compose steps. That forced a single model path to simultaneously understand the company, infer the buyer, choose positioning, and write copy. Operators could not inspect the reasoning. Approval rates suffered from generic messaging, wrong-industry assumptions, and AI-sounding prose.

Client Playbooks ([ADR-015](ADR-015_Strategy_Lives_in_the_Playbook.md)) hold brand/offer strategy. Company Intelligence ([ADR-017](ADR-017_Intelligence_Before_Execution.md)) holds facts. Neither is a per-prospect sell-strategy artifact.

## Decision

1. **Pulseforge shall separate business reasoning from language generation.**
2. Models / capabilities must first determine:
   - Who the buyer is
   - What the buyer likely cares about
   - Why Anchor (or the client) is relevant
   - What evidence supports those conclusions
3. **Only after strategy is established** may channel-specific generators produce language.
4. The structured artifact is `SalesIntelligenceProfile` (`sales_intelligence_profile`).
5. **Optimize for approval, not generation.** Operator Approval Rate (share of outreach approved without substantive edits) is the primary quality metric — not volume of drafts.
6. Operator Confidence Score is advisory and never replaces human approval ([ADR-021](ADR-021_Human_Approval_Before_Execution.md)).
7. Implementing contract: [SPEC-048 Sales Intelligence Engine](../specs/SPEC-048_Sales_Intelligence_Engine.md).

## Rationale

Most AI systems measure success by how much content they produce. Pulseforge measures success by how often experienced operators approve that content without meaningful edits. Hiding reasoning inside generation prevents inspection, gates, and consistent multi-channel reuse.

## Consequences

### Positive

- One sales strategy feeds Direct Mail, Email, LinkedIn, call scripts, proposals, and future channels
- Operators review Sales Intelligence → Messaging Strategy → Letter (reasoning before language)
- Quality gates can reject wrong industry, unsupported personalization, and prospect-after-Anchor openings before send

### Negative / tradeoffs

- Adds a pipeline stage between Ranking and Campaign/Mail
- Thin Company Intelligence (SPEC-030 still proposed) yields lower-confidence profiles; generators must respect `sendable` / gates

### Follow-ups

- [x] SPEC-048 v1 capability + Mail Package consumer + Review Workspace surface
- [x] SPEC-053 Business Intelligence as richer analytical input (ADR-037)
- [ ] Email / LinkedIn / phone script generators consume the same profile
- [ ] Durable Operator Approval Rate dashboard
- [ ] Full SPEC-030 Company Enrichment as richer Level-1 fact input
