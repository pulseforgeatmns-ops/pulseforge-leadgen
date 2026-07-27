# ADR-015 — Strategy Lives in the Playbook

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-27 |
| **Spec** | [SPEC-028](../specs/SPEC-028_Client_Playbook_Capability.md) |
| **Supersedes** | — |
| **Related** | [ADR-010](ADR-010_Mission_Engine.md), [ADR-011](ADR-011_Capability_Framework.md), [ADR-014](ADR-014_Personalized_by_Default.md), [ADR-002](ADR-002_Explainable_AI.md), [ADR-003](ADR-003_Human_Approval.md) |

## Context

Discovery Profiles ([SPEC-024](../specs/SPEC-024_Prospect_Discovery_Capability.md)) answer *who* to target. Selling strategy — channels, sequences, offers, brand voice, constraints, success metrics — was either hardcoded in capability stubs or re-entered per mission. That duplicates configuration across Campaign Builder and Proposal Generator, makes historical missions unexplained, and forces code changes when a client refines how they win work.

Capabilities must remain the stable execution API ([ADR-011](ADR-011_Capability_Framework.md)). They should not become the system of record for client strategy.

## Decision

1. **Client strategy belongs in the Client Playbook**, not in individual capabilities.
2. **Capabilities execute strategy.** The Playbook **defines** strategy.
3. Discovery Profiles remain the source of truth for targeting (*who*). Playbooks are the source of truth for selling (*how*).
4. Missions that build campaigns or proposals **pin an immutable Playbook version** in constraints. Historical runs never silently adopt a newer version.
5. Downstream capabilities **must not hardcode** outreach channel order, sequence timing, offers, or brand voice when a Playbook is present.
6. Future playbook improvement suggestions (learning loop) are **advisory only**; operator approval is always required ([ADR-003](ADR-003_Human_Approval.md)).

## Consequences

### Positive

- One source of truth per client for how they sell
- Consistent behavior across Proposal Generator, Campaign Builder, and (future) Execution Engine
- Strategy refinement without code changes
- Explainable decisions: every mission cites playbook id + version
- Aligns with personalized proposals ([ADR-014](ADR-014_Personalized_by_Default.md)) using client-owned language

### Negative / tradeoffs

- Onboarding must capture a playbook (guided interview UI is future work)
- Capabilities need a shared resolve path; stubs must degrade gracefully when no playbook exists
- Multi-segment playbooks (later) need explicit mission selection rules

### Follow-ups

- [x] SPEC-028 Client Playbook Capability (v1 store + selector + Campaign / Proposal wiring)
- [ ] Guided onboarding interview + visual Playbook editor
- [ ] Advisory learning recommendations against success metrics
- [ ] Execution Engine enforcement of playbook constraints at send time ([SPEC-029](../specs/SPEC-029_Execution_Engine.md) / [ADR-016](ADR-016_Execution_Does_Not_Decide.md))
