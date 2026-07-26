# SPEC-002 — Max Reasoning Engine

| Field | Value |
|---|---|
| **Status** | Draft — queued after SPEC-001 |
| **Target Version** | v0.9.0 |
| **Priority** | High |
| **Owner** | TBD |
| **Created** | 2026-07-26 |

## Objective

Evolve Max from briefing + shadow orchestration into an explainable reasoning engine that reads Business Knowledge Graph context and produces ranked, evidenced recommendations—without silent irreversible customer-facing actions.

## Vision References

- `docs/vision/Intelligence_Architecture.md`
- `docs/vision/Product_Constitution.md`
- `docs/architecture/Agent_Architecture.md`
- [ADR-001](../adr/ADR-001_Conversation_First.md)
- [ADR-002](../adr/ADR-002_Explainable_AI.md)
- [ADR-003](../adr/ADR-003_Human_Approval.md)
- [ADR-004](../adr/ADR-004_Knowledge_Graph.md)

## Problem

Max can summarize and score in shadow, but operators still lack a first-class reasoning loop: graph-grounded answers, explicit recommendations, and a path to approval-gated execution.

## Scope

- Reasoning API/service that loads KG neighborhood + relevant evidence
- Structured recommendation objects with explanation components
- Integration with existing Max orchestration shadow audits
- Operator-visible review surface (minimal) for recommendations
- Feature flags; mutating executions remain approval-gated / shadow-default

## Out of Scope

- Fully autonomous multi-channel campaigns
- Bypassing DNC or approval constitution
- Replacing Emmett/Riley/Scout ownership of their channels
- v1.0 conversation UI polish (may stub interfaces only)

## Dependencies

- SPEC-001 KG read path available
- Max orchestration schema (existing)
- ADR-002 / ADR-003

## Architecture

Max reasons; specialist agents execute. LLM calls may draft language; graph + rules constrain facts. All material outputs logged with evidence IDs.

## Data Model

- Recommendation records with status (`proposed`, `accepted`, `rejected`, `shadow_skipped`)
- Links to KG nodes/edges and source events
- Reuse/extend `max_actions` where appropriate rather than duplicating concepts blindly

## Implementation Plan

1. Define recommendation schema + flags
2. Build graph-grounded context assembler
3. Produce shadow recommendations for a pilot client
4. Review UI or CLI (`max:review` evolution)
5. Document graduation criteria toward limited write (separate ADR if needed)

## Migration Strategy

- Additive; no change to default-off safety
- Existing briefing path remains

## Testing

- Golden-fixture explanations contain required evidence refs
- Isolation tests per `client_id`
- Shadow guarantees: zero outbound side effects in default config

## Acceptance Criteria

- [ ] Max can answer a bounded “why this prospect / what next?” with evidence refs from KG
- [ ] Recommendations persist with explanations
- [ ] Default config produces no external sends or non-shadow state transitions
- [ ] Docs + CURRENT_STATE + CHANGELOG updated for v0.9.0

## Future Work

- Conversation-first UI (v1.0)
- Limited auto-apply for non-customer-visible tasks under new ADR
- Tighter coupling to Inquiry Command Center
