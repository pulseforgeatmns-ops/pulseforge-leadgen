# SPEC-152 — Concept Graph Reasoning

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Max |
| **Created** | 2026-08-24 |
| **Depends on** | [SPEC-150](SPEC-150_Conversational_State_Machine.md), [SPEC-151](SPEC-151_Operating_Model_Reasoning.md) (code) |

## Objective

Replace one-to-one intent routing with **concept-based reasoning**. Max identifies the concepts involved, traverses relationships between them, and synthesizes an answer. The unit of reasoning becomes **concepts**, not paragraphs.

## Problem

Prior reasoning resembled:

```
Question → Intent → Knowledge Section → Generate Answer
```

Multi-concept questions failed because the engine assumed every question had one destination.

## Design

### New modules (`packages/max/reasoning/ConceptGraph/`)

| Module | Role |
|---|---|
| `ConceptGraph.js` | Core graph — concepts + relationships, traversal |
| `seedFromOperatingModel.js` | Seeds graph from SPEC-151 `OPERATING_MODEL` |
| `ConceptPlanner.js` | Question → `{ concepts, goal }` |
| `ConceptReasoner.js` | Plan + graph → synthesized prose via traversal |

### Core objects

```js
// Concept
{ id, label, category, description }

// Relationship
{ from, to, relation }
```

Relations include: `owns`, `depends_on`, `coordinates`, `specializes_in`, `delegates_to`, `reports_to`, `cannot_override`, `supports`, `requires`, `explains`, `retains_authority`, `balances`.

### Conversation integration (SPEC-150)

Session state now tracks `activeConcepts` alongside subject. Follow-ups merge concepts instead of replacing them.

### Identity integration

`IdentityReasoning.js` delegates planning and synthesis to ConceptPlanner/ConceptReasoner while preserving SPEC-151 test compatibility via `REASONING_TARGETS` mapping.

## Acceptance criteria

| Scenario | Requirement |
|---|---|
| Identity continuity chain | Role → Why → Compare Scout → Why not Scout → Ignore advice — stays in concept graph |
| Conflict reasoning | Scout vs Paige disagreement — graph traversal, operator authority |
| Authority reasoning | Can Scout approve outreach? — no Blueprint fallback |
| Relationship reasoning | Scout/Paige dependency — generated through relationships |
| Multi-hop reasoning | Why shouldn't Scout make business decisions? — Operator → Max → Scout → Discovery |

## Architectural principle

Every major PulseForge subsystem has followed the same evolution:

| Generation | Representation |
|---|---|
| Client Intelligence | Interview → Blueprint |
| Mission Engine | Intent → Mission Plan |
| Scout | Search → Intelligence Pipeline |
| Conversation | Turns → Conversation State |
| Identity | Paragraph → Operating Model |
| **Reasoning** | **Documents → Concept Graph** |

See [ADR-060](../adr/ADR-060_Knowledge_as_Relationships.md).
