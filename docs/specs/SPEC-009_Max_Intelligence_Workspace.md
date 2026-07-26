# SPEC-009 — Max Intelligence Workspace

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v1.0.0 |
| **Priority** | Critical |
| **Owner** | TBD |
| **Created** | 2026-07-26 |
| **Completed** | 2026-07-26 |
| **Version** | v1.0.0 |

## Objective

Transform Max from a chat window into a contextual intelligence workspace. Every conversation begins with an explicit context envelope; the deterministic intelligence stack produces a grounded Structured Response Object; Claude only translates that object into natural language ([ADR-005](../adr/ADR-005_LLM_Presentation_Engine.md)).

## Vision References

- `docs/vision/Product_Constitution.md` (§11 Cognitive load)
- `docs/vision/Product_Experience.md`
- `docs/vision/Intelligence_Architecture.md`
- [SPEC-006](SPEC-006_Command_Deck.md) — Ask Max modal / Intelligence Workspace
- [SPEC-008](SPEC-008_Command_Deck_UI.md) — Command Deck launcher (invitation → workspace)
- [ADR-001](../adr/ADR-001_Conversation_First.md)
- [ADR-002](../adr/ADR-002_Explainable_AI.md)
- [ADR-005](../adr/ADR-005_LLM_Presentation_Engine.md)

## Problem

SPEC-008 ships a pinned Ask Max launcher that holds questions for a later release. Operators still get an empty or CRM-shaped chat (`/api/max/ask`) that reconstructs pipeline state from ad-hoc SQL instead of receiving the page context they are already looking at. Max does not feel like the conversational interface to the Intelligence Platform.

## Scope

- Context-aware full-height Max modal opened from Command Deck entry points:
  - Morning Brief
  - Highest Leverage Action
  - Priority Queue items
  - Watch Alerts
  - Ask Max launcher / submit
- Explicit `MaxContext` envelope (UI never asks Max to reconstruct state)
- Deterministic opening state + contextual suggested investigations
- Session memory within a process; context-switch acknowledgement
- `StructuredResponseObject` from Knowledge → Reasoning → Memory → Briefing → Policy facts in the envelope
- Claude PresentationEngine (NL only) + deterministic fallback without API key
- Collapsed response metadata (“Generated from …”)
- Expandable evidence panel
- Recommended actions that lead back into the product
- `MaxContext.page` supports `command-deck` | `company` | `recommendation` | `timeline` | `market` for future pages

## Out of Scope

- Stub Company / Recommendation / Timeline / Market pages
- Replacing dashboard legacy `/api/max/ask`
- Durable cross-process session store
- Autonomous execution of recommended actions
- Live knowledge dual-write (fail closed when stack is empty)

## Dependencies

- ✅ SPEC-007 Command Deck Composition Engine
- ✅ SPEC-008 Command Deck UI (launcher)
- ✅ SPEC-002 / 003 / 004 / 005 intelligence stack
- → Completes Ask Max half of [SPEC-006](SPEC-006_Command_Deck.md)

## Architecture

```text
Command Deck entry
        │
        ▼
MaxContext envelope (explicit)
        │
        ▼
WorkspaceEngine.open / ask
        │
        ├── OpeningStateBuilder + SuggestionEngine
        │
        └── ResponseComposer
                │
                ▼
        StructuredResponseObject
                │
                ▼
        PresentationEngine (Claude | fallback)
                │
                ▼
        Full-height Intelligence Workspace modal
```

### Pipeline (ADR-005)

```text
User Question
  → Context Envelope
  → Knowledge → Reasoning → Memory → Briefing → Policy
  → Structured Response Object
  → LLM (Claude)
  → Natural Language Response
```

### MaxContext

```ts
interface MaxContext {
  page: "command-deck" | "company" | "recommendation" | "timeline" | "market";
  tenantId: string;
  companyId?: string;
  recommendationId?: string;
  visibleCards: IntelligenceCard[];
  briefing?: MorningBrief;
  selectedEntity?: EntityReference;
  deck?: CommandDeckModel; // optional snapshot for evidence assembly
}
```

### StructuredResponseObject

```ts
interface StructuredResponseObject {
  answer: string;
  reasoning: string[];
  supportingEvidence: EvidenceRef[];
  contradictingEvidence: EvidenceRef[];
  confidence: number | null;
  nextInvestigations: string[];
  recommendedActions: RecommendedAction[];
  metadata: ResponseMetadata;
}
```

## Data Model

No new intelligence tables. Sessions are in-process (`SessionStore` Map). Presentation preferences may use sessionStorage only.

## Implementation Plan

1. ADR-005 + this spec + indexes
2. `packages/max/workspace/` engine
3. `POST /api/v1/max/workspace/open` and `/ask`
4. Command Deck modal UI + entry wiring
5. Tests + CURRENT_STATE / CHANGELOG

## Migration Strategy

- Ship behind Command Deck route; `/dashboard` Ask Max unchanged
- No schema migration
- Rollback: hide modal; launcher can revert to hold-note behavior

## Testing

- Envelope validation; opening + suggestions per page
- Context-switch acknowledgement
- ResponseComposer never invents evidence; confidence null stays null
- PresentationEngine fallback preserves SRO meaning
- Metadata source flags match what was used

## Acceptance Criteria

- [x] Context-aware modal opens from every supported Command Deck entry point
- [x] Context envelope passed explicitly
- [x] Opening state reflects current page / entity
- [x] Contextual suggested investigations rendered
- [x] Responses expose evidence and reasoning
- [x] Conversation maintains session context
- [x] Context switches acknowledged
- [x] No response invents unsupported facts
- [x] Response metadata strip present (collapsed by default)
- [x] Workspace feels like an intelligence console rather than a generic chat
- [x] ADR-005 boundary enforced in PresentationEngine

## Future Work

- Dedicated Company / Recommendation / Timeline / Market entry pages
- Durable session store
- Migrate dashboard `/api/max/ask` onto WorkspaceEngine
- Deeper live graph queries once dual-write is live
