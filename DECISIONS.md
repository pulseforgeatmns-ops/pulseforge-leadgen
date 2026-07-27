# Decisions Index

Canonical Architectural Decision Records live in [`docs/adr/`](docs/adr/). This file is the human-readable index.

| ID | Title | Status | Summary |
|---|---|---|---|
| [ADR-001](docs/adr/ADR-001_Conversation_First.md) | Conversation First | Accepted | Primary product interaction model is conversational intelligence, not dashboard-only CRUD |
| [ADR-002](docs/adr/ADR-002_Explainable_AI.md) | Explainable AI | Accepted | Material scores and recommendations must be auditable and explainable |
| [ADR-003](docs/adr/ADR-003_Human_Approval.md) | Human Approval | Accepted | Customer-visible sends/posts require explicit human approval unless a later ADR narrows the gate |
| [ADR-004](docs/adr/ADR-004_Knowledge_Graph.md) | Knowledge Graph | Accepted | Durable business memory is modeled as a knowledge graph (SPEC-001) |
| [ADR-005](docs/adr/ADR-005_LLM_Presentation_Engine.md) | LLM Presentation Engine | Accepted | LLMs communicate verified intelligence only — they never create business intelligence |
| [ADR-006](docs/adr/ADR-006_Live_Intelligence_Evolution.md) | Live Intelligence Evolution | Accepted | Intelligence evolves in place via IntelligenceEvent; refresh is a fallback, not the product language |
| [ADR-007](docs/adr/ADR-007_Operator_Intelligence.md) | Operator Intelligence | Accepted | Operator behavior learning personalizes presentation only — never evidence, confidence, reasoning, or policy |
| [ADR-008](docs/adr/ADR-008_Outcome_Intelligence.md) | Outcome Intelligence | Accepted | Evaluates whether intelligence was right — measures, calibrates, reports; never alters reasoning or confidence |
| [ADR-009](docs/adr/ADR-009_Evidence_Platform_Architecture.md) | Evidence Platform Architecture | Accepted | Domain-neutral Evidence Core + injectable Strategy Packs; core must not contain domain business logic |
| [ADR-010](docs/adr/ADR-010_Mission_Engine.md) | Mission Engine | Accepted | Intent → Capability Registry; durable missions; review before outreach; Mission-First UX on Command Deck |
| [ADR-011](docs/adr/ADR-011_Capability_Framework.md) | Capability Framework | Accepted | Capabilities are the stable API of Pulseforge; agents are implementation details |
| [ADR-014](docs/adr/ADR-014_Personalized_by_Default.md) | Personalized by Default | Accepted | Proposal generation optimizes for relevance, not speed; interchangeable name-swap decks fail |
| [ADR-015](docs/adr/ADR-015_Strategy_Lives_in_the_Playbook.md) | Strategy Lives in the Playbook | Accepted | Client strategy lives in versioned Client Playbooks; capabilities execute strategy |
| [ADR-016](docs/adr/ADR-016_Execution_Does_Not_Decide.md) | Execution Does Not Decide | Accepted | Execution Engine carries out approved strategy only — never creates it |
| [ADR-017](docs/adr/ADR-017_Intelligence_Before_Execution.md) | Intelligence Before Execution | Accepted | Company Intelligence packages before Ranking/Campaign/Proposal/Execution; never fabricate |
| [ADR-018](docs/adr/ADR-018_Time_Matters.md) | Time Matters | Accepted | Business intelligence is time-aware; recent verified signals increase relevance; expired signals decay |
| [ADR-019](docs/adr/ADR-019_Missions_Are_Conversations.md) | Missions Are Conversations | Accepted | A Mission is a persistent collaborative workspace; follow-ups refine in place; capabilities consume the current revision; execution uses the latest approved revision |

## How to add a decision

1. Copy [`docs/adr/TEMPLATE.md`](docs/adr/TEMPLATE.md).
2. Number sequentially (`ADR-005`, …).
3. Link from this index.
4. Reference from the implementing spec and CHANGELOG.
5. Update CURRENT_STATE **Upcoming Decisions** when resolved.

## Related operational notes

Pre-foundation decisions are scattered across `CLAUDE.md` (Known Architectural Notes), `AGENT_RULES.md`, and flat `docs/*` runbooks. New architectural choices must use ADRs; do not only append to chat or CLAUDE.md.
