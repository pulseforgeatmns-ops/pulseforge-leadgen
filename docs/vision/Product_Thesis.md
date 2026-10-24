# Product Thesis

## The bet

Local service businesses do not fail from lack of activity. They fail from **lost context**: opens without follow-up, inquiries without ownership, bookings without preparation, and tribal knowledge that leaves when a person does.

Pulseforge bets that an **agentic system grounded in durable business memory** outperforms both (a) generic chatbots and (b) traditional CRMs, when three constraints hold:

1. **Memory is structured** — entities, relationships, and events form a knowledge graph, not a chat scrollback.
2. **Reasoning is explainable** — Max (and peer agents) justify recommendations from evidence.
3. **Action is gated** — humans approve customer-visible outcomes; automation earns trust in shadow first.

## Today vs direction

| Today (shipped / in-tree) | Direction (roadmap) |
|---|---|
| Multi-agent lead-gen CRM | Conversation-first operator experience |
| Tables + touchpoints + agent_log | Business Knowledge Graph (SPEC-001) |
| Max briefing + shadow orchestration | Max Reasoning Engine (SPEC-002) |
| Dashboards for setter/closer/operator | Chat + command surfaces over the same truth |

## Non-goals (thesis level)

- Fully autonomous outbound without approval
- Replacing human closers
- One global model that ignores client tenancy and brand voice

## Implications for engineering

- Prefer append-only evidence and explicit state transitions over silent field overwrites
- Prefer shadow modes and feature flags for new mutation paths
- Prefer specs and ADRs over ad-hoc “just ship it” architecture

Related: [Product_Constitution.md](Product_Constitution.md), [Intelligence_Architecture.md](Intelligence_Architecture.md), [ADR-001](../adr/ADR-001_Conversation_First.md), [ADR-004](../adr/ADR-004_Knowledge_Graph.md).
