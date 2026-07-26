# Architecture Decision Records

ADRs capture **why** we locked an architectural choice. They are permanent. Status changes via a new ADR that supersedes an old one—do not silently rewrite history.

## Index

| ADR | Title | Status |
|---|---|---|
| [ADR-001](ADR-001_Conversation_First.md) | Conversation First | Accepted |
| [ADR-002](ADR-002_Explainable_AI.md) | Explainable AI | Accepted |
| [ADR-003](ADR-003_Human_Approval.md) | Human Approval | Accepted |
| [ADR-004](ADR-004_Knowledge_Graph.md) | Knowledge Graph | Accepted |

Also listed in root [`DECISIONS.md`](../../DECISIONS.md).

## When to write an ADR

- New data store or memory model
- Changes to agent authority / automation boundaries
- Auth, tenancy, or approval policy changes
- Any decision that would be expensive to reverse

## Process

1. Copy [TEMPLATE.md](TEMPLATE.md)
2. Discuss in PR with the implementing spec
3. Merge as Accepted (or Proposed if still debating—avoid lingering Proposed without owner)
4. Link from DECISIONS.md, spec, and CHANGELOG
