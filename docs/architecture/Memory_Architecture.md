# Memory Architecture

## What “memory” means here

Pulseforge memory is **durable, client-scoped business context**—not model weights and not chat transcript alone.

## Current memory substrates

| Substrate | Stores | Strength | Weakness |
|---|---|---|---|
| `prospects` / `companies` | Entities + status fields | Queryable CRM truth | Easy to overwrite without provenance |
| `touchpoints` | Channel events | Chronology | Limited graph semantics |
| `agent_log` | Run audits | Debug + metrics | Not a product memory API |
| Max decision / signal tables | Scores, decisions, actions | Explainable components (shadow) | Not full business graph |
| Inquiry events / work items | Inbound journey | Strong workflow audit | Separate from outbound CRM mentally |
| Google Sheets (warm flags) | Operator convenience | Visible to setters | Not canonical |

## Design rules

1. **Evidence first** — Prefer inserting an event to silently changing history.
2. **Idempotency** — External events need stable keys (see Max canonical source assessment).
3. **Provenance** — Derived claims should point at evidence IDs.
4. **No fake coverage** — Do not claim meetings cancelled/showed without durable event identity.
5. **Client isolation** — Memory queries always filter `client_id`.

## Target memory (SPEC-001)

A Knowledge Graph layer that:

- Unifies companies, people, inquiries, opportunities, and interactions as nodes/edges
- Carries provenance and confidence
- Supports Max reasoning and conversation retrieval
- Projects into existing dashboards without dual-write chaos

See [Knowledge_Graph_Architecture.md](Knowledge_Graph_Architecture.md) and [vision/Intelligence_Architecture.md](../vision/Intelligence_Architecture.md).

## Anti-patterns

- Treating LLM context windows as system of record
- Duplicating the same fact into dashboard-only tables
- Inferring outcomes (no-show, cancelled) from missing data
