# PROJECT_CONTEXT

**Required reading for every AI contributor before implementation.**

This file is the orientation layer: what Pulseforge is, how we build, and where authority lives. Product philosophy details are in `docs/vision/`. Engineering design is in `docs/architecture/`. Live status is in `CURRENT_STATE.md`.

---

## What Pulseforge is

Pulseforge is an AI-powered lead generation and outreach operating system for local service businesses. Today it is a multi-tenant Express + PostgreSQL application that:

- Discovers and scores prospects (Scout / `leadgen.js`)
- Runs named agents for email, SMS, social, calendar, enrichment, and reporting
- Surfaces pipeline state to setters, closers, and operators through authenticated dashboards
- Enforces human approval before public posts and (for Inquiry) before external sends

The product direction is a **conversation-first**, **explainable**, **approval-gated** intelligence layer: a Business Knowledge Graph (SPEC-001) plus Max as a reasoning engine (SPEC-002), not a black-box autopilot.

---

## Product thesis (short)

Local operators drown in fragmented signals (opens, calls, inquiries, bookings). Pulseforge turns those signals into durable business memory and recommended next actions that a human can trust, override, and approve. See `docs/vision/Product_Thesis.md`.

---

## Coding philosophy

1. **Repository over chat** — Decisions live in ADRs, specs, and CURRENT_STATE.
2. **Safety by default** — New outbound or lifecycle mutation paths ship shadow / flag-off first.
3. **Explainability** — Scores, recommendations, and state changes must be auditable.
4. **Human approval** — Public content and external customer messages require an explicit approve path unless a spec says otherwise.
5. **Client scoping** — Never silently default missing `client_id` to Pulseforge `1` on new code paths; fail closed.
6. **Shared pool** — Never call `pool.end()` in agents.
7. **Small diffs** — Implement the active spec slice; do not drive-by refactor.
8. **Backwards compatibility** — Preserve unless the spec/ADR explicitly approves a break.

---

## Repository structure

```text
├── README.md, CURRENT_STATE.md, PROJECT_CONTEXT.md, CONTRIBUTING.md, …
├── packages/
│   └── knowledge/        # SPEC-001A knowledge layer (KnowledgeService)
├── docs/
│   ├── 00_START_HERE.md
│   ├── vision/           # product philosophy
│   ├── architecture/     # engineering design
│   ├── specs/            # implementation contracts
│   ├── adr/              # decision records
│   ├── releases/         # version plans
│   └── *.md              # operational runbooks (legacy flat)
├── server.js, routes/, services/, utils/, migrations/
├── *Agent.js, leadgen.js # agents
├── CLAUDE.md, AGENTS.md, AGENT_RULES.md  # operational agent reference
└── test/, scripts/, public/
```

Knowledge graph operations must go through `packages/knowledge` (`KnowledgeService` / events). Do not write a storage-specific graph client from agents.
---

## Naming conventions

- Specs: `SPEC-NNN_Title.md`
- ADRs: `ADR-NNN_Title.md`
- Agents: `{name}Agent.js` (Scout is `leadgen.js`)
- Migrations: `YYYY-MM-DD-kebab-description.sql`
- Routes: domain files under `routes/`

---

## Agent responsibilities (summary)

| Agent | Responsibility |
|---|---|
| Scout (`leadgen.js`) | Discover, enrich, score, insert companies/prospects |
| setterHandoff | Qualify `setter_visible` from ICP threshold |
| Emmett | Outbound email sequences (Brevo) |
| Riley | Inbound triage + Brevo event handling |
| Max | Manager briefing; orchestration scoring/recommendations (shadow-first) |
| Paige / Link / Faye / Vera | Content drafts → `pending_comments` for approval |
| Sam / Cal / Cal Batch | SMS / calendar / Bland calling |
| Rex / Analytics / Penny / Sketch / Mira* | Reporting, analytics, finance, mockups, call intelligence |

Operational failure modes: `AGENT_RULES.md`. Architecture detail: `docs/architecture/Agent_Architecture.md`.

---

## Max responsibilities

**Today:** daily briefing; shadow orchestration (lifecycle signals, warmth scores, skipped recommendations under `SHADOW_MODE`).

**Direction (SPEC-002):** reasoning over the Business Knowledge Graph — explainable recommendations, never silent irreversible outbound without approval gates defined in ADRs.

Max does **not** own public posting or bypass DNC / client scope.

---

## Architectural principles

1. Conversation-first product surface (ADR-001)
2. Explainable AI — every material score/decision has an audit trail (ADR-002)
3. Human approval for customer-visible actions (ADR-003)
4. Knowledge graph as durable business memory (ADR-004)
5. Multi-tenant isolation via `client_id`
6. Migrations are explicit (`npm run db:migrate`); no silent production DDL

---

## Documentation standards

- Vision docs describe *why* and *what* — not SQL or route tables.
- Architecture docs describe *how* the system is shaped — not sprint status.
- Specs are the contract for implementation.
- ADRs record irreversible or hard-to-reverse choices.
- CURRENT_STATE is the only place for “what are we doing this week.”

---

## Spec workflow

1. Draft from `docs/specs/TEMPLATE.md`.
2. Link vision + ADRs.
3. Set as Current Spec in CURRENT_STATE when approved.
4. Implement behind PR checklist in CONTRIBUTING.md.
5. Close out Acceptance Criteria; point Next Spec forward.

---

## Review process

See CONTRIBUTING.md. AI contributors must self-check the PR checklist before declaring done.

---

## Safe places to change

| Goal | Start here |
|---|---|
| New product capability | Spec → ADR if needed → `services/` + `routes/` + migration |
| Agent behavior | Agent module + `AGENT_RULES.md` + tests |
| Schema | `migrations/` + schema util + tests |
| Operator UI | `public/` + matching route |
| Docs-only | This hierarchy; keep CURRENT_STATE honest |

Do **not** add production senders, disable shadow flags, or deploy Inquiry Foundation without an explicit approved spec and operator authorization.
