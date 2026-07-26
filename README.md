# Pulseforge

**AI-powered business intelligence and multi-agent outreach for local service companies.**

Pulseforge scrapes and scores leads, runs multi-channel outreach through a roster of named AI agents, and surfaces pipeline truth through authenticated operator dashboards. It is evolving from a lead-gen CRM into a conversation-first operating system grounded in a business knowledge graph and an explainable reasoning engine (Max).

| | |
|---|---|
| **Version** | v0.9.2 |
| **Current milestone** | Command Deck UI (v1.0) — consume `GET /api/v1/command-deck` |
| **Current spec** | [SPEC-006](docs/specs/SPEC-006_Command_Deck.md) (Approved) |
| **Last shipped** | [SPEC-007](docs/specs/SPEC-007_Command_Deck_Composition_Engine.md) Composition Engine |
| **Next** | Implement Command Deck UI; parallel shadow CRM → GraphSyncEngine |
| **Deploy** | Railway · `node server.js` |

---

## Start here (15 minutes)

1. **[docs/00_START_HERE.md](docs/00_START_HERE.md)** — navigation map
2. **[CURRENT_STATE.md](CURRENT_STATE.md)** — what is true *right now*
3. **[docs/vision/Mission.md](docs/vision/Mission.md)** — why Pulseforge exists
4. **[PROJECT_CONTEXT.md](PROJECT_CONTEXT.md)** — required reading for AI contributors
5. **[docs/architecture/System_Architecture.md](docs/architecture/System_Architecture.md)** — how the system is shaped

Product philosophy lives under `docs/vision/`. Engineering truth lives under `docs/architecture/`, `docs/specs/`, and `docs/adr/`. Operational runbooks remain under `docs/` (legacy flat files) until migrated.

---

## What this repository is

| Layer | Role |
|---|---|
| `server.js` + `routes/` | Authenticated Express app, cron, webhooks |
| `*Agent.js` | Named agents (Max, Emmett, Scout/`leadgen.js`, Riley, …) |
| `services/` + `utils/` | Domain services and shared helpers |
| `migrations/` | Tracked PostgreSQL schema |
| `docs/` | Source of truth for vision, architecture, specs, ADRs, releases |

Legacy architecture notes also live in `CLAUDE.md` and `AGENTS.md`. Prefer the new hierarchy for product and planning decisions; keep those files for operational agent detail until they are folded in.

---

## Quick start (local)

```bash
cp .env.example .env   # if present; otherwise set DATABASE_URL and secrets locally
npm install
npm test
npm start              # node server.js
```

Staging/production require `DATABASE_URL` and a non-empty `CRON_SECRET`. Migrations are explicit:

```bash
npm run db:migrate:status
npm run db:migrate
```

See [docs/architecture/Deployment.md](docs/architecture/Deployment.md) and [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

---

## Documentation map

| Path | Purpose |
|---|---|
| [CURRENT_STATE.md](CURRENT_STATE.md) | Project heartbeat — version, sprint, blockers |
| [PROJECT_CONTEXT.md](PROJECT_CONTEXT.md) | AI contributor onboarding |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Spec workflow, PR rules, review process |
| [DECISIONS.md](DECISIONS.md) | Index of architectural decisions |
| [CHANGELOG.md](CHANGELOG.md) | Human-readable release history |
| [docs/vision/](docs/vision/) | Mission, thesis, constitution, roadmap |
| [docs/architecture/](docs/architecture/) | System, data, agents, memory, KG, security, deploy |
| [docs/specs/](docs/specs/) | Implementation specs (SPEC-NNN) |
| [docs/adr/](docs/adr/) | Architecture Decision Records |
| [docs/releases/](docs/releases/) | Release plans v0.7 → v1.0 |

---

## Development rules (summary)

Every pull request must:

1. Update `CURRENT_STATE.md` if project state changes.
2. Update `CHANGELOG.md`.
3. Create an ADR if architecture changes.
4. Link back to the relevant spec.
5. Preserve backwards compatibility unless explicitly approved.

Full rules: [CONTRIBUTING.md](CONTRIBUTING.md).

---

## License

ISC — see `package.json`.
