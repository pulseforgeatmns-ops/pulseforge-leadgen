# Pulseforge

**A modular AI platform for workflow automation, operational intelligence, and human-governed decision support.**

Pulseforge is a modular AI platform built to reduce operational burden for service businesses. Rather than acting as a standalone chatbot, it combines specialized AI agents, workflow orchestration, knowledge management, and human-governed decision support into a production-oriented operating system for business operations.

This repository is intentionally public as an engineering portfolio: it shows the architecture, implementation history, specifications, tests, and decision records behind the platform.

| | |
|---|---|
| **Current version** | v0.9.2 |
| **Runtime** | Node.js / Express / PostgreSQL |
| **Primary architecture** | Multi-agent workflows + knowledge graph + deterministic reasoning + human approval |
| **Deployment target** | Railway (`node server.js`) |
| **Portfolio** | [portfolio.jacobmaynard.co](https://portfolio.jacobmaynard.co) |
| **LinkedIn** | [linkedin.com/in/jacob-maynard7](https://www.linkedin.com/in/jacob-maynard7/) |

---

## What This Project Demonstrates

Pulseforge is not a single chatbot or prompt wrapper. It is a modular platform organized around explicit service boundaries and governed execution.

Core capabilities include:

- **Prospect intelligence** - discovery, enrichment, scoring, and prioritization.
- **CRM automation** - lifecycle state, setter/closer workflows, and client-scoped pipeline views.
- **Workflow orchestration** - named agents, cron/API triggers, routing, and operational runbooks.
- **Knowledge management** - graph-backed memory, claims, evidence, query, timeline, and explainability surfaces.
- **Reasoning and recommendations** - Max reasoning, policy checks, command deck composition, and outcome review.
- **Human-in-the-loop controls** - approval gates before customer-visible actions.
- **Reporting and observability** - dashboards, agent logs, production readiness checks, and release evidence.

The project has been validated against real operating constraints through Anchor Cleaning and other local-service workflows.

---

## Why I Built Pulseforge

I spent more than a decade operating service businesses where the biggest constraint wasn't finding ideas. It was having enough time and attention to execute them consistently.

Pulseforge started as a lead generation system for my own business and evolved into a modular AI platform for operational intelligence. Every major subsystem in this repository was built to solve a real operational problem before being generalized into reusable architecture.

---

## Architecture at a Glance

```text
Operator / Dashboard
        |
        v
Express App + Auth + Routes
        |
        v
Agent & Workflow Layer
        |
        +--> Prospect Intelligence
        +--> CRM Automation
        +--> Communications
        +--> Reporting
        +--> Max Reasoning
        |
        v
Knowledge + Evidence Layer
        |
        v
Policy / Approval / Execution Boundaries
        |
        v
External Systems
Brevo · Twilio · Bland · Google · Prospeo · Hunter · Stripe
```

**The important architectural constraint: language models are used inside governed workflows. They do not silently execute customer-visible actions, overwrite business truth, or bypass approval policy.**

---

## Recommended Review Path

If you are evaluating this repository for an AI engineering, solutions architecture, or forward-deployed engineering role, start here:

| Time | Read | Why |
|---:|---|---|
| 2 min | [docs/RECRUITER_GUIDE.md](docs/RECRUITER_GUIDE.md) | Fast map of the portfolio-relevant parts of the repo |
| 5 min | [docs/00_START_HERE.md](docs/00_START_HERE.md) | Contributor orientation and documentation hierarchy |
| 5 min | [docs/vision/Product_Thesis.md](docs/vision/Product_Thesis.md) | Product thesis and operating philosophy |
| 10 min | [docs/architecture/System_Architecture.md](docs/architecture/System_Architecture.md) | Runtime topology, route ownership, and control planes |
| 10 min | [packages/max/README.md](packages/max/README.md) | Reasoning, policy, briefing, command deck, memory, and outcome intelligence |
| 10 min | [packages/knowledge/README.md](packages/knowledge/README.md) | Knowledge graph, evidence, claims, query engine, and storage abstraction |
| 10 min | [docs/adr/README.md](docs/adr/README.md) | Architecture decisions and design rationale |

---

## Repository Map

| Path | Purpose |
|---|---|
| `server.js`, `routes/` | Express application, authenticated pages, API routes, cron endpoints, webhooks |
| `*Agent.js` | Named operational agents including Scout, Emmett, Riley, Max, Paige, Vera, and routing agents |
| `packages/max/` | Reasoning engine, policy engine, command deck, live loop, operator intelligence, outcome intelligence |
| `packages/knowledge/` | Storage-agnostic knowledge graph, event bus, evidence, claims, query, sync, Postgres repository |
| `packages/mission-engine/` | Mission planning, artifact resolution, execution routing, intent understanding |
| `packages/capabilities/` | Capability framework for discovery, ranking, sales intelligence, business intelligence, playbooks, inbox |
| `packages/reasoning-runtime/` | Domain-neutral runtime for reasoning providers and strategy packs |
| `packages/eql/` | Evidence Query Language parser, planner, executor, and tests |
| `public/` | Operator dashboards, command deck UI, scorecard, shared browser assets |
| `docs/vision/` | Mission, product thesis, constitution, roadmap, intelligence architecture |
| `docs/architecture/` | System, agent, data, memory, knowledge graph, security, deployment |
| `docs/specs/` | Numbered implementation specs and acceptance criteria |
| `docs/adr/` | Architecture Decision Records |
| `migrations/` | PostgreSQL schema evolution |
| `test/`, `packages/**/tests/` | Node test suites and package-level validation |

---

## Design Principles

- **Deterministic before autonomous** - use explicit state and workflow contracts where business risk is high.
- **Evidence before recommendation** - recommendations must be explainable from stored business signals.
- **Human approval before execution** - customer-visible actions require review unless explicitly permitted.
- **Tenant isolation by default** - business state is scoped and guarded by `client_id`.
- **Observability over opacity** - agent work is logged, reviewed, and explainable.
- **Architecture through specs and ADRs** - meaningful changes are captured in durable design records.

---

## Running Locally

```bash
npm install
npm test
npm start
```

Production and staging require environment-specific secrets such as `DATABASE_URL`, `CRON_SECRET`, and provider credentials. Migrations are explicit:

```bash
npm run db:migrate:status
npm run db:migrate
```

See [docs/architecture/Deployment.md](docs/architecture/Deployment.md) for deployment notes.

---

## Test Surfaces

The repository includes focused test scripts for major subsystems:

```bash
npm test
npm run test:knowledge
npm run test:max
npm run test:mission
npm run test:capabilities
npm run test:eql
npm run test:replay
```

Some tests require local or disposable PostgreSQL configuration. The core documentation cleanup in this branch does not change runtime behavior.

---

## Status

Pulseforge remains an actively developed founder-led platform. Current sprint and production state live in [CURRENT_STATE.md](CURRENT_STATE.md). Release history lives in [CHANGELOG.md](CHANGELOG.md) and [docs/releases/](docs/releases/).

For a concise external summary, see the portfolio:

[https://portfolio.jacobmaynard.co](https://portfolio.jacobmaynard.co)

---

## License

ISC - see [package.json](package.json).
