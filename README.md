# Pulseforge

**A production-oriented, multi-tenant AI operating system: probabilistic reasoning inside deterministic execution boundaries.**

Pulseforge is a modular AI platform built to reduce operational burden for service businesses. Rather than acting as a standalone chatbot, it combines specialized AI agents, workflow orchestration, knowledge management, and human-governed decision support into a production-oriented operating system for business operations.

This repository is intentionally public as an engineering portfolio: it shows the architecture, implementation history, specifications, tests, and decision records behind the platform.

| | |
|---|---|
| **Current version** | v0.9.2 |
| **Runtime** | Node.js / Express / PostgreSQL |
| **Primary architecture** | Mission-first routing + bounded specialist capabilities + evidence + governed state and execution |
| **Deployment target** | Railway (`node server.js`) |
| **Portfolio** | [portfolio.jacobmaynard.co](https://portfolio.jacobmaynard.co) |
| **LinkedIn** | [linkedin.com/in/jacob-maynard7](https://www.linkedin.com/in/jacob-maynard7/) |

---

## What This Project Demonstrates

Pulseforge is not a single chatbot or prompt wrapper. It is a modular platform organized around explicit service boundaries and governed execution.

Core capabilities include:

- **Prospect intelligence** - discovery, enrichment, scoring, and prioritization.
- **CRM automation** - lifecycle state, setter/closer workflows, and client-scoped pipeline views.
- **Workflow orchestration** - mission-first routing, canonical execution requests, specialist contracts, and transactional stage commits.
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

The canonical acquisition mission path is:

```text
Operator objective / approval (workspace or API)
    → Mission ownership + Canonical Execution Request
    → Execution Router: permission, policy, runtime checks
    → Transactional Mission Execution
    → Specialist contract: Scout / Max / Paige / Emmett
    → Validated contributions + lifecycle state → PostgreSQL
    → Mission inspection / next operator decision

Prepared outbound + bound execution approval
    → Explicit execution request → outbound adapter → provider evidence
```

**Max reasons over objectives and evidence; software governs its available capabilities, mission transitions, tenant scope, approval requirements, and execution.** See the [canonical router specification](docs/specs/SPEC-171_Canonical_Execution_Router.md) and the [recruiter guide's implementation evidence](docs/RECRUITER_GUIDE.md#what-to-look-for). Legacy agent/cron paths and other mission domains still coexist; the diagram describes the canonical acquisition path.

---

## Recommended Review Path

If you are evaluating this repository for an AI engineering, solutions architecture, or forward-deployed engineering role, start here:

| Time | Read | Why |
|---:|---|---|
| 2 min | [Recruiter guide](docs/RECRUITER_GUIDE.md) | What Jacob built and where to inspect the work |
| 5 min | [Current architecture and evidence](docs/RECRUITER_GUIDE.md#what-to-look-for) + [SPEC-171](docs/specs/SPEC-171_Canonical_Execution_Router.md) | Mission, capability, state, and execution boundaries |
| 5 min | [AUDIT-049](docs/audits/AUDIT-049_Mission_Runtime_Ownership_Crossover.md), or another [selected audit](docs/RECRUITER_GUIDE.md#selected-engineering-audits--proof-of-work) | A real failure and its investigation |
| 5 min | [SPEC-170](docs/specs/SPEC-170_Mission_Runtime_Ownership_Boundaries.md) + [ownership regression tests](packages/acquisition-mission/tests/spec170MissionRuntimeOwnership.test.js) | The resulting contract and tested correction |
| Optional | [Max workspace](packages/max/workspace/), [mission engine](packages/mission-engine/), [knowledge](packages/knowledge/README.md), [specialist contracts](packages/acquisition-mission/SpecialistExecutionContract.js), [ADRs](docs/adr/README.md) | Implementation depth and tradeoffs |

---

## Repository Map

| Path | Purpose |
|---|---|
| `server.js`, `routes/` | Express application, authenticated pages, API routes, cron endpoints, webhooks |
| `*Agent.js` | Named operational agents including Scout, Emmett, Riley, Max, Paige, Vera, and routing agents |
| `packages/max/` | Reasoning engine, policy engine, command deck, live loop, operator intelligence, outcome intelligence |
| `packages/knowledge/` | Storage-agnostic knowledge graph, event bus, evidence, claims, query, sync, Postgres repository |
| `packages/mission-engine/` | Mission planning, artifact resolution, execution routing, intent understanding |
| `packages/acquisition-mission/` | Acquisition lifecycle, canonical execution, specialist contracts, approval binding, mission inspection |
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
- **Tenant isolation by default** - acquisition missions use `tenantId` / `tenant_id`; CRM records use `client_id`. Canonical mission entry points reject missing or mismatched tenant scope.
- **Observability over opacity** - agent work is logged, reviewed, and explainable.
- **Architecture through specs and ADRs** - meaningful changes are captured in durable design records.

---

## Running Locally

```bash
npm install
npm test
npm start
```

Production and staging require environment-specific secrets such as `DATABASE_URL`, `CRON_SECRET`, and provider credentials. Schema changes are recorded in [migrations/](migrations/); follow the applicable migration's deployment instructions.

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

Some tests require local or disposable PostgreSQL configuration. The [recruiter guide](docs/RECRUITER_GUIDE.md#selected-engineering-audits--proof-of-work) links focused regression tests for the selected engineering cases.

---

## Status

Pulseforge remains an actively developed founder-led platform. A dated repository architecture snapshot and historical operating notes live in [CURRENT_STATE.md](CURRENT_STATE.md). Release history lives in [CHANGELOG.md](CHANGELOG.md) and [docs/releases/](docs/releases/).

For a concise external summary, see the portfolio:

[https://portfolio.jacobmaynard.co](https://portfolio.jacobmaynard.co)

---

## License

ISC - see [package.json](package.json).
