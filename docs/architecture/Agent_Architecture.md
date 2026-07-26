# Agent Architecture

## Model

Each agent is a standalone Node module. Triggers:

- `POST /api/run/:agent` (dashboard)
- `GET|POST /cron/:agent?secret=…&client_id=…` (Railway cron)

Agents read/write Postgres through the shared pool and log to `agent_log` / `touchpoints` / domain tables.

## Roster (abbreviated)

| Module | Name | Role |
|---|---|---|
| `leadgen.js` | Scout | Discover, enrich, score, insert |
| `setterHandoffAgent.js` | handoff_utility | `setter_visible` qualification |
| `emmettAgent.js` | Emmett | Outbound email |
| `rileyAgent.js` | Riley | Inbound triage |
| `maxAgent.js` | Max | Briefing + orchestration entry |
| `maxDecayAgent.js` | Max decay | Warmth/time decay jobs |
| `paigeAgent.js` | Paige | Content drafts → approval |
| `linkedinAgent.js` / `facebookAgent.js` | Link / Faye | Social comments → approval |
| `veraAgent.js` | Vera | Review responses |
| `samAgent.js` | Sam | SMS |
| `calAgent.js` / `calBatchAgent.js` | Cal | Calendar / Bland batch |
| `warmSignalAgent.js` | Warm Signal | Multi-open flags |
| `enrichProspects.js` / tiered enrichment | Enrichment | Phone/email backfill |
| `rexAgent.js`, `analyticsAgent.js`, … | Reporting / analytics | Metrics |

Operational rules and failure modes: root `AGENT_RULES.md`.

## Boundaries

```text
Evidence in  →  Agent decides  →  Log + optional mutate  →  Human approval if customer-visible
```

- **DNC** before any outreach
- **Client scope** on every query
- **Paige/Link/Faye/Vera** do not publish directly — `pending_comments` + `publishPipeline.js`
- **Max orchestration** defaults to shadow; recommendations may be recorded as skipped
- **Inquiry intake** never auto-sends externally

## Max specifically

| Mode | Behavior |
|---|---|
| Briefing | Snapshot + AI summary email/digest (legacy agent) |
| Orchestration (shadow) | Scores, decisions, skipped actions |
| Reasoning engine (SPEC-002 / v0.8.0) | Graph-aware recommendations with explanations (`packages/max`) |
| Temporal memory (SPEC-003 / v0.8.1) | Snapshot diffs, trends, watches — transition tracking (`packages/max/memory`) |
| Briefing engine (SPEC-004 / v0.9.0) | Assembles Knowledge + Reasoning + Memory into structured briefings (`packages/max/briefing`) |
| Policy engine (SPEC-005 / v0.9.1) | Evaluates recommendations against tenant rules — allow/warn/requireApproval/block (`packages/max/policy`) |
| Command Deck composer (SPEC-007 / v0.9.2) | Assembles Briefing + Policy into one immutable CommandDeckModel (`packages/max/commandDeck`); `GET /api/v1/command-deck` |

Max must not bypass approval constitution or DNC.

## Adding an agent

1. Spec the behavior.
2. Implement module; register in cron map / dashboard run list as needed.
3. Log consistently (`agent_name` stable string).
4. Document in PROJECT_CONTEXT / Agent Architecture / AGENT_RULES if operationally sharp.
5. Default dangerous side effects off.
