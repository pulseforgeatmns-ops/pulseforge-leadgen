# Data Architecture

## Principles

- PostgreSQL is the system of record.
- `client_id` is the tenancy key.
- Prefer append-only evidence (`touchpoints`, `agent_log`, inquiry events, Max decision audits) over silent overwrites.
- Schema changes ship as tracked files in `migrations/` and apply via `npm run db:migrate`.
- Prospect segment field is `vertical` (TEXT), not `industry`.

## Core tables (current)

| Table | Role |
|---|---|
| `clients` | Tenant config, brand, sequences, service area, agent settings, scoring_profile |
| `companies` | Scraped organizations |
| `prospects` | Contacts, ICP, status, setter/closer fields, DNC |
| `touchpoints` | Channel actions and outcomes |
| `agent_log` | Agent run audit |
| `agent_actions` | Action cards for humans |
| `pending_comments` | Content awaiting approval |
| `activity_log` | Setter contact log |
| `users` | Auth roles |
| `email_performance` | Sequence/subject metrics |
| `commissions` | Closer commission records |
| `session` | express-session store |
| Inquiry / outbox / quotes | Foundation tables (see migrations + Inquiry docs) |
| Max orchestration tables | Lifecycle, scores, decisions, actions (shadow-oriented) |

## Scoring profiles

`clients.scoring_profile`:

- `NULL` — default Pulseforge lead-gen rubric
- `'cleaning_buyer'` — Anchor commercial-cleaning buyer rubric (`scoreCleaningLead`)

Setter visibility threshold authority: `utils/setterVisibility.js` (typically `icp_score >= 70`).

## Migration policy

| Environment | Behavior |
|---|---|
| development / test | Optional boot DDL helpers for Inquiry; migrate command supported |
| staging / production | No silent DDL; require `DATABASE_URL` + `CRON_SECRET`; migrate explicitly |

Rollback SQL (`*.rollback.sql`) is never auto-applied.

## Future (SPEC-001)

Knowledge Graph tables or projections will sit beside (then gradually unify) these entities with explicit provenance. Until then, do not invent a second conflicting CRM model in application code.

Detailed operational schema notes: `CLAUDE.md` Database Schema section.
