# System Architecture

## Runtime

```text
                    ┌──────────────────────┐
   Browser ────────►│  Express (server.js) │
                    │  session auth        │
                    └──────────┬───────────┘
           ┌───────────────────┼───────────────────┐
           ▼                   ▼                   ▼
      routes/api          routes/cron         routes/webhooks
      routes/setter       routes/approvals    routes/inquiries
      routes/closer       routes/operator     …
           │                   │                   │
           └───────────────────┼───────────────────┘
                               ▼
                    ┌──────────────────────┐
                    │ services/ + utils/   │
                    │ *Agent.js modules    │
                    └──────────┬───────────┘
                               ▼
                    ┌──────────────────────┐
                    │ PostgreSQL (shared   │
                    │ pg.Pool via db.js)   │
                    └──────────────────────┘
                               ▲
         Brevo / Twilio / Bland / Google / Prospeo / Hunter / …
```

**Entry point:** `node server.js` (Railway `railway.json`).

**Shared DB pool:** All agents import `db.js`. Never call `pool.end()` inside an agent.

## Route ownership

| Area | Module |
|---|---|
| Auth, session, core pages | `server.js` |
| Dashboard APIs | `routes/api.js` |
| Cron triggers | `routes/cron.js` (`CRON_SECRET`) |
| Webhooks | `routes/webhooks.js` |
| Approvals / pending comments | `routes/approvals.js` |
| Setter | `routes/setter.js` |
| Closer | `routes/closer.js` |
| Inquiries / operator | `routes/inquiries.js`, operator routes |

New HTTP endpoints go under `routes/`, not back into `server.js` except auth/session necessities.

## Multi-tenancy

Almost all primary tables carry `client_id`. Agents accept `client_id` via cron/API query params. Session stores `active_client_id` for dashboard context. New code must fail closed on missing client scope.

## Control planes

1. **Human dashboards** — CRUD-ish operator work
2. **Cron** — scheduled agent runs
3. **Webhooks** — Brevo/Bland (and future calendar) events
4. **Approvals** — gate before publish/send
5. **Outbox** — durable send intent (Inquiry path; shadow/local)

## Evolution

Toward Knowledge Graph memory + Max reasoning + conversation surfaces — see vision Intelligence Architecture. Existing tables remain the system of record until SPEC-001 migrates/projects them.

Related: [Data_Architecture.md](Data_Architecture.md), [Agent_Architecture.md](Agent_Architecture.md), [Deployment.md](Deployment.md), [Security.md](Security.md).
