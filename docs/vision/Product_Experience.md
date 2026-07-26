# Product Experience

## Guiding experience

Pulseforge should feel like a **trusted operating partner**: you ask what matters, you see why, you approve the next move, and the system remembers.

## Experience principles

1. **Conversation first** — Primary interaction trends toward natural language over the business graph (ADR-001), with dashboards as focused instruments, not the only surface.
2. **One composition of truth** — Counts and statuses match underlying records; no decorative fake metrics.
3. **Attention over inventory** — Surfaces highlight what needs a human now (due work, blocked sends, warm signals), not every row in the database.
4. **Explain then act** — Recommendations show evidence before commit.
5. **Role-fit** — Setters, closers, admins, and client users see only what their role and client scope allow.
6. **Calm automation** — Background agents work quietly; interruptions are actionable.

## Current surfaces (as of v0.7.0)

| Surface | Audience | Notes |
|---|---|---|
| `/dashboard` | Admin/manager/viewer | Agents, activity, analytics |
| `/setter` | Setter | Queue, callbacks, hot flags |
| `/closer` | Closer | Booked pipeline, commissions |
| `/approvals` | Operators | Pending social/content |
| `/operator/command-center` | Internal | Inquiry attention view; not production-authorized |
| Cron + webhooks | System | Agent triggers, Brevo/Bland events |

## Near-term experience bets

- Operator Command Center as the inquiry attention model (local → later prod when authorized)
- Max recommendations visible with explanations before any non-shadow action
- Conversation UI over KG queries (post SPEC-001/002)

## Anti-patterns

- Dashboard-only design that hides why a lead is warm
- Autopilot sends from intake without approval
- Cross-client bleed in queues or metrics
