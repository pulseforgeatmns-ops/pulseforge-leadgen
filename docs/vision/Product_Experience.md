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

## Current surfaces (as of v0.9.2+)

| Surface | Audience | Notes |
|---|---|---|
| `/command-deck` | Admin/manager/viewer/client | Intelligence briefing + investigation graph + live evolution + adaptive presentation (SPEC-008 / SPEC-010 / SPEC-011 / SPEC-012) |
| `/dashboard` | Admin/manager/viewer | Agents, activity, analytics (still available) |
| `/setter` | Setter | Queue, callbacks, hot flags |
| `/closer` | Closer | Booked pipeline, commissions |
| `/approvals` | Operators | Pending social/content |
| `/operator/command-center` | Internal | Inquiry attention view; not production-authorized |
| Cron + webhooks | System | Agent triggers, Brevo/Bland events |

## Near-term experience bets

- **Command Deck UI (SPEC-008)** as the render-only briefing surface — already on `/command-deck`
- **Max Intelligence Workspace (SPEC-009)** — contextual Ask Max modal; LLM as presentation only (ADR-005)
- **Intelligence Navigation (SPEC-010)** — trail, Related Intelligence, Company + Recommendation destinations; continuous investigation on `/command-deck`
- **Live Intelligence Loop (SPEC-011)** — soft evolution, Max awareness, material-only notifications (ADR-006)
- **Operator Intelligence (SPEC-012)** — learn what operators act on; adapt presentation and Max chips only (ADR-007)
- **Outcome Intelligence (SPEC-013)** — measure whether recommendations were right; calibrate confidence empirically; never change reasoning (ADR-008)
- Highest Leverage Action + Priority Queue powered via composer (no UI-side ranking)
- Ask Max as contextual investigation (not navigation); graph clicks navigate; Max keeps trail focus
- Operator Command Center as the inquiry attention model (local → later prod when authorized)
- Max recommendations visible with explanations before any non-shadow action

## Anti-patterns

- Dashboard-only design that hides why a lead is warm
- Autopilot sends from intake without approval
- Cross-client bleed in queues or metrics
