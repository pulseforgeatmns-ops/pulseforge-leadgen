# SPEC-117 — Emmett Outbound Infrastructure Intelligence

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Emmett |
| **Created** | 2026-08-19 |
| **Depends on** | [ADR-003](../adr/ADR-003_Human_Approval.md), [SPEC-093](SPEC-093_Paige_Outcome_Learning_Loop.md), [SPEC-092](SPEC-092_Content_Outcome_Intelligence.md), [SPEC-094](SPEC-094_Max_to_Paige_Campaign_Content_Delegation.md), [SPEC-100](SPEC-100_Max_Scout_Acquisition_Intelligence_Loop.md), [SPEC-013](SPEC-013_Outcome_Intelligence.md) |
| **ADR** | [ADR-054 Reputation Is Capital](../adr/ADR-054_Reputation_Is_Capital.md) |

> **Numbering note:** The product brief called this SPEC-110. Repository SPEC-110 is [Business Intelligence Synthesis](SPEC-110_Business_Intelligence_Synthesis.md) — Max's operator-facing conclusions. This spec is numbered **117**.

## Objective

Provide safe, observable, evidence-based outbound execution.

Emmett is not a copywriter.
Emmett is not a BDR.
Emmett is not a sales strategist.
Emmett is responsible for the health of outbound infrastructure.
Its job is protecting the operator's ability to continue reaching prospects tomorrow.

## Vision References

- [ADR-054 Reputation Is Capital](../adr/ADR-054_Reputation_Is_Capital.md)
- [ADR-003 Human Approval](../adr/ADR-003_Human_Approval.md)
- [ADR-021 Human Approval Before Execution](../adr/ADR-021_Human_Approval_Before_Execution.md)
- [ADR-017 Intelligence Before Execution](../adr/ADR-017_Intelligence_Before_Execution.md)
- [ADR-008 Outcome Intelligence](../adr/ADR-008_Outcome_Intelligence.md)
- [SPEC-094 Max to Paige Campaign Content Delegation](SPEC-094_Max_to_Paige_Campaign_Content_Delegation.md)
- [SPEC-093 Paige Outcome Learning Loop](SPEC-093_Paige_Outcome_Learning_Loop.md)
- [SPEC-100 Max ↔ Scout Acquisition Intelligence Loop](SPEC-100_Max_Scout_Acquisition_Intelligence_Loop.md)

## Philosophy

```text
Every email sent spends reputation.
Reputation is capital.
Emmett protects that capital.
```

Instead of:

> You can send 50.

Emmett should say:

> Based on today's reputation, I recommend 18.

## Problem

Outbound today uses static caps, mixed specialist roles, and send-then-hope:

| Today | Required |
|---|---|
| `gmail = 50` | Reasoned capacity from inbox evidence |
| Opaque bounce halt | Explainable Inbox Health 0–100 with reasons |
| Emmett writes and sends copy | Paige communicates; Emmett evaluates Safe / Queue / Capacity / Timing |
| Autosend without a daily plan | Operator approves today's queue before send |
| Max can be asked to "just send" | Max cannot silently override Pause / Emergency |
| Events stay in `email_events` | Outcomes route into Paige, Scout, Max, and Emmett learning |

## Scope (v1 thin slice)

1. Inbox Intelligence snapshot (health, history, volume, warmup, auth, bounce, reply, complaint)
2. Explainable Inbox Health 0–100
3. Capacity Intelligence — reasoned daily volume + confidence + tomorrow outlook
4. Safe Send Governor — Proceed / Slow / Pause / Emergency before every send
5. Operator acknowledgement required to clear Pause / Emergency; Max cannot override silently
6. Queue Intelligence — today's recommended N, ranked by priority, signal freshness, expected response, capacity, campaign diversity
7. Campaign pacing — vertical variation, not 100 identical emails
8. Deliverability recommendations that teach (timing, warmup, Friday afternoon)
9. Operator dashboard: health, capacity, scheduled vs remaining, warnings, suggestions
10. Human approval of today's plan is mandatory before sends
11. Outbound outcomes persist and fan out to Paige / Scout / Max / Emmett
12. Specialist boundaries enforced in the send gate
13. Competency `emmett_outbound_infrastructure`

## Out of Scope

- Emmett writing subject lines, bodies, or CTAs
- Emmett choosing ICP, priority, or sales strategy
- Multi-domain orchestration and automatic inbox balancing
- Infrastructure provisioning recommendations
- ISP-specific reputation models
- Adaptive send-time ML
- Cross-channel coordination (LinkedIn, SMS) — Max still decides the next touch
- Silent Max override of Pause / Emergency
- Auto-adopting learning into live campaigns

## Specialist boundaries

```text
Scout discovers.
Max decides.
Paige communicates.
Emmett protects and executes.
```

| Specialist | Provides | Must not |
|---|---|---|
| Scout | Prospects, buying signals | Send mail or set volume |
| Max | Priority, reasoning, business context | Override Pause/Emergency without the operator |
| Paige | Email, subject, CTA, variants | Set capacity or bypass the governor |
| Emmett | Safe? Queue? Capacity? Timing? | Write copy, pick ICP, or sell |

## Integration

```text
Scout provides Prospects
        ↓
Max provides Priority · Reasoning · Business Context
        ↓
Paige provides Email · Subject · CTA · Variants
        ↓
Emmett evaluates Safe? · Queue? · Capacity? · Timing?
        ↓
Operator approves
        ↓
Send
        ↓
Outcomes
        ↓
Max Memory · Paige Learning · Scout ICP · Emmett capacity
```

## Inbox Health

Every inbox receives **Inbox Health 0–100**. Never a bare number.

Factors: SPF, DKIM, DMARC, Warm-up, Bounce %, Reply %, Open %, Age, Blacklist status, Recent sending velocity, Historical consistency, Operator overrides.

Example:

```text
62
Reason
  High bounce rate this week
  Rapid increase in sending
  Domain still warming
  No blacklist concerns
```

## Capacity Intelligence

Capacity is reasoned from the snapshot. The provider ceiling (for example Gmail 50) is a cap, not the recommendation.

Example:

```text
Inbox age: 47 days
Domain: Properly authenticated
Reply rate: 9.4%
Open rate: 63%
Recent bounces: 0
Spam complaints: 0
Warm-up: Healthy

Today's safe capacity: 22 emails
Confidence: 0.84
Tomorrow this might become 31 or 15 or Pause entirely.
```

## Safe Send Governor

Before every send Emmett asks: **Should this email be sent?**

| Outcome | Meaning |
|---|---|
| Proceed | Healthy. Safe. Send. |
| Slow | Reduce today's volume. Increase tomorrow if metrics recover. |
| Pause | Reputation risk too high. Do not send. |
| Emergency | Possible blacklist. Stop immediately. |

Max cannot override Pause or Emergency silently. Operator acknowledgement is required.

## Queue Intelligence

Instead of "200 prospects", Emmett creates **Today's Queue**: recommended N, highest expected ROI, safest delivery order.

Ranked by: Max priority, buying-signal freshness, expected response, remaining inbox capacity, campaign diversity.

Pacing example: Law firms → CPA firms → Property Management → Medical → Restaurants.

## Deliverability Recommendations

Emmett teaches; it does not only execute.

Examples: pause Friday afternoon when that audience historically replies 37% less after 2PM; replies increased after Tuesday morning sends.

## Operator Dashboard

- Inbox: Healthy 82
- Today's Capacity: 22 recommended · 18 scheduled · 4 remaining
- Warnings: Domain warming — no action needed
- Suggestions: Increase gradually Monday

## Learning Loop

Every send becomes evidence. Store: Delivery, Open, Reply, Bounce, Unsubscribe, Spam complaint, Meeting booked, Opportunity created, Revenue.

Route into:

- Paige — messaging and creative
- Scout — ICP and buying-signal weighting
- Max — prioritization and recommendations
- Emmett — capacity estimation and deliverability decisions

Learnings never auto-mutate live campaigns.

## Architecture

```text
Inbox snapshot (auth, warmup, events, velocity, overrides)
        ↓
packages/emmett-outbound
  Health → Capacity → Governor → Queue + Pacing → Recommendations
        ↓
Operator dashboard / today's plan
        ↓
Operator approval
        ↓
evaluateSend() before every send
        ↓
Outcomes → learning sinks (Paige, Scout, Max, Emmett)
```

v1 reasoning is deterministic. No LLM invents a health score or a send decision.

## Data Model

Tables: `emmett_inbox_snapshots`, `emmett_send_plans`, `emmett_governor_acks`, `emmett_outbound_outcomes`, `emmett_outbound_learning`.

Tenant isolation: `tenant_id` / `client_id`. Cross-tenant reads fail closed.

Plan status: `draft` | `approved` | `superseded`. Sends require `approved` for the local send date.

## Implementation Plan

1. Spec + ADR-054 + competency
2. `packages/emmett-outbound` deterministic engine
3. Persistence + APIs + `/emmett-outbound` dashboard
4. Governor + approval gate in `emmettAgent.js`
5. Webhook outcomes → learning sinks
6. Tests

## Migration Strategy

Additive. `migrations/2026-08-19-emmett-outbound-intelligence.sql` plus rollback. Existing tenants start with no approved plan (fail closed: do not send). Schema is also ensured on first service use.

## Testing

- `packages/emmett-outbound/tests/eoi.test.js` — health explainability, capacity example, governor, Max override, approval, queue pacing, outcomes, boundaries, tenant isolation
- `test/emmettOutbound.test.js` — service, routes, competency, send gate, webhook fan-out

## Acceptance Criteria

- [x] Human approval remains mandatory before sends
- [x] Every send is evaluated by the Safe Send Governor
- [x] Inbox Health and Capacity are explainable, not opaque scores
- [x] Queue creation balances business priority with deliverability risk
- [x] All outbound outcomes are persisted and routed into Pulseforge learning systems
- [x] Specialist boundaries remain intact: Scout discovers. Max decides. Paige communicates. Emmett protects and executes.

## Future Work

Once the human-approved workflow is stable:

- Multi-domain orchestration
- Automatic reputation balancing across inboxes
- Infrastructure provisioning recommendations
- Deliverability anomaly detection
- ISP-specific reputation modeling
- Adaptive send-time optimization using recipient behavior
- Cross-channel coordination with Max deciding the best next touch
