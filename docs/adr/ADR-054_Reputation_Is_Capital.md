# ADR-054 — Reputation Is Capital

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-19 |
| **Spec** | [SPEC-117](../specs/SPEC-117_Emmett_Outbound_Infrastructure_Intelligence.md) |
| **Related** | [ADR-003](ADR-003_Human_Approval.md), [ADR-021](ADR-021_Human_Approval_Before_Execution.md), [ADR-017](ADR-017_Intelligence_Before_Execution.md), [ADR-008](ADR-008_Outcome_Intelligence.md) |

## Context

The product brief called this SPEC-110. Repository SPEC-110 is Max Business Intelligence Synthesis. This decision belongs to **SPEC-117**.

Every email spends sender reputation. Static daily caps (`gmail = 50`) treat volume as a quota. Emmett currently writes copy, selects prospects, and sends — mixing copywriting, BDR work, and infrastructure protection. Autosend can fire without a human-approved plan, which conflicts with ADR-003.

Pilot 0 needs outbound that is safe, observable, and evidence-based. Emmett's job is protecting the operator's ability to reach prospects tomorrow.

## Decision

**Reputation is capital. Emmett protects that capital.**

1. **Emmett is not a copywriter, BDR, or sales strategist.** Scout discovers. Max decides. Paige communicates. Emmett protects and executes.
2. **Capacity is reasoned, not static.** Inbox health, authentication, warmup, bounce/reply/open/complaint rates, age, velocity, and operator overrides produce an explainable recommended send volume. "You can send 50" is replaced by "Based on today's reputation, I recommend 18."
3. **Inbox Health is explainable.** A 0–100 score is never presented without reasons.
4. **The Safe Send Governor evaluates every send.** Outcomes are Proceed, Slow, Pause, or Emergency. Pause and Emergency stop sending.
5. **Max cannot override Pause or Emergency silently.** Clearing a halt requires explicit operator acknowledgement.
6. **Human approval remains mandatory before sends.** Today's queue is a recommendation. The operator approves. Then Emmett may execute up to the approved capacity.
7. **Outcomes are evidence.** Delivery, open, reply, bounce, unsubscribe, spam complaint, meeting booked, opportunity created, and revenue persist and route into Paige, Scout, Max, and Emmett learning — they do not auto-mutate strategy.

## Consequences

### Positive

- Operators keep the ability to send tomorrow
- Volume tracks reputation instead of a provider ceiling
- Specialist boundaries stay intact
- Emergency stops are auditable and require a human

### Negative / tradeoffs

- Legacy autosend without an approved daily plan does not send
- Paige must provide subject/body/CTA before a queue item is sendable (legacy sequences require an explicit operator override)
- v1 scoring is deterministic heuristics, not ISP-specific reputation models

### Follow-ups

- [x] SPEC-117 Emmett Outbound Infrastructure Intelligence (v1 thin slice)
- [ ] Multi-domain orchestration and automatic reputation balancing
- [ ] ISP-specific reputation modeling
- [ ] Adaptive send-time optimization from recipient behavior
