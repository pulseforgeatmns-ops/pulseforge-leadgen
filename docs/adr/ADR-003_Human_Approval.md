# ADR-003 — Human Approval

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-26 |
| **Spec** | SPEC-000 (policy) |
| **Supersedes** | — |

## Context

Pulseforge posts and messages represent real brands (Pulseforge, MSHI, Anchor, etc.). Existing patterns already gate social content via `pending_comments` and inquiry external sends via approve-response. Autonomous send is a trust cliff.

## Decision

**Customer-visible** actions (public posts, external emails/SMS to prospects or inquirers) require an **explicit human approval path**, unless a future ADR documents a narrow, audited exception with clear blast-radius limits.

Internal-only artifacts (drafts, shadow recommendations, operator alerts to configured owner emails as system notifications) may proceed under existing agent rules without this ADR’s customer-visible bar—but must still honor DNC, tenancy, and secret scrubbing.

New automation ships **shadow / default-off** before any approval-bypass discussion.

## Consequences

### Positive

- Brand safety and constitutional alignment
- Clear product promise to clients

### Negative / tradeoffs

- Lower fully-automatic throughput
- Requires good approval UX (already partially present)

### Follow-ups

- Keep Inquiry and Paige/Link/Faye/Vera gates intact
- Any auto-send proposal needs a new ADR + spec
