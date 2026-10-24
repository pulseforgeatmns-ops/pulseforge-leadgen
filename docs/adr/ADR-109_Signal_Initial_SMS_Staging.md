# ADR-109 — Default-off initial SMS staging and explicit unknown evidence

Status: Proposed, local review only. Date: 2026-10-24.
Related [SPEC-266](../specs/SPEC-266_Signal_Default_Off_Initial_SMS.md).
Supersedes: none. This is not an accepted production activation decision.

## Context
PR 980 separates initial caller notification from market enrichment and exposes
the limits of the saved operational risk view. Sharing email retry semantics
would risk resending an ambiguous SMS; conflating operational evidence with
research status or handset receipt would overstate what is known.

## Decision proposal
Stage an independent, default-off, tenant/namespace-scoped durable SMS lane.
Require both exact-string enablement and consent, a fixed configured recipient,
an explicit window no longer than 24 hours, and atomic non-refundable attempt
reservations. Retain UNKNOWN on ambiguous/crashed sends with no automatic replay.
Preserve existing operator authorization, immutable email payloads, empirical
guards and distribution behavior. The initial and distribution SMS caps remain
separate and require combined budget approval before any future activation.

Keep the [two-table schema](../sql-drafts/2026-10-22-signal-initial-sms.sql) manual,
outside startup migration discovery; it has not been applied. Display only saved
operational measurements. Missing structural, flow-trend and FIRST_CALLER evidence
stays UNKNOWN; retained heuristic risk and provider acceptance prove neither
safety nor delivery. No research join, new collector or trading behavior is added.

## Consequences and follow-ups
Default-off and missing-schema failure leave existing email available. When
enabled, startup awaits the shared database without a timeout; this design does
not guarantee isolation from a stalled database. Ambiguity favors no
duplicate send over automatic recovery; observed receipt needs separate proof.
One-second scheduling is not a zero-latency guarantee. Review-only code and tests
do not establish runtime readiness. Native PostgreSQL validation remains pending
on final-head hosted CI because local socket setup is blocked.

This document grants no publication, deployment, schema, activation or real-message
authority; a separately approved disabled release does not authorize SQL or sends.
Authorized read-only inspection verified both new SMS opt-ins absent/off at
13:34 UTC on 2026-10-24; no values changed. No paid/frozen research, aggregate,
provider-budget, production configuration or strategy changes are authorized.
See the [local rollout proposal](../signal/INITIAL_SMS_LOCAL_REVIEW_2026-10-22.md).
