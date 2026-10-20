# SPEC-265: Anchor research-provider pause

Status: In progress, authorized local preparation only. Publication, deployment and arming require separate approval.

## Objective
Provide one default-off pause for tenant 10 paid discovery, contact-enrichment and email-verification handoffs across the reviewed caller inventory.

## Vision References
[PROJECT_CONTEXT](../../PROJECT_CONTEXT.md); [Product Thesis](../vision/Product_Thesis.md). Explicit tenant scope and human-controlled operation boundaries.

## Problem
The independent enrichment scheduler and manual/shared callers are not covered by Max's recovery research gate. A caller-only check can be bypassed by a later retry or fallback; missing tenant metadata cannot be inferred from a business name.

## Scope / Out of Scope
Use ANCHOR_RESEARCH_PROVIDERS_PAUSED=true only when separately approved. Default unarmed behavior is unchanged. Known tenant 10 research roots stop before new provider work; valid explicit non-10 keeps existing network/SQL behavior. Marked research/diagnostic calls with missing, invalid or conflicting context reject while armed. Generic research diagnostics/phone CLI therefore require explicit tenant selection while armed; disclose that compatibility requirement. Preserve field-route geocoding, call/SMS authorization, unrelated presentation LLM and email-send gates. No budget/resume permission, schema/DDL, public admin endpoint, secret handling, global tenant deactivation or provider flag workaround.

## Dependencies
Exact deployed base main 93bcaee9f1f2c2c225b009c245c5ae92a5a7ccd7 / tree 24661f3ff2a774ecc4d604390bfb93a9bdbc9637. PR 955 is separate unmerged compatibility input. Existing permanent email quarantine is not changed. [ADR-107](../adr/ADR-107_Explicit_Research_Provider_Pause.md).

## Architecture
Capture authoritative row/request/constant tenant in dedicated immutable AsyncLocalStorage. Never use mutable CONFIG/ACTIVE_CLIENT_ID or Places attribution overrides as leaf authority. Recheck the environment control immediately before every paid HTTP handoff, after reservation/throttle waits. Typed control errors escape retry/fallback/provider-failure conversion and do not invalidate candidates or consume enrichment retry counts merely because the pause rejected before a call. Preserve already completed work and existing consumed reservations.

## Data Model
None. No persistence, credentials, provider account calls or automatic schema setup are added by the guard.

## Implementation Plan
1. Reviewed caller/leaf matrix, with explicit operator-context contract and exclusions.
2. Small pure control/context helper, targeted root binding and actual transport assertions.
3. Synthetic interrupted-call, concurrent-tenant, retry/fallback, mutation and exclusion regressions.
4. Independent review, exact patch replay and separate publication decision.

## Migration Strategy
No DDL. A later approved environment change propagates only when each deployed process sees it. Rolling restarts and replica convergence must be verified; already in-flight remote work is not cancelled. A pause is not zero total spend or a whole-network shutdown. Unarming restores only pre-existing behavior and does not grant any research budget, recovery or send authority.

## Testing
No production or paid calls. Intercept provider transports. Cover known roots, each provider leaf, mixed-tenant batches, explicit context conflict, unknown research diagnostics, flag changes across awaits/retries, no false candidate mutations, and unchanged non-10/field-route semantics. Existing native and relevant aggregate suites must remain green.

## Acceptance Criteria
Every reviewed attributable tenant 10 paid research handoff is blocked when the process observes pause; no fallback bypass; missing research scope is explicitly rejected under the reviewed diagnostic contract; unarmed/default and excluded operations preserve existing behavior. Any unclassified caller remains a documented coverage blocker. Do not claim an all-provider kill.

## Future Work
Bounded research-resume/credit budgets remain a separate design and approval. No automatic renewal or runtime provider-budget policy is implemented here.

## Operator contract

See the [operator proposal](../anchor-research-provider-pause.md) for the exact flag, armed-mode diagnostic context requirement, exclusions and later approval sequence. The enumerated release footprint spans multiple roots/leaves because they otherwise bypass the shared context or swallow typed interruption; this is not a one-file switch.
