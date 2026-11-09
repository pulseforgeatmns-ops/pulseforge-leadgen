# Conversational continuity foundation

`SessionStore` stores live conversational state in a process Map. Railway
redeployment replaces that process. The architecture assessment records three
such resets in 35 minutes; this PR does not change deployment policy.

This PR bounds live memory to 1,024 sessions with a two-hour idle expiry. The
optional `MAX_CONVERSATION_CONTEXT=true` observer writes a **separate**, bounded
continuity projection after completed turns. It defaults off. Repository writes
use a tenant/session composite key, two-hour expiry and revision compare-and-swap.
Concurrent writers report a conflict rather than silently overwriting context.
Missing schema, write errors and capacity exhaustion cannot break a chat turn.

Only candidate mission IDs, the recent mission focus ID, and evidence timestamps
survive. Customer records, transcript text, prepared-package presentation,
authorization state, pending decisions and execution policy are excluded. Reads
validate the complete projection. Evidence always requires a fresh tenant-scoped
read. This data is a referent hint, never evidence of current authority.

`WorkspaceEngine.resumeConversationContext({tenantId,sessionId})` retrieves that
projection after a process restart. It must be called with an authenticated tenant
scope; it is not an HTTP endpoint and must not be exposed with a client-supplied
tenant. It deliberately does not hydrate old sessions or install old mission
bindings into the active router. This is a foundation for the shadow interpreter
and future ledger integration, not active semantic routing or complete live
session recovery. Automatic referent recovery into active owners remains gated.

Apply `migrations/2026-11-09-max-conversation-context.sql` only after production
schema review and explicit approval. The additive transaction can be replayed.
Unset the flag for immediate rollback; no table drop is needed. Recovery from
unavailability starts a normal conversation with no restored authority.

Privacy: projections are capped at 16 KiB, contain no raw prose, and expire after
two hours. Schedule `purgeExpired()` in the governed maintenance workflow before
enabling writes so expired rows are physically deleted. `remove(tenant,session)`
is the explicit erasure path. Retention and principal access must be reviewed
before rollout; tenant scoping is enforced by repository predicates, not RLS.

Tests cover projection privacy, invalid/corrupt payload rejection, flag-off and
unavailable-store behavior, idle eviction, migration replay, restart recovery,
concurrent CAS, cross-tenant reads/deletes, expiry, stale revision rejection,
recreation and physical expiry deletion using disposable PostgreSQL. No
production schema or data is changed by tests.
