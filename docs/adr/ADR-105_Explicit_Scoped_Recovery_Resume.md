# ADR-105: Explicit scoped recovery resume

Status: Proposed/in progress local-only, 2026-10-19. See SPEC-262.

Keep the durable closure latch and immutable quarantine. Resume tenant-10 maintenance only by selecting an explicit finite scoped grant inside the existing control JSONB union. Unknown or revoked state closes again; an old SPEC261 parser closes on the new manifest kind, but its broad binary is not an acceptable rollback target.

A reviewed identity batch is independent of envelopes and local days, but every claim/send remains bound to current canonical artifacts and approval. Research, preparation, canonical approval delegation and email are independent explicit permissions, intersecting existing budgets. No provenance-derived automatic admission or true-hold release is inferred. Count program attempts across grant IDs and retain independent immutable dispatch CAS and research/preparation consumption.

Add the missing review boolean only through separately approved nullable/no-default/no-backfill schema work; initialize only explicitly reviewed NULL identities under CAS and audit. Preserve other environments' existing values/settings. Calls retain their prior semantics; SMS receives no new authority; tenant-10 manual email/other unapproved Anchor programs stay closed. Every actual tenant-10 CRM email route always enforces quarantine against its actual payload.

Consequences: a bounded approved batch can flow through normal daily envelopes without broad release. Expiry/revoke returns its scopes to closure; other unapproved tenant-10 programs remain closed; non-Anchor programs keep their legacy behavior. This is not automatic whole-system recovery or authority to spend on unspecified providers.

## Superseding the unpublished broad runtime gate
The final local design is tenant-10 isolated. Non-Anchor and non-email paths return to original authorization/query/lock/preparation behavior before recovery inspection. Shared nullable DDL requires approval for schema locking and shared review-flag implications, not a permanent global runtime pause. The previous frozen SPEC261 artifact remains historical evidence and is not recommended for rollout.
