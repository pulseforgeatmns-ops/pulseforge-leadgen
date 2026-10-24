# ADR-102 — Explicit immutable legacy coverage epoch

| Field | Value |
|---|---|
| **Status** | Proposed; locally implemented, production approval pending |
| **Date** | 2026-10-19 |
| **Spec** | [SPEC-SCOUT-CONT-002](../specs/SPEC-SCOUT-CONT-002_Explicit_Legacy_Coverage_Epoch.md) |

## Context

Historical Scout candidate checkpoints lack Places continuation tokens. A handled old query is neither proof of complete market exhaustion nor permission to repeat paid discovery. Changing the mission fingerprint or deleting checkpoints would lose deduplication and accountability. Any additional acquisition needs an exact, bounded approval and must coexist with unresolved old tickets.

## Decision

Keep the mission fingerprint and historical rows intact. A default-off explicit epoch1 manifest identifies exact original root hashes, tenant and activation window. One immutable scope-wide receipt pins the complete allocation. Per-root mappings derive separate immutable page identities; the ordinary page store preserves tokens, candidate payloads and three-page limits. Mapping creation and each reservation check the same receipt and prior durable handoff. Missing/expired permission blocks new reservations while existing fetched pages and candidate tickets remain recoverable. Natural diagnostics expose bounded hashes to prepare approval, never candidate data or tokens.

## Consequences

### Positive

- Exact query scope, cumulative limits, restart safety and concurrency share one durable authority boundary.
- Old raw candidates and attempt history stay intact; unchanged facts are not repeatedly admitted.
- Configuration removal stops new paid work without stranding already-fetched data.

### Negative / tradeoffs

- Planning pins the full manifest even for roots deferred by the current session budget. The window and allocation cannot later be extended or rotated.
- Unknown request outcomes stop for review rather than risking duplicate spend.
- Finite same-query coverage can still yield no eligible businesses or no clean founders; live outcome verification remains necessary.

### Follow-ups

Complete independent review and combined local acceptance. Production migration, activation, exact manifest and cumulative spending require separate approval. See the [release boundary](../engineering/scout-explicit-legacy-coverage-epoch.md). No production access or action is authorized by this ADR.
