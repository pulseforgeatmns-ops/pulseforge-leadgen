# P0-MAX-REASONING-001 PR 3 — ConversationLedger

Status: **implemented and tested on a branch. Not merged, not deployed, not enabled.** Default-off (`MAX_CONVERSATION_LEDGER`). No migration: durability reuses the existing `max_conversation_context` table.

## What it is for

The ledger tracks what an operator conversation is **still about**, **what changed**, **what was decided** and **what is unresolved**. It does not store conversation history. It keeps three things strictly apart:

| | Holds | Never |
|---|---|---|
| **Remembered context** (ledger) | referents (ids plus generic labels), focus stack, corrections, open questions, prohibitions and preferences, decision history, last validated frame | record contents, operator wording, transcripts, credentials |
| **Evidence** | only *references*: domain, referent and timestamp, with a freshness verdict | presented as current: every reference is `mustReread`, and recovered references are always stale |
| **Authority** | nothing: `authority: { grantsAuthority: false, executionPermitted: false, consumableApprovals: [] }` | approvals, execution permission, mission state. These stay with the pending-decision resolver, `AuthorizationConstraints`, SPEC-136 checks and `routeExecutionRequest`. |

## Components (`packages/max/context/`)

- `ConversationLedger.js`: pure, deterministic, bounded model. Covers `applyTurn`, `resolveFocus`, `viewLedger`, strict `validateLedger`, `projectLedger` (always ≤ 12 KB, inside the store's 16 KiB row cap), `recoverLedger` and the TTL (2 h, the same as the store).
- `LedgerTurnAdapter.js`: derives one turn from the turn's **own production result**:
  - record-intelligence items `prospect:<tenant>:<id>`, tenant-checked; other tenants' refs are dropped and noted as a foreign reference
  - Scout discovery (`launchedScout`)
  - session-bound mission and its pending gate
  - production's pending-decision resolution or approval-route action
  - operational-inspection cross-tenant refusal
  - retrieval status
  - prohibitions from `NegatedDirective.extractNegatedClauses`, stored as action classes, never text
- `LedgerDiscourse.js`: the **degraded-path** discourse signal: correction, refinement, topic switch or return, and existing-vs-new record scope. It reuses `MessageTypeClassifier.CORRECTION_RES`. A model TurnFrame's `relationToPrior` always takes precedence. Nothing in routing reads this module; widening the production classifier would have changed routing.
- `ConversationLedgerRecorder.js`: the default-off hook `WorkspaceEngine` calls after production answers and before `ConversationContinuity` snapshots the session. It never throws into a turn.

## Behaviour

- **Owner transitions never clear the subject.** A turn handled by another component keeps the focus. Records Max just showed become the subject. A session-bound mission is ambient context: it takes focus only when nothing else is focused, on an explicit topic switch, or when the operator talks about missions.
- **Corrections are deterministic.** "No, I meant the existing prospects":
  - the focused set is marked `superseded`;
  - a replacement with scope `existing` takes its place, marked `pendingEvidence`, so nothing is claimed about it yet;
  - one correction entry and a `record_scope:existing` preference are recorded;
  - the next record read **fills** the replacement instead of creating a duplicate.

  A correction that does not say what to change opens `unresolved_correction` instead of guessing.
- **Topic switches** push onto the focus stack. "Back to …" restores the earlier subject.
- **Tenants.** The ledger is keyed by (tenant, session), the same key as the durable row. Sessions are tenant-bound (the route refuses a mismatched session), so switching to Anchor Cleaning uses a separate ledger, and returning to Levi finds Levi's intact. A ledger found on a session for another tenant or session is discarded.
- **Pending gates.** A mission has one current pending gate: a newer gate supersedes the older referent, and a reply is attributed to the gate pending *before* the turn.
- **Expiry.** After 2 idle hours the ledger is discarded (`expiredPredecessor: true`). "Which of those…" then opens a clarification instead of resolving to stale records.
- **Bounds.** 24 referents (focus is never evicted), 25 members per set, focus 8, corrections 16, open questions 8, constraints 16, decisions 12, evidence 16, labels 80 chars. Sessions themselves stay bounded by `SessionStore` (1,024 sessions, 2 h idle).

## Durability and recovery (needs `MAX_CONVERSATION_CONTEXT` too)

- `ConversationContextStore` accepts **version 2**: the unchanged version-1 fields plus a strictly validated ledger whose tenant and session must equal the row key. Version 1 is byte-identical when the ledger flag is off.
- `recoverConversationSession` (unchanged when the ledger flag is off) also re-confirms the ledger's mission ids through the existing tenant-scoped loader. On recovery:
  - unconfirmed missions and gates are dropped;
  - every other referent is marked `requiresRevalidation`;
  - all evidence is stale;
  - decisions remain history with `authority: 'none'`, enforced by the validator, which rejects any other value, extra field, or `executionCandidate: true`.
- A record-only conversation (no missions) can be recovered when the ledger flag is on. Without it, behaviour is unchanged (`no_valid_references`).
- Recovery never restores `presentedExecutionDecision`, so an execution approval still requires the package to be shown again in the new session.
- Failed recovery yields **no** referents: unavailable store, malformed or tampered row, expiry, another tenant, or all references deleted.

## Field-note memory (M3) — root cause and interaction

`test/maxConversationMemory.test.js` M3 failed on `main` because the test read with the wall clock while writing with a fixed fixture clock, so its 7-day records expired once real time passed the fixture date. Fixing the clock exposed two real defects in `packages/max/understanding`. Both are fixed in a separate commit so they can be split out:

- the wrong supersede key (`dm:<priorValue>` instead of `dm:<contact>`);
- a stale same-name contact record left active beside the corrected one.

The fix also aligns the in-memory repository with Postgres when a record is re-asserted.

**Interaction:** field-note memory owns facts about contacts and accounts and their corrections. The ledger owns what the conversation refers to. The ledger never stores contact facts, so there is one correction mechanism per kind of thing.

## Activation (separate approval)

1. Optional in-memory only: `MAX_CONVERSATION_LEDGER=true`. Nothing reads the ledger in routing yet. The read API is `WorkspaceEngine.getConversationLedger({ tenantId, sessionId })`.
2. Durable: additionally `MAX_CONVERSATION_CONTEXT=true`, which needs the already-reviewed `2026-11-09-max-conversation-context.sql` applied and `purgeExpired()` scheduled.
3. Rollback: unset either flag. Version-2 rows remain readable, expire within 2 h, and are ignored when the ledger flag is off.

## Known limits

- The lexical producer never marks corrections, so the degraded path relies on `LedgerDiscourse` markers. Unrecognised phrasings record no correction (a safe direction). The model frame removes this limit once wired (PR 5).
- Prospect referents are recovered as ids to revalidate. Verifying them against the database belongs to the EvidenceRegistry (PR 4).
- Freshness of individual records ("which has the freshest information?") is resolved to the right set here. Comparing record timestamps needs a fresh read (PR 4/5).
