# P0-MAX-REASONING-001 — Integration readiness and acceptance report

Status: **reviewable. Nothing merged, deployed, migrated or enabled.** Date: 2026-11-18 (remediation phase added the same day; see §R).
Integration branch: `claude/p0-max-reasoning-001-integration`, which is `origin/main` (`ee88320`) plus PRs #1131–#1135 merged in order, CI promotion, and this report. The branch is for verification only. Do not merge it; merge the five PRs themselves.

## R. Remediation phase (Nov 18)

| Item | PR | State |
|---|---|---|
| Field-note correction extracted from #1133 | **#1137** | Open. The two field-note commits are reverted on #1133 (`f5e7efe3`, `b8b300a8`) and its docs now point to #1137. #1133 does not depend on it. Until #1137 merges, `maxConversationMemory` M3 keeps failing on `main` exactly as before. |
| R-1 retraction / contradiction fix | **#1138** | Open. Fail-closed. 279 tests at all six decision kinds and all four reachable gates; 133 fail without the fix. |
| Gate-bound approval semantics | **#1139** | Open. Default-off `MAX_GATE_BOUND_APPROVAL`, narrowing only. See `docs/P0-MAX-GATE-BOUND-APPROVAL.md`. |
| Verification-only integration PR | **#1136** (draft) | Do not merge. CI runs on the combined set: PRs 1–5 + #1137 + #1138 + #1139. |

Recommended merge order: #1137 and #1138 (independent of each other and of PRs 1–5), then #1131 → #1135, then #1139 (flag off). Merge only with owner approval.

## M. Merge plan (Nov 18, merge-readiness review)

All heads were verified against current `main` (`ee88320`). Every PR is green on its own head, with no blockers. Do not merge without owner approval.

| # | PR | Head | Own CI | Merge into `main` (in this order) |
|---|---|---|---|---|
| 1 | #1137 field-note correction | `24cdfff` | green, incl. the new `field-note-memory` job | clean |
| 2 | #1138 R-1 retraction fix | `ba009eb` | green | clean |
| 3 | #1131 PR 1 eval harness | `81e8ae7` | green | clean |
| 4 | #1132 PR 2 shadow readiness | `afaaed3` | green | **conflict after #1131** (see below) |
| 5 | #1133 PR 3 ConversationLedger | `39c2d7d`* | green after the CI-split fix | clean |
| 6 | #1134 PR 4 EvidenceRegistry | `3611c7e` | green | clean |
| 7 | #1135 PR 5 frame-directed reasoning | `9b47b35` | green | clean; enable the cross-PR job after merge (below) |
| 8 | #1139 gate-bound approvals (flag off) | `a377c82` | green | **conflict after #1133** (see below) |
| 9 | #1140 Gemini adapter (draft) | `59ba866` | pending | after #1131 + #1132; the diff narrows to one commit |
| 10 | follow-up (to open after 1–8) | — | — | integration-only items (below) |

\* The #1133 head after today's CI-split fix.

**Defect found and fixed in this review.** When the field-note commits were split out of #1133, its `max-context.yml` still ran `maxConversationMemory` and the deleted Postgres correction test, so #1133's `context` job was red.
- #1133 no longer runs them.
- #1137 now carries its own `max-field-note-memory.yml`, a separate file so the two PRs cannot conflict.
- Both reran green.

**Conflict resolutions.** Resolve each by merging `main` into the PR branch *after* the earlier PR lands, so CI runs on the resolved tree before the merge:
- **#1132 after #1131:**
  - `scripts/maxInterpretationEval.js`: keep `main`'s version (PR 1's, stricter `--model-id`).
  - `CLAUDE.md`: **one** combined "Unified reasoning architecture" line, exactly as in integration commit `1267aeff`. A naive keep-both duplicates the line (caught while building #1140).
- **#1139 after #1133:** `WorkspaceEngine.js`, keep both post-turn hooks in this order: `recordPresentedExecutionDecision`, then `gateBoundApproval.recordPresentedDecision`, then the ledger `observe`, then continuity `remember`.

**Equivalence.** A sequential trial merge of 1–8 onto `main` with those resolutions differs from the combined branch (#1136, 20/20 green) only by integration-only files. Those go in the follow-up PR:
- `.github/workflows/max-frame-reasoning.yml`: enables the cross-PR job, which needs PRs 3–5 on `main`.
- Doc corrections: the stale §1.6/#1094 notes in `CLAUDE.md`, the architecture doc and the PR 5 doc.
- This report and `docs/P0-MAX-INTERPRETATION-EVAL-PROVIDER.md`.
- `packages/max/workspace/tests/p0AuthorizationReadiness.test.js` (needs #1133 and #1138).

After the follow-up merges, close #1136 unmerged.

**Not blockers, but needed before enabling anything:**
- #1139's flag needs its 33 contract-change tests adapted.
- #1140's paid run needs a dedicated dev Gemini key and network access to `generativelanguage.googleapis.com`.

## 0. Verdict

| | |
|---|---|
| Code merge readiness (PRs 1–5) | **Ready for human review, in order 1 → 5.** All five merge cleanly onto current `main` once PR 1/PR 2 is resolved as documented (§1.2). No production behavior changes while the flags are off (§1.3). |
| Activation readiness (shadow, ledger, frame reasoning) | **Not ready.** The model evaluation has not run (§4), so no §10 promotion gate is measured. Frame reasoning and the evidence registry are not wired into routing by design. |
| Authorization safety | **No new authority path.** Two pre-existing defects listed as open were already fixed on `main` (#1096). The new legacy-path class R-1 is **fixed in #1138** (§3). Gate-bound approvals are available default-off (#1139). |
| Blockers before any rollout | No model-backed evaluation yet. The Gemini path (§4) needs a small provider-port PR, a dedicated dev key and approval. Review of #1137/#1138/#1139. |

## 1. Phase 1 — integration review

### 1.1 Per-PR findings

| PR | Scope | Production files touched | Findings | Readiness |
|---|---|---|---|---|
| #1131 PR 1 — interpretation eval harness | `TurnFrame`, lexical/model producers, held-out eval, paid-eval CLI with in-process spend cap | `ModelFrameProducer`, `scripts/maxInterpretationEval.js` | Fixed during integration: the eval CLI fake-API test bound `127.0.0.1` explicitly, which the unit lane's network guard rejects (`81e8ae7`, now `listen(0)`). Truncation guard and cap are covered by tests. | Ready |
| #1132 PR 2 — semantic shadow readiness | Explicit-only interpreter model, pending-decision kind tee, degraded/incomparable frames, 90-day purge | `ShadowInterpreter`, `ModelFrameProducer`, `WorkspaceEngine` (observer tee), eval/review scripts, `maxOperationalReadiness` | Conflicts with PR 1 in `scripts/maxInterpretationEval.js` and `CLAUDE.md` (§1.2). The tee passes only the decision **kind**, never the prompt. With `MAX_INTERPRETATION_MODEL` unset, no calls are made. | Ready after PR 1 |
| #1133 PR 3 — ConversationLedger | Bounded per-(tenant, session) memory, durable as v2 of `max_conversation_context`, recovery | `ConversationContextStore`, `ConversationContinuity`, `packages/max/understanding/*` persistence/repository, `WorkspaceEngine` recorder + recovery | Two fixes pushed during review: redundant scope correction (§1.5) and a field-note regression (§1.4). Off: v1 rows are byte-identical. **Rollback note:** code from before PR 3 rejects v2 rows. That degrades to a fresh conversation, and rows expire in 2 h. | Ready; split recommended (§1.4) |
| #1134 PR 4 — EvidenceRegistry | Tenant-scoped, read-only registry over existing loaders | **none** | Not imported by production (structural test). All SQL goes through `readOnlyQueryable`. | Ready |
| #1135 PR 5 — frame-directed reasoning | Reasoner + presentation guard, cross-PR suite | **none** | Not imported by production (structural test). Normalizes the ledger `members`/`memberIds` shape divergence (§6). | Ready after 3 and 4 |

No external callers of a changed module were affected (checked by grep across `routes/`, `services/`, agents and `packages/`).

### 1.2 PR 1 / PR 2 conflict (documented resolution, verified)

Merge PR 1, then PR 2:
- `scripts/maxInterpretationEval.js`: take PR 1's version (`--ours` when merging PR 2 onto PR 1). PR 2's edits to it are a subset of PR 1's.
- `CLAUDE.md`: keep both additions.

The integration branch was built exactly this way. Everything listed in §2 passes on it.

### 1.3 Unintended production changes

None found while every flag is off:
- `MAX_INTERPRETATION_MODEL` (unset means no model calls),
- `MAX_CONVERSATION_LEDGER`,
- `MAX_CONVERSATION_CONTEXT`,
- `MAX_FRAME_REASONING`.

The default-off parity test (`conversationLedgerRecovery.test.js`) shows identical answers and no durable write. PRs 4 and 5 add only unreferenced modules.

### 1.4 Field-note memory fix: review separately

PR 3 includes a change to `packages/max/understanding/conversationMemoryPersistence.js`: a contact correction now supersedes that contact's previous `ACTIVE_CONTACT` records. This **changes existing state ingestion**, and it is live code, not behind the ledger flag.

- **Regression found and fixed during this review (`efe0b5d`).** The first version collapsed two different people with the same name at different accounts into one account-less record, which lost data. The fix supersedes only when every active and replacement record of that name involves at most one account. If the check is ambiguous, or the memory load fails, it supersedes nothing. Covered by M3b (two Daves kept), M3c (unreadable memory supersedes nothing) and a Postgres case.
- **Done:** extracted into **#1137** and reverted on #1133.

### 1.5 PR 3 redundant-correction fix

Before the fix, "only the review-only ones", when the subject already had that scope, opened an `unresolved_correction`. The smallest safe fix (`ae8ab5f`) treats a correction to a scope the subject already has as a confirmation. It records a `record_scope:` preference and no open question. One branch in `applyTurn`; a mutation check confirms the new test catches a regression.

### 1.6 CI promotion

`.github/workflows/max-frame-reasoning.yml`:
- an `offline` job (hermetic suites),
- a `cross-pr` job: `setup-postgres-environment`, then `node --test packages/max/frameReasoning/integration/combinedArchitecture.integration.cjs`.

The path filters cover interpretation, context, evidence, understanding, frameReasoning, `WorkspaceEngine`/`WorkspaceTypes`, `OperationalInspection`, `ProspectRecordIntelligence`, `disposablePostgres` and the context migrations. The cross-PR suite fails, and never skips, when a PR is missing. It therefore belongs in CI only once all five PRs are on `main`. The workflow lives on the integration branch and should be moved onto PR 5 (or a follow-up) at merge time.

## 2. Combined test results (integration branch, local)

| Suite | Result |
|---|---|
| Max unit lane (`scripts/maxTestLanes.cjs unit`, offline network guard) | 2,568 tests: 2,567 pass, 0 fail, 1 todo |
| Interpretation (`test:max:interpretation`) | 81/81 |
| Semantic ratchet (`test:max:semantic`) | 9 pass, 1 todo; baseline unchanged |
| Frame reasoning + evidence (hermetic) | 34/34 |
| Ledger, context, field-note, readiness | 58/59. The failure is the pre-existing stale `implementedInDeployedCode` assertion in `test/maxOperationalReadiness.test.js:34`, which fails identically on `main`. |
| Decision service (JEV) | 115/115 |
| Authorization suites | 576/576 |
| All Postgres suites, including the cross-PR acceptance suite | 38/38 |
| **New:** `p0AuthorizationReadiness.test.js` (§3) | 116 tests: 84 pass, 0 fail, 32 todo (R-1, every todo fails as expected) |
| Lexical interpretation baseline | 53.2% (n=62, corpus `950eb659cd58`). Safety violations: C6, E1, E4, N1, X0c, X1c, X2c. These are frame nominations, never dispatches. The original 47.7% record is unchanged. |

Other pre-existing failure on `main`, unchanged: `test/aoAdminImpersonation.test.js`. GitHub CI has not run on the integration branch yet (§8).

## 3. Phase 2 — authorization safety

### 3.1 Status of the defects listed as open

| Defect | Status on `main` | Evidence |
|---|---|---|
| §1.6 hedged/conditional approvals ("Maybe approve the plan? Not sure yet.", "Approve it, but hold the emails until Monday.") | **Fixed by #1096** (Nov 8). The architecture doc, `CLAUDE.md` and the PR 5 doc still said "open". All three are corrected on this branch. | `p0NegatedAuthorization.test.js`: 11 qualified phrasings × 6 gate kinds (resolver), × 4 reachable gates end to end. 0 dispatch, mission fingerprint unchanged. |
| #1094 bare "Cancel the mission." treated as exit | **Fixed by #1096.** With a bound mission it reaches the governed router as `CANCEL_PLAN`. "Exit this mission." never cancels. An unbound cancellation dispatches nothing. | Same file: "bare cancellation reaches the governed router", "leaving the conversation never cancels". **The GitHub issue is still open.** Recommend closing it with a reference to #1096 (owner's call). |

### 3.2 New characterization: `packages/max/workspace/tests/p0AuthorizationReadiness.test.js`

This file drives the real `WorkspaceEngine.ask()` at the plan, discovery, prioritization and execution gates. It does not change authorization behavior.

| Class | Cases | Result |
|---|---|---|
| Ambiguous | "Hmm.", "ok?", "Yes and no.", "I think so, maybe.", "Not sure." | **Hold** at every gate. 0 dispatch, decision pending. |
| Withdrawn, same turn | "Approved. Actually, wait — hold on.", "Yes, go ahead. Never mind.", "Approve it. On second thought, don't.", "Approved — no, wait.", "Yes. Disregard that." | **Hold.** |
| Withdrawn, same turn: retraction idioms | "Go ahead. Scratch that.", "Go ahead. Forget it.", "Approved. Strike that.", "Approve it. Cancel that." | **DEFECT R-1** (see below). Recorded as `todo`. |
| Withdrawn, next turn | "Approved." then "Wait, I take that back." | Holds: neither approves the next gate nor cancels. Nothing undoes the first approval; that is by design, since reversal is an explicit cancel. |
| Stale approval across restart | Package shown → restart → recover → "Approved." | **Safe, but defended late.** Chat still submits `APPROVE_EXECUTION`, with no reviewed revision, because the presentation binding is intentionally not restored. The TME stage rolls it back as unreviewed: nothing committed, mission unchanged, reply "Not executed … Nothing was approved or sent." |
| Pending decision across recovery | Approve the plan → restart → "Same as before." / "Do what I approved earlier." / "Continue with what we decided." | Hold. Recovered decisions carry `authority: none`, `consumableApprovals` is empty, and the pending decision comes from the mission store. |
| Replayed bare approval | "Approved." sent twice at the plan gate | Approves plan, **then discovery**. Pinned as characterization. #1139 binds approvals to the presented decision, which closes drift, revision, other-surface and never-shown cases. A double send is **not** closed, because the first answer presents the next gate. That needs a client message id or a `decisionKey` echoed by the UI (follow-up). |

**Defect R-1: unmarked retraction idioms.** A retraction with no negation or uncertainty marker is ignored, so the earlier clause resolves:
- "Go ahead. Scratch that." / "Forget it." / "Strike that." dispatch the gate's approval at every gate. After the package is shown, that includes `APPROVE_EXECUTION`, which consumes the approval.
- "Approve it. Cancel that." dispatches **`CANCEL_PLAN`**: the operator meant the approval by "that", and the mission is cancelled instead.

Exposure today is limited, because outbound is in maintenance with `recovery_kill_switch` active. But plan, discovery and prioritization approvals advance mission state, and a cancellation is a durable state change.

**Fixed in #1138.** The approach as originally recommended:
- In `PendingDecisionResolver`, a clause that follows an authorization and refers back to it ("that", "it") with a retraction or dismissal verb makes the turn AMBIGUOUS, which means a confirmation request.
- Anaphoric "cancel that" after an approval must not resolve to `CANCEL_PLAN`.
- Remove the `todo` markers in the new suite as the acceptance test (done on this branch; the suite passes 116/116 with #1138 merged in).

A durable fix is the two-key gate (architecture doc §3.4, Phase F).

### 3.3 Context never becomes authority (the new system)

By construction and by test:
- `TurnFrame.authority.grantsAuthority` is always false, and the validator rejects anything else.
- The ledger rejects authority-bearing rows on load. Recovered decisions have `authority: none`.
- The EvidenceRegistry is SELECT-only. `FrameDirectedReasoner` never calls a runtime, router or approval. State-changing frames return `not_executed`, naming the deterministic pathway.
- Nothing in routing reads the ledger, registry or reasoner (three structural tests).
- The cross-PR suite asserts an empty router audit and an unchanged mission fingerprint across scenarios A–G.

R-1 is a defect of the **legacy** deterministic resolver, not of the new path. On the new path, an R-1 phrasing yields a frame that is at most a nomination, and the reasoner cannot act on it.

## 4. Phase 3 — model evaluation: not run (provider direction changed)

- **Nov 18 direction:** Pulseforge is moving away from Anthropic API dependencies. The Anthropic key and the Sonnet 5.5 evaluation are **not pursued**.
- Earlier attempt: a fresh session found no key and stopped before any call. **$0 was spent on evaluation.** No credential was requested or stored, and nothing was substituted.
- **The 85% promotion gate remains unmeasured.** The lexical 53.2% is not a stand-in.
- **Next path:** run the same evaluation, with the same gates, on Gemini (`gemini-3.1-flash-lite`), the only non-Anthropic text-generation provider already priced and metered in the repo. Recommendation, code changes, cost bound (≤ $0.87 for 62 × 3) and the approvals needed are in `docs/P0-MAX-INTERPRETATION-EVAL-PROVIDER.md`. No provider, credential or configuration has been changed.

## 5. Evidence quality and blind spots (PR 4/5)

- Scout acquisition state is not consulted, because its store runs DDL on read. Company provenance stays "on file", never "Scout-found".
- The Anchor outbound gate is a fixture in the Postgres tests, not the production program row.
- Record reads are bounded at 1,000. Beyond that, ids are `not_verifiable_within_bound`, never "absent".
- Deleted and transferred-to-another-tenant records are indistinguishable (`not_found_in_tenant`), by design: no cross-tenant read.
- Contradictions are surfaced and never reconciled. Unknowns are never treated as proof that something did not happen.
- The presentation guard is pattern-based. The deterministic text stays the default, and any model presenter must pass the guard.

## 6. Known shape/contract divergence

The ledger's `resolveFocus()` returns `memberIds`, while stored referents use `members`. PR 5 normalizes both, and `LedgerReferentContract` accepts `memberIds`. Recommend aligning the ledger on one name in a follow-up, before any production consumer reads the ledger.

## 7. Production schema requirements (none run)

| Object | File | Status | Needed for |
|---|---|---|---|
| `max_interpretation_shadow` | `migrations/2026-11-09-max-interpretation-shadow.sql` (+ `.rollback.sql`) | On `main`. Application status in production not checked here. | Shadow observations (PR 2). Schedule `scripts/maxInterpretationShadowPurge.js` (90 days). |
| `max_conversation_context` | `migrations/2026-11-09-max-conversation-context.sql` | On `main`, previously reviewed | Durable ledger (`MAX_CONVERSATION_CONTEXT=true`). Schedule `purgeExpired()`. PR 3 adds no table. |
| JEV shadow persistence | JEV-006 operator migration | Production writes failing (`schema_column_missing`) per the architecture doc | Not required by these PRs; required for the decision-shadow corpus used by Phase F gates |

## 8. Proposed staging and shadow rollout gates

1. **Merge gate (code only):**
   - human review of PRs 1 → 5 in order;
   - the field-note change split out (§1.4);
   - the CI workflow moved onto PR 5;
   - GitHub CI green, including `cross-pr`.

   Flags stay off.
2. **Model evaluation gate:** the authorized Sonnet 5.5 run completes within $7, with 0 truncations, 0 authority-granting frames, and per-category accuracy recorded. The §10 thresholds (85% semantic interpretation) decide whether shadowing is worth enabling. Do not promote below them.
3. **R-1 fix gate:** a separate PR. The new suite's `todo` cases pass, and the #1093/#1096 suites stay green.
4. **Staging shadow (read-only Render staging, #1128):**
   - `MAX_INTERPRETATION_MODEL` set;
   - shadow migration applied in staging;
   - purge scheduled;
   - 0 response-path latency change;
   - persisted observations > 0;
   - degraded rate measured.
5. **Ledger in staging:** `MAX_CONVERSATION_LEDGER` + `MAX_CONVERSATION_CONTEXT`. Default-off parity is re-verified against staging traffic, restart recovery is exercised across a redeploy, and 0 authority-bearing rows are accepted.
6. **Production shadow** only after 4–5 hold for an agreed window, with owner approval.
7. Frame reasoning and two-key authority (Phases E/F) stay out of scope until a model-backed evaluation and a production disagreement corpus exist.

## 9. Explicit blockers

1. No model-backed evaluation. Anthropic is no longer pursued; the Gemini path is in `docs/P0-MAX-INTERPRETATION-EVAL-PROVIDER.md`. It needs a provider-port PR, a dedicated dev Gemini key (network secret preferred) and approval of the cap.
2. R-1 is fixed in #1138, pending review. Structured plan clarifications capture uncertain free text ("Not sure.") as the field value. That is input quality, not authority (plan approval is still required), and it is not yet addressed.
3. Field-note ingestion change: split out into #1137, pending review.
4. Pre-existing failing tests on `main`: `maxOperationalReadiness` (stale assertion) and `aoAdminImpersonation`. They are unrelated, but they keep the full suite from being green.
5. GitHub CI has not yet run on the integrated set.
6. JEV production shadow persistence is still failing (pre-existing; blocks the Phase F corpus).
7. Gate-bound approvals (#1139) are default-off. Enabling them is a chat contract change: existing tests that approve without presenting the decision need a presenting turn first (counted in #1139). Duplicate-message protection needs a client-side id.

## 10. Recommended next actions

1. Review and merge PRs #1131 → #1135 in order, after splitting out the field-note change. Keep all flags off.
2. Review #1137 and #1138 (R-1), then #1139 (gate binding, flag off).
3. Approve the Gemini evaluation path (provider-port PR, dedicated dev key, $2 cap), run it against the unchanged gates, then decide on the staging shadow. The shadow observer itself is still Anthropic-bound and needs the same port before it can be enabled.
4. Close #1094 with a reference to #1096. Fix or update the two stale tests on `main`.
5. Align the ledger `members`/`memberIds` naming before any production consumer.
